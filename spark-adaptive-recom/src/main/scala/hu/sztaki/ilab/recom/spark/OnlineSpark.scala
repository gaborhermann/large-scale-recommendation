package hu.sztaki.ilab.recom.spark

import hu.sztaki.ilab.recom.core.{ItemVector => _, UserVector => _, _}
import hu.sztaki.ilab.recom.spark.OnlineSpark.{LocallyCheckpointedRDD, NotCheckpointedRDD, PossiblyCheckpointedRDD}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

import scala.collection.mutable
import scala.reflect.ClassTag

class OnlineSpark {

  type UserVector = (Int, Array[Double])
  type ItemVector = (Int, Array[Double])

  case class UserVectorUpdate(id: Int, vec: Array[Double])

  case class ItemVectorUpdate(id: Int, vec: Array[Double])

  def buildModelCombineOffline(mapInit: mutable.Map[Int, Array[Double]])
                              (ratings: DStream[Rating],
                               factorInit: FactorInitializerDescriptor,
                               factorUpdate: FactorUpdater,
                               parameters: Map[String, String],
                               checkpointEvery: Int,
                               offlineEvery: Int,
                               numberOfIterations: Int,
                               offlineAlgorithm: String,
                               numFactors: Int):
  DStream[Either[UserVectorUpdate, ItemVectorUpdate]] = {

    val ssc = ratings.context

    val users0: PossiblyCheckpointedRDD[UserVector] =
      NotCheckpointedRDD(ssc.sparkContext.makeRDD(Seq()))
    val items0: PossiblyCheckpointedRDD[ItemVector] =
      NotCheckpointedRDD(ssc.sparkContext.makeRDD(Seq()))

    var (userRDD, itemRDD) = (users0, items0)

    var ratingsHistory: PossiblyCheckpointedRDD[Rating] =
      NotCheckpointedRDD(ssc.sparkContext.makeRDD(Seq()).cache())

    var checkpointCnt = checkpointEvery
    var offlineCnt = offlineEvery

    import OfflineSpark._

    val updates: DStream[Either[UserVectorUpdate, ItemVectorUpdate]] = ratings.transform { rs =>
      checkpointCnt -= 1
      val checkpointCurrent = checkpointCnt <= 0
      if (checkpointCurrent) {
        checkpointCnt = checkpointEvery
      }

      offlineCnt -= 1
      val offlineCurrent = offlineCnt <= 0
      if (offlineCurrent) {
        offlineCnt = offlineEvery
      }

      val oldRatingsHistory = ratingsHistory.get
      rs.cache()
      ratingsHistory = NotCheckpointedRDD(ratingsHistory.get.union(rs.cache()).cache())

      val (uUpdates, iUpdates) = if (!offlineCurrent) {
        // online updates

        val (userUpdates, itemUpdates) =
          offlineDSGDUpdatesOnly(rs, userRDD.get, itemRDD.get,
            factorInit, factorUpdate,
            ssc.sparkContext.defaultParallelism, _.hashCode(), 1)

        def applyUpdatesAndCheckpointOrCache(oldRDD: PossiblyCheckpointedRDD[(Int, Array[Double])],
                                             updates: RDD[(Int, Array[Double])]):
        PossiblyCheckpointedRDD[(Int, Array[Double])] = {
          // merging old values with updates
          // full outer join is needed because
          //   1. there might be vectors in oldRDD that has not been updated
          //   2. there might be updates for vectors not yet present in oldRDD
          val rdd = oldRDD.get.fullOuterJoin(updates)
            .map {
              case (id, (oldOpt, updatedOpt)) => (id, updatedOpt.getOrElse(oldOpt.get))
            }

          // checkpoint or cache
          val nextRDD = if (checkpointCurrent) {
            val persisted = rdd.persist(StorageLevel.DISK_ONLY)
              .localCheckpoint()
            LocallyCheckpointedRDD(persisted)
          } else {
            NotCheckpointedRDD(rdd.cache())
          }

          // clear old values if not checkpointed
          oldRDD match {
            case NotCheckpointedRDD(x) => x.unpersist()
            case _ => ()
          }
          updates.unpersist()

          nextRDD
        }

        userRDD = applyUpdatesAndCheckpointOrCache(userRDD, userUpdates)
        itemRDD = applyUpdatesAndCheckpointOrCache(itemRDD, itemUpdates)

        (userUpdates, itemUpdates)
      } else {
        // offline

        val (userUpdates, itemUpdates) = offlineAlgorithm match {
          case "DSGD" =>
            offlineDSGD(ratingsHistory.get,
              ssc.sparkContext.makeRDD(Seq()),
              ssc.sparkContext.makeRDD(Seq()),
              factorInit, factorUpdate,
              ssc.sparkContext.defaultParallelism, _.hashCode(), numberOfIterations)
          case "ALS" =>
            val model = ALS.train(ratingsHistory.get.map {
              case hu.sztaki.ilab.recom.core.Rating(u,i,r) =>
                org.apache.spark.mllib.recommendation.Rating(u,i,r)
            }, numFactors, numberOfIterations, 0.1)

            (model.userFeatures.cache(), model.productFeatures.cache())
        }

        val oldUserRDD = userRDD
        val oldItemRDD = itemRDD

        userRDD = NotCheckpointedRDD(userUpdates)
        itemRDD = NotCheckpointedRDD(itemUpdates)

        // clear old values if not checkpointed
        oldUserRDD match {
          case NotCheckpointedRDD(x) => x.unpersist()
          case _ => ()
        }
        oldItemRDD match {
          case NotCheckpointedRDD(x) => x.unpersist()
          case _ => ()
        }

        (userUpdates, itemUpdates)
      }

      val userUpdatesEither: RDD[Either[UserVectorUpdate, ItemVectorUpdate]] = uUpdates
        .map { case (id, vec) => Left(UserVectorUpdate(id, vec)) }
      val itemUpdatesEither: RDD[Either[UserVectorUpdate, ItemVectorUpdate]] = iUpdates
        .map { case (id, vec) => Right(ItemVectorUpdate(id, vec)) }

      userUpdatesEither.union(itemUpdatesEither)
    }

    updates
  }

  def buildModelWithMap(mapInit: mutable.Map[Int, Array[Double]])
                       (ratings: DStream[Rating],
                        factorInit: FactorInitializerDescriptor,
                        factorUpdate: FactorUpdater,
                        parameters: Map[String, String],
                        checkpointEvery: Int): DStream[(Boolean, (Int, Array[Double]))] = {

    val ssc = ratings.context

    val users0: PossiblyCheckpointedRDD[UserVector] =
      NotCheckpointedRDD(ssc.sparkContext.makeRDD(Seq()))
    val items0: PossiblyCheckpointedRDD[ItemVector] =
      NotCheckpointedRDD(ssc.sparkContext.makeRDD(Seq()))

    var (userRDD, itemRDD) = (users0, items0)

    var cnt = checkpointEvery

    import OfflineSpark._

    val updates = ratings.transform { rs =>
      cnt -= 1
      val checkpointCurrent = cnt <= 0
      if (checkpointCurrent) {
        cnt = checkpointEvery
      }

      val (userUpdates, itemUpdates) =
        offlineDSGDUpdatesOnly(rs, userRDD.get, itemRDD.get,
          factorInit, factorUpdate,
          ssc.sparkContext.defaultParallelism, _.hashCode(), 1)

      def applyUpdatesAndCheckpointOrCache(oldRDD: PossiblyCheckpointedRDD[(Int, Array[Double])],
                                           updates: RDD[(Int, Array[Double])]):
      PossiblyCheckpointedRDD[(Int, Array[Double])] = {
        // merging old values with updates
        val rdd = oldRDD.get.fullOuterJoin(updates)
          .map {
            case (id, (oldOpt, updatedOpt)) => (id, updatedOpt.getOrElse(oldOpt.get))
          }

        // checkpoint or cache
        val nextRDD = if (checkpointCurrent) {
          val persisted = rdd.persist(StorageLevel.DISK_ONLY)
            .localCheckpoint()
          LocallyCheckpointedRDD(persisted)
        } else {
          NotCheckpointedRDD(rdd.cache())
        }

        // clear old values if not checkpointed
        oldRDD match {
          case NotCheckpointedRDD(x) => x.unpersist()
          case _ => ()
        }
        updates.unpersist()

        nextRDD
      }

      userRDD = applyUpdatesAndCheckpointOrCache(userRDD, userUpdates)
      itemRDD = applyUpdatesAndCheckpointOrCache(itemRDD, itemUpdates)

      // user updates are marked with true, while item updates with false
      userUpdates.map((true, _)).union(itemUpdates.map((false, _)))
    }

    updates
  }

}

object OnlineSpark {

  sealed trait PossiblyCheckpointedRDD[A] {
    def get: RDD[A]
  }

  case class LocallyCheckpointedRDD[A](rdd: RDD[A])(implicit classTag: ClassTag[A])
    extends PossiblyCheckpointedRDD[A] {
    def get: RDD[A] = rdd
  }

  case class NotCheckpointedRDD[A](rdd: RDD[A])(implicit classTag: ClassTag[A])
    extends PossiblyCheckpointedRDD[A] {
    def get: RDD[A] = rdd
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("online")
    val ssc = new StreamingContext(conf, Seconds(1))

    val q = mutable.Queue(10, 20, 30)
      .map(x => ssc.sparkContext.makeRDD(x to x + 3, numSlices = 4))
    val xs = ssc.queueStream(q, oneAtATime = true)

    val s0 = ssc.sparkContext.makeRDD(Seq(-1, -2))

    def p(sRDD: RDD[Int], xRDD: RDD[Int]): RDD[Int] = {
      sRDD.union(xRDD)
    }

    val stateDStream = dstreamFold[Int, Int](xs)(s0, p)
    stateDStream.print()

    ssc.start()

    Thread.sleep(5000)

  }

  def dstreamFold[S, A](xs: DStream[A])
                       (s0: RDD[S], p: (RDD[S], RDD[A]) => RDD[S])
                       (implicit sClassTag: ClassTag[S], aClassTag: ClassTag[A]): DStream[S] = {
    var rddS = s0

    val dstreamS = xs.transform { rddXs =>
      rddS = p(rddS, rddXs)
      rddS
    }

    dstreamS
  }

  def dstreamFold2[S1, S2, A](xs: DStream[A])
                             (s0: (RDD[S1], RDD[S2]),
                              p: (RDD[S1], RDD[S2], RDD[A]) => (RDD[S1], RDD[S2]))
                             (implicit
                              s1ClassTag: ClassTag[S1],
                              s2ClassTag: ClassTag[S2],
                              aClassTag: ClassTag[A]): (DStream[S1], DStream[S2]) = {
    var (s1, s2) = s0

    val stateStream: DStream[Either[S1, S2]] = xs
      .transform { rddXs =>
        val (nextS1, nextS2) = p(s1, s2, rddXs)
        s1 = nextS1
        s2 = nextS2
        val eitherS1: RDD[Either[S1, S2]] = s1.map(Left(_))
        val eitherS2: RDD[Either[S1, S2]] = s2.map(Right(_))
        eitherS1.union(eitherS2)
      }

    var e: Either[Int, String] = null

    val s1Stream = stateStream.flatMap {
      case Left(x) => Some(x)
      case _ => None
    }

    val s2Stream = stateStream.flatMap {
      case Right(x) => Some(x)
      case _ => None
    }

    (s1Stream, s2Stream)
  }
}
