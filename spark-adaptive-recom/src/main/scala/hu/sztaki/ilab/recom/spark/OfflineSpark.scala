package hu.sztaki.ilab.recom.spark

import hu.sztaki.ilab.recom.core._
import org.apache.spark._
import org.apache.spark.rdd._

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object OfflineSpark {

  type UserVector = (Int, Array[Double])
  type ItemVector = (Int, Array[Double])

  case class ShiftedIntHasher(partitions: Int,
                              nonNegativeHash: Int => Int,
                              shift: Int) extends Partitioner {

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      (nonNegativeHash(key.asInstanceOf[Int]) + shift) % partitions
    }

  }

  class UpdateSeparatedHashMap[A, B](val initial: mutable.HashMap[A, B])
    extends Serializable with mutable.Map[A, B] {
    private val updateMap = new mutable.HashMap[A, B]()

    override def getOrElse[B1 >: B](key: A, default: => B1): B1 = {
      updateMap.getOrElse(key, initial.getOrElse(key, default))
    }

    override def update(key: A, value: B): Unit = {
      updateMap.update(key, value)
    }

    def updates: Iterator[(A, B)] = updateMap.iterator

    def all: Iterator[(A, B)] =
      HashMap(initial.toIndexedSeq ++ updateMap.toIndexedSeq: _*).toIterator

    override def +=(kv: (A, B)): UpdateSeparatedHashMap.this.type = {
      update(kv._1, kv._2)
      this
    }

    override def -=(key: A): UpdateSeparatedHashMap.this.type = {
      throw new UnsupportedOperationException("Remove not supported")
    }

    override def get(key: A): Option[B] = {
      updateMap.get(key) match {
        case None => initial.get(key)
        case opt => opt
      }
    }

    override def iterator: Iterator[(A, B)] = all
  }

  def offlineDSGD(ratings: RDD[Rating],
                  users: RDD[UserVector],
                  items: RDD[ItemVector],
                  factorInit: FactorInitializerDescriptor,
                  factorUpdate: FactorUpdater,
                  numPartitions: Int,
                  hash: Int => Int,
                  iterations: Int): (RDD[UserVector], RDD[ItemVector]) = {
    val (userBlocks, itemBlocks) =
      offlineDSGDWithCustomMap(iter => mutable.HashMap(iter.toIndexedSeq: _*))(
        ratings, users, items, factorInit, factorUpdate, numPartitions, hash, iterations)

    // flattening the partition HashMaps
    val result = (userBlocks.mapPartitions(_.next()._2.toIterator).cache(), itemBlocks
      .mapPartitions(_.next()._2.toIterator).cache())

    userBlocks.unpersist()
    itemBlocks.unpersist()

    result
  }

  def offlineDSGDUpdatesOnly(ratings: RDD[Rating],
                             users: RDD[UserVector],
                             items: RDD[ItemVector],
                             factorInit: FactorInitializerDescriptor,
                             factorUpdate: FactorUpdater,
                             numPartitions: Int,
                             hash: Int => Int,
                             iterations: Int): (RDD[UserVector], RDD[ItemVector]) = {
    val (userBlocks, itemBlocks) =
      offlineDSGDWithCustomMap(iter =>
        new UpdateSeparatedHashMap(mutable.HashMap(iter.toIndexedSeq: _*)))(
        //        mutable.HashMap(iter.toIndexedSeq: _*))(
        ratings, users, items, factorInit, factorUpdate, numPartitions, hash, iterations)

    // flattening the partition HashMaps
    val result = (userBlocks.mapPartitions(_.next()._2.updates).cache(), itemBlocks
      .mapPartitions(_.next()._2.updates).cache())

    userBlocks.unpersist()
    itemBlocks.unpersist()

    result
  }

  def offlineDSGDWithCustomMap[CustomMap[A, B] <: mutable.Map[A, B]](customMapInit: Iterator[(Int, Array[Double])] => CustomMap[Int, Array[Double]])
                                                                    (ratings: RDD[Rating],
                                                                     users: RDD[UserVector],
                                                                     items: RDD[ItemVector],
                                                                     factorInit: FactorInitializerDescriptor,
                                                                     factorUpdate: FactorUpdater,
                                                                     numPartitions: Int,
                                                                     hash: Int => Int,
                                                                     iterations: Int)
                                                                    (implicit classTag: ClassTag[CustomMap[Int, Array[Double]]]):
  (RDD[(Int, CustomMap[Int, Array[Double]])], RDD[(Int, CustomMap[Int, Array[Double]])]) = {

    def shiftedPartitioner(shift: Int) = ShiftedIntHasher(numPartitions, hash, shift)

    val hashPartitioner = shiftedPartitioner(0)

    // ------------------------------
    // WARNING! cache()/unpersist() has semantic difference here because we use mutable HashMaps.
    // ------------------------------

    val ratingsByUser =
      ratings.keyBy(_.user).partitionBy(hashPartitioner)
        .mapPartitions(ratingIterByUser => {
          type RatingBlock = ArrayBuffer[Rating]
          val blocksByItems: Array[RatingBlock] = Array.fill(numPartitions)(new ArrayBuffer[Rating]())

          ratingIterByUser.map(_._2).foreach {
            case rating@Rating(u, i, r) =>
              blocksByItems(Math.abs(i.hashCode()) % numPartitions).append(rating)
          }

          Iterator(blocksByItems.map(_.toArray))
        }, preservesPartitioning = true)
        .cache()

    def partitionToHashMaps(rdd: RDD[(Int, Array[Double])]): RDD[(Int, CustomMap[Int, Array[Double]])] =
      rdd.partitionBy(hashPartitioner)
        .mapPartitionsWithIndex {
          case (partitionId, iter) =>
            Iterator((partitionId, customMapInit(iter)))
        }

    var userBlocksPartitioned = partitionToHashMaps(users)
      .cache()

    var itemBlocks = partitionToHashMaps(items)
      .cache()

    for (iter <- 0 until iterations) {
      for (i <- 1 to numPartitions) {

        val itemsBlocksPartitioned = itemBlocks

        val updated = ratingsByUser
          .zipPartitions(
            userBlocksPartitioned, itemsBlocksPartitioned, preservesPartitioning = true) {
            case (ratingBlockIter, userIter, itemIter) =>
              val (userPartitionId, users) = userIter.next()
              val (itemPartitionId, items) = itemIter.next()

              val currentRatingBlock: Array[Rating] = ratingBlockIter.next()(itemPartitionId)

              val factorInitializer = factorInit.open()

              currentRatingBlock.foreach { case Rating(userId, itemId, r) =>
                val user = users.getOrElse(userId, factorInitializer.nextFactor(userId))
                val item = items.getOrElse(itemId, factorInitializer.nextFactor(itemId))

                val (nextUser, nextItem) = factorUpdate.nextFactors(r, user, item)

                users.update(userId, nextUser)
                items.update(itemId, nextItem)
              }

              Iterator(((userPartitionId, users), (itemPartitionId, items)))
          }

        var prevUserBlocks = userBlocksPartitioned
        userBlocksPartitioned = updated.map { case (userBlock, _) => userBlock }.cache()
        prevUserBlocks.unpersist()

        var prevItemBlocks = itemBlocks
        itemBlocks = updated
          .map { case (_, itemBlock) => itemBlock }
          // Map to Array block
          .map { case (pId, map) => (pId, map.toArray) }
          .partitionBy(shiftedPartitioner(i))
          // Array block to Map
          .map { case (pId, array) => (pId, customMapInit(array.iterator)) }
          .cache()
        prevItemBlocks.unpersist()
      }
    }

    ratingsByUser.unpersist()
    (userBlocksPartitioned, itemBlocks)
  }

}
