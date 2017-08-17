package hu.sztaki.ilab.recom.spark

import hu.sztaki.ilab.recom.core._
import org.apache.spark.{rdd, _}
import org.apache.spark.rdd._

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object OfflineSpark {
  type Vector[I] = (I, Array[Double])

  case class ShiftedIntHasher(partitions: Int,
                              nonNegativeHash: Int => Int,
                              shift: Int) extends Partitioner {
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      (nonNegativeHash(key.hashCode()) + shift) % partitions
    }

  }

  /**
    * Hash map storing the initial values and updated values separately.
    * Used so that we can output only the updates in an online scenario.
    */
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

  def offlineDSGD[QI: ClassTag, PI: ClassTag](ratings: RDD[Rating[QI, PI]],
                  users: RDD[FactorVector[QI]],
                  items: RDD[FactorVector[PI]],
                  factorInitializerForQI: FactorInitializerDescriptor[QI],
                  factorInitializerForPI: FactorInitializerDescriptor[PI],
                  factorUpdate: FactorUpdater,
                  numPartitions: Int,
                  hash: Int => Int,
                  iterations: Int): (RDD[FactorVector[QI]], RDD[FactorVector[PI]]) = {
    val mapForQI = (iter: Iterator[(QI, Array[Double])]) =>
      //        mutable.HashMap(iter.toIndexedSeq: _*))(
      new UpdateSeparatedHashMap(mutable.HashMap(iter.toIndexedSeq: _*))
    val mapForPI = (iter: Iterator[(PI, Array[Double])]) =>
      new UpdateSeparatedHashMap(mutable.HashMap(iter.toIndexedSeq: _*))

    val (userBlocks, itemBlocks) =
      offlineDSGDWithCustomMap(mapForQI, mapForPI)(
        ratings, users, items, factorInitializerForQI, factorInitializerForPI,
        factorUpdate, numPartitions, hash, iterations)

    // flattening the partition HashMaps
    val result =
      toFactorRDD { userBlocks.mapPartitions(_.next()._2.toIterator).cache() } ->
      toFactorRDD { itemBlocks.mapPartitions(_.next()._2.toIterator).cache() }

    userBlocks.unpersist()
    itemBlocks.unpersist()

    result
  }

  def offlineDSGDUpdatesOnly[QI: ClassTag, PI: ClassTag](
                             ratings: RDD[Rating[QI, PI]],
                             users: RDD[Vector[QI]],
                             items: RDD[Vector[PI]],
                             factorInitializerForQI: FactorInitializerDescriptor[QI],
                             factorInitializerForPI: FactorInitializerDescriptor[PI],
                             factorUpdate: FactorUpdater,
                             numPartitions: Int,
                             hash: Int => Int,
                             iterations: Int): (RDD[Vector[QI]], RDD[Vector[PI]]) = {
    val mapForQI = (iter: Iterator[(QI, Array[Double])]) =>
      //        mutable.HashMap(iter.toIndexedSeq: _*))(
      new UpdateSeparatedHashMap(mutable.HashMap(iter.toIndexedSeq: _*))
    val mapForPI = (iter: Iterator[(PI, Array[Double])]) =>
      new UpdateSeparatedHashMap(mutable.HashMap(iter.toIndexedSeq: _*))

    val (userBlocks, itemBlocks) =
      offlineDSGDWithCustomMap(mapForQI, mapForPI)(
        ratings, users, items, factorInitializerForQI, factorInitializerForPI,
        factorUpdate, numPartitions, hash, iterations)

    // flattening the partition HashMaps
    val result = (userBlocks.mapPartitions(_.next()._2.updates).cache(), itemBlocks
      .mapPartitions(_.next()._2.updates).cache())

    userBlocks.unpersist()
    itemBlocks.unpersist()

    result
  }

  def offlineDSGDWithCustomMap[QI: ClassTag, PI: ClassTag, CustomMap[A, B] <: mutable.Map[A, B]](
    mapForQI: Iterator[(QI, Array[Double])] => CustomMap[QI, Array[Double]],
    mapForPI: Iterator[(PI, Array[Double])] => CustomMap[PI, Array[Double]])(
    ratings: RDD[Rating[QI, PI]],
    users: RDD[Vector[QI]],
    items: RDD[Vector[PI]],
    factorInitializerForQI: FactorInitializerDescriptor[QI],
    factorInitializerForPI: FactorInitializerDescriptor[PI],
    factorUpdate: FactorUpdater,
    numPartitions: Int,
    hash: Int => Int,
    iterations: Int)(
    implicit classTag: ClassTag[CustomMap[PI, Array[Double]]]):
  (RDD[(Int, CustomMap[QI, Array[Double]])], RDD[(Int, CustomMap[PI, Array[Double]])]) = {

    def shiftedPartitioner(shift: Int) = ShiftedIntHasher(numPartitions, hash, shift)

    val hashPartitioner = shiftedPartitioner(0)

    // ------------------------------
    // WARNING! cache()/unpersist() has semantic difference here because we use mutable HashMaps.
    // ------------------------------

    val ratingsByUser =
      ratings.keyBy[QI](_.user).partitionBy(hashPartitioner)
        .mapPartitions(ratingIterByUser => {
          type RatingBlock = ArrayBuffer[Rating[QI, PI]]
          val blocksByItems: Array[RatingBlock] = Array.fill(numPartitions)(new ArrayBuffer[Rating[QI, PI]]())

          ratingIterByUser.map(_._2).foreach {
            case rating@Rating(u, i, r) =>
              blocksByItems(Math.abs(i.hashCode()) % numPartitions).append(rating)
          }

          Iterator(blocksByItems.map(_.toArray))
        }, preservesPartitioning = true)
        .cache()

    def partitionToHashMaps[I: ClassTag](
      rdd: RDD[(I, Array[Double])],
      mapForI: Iterator[(I, Array[Double])] => CustomMap[I, Array[Double]])
    : RDD[(Int, CustomMap[I, Array[Double]])] = {
      rdd
        .partitionBy(hashPartitioner)
        .mapPartitionsWithIndex {
          case (partitionId: Int, iter: Iterator[(I, Array[Double])]) =>
            Iterator((partitionId, mapForI(iter)))
        }
    }

    var userBlocksPartitioned = partitionToHashMaps[QI](users, mapForQI)
      .cache()

    var itemBlocks = partitionToHashMaps[PI](items, mapForPI)
      .cache()

    for (_ <- 0 until iterations) {
      for (i <- 1 to numPartitions) {
        val itemsBlocksPartitioned = itemBlocks

        val updated = ratingsByUser
          .zipPartitions(
            userBlocksPartitioned, itemsBlocksPartitioned, preservesPartitioning = true) {
            case (ratingBlockIter, userIter, itemIter) =>
              val (userPartitionId, users) = userIter.next()
              val (itemPartitionId, items) = itemIter.next()

              val currentRatingBlock: Array[Rating[QI, PI]] = ratingBlockIter.next()(itemPartitionId)

              val factorInitializerQI = factorInitializerForQI.open()
              val factorInitializerPI = factorInitializerForPI.open()

              currentRatingBlock.foreach { case Rating(userId: QI, itemId: PI, r) =>
                val user = users.getOrElse(userId, factorInitializerQI.nextFactor(userId))
                val item = items.getOrElse(itemId, factorInitializerPI.nextFactor(itemId))

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
          .values
          .partitionBy(shiftedPartitioner(i))
          .cache()
        prevItemBlocks.unpersist()
      }
    }

    ratingsByUser.unpersist()
    (userBlocksPartitioned, itemBlocks)
  }

}
