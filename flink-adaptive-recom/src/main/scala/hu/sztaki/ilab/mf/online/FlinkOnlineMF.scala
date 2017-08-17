package hu.sztaki.ilab.mf.online

import hu.sztaki.ilab.mf.utils.{LockableState, LockableStateWithQueue}
import hu.sztaki.ilab.mf.utils.Utils.CustomHasher
import hu.sztaki.ilab.recom.core._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable

class FlinkOnlineMF[QI, PI](iterationWaitTime: Long = 0) {
  private val log = LoggerFactory.getLogger(classOf[FlinkOnlineMF[QI, PI]])

  def buildModel(ratings: DataStream[Rating[QI, PI]],
                 factorInitializerForQI: FactorInitializerDescriptor[QI],
                 factorInitializerForPI: FactorInitializerDescriptor[PI],
                 factorUpdate: FactorUpdater,
                 parameters: Map[String, String] = Map.empty):
  DataStream[(FactorVector[QI], FactorVector[PI])] = {

    def stepFunc(users: ConnectedStreams[Rating[QI, PI], FactorVector[QI]]):
    (DataStream[FactorVector[QI]], DataStream[(FactorVector[QI], FactorVector[PI])]) = {

      val ratingsWithUserVector: DataStream[(Rating[QI, PI], Array[Double])] =
        users.flatMap(new UserOperator(factorInit))

      val updatedVectors = ratingsWithUserVector
        // partition by item
        .partitionCustom(new CustomHasher(), _._1.item)
        .flatMap(new ItemOperator(factorInit, factorUpdate))

      val feedbackUpdatedUserVectors =
        updatedVectors.map(_._1)
          .partitionCustom(new CustomHasher(), _.id)

      (feedbackUpdatedUserVectors, updatedVectors)
    }

    val updatedFactors = ratings
      // partition by users
      .partitionCustom(new CustomHasher(), _.user)
      .iterate(stepFunc _, iterationWaitTime)

    updatedFactors
  }

  // operator storing the user vectors
  class UserOperator(factorInitializerDescriptor: FactorInitializerDescriptor)
    extends RichCoFlatMapFunction[Rating, UserVector, (Rating, Array[Double])] {

    var factorInitializer: FactorInitializer = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      factorInitializer = factorInitializerDescriptor.open()
    }

    val userVectors = new mutable.HashMap[Int, LockableState[Rating, Array[Double]]]

    var currentBufferSize = 0

    private def incCurrentBufferSize(): Unit = {
      currentBufferSize += 1
      logBufferSize()
    }

    private def decCurrentBufferSize(): Unit = {
      currentBufferSize -= 1
      logBufferSize()
    }

    private def logBufferSize(): Unit = {
      if (log.isDebugEnabled && currentBufferSize > 0 && currentBufferSize % 10 == 0) {
        log.debug(s"current buffer @${getRuntimeContext.getIndexOfThisSubtask}" +
          s" has $currentBufferSize elements")
      }
    }

    // When a rating comes in it gets sent forward with the corresponding user vector.
    // The user vector is locked until the updated vector arrives (in flatMap2),
    // and the incoming ratings are cached.
    override def flatMap1(rating: Rating,
                          out: Collector[(Rating, Array[Double])]): Unit = {

      val Rating(user, _, _) = rating

      incCurrentBufferSize()
      userVectors
        .getOrElseUpdate(user, new LockableStateWithQueue(factorInitializer.nextFactor(user)))
        .getStateOrCacheValue(rating)
        .foreach(out.collect)
    }

    // When the updated user vector arrives (from iteration tail) the user vector is updated.
    // If there are ratings in the cache, the oldest gets emitted.
    override def flatMap2(updatedUserVector: UserVector,
                          out: Collector[(Rating, Array[Double])]): Unit = {

      val FactorVector(userId, userVector) = updatedUserVector

      decCurrentBufferSize()
      userVectors.getOrElse(userId, throw new RuntimeException("IMPOSSIBLE!"))
        .setStateAndSendNext(userVector)
        .foreach(out.collect)
    }
  }

  class ItemOperator(factorInitializerDescriptor: FactorInitializerDescriptor,
                     factorUpdater: FactorUpdater)
    extends RichFlatMapFunction[(Rating, Array[Double]), (UserVector, ItemVector)] {

    var factorInitializer: FactorInitializer = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      factorInitializer = factorInitializerDescriptor.open()
    }

    val itemVectors = new mutable.HashMap[Int, Array[Double]]

    override def flatMap(value: (Rating, Array[Double]),
                         out: Collector[(UserVector, ItemVector)]): Unit = {
      val (Rating(user, item, rating), userVector) = value

      val itemVector = itemVectors.getOrElseUpdate(item, factorInitializer.nextFactor(item))

      val (nextUserVector, nextItemVector) =
        factorUpdater.nextFactors(rating, userVector, itemVector)

      itemVectors.put(item, nextItemVector)
      out.collect((FactorVector(user, nextUserVector), FactorVector(item, nextItemVector)))
    }
  }

}
