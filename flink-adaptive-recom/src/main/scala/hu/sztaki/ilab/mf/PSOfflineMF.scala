package hu.sztaki.ilab.mf

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.{Condition, ReentrantLock}

import com.sun.jmx.remote.internal.ArrayQueue
import hu.sztaki.ilab.ps.client.receiver.SimpleClientReceiver
import hu.sztaki.ilab.ps.client.sender.SimpleClientSender
import hu.sztaki.ilab.ps.entities.{WorkerIn, WorkerOut}
import hu.sztaki.ilab.ps.server.SimplePSLogic
import hu.sztaki.ilab.ps.server.receiver.SimplePSReceiver
import hu.sztaki.ilab.ps.server.sender.SimplePSSender
import hu.sztaki.ilab.ps.{FlinkPS, ParameterServerClient, WorkerLogic}
import hu.sztaki.ilab.recom.core._
import org.apache.flink.api.common.functions.{FlatMapFunction, Partitioner, RichFlatMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class PSOfflineMatrixFactorization {
}

/**
  * A use-case for PS: matrix factorization with SGD.
  * Reads data into workers, then iterates on it, storing the user vectors at the worker and item vectors at the PS.
  * At every iteration, the worker pulls the relevant item vectors, applies the SGD updates and pushes the updates.
  *
  */
object PSOfflineMatrixFactorization {

  private val log = LoggerFactory.getLogger(classOf[PSOfflineMatrixFactorization])
  type Rating = (UserId, ItemId, Double)
  type Vector = Array[Double]

  def offline(ratings: DataStream[Rating],
              readParallelism: Int,
              workerParallelism: Int,
              psParallelism: Int,
              numFactors: Int,
              learningRate: Int,
              iterations: Int,
              pullLimit: Int,
              iterationWaitTime: Int): Unit = {


    val src = ratings
      .flatMap(new RichFlatMapFunction[Rating, Rating] {

        var collector: Option[Collector[Rating]] = None

        override def flatMap(r: Rating, out: Collector[Rating]): Unit = {
          out.collect(r)
        }

        override def close(): Unit = {
          collector match {
            case Some(c: Collector[Rating]) =>
              for (i <- 0 until readParallelism)
                c.collect((i, -getRuntimeContext.getIndexOfThisSubtask, -1.0))
            case _ => log.error("Nothing to collect from the source, quite tragic.")
          }
        }
      }).setParallelism(readParallelism)
      .partitionCustom(new Partitioner[UserId] {
        override def partition(key: UserId, numPartitions: Int): Int = key % numPartitions
      }, x => x._1)
    // initialization method and update method
    val factorInitDesc = RandomFactorInitializerDescriptor(numFactors)

    val factorUpdate = new SGDUpdater(learningRate)

    val workerLogic = new WorkerLogic[Rating, Vector, (UserId, Vector)] {

      val userVectors = new mutable.HashMap[UserId, Vector]()
      // we store ratings by items, so that we can apply updates for all relevant users at a pull answer
      val itemRatings = new mutable.HashMap[ItemId, ArrayBuffer[(UserId, Double)]]()

      @transient
      lazy val factorInit: FactorInitializer = factorInitDesc.open()

      var workerThread: Thread = null

      // We need to check if all threads finished already
      val EOFsReceived = new AtomicInteger(0)

      var pullCounter = 0
      val psLock = new ReentrantLock()
      val canPull: Condition = psLock.newCondition()

      override def onRecv(data: Rating,
                          ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {

        data match {
          case (workerId, minusSourceId, -1.0) =>
            log.info(s"Received EOF @$workerId from ${-minusSourceId}")
            EOFsReceived.getAndIncrement()
            // Start working when all the threads finished reading.
            if (EOFsReceived.get() >= readParallelism) {
              // This marks the end of input. We can do the work.
              log.info(s"Number of received ratings: ${itemRatings.values.map(_.length).sum}")

              // We start a new Thread to avoid blocking the answers to pulls.
              // Otherwise the work would only start when all the pulls are sent (for all iterations).
              workerThread = new Thread(new Runnable {
                override def run(): Unit = {
                  log.debug("worker thread started")
                  for (iter <- 1 to iterations) {
                    for (item <- itemRatings.keys) {
                      // we assume that the PS client is not thread safe, so we need to sync when we use it.
                      psLock.lock()
                      try {
                        while (pullCounter >= pullLimit) {
                          canPull.await()
                        }
                        pullCounter += 1
                        log.debug(s"pull inc: $pullCounter")

                        ps.pull(item)
                      } finally {
                        psLock.unlock()
                      }
                    }
                  }
                  log.debug("pulls finished")
                }
              })
              workerThread.start()
            }
          case rating
            @(u, i, r) =>
            // Since the EOF signals likely won't arrive at the same time, an extra check for the finished sources is needed.
            if (workerThread != null) {
              throw new IllegalStateException("Should not have started worker thread while waiting for further " +
                "elements.")
            }

            val buffer = itemRatings.getOrElseUpdate(i, new ArrayBuffer[(UserId, Double)]())
            buffer.append((u, r))
        }
      }

      override def onPullRecv(item: ItemId,
                              itemVec: Vector,
                              ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {
        // we assume that the PS client is not thread safe, so we need to sync when we use it.
        psLock.lock()
        try {
          for ((user, rating) <- Random.shuffle(itemRatings(item))) {
            val userVec = userVectors.getOrElseUpdate(user, factorInit.nextFactor(user))
            val (deltaUserVec, deltaItemVec) = factorUpdate.delta(rating, userVec, itemVec)
            userVectors(user) = userVec.zip(deltaUserVec).map(x => x._1 + x._2)
            ps.push(item, deltaItemVec)
          }
          pullCounter -= 1
          canPull.signal()
          log.debug(s"pull dec: $pullCounter")
        } finally {
          psLock.unlock()
        }
      }

      override def close(): Unit = {
        userVectors.foreach {
          case (id: Int, vector: Array[Double]) =>
            log.info(s"###PS###u;${id};[${vector.mkString(",")}]")
        }
      }
    }

    val workerLogic2 = new WorkerLogic[Rating, Vector, (UserId, Vector)] {

      val rs = new ArrayBuffer[Rating]()
      val userVectors = new mutable.HashMap[UserId, Vector]()
      val itemRatings = new mutable.HashMap[ItemId, mutable.Queue[(UserId, Double)]]()

      @transient
      lazy val factorInit: FactorInitializer = factorInitDesc.open()

      var workerThread: Thread = null

      // We need to check if all threads finished already
      val EOFsReceived = new AtomicInteger(0)

      var pullCounter = 0
      val psLock = new ReentrantLock()
      val canPull: Condition = psLock.newCondition()

      override def onRecv(data: Rating,
                          ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {

        data match {
          case (workerId, minusSourceId, -1.0) =>
            log.info(s"Received EOF @$workerId from ${-minusSourceId}")
            EOFsReceived.getAndIncrement()
            // Start working when all the threads finished reading.
            if (EOFsReceived.get() >= readParallelism) {
              // This marks the end of input. We can do the work.
              log.info(s"Number of received ratings: ${rs.length}")

              // We start a new Thread to avoid blocking the answers to pulls.
              // Otherwise the work would only start when all the pulls are sent (for all iterations).
              workerThread = new Thread(new Runnable {
                override def run(): Unit = {
                  log.debug("worker thread started")
                  for (iter <- 1 to iterations) {
                    Random.shuffle(rs)
                    for ((u, i, r) <- rs) {
                      // we assume that the PS client is not thread safe, so we need to sync when we use it.
                      psLock.lock()
                      try {
                        while (pullCounter >= pullLimit) {
                          canPull.await()
                        }
                        pullCounter += 1
                        log.debug(s"pull inc: $pullCounter")

                        itemRatings.getOrElseUpdate(i, mutable.Queue[(UserId, Double)]())
                          .enqueue((u, r))

                        ps.pull(i)
                      } finally {
                        psLock.unlock()
                      }

                    }
                  }
                  log.debug("pulls finished")
                }
              })
              workerThread.start()
            }
          case rating
            @(u, i, r) =>
            // Since the EOF signals likely won't arrive at the same time, an extra check for the finished sources is needed.
            if (workerThread != null) {
              throw new IllegalStateException("Should not have started worker thread while waiting for further " +
                "elements.")
            }

            rs.append((u, i, r))
        }
      }

      override def onPullRecv(item: ItemId,
                              itemVec: Vector,
                              ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {
        // we assume that the PS client is not thread safe, so we need to sync when we use it.
        psLock.lock()
        try {
          val (user, rating) = itemRatings(item).dequeue()
          val userVec = userVectors.getOrElseUpdate(user, factorInit.nextFactor(user))
          val (deltaUserVec, deltaItemVec) = factorUpdate.delta(rating, userVec, itemVec)
          userVectors(user) = userVec.zip(deltaUserVec).map(x => x._1 + x._2)
          ps.push(item, deltaItemVec)

          pullCounter -= 1
          canPull.signal()
          log.debug(s"pull dec: $pullCounter")
        } finally {
          psLock.unlock()
        }
      }

      override def close(): Unit = {
        userVectors.foreach {
          case (id: Int, vector: Array[Double]) =>
            log.info(s"###PS###u;${id};[${vector.mkString(",")}]")
        }
      }
    }
    val serverLogic =
      new SimplePSLogic[Array[Double]](
        x => factorInitDesc.open().nextFactor(x), { case (vec, deltaVec) => vec.zip(deltaVec).map(x => x._1 + x._2) })

    val paramPartitioner: WorkerOut[Array[Double]] => Int = {
      case WorkerOut(partitionId, msg) => msg match {
        case Left(paramId) => Math.abs(paramId) % psParallelism
        case Right((paramId, delta)) => Math.abs(paramId) % psParallelism
      }
    }

    val wInPartition: WorkerIn[Array[Double]] => Int = {
      case WorkerIn(id, workerPartitionIndex, msg) => workerPartitionIndex
    }

    val modelUpdates = FlinkPS.psTransform(src, workerLogic2, serverLogic,
      new SimpleClientReceiver[Array[Double]](),
      new SimpleClientSender[Array[Double]](),
      new SimplePSReceiver[Array[Double]](),
      new SimplePSSender[Array[Double]](),
      paramPartitioner = paramPartitioner,
      wInPartition = wInPartition,
      workerParallelism,
      psParallelism,
      iterationWaitTime)
      .flatMap(new RichFlatMapFunction[Either[(UserId, Vector), (ItemId, Vector)],
        Either[(UserId, Vector), (ItemId, Vector)]] {
        val itemMap: mutable.HashMap[ItemId, Vector] = mutable.HashMap()
        val userMap: mutable.HashMap[UserId, Vector] = mutable.HashMap()

        var collector: Collector[Either[(UserId, Vector), (ItemId, Vector)]] = null

        override def close(): Unit = {
          itemMap.foreach {
            item => collector.collect(Right(item))
          }
          userMap.foreach {
            user => collector.collect(Left(user))
          }
        }

        override def flatMap(value: Either[(UserId, Vector), (ItemId, Vector)],
                             out: Collector[Either[(UserId, Vector), (ItemId, Vector)]]): Unit = {
          if (collector == null) {
            collector = out
          }

          value match {
            case Left(user) => userMap += user
            case Right(item) => itemMap += item
          }
        }
      })

  }

  def main(args: Array[String]): Unit = {

  }

}
