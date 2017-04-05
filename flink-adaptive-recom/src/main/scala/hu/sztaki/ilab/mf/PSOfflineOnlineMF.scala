package hu.sztaki.ilab.mf

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.{Condition, ReentrantLock}

import hu.sztaki.ilab.ps.client.receiver.SimpleClientReceiver
import hu.sztaki.ilab.ps.client.sender.SimpleClientSender
import hu.sztaki.ilab.ps.entities.{WorkerIn, WorkerOut}
import hu.sztaki.ilab.ps.server.receiver.SimplePSReceiver
import hu.sztaki.ilab.ps.server.sender.SimplePSSender
import hu.sztaki.ilab.ps._
import hu.sztaki.ilab.recom.core._
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class PSOfflineOnlineMF {
}

object PSOfflineOnlineMF {

  private val log = LoggerFactory.getLogger(classOf[PSOfflineOnlineMF])

  sealed trait TrainingStatus

  case class Batch() extends TrainingStatus

  case class Online() extends TrainingStatus

  case class BatchInit() extends TrainingStatus

  def offlineOnlinePS(ratings: DataStream[(Int, Int, Double)],
                      batchTrainingTrigger: DataStream[Unit],
                      factorInitDesc: FactorInitializerDescriptor,
                      factorUpdate: FactorUpdater,
                      workerParallelism: Int,
                      psParallelism: Int,
                      iterations: Int,
                      pullLimit: Int,
                      pullLimitOnline: Int,
                      iterationWaitTime: Long = 10000):
  DataStream[Either[(Int, Array[Double]), (Int, Array[Double])]] = {

    // A user rates an item with a Double rating
    type Rating = (UserId, ItemId, Double)
    type Vector = Array[Double]

    val workerLogic = new WorkerLogic[Rating, Vector, (UserId, Vector)] {

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

      var currentTrainingStatus: TrainingStatus = Online()

      val onlinePullQueue: mutable.Queue[Rating] = mutable.Queue[Rating]()

      override def onRecv(data: Rating,
                          ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {

        data match {
          case (workerId, _, -1.0) =>
            // batch training start
            currentTrainingStatus match {
              case Batch() | BatchInit() =>
                throw new IllegalStateException("Previous batch training has not finished yet." +
                  "Maybe you should wait more between the periodic batch training.")
              case Online() =>
                currentTrainingStatus = BatchInit()
                log.info(s"Worker $workerId changed training status from Online to BatchInit")

                // notifying PS that batch has started
                for (psId <- 0 until psParallelism) {
                  // empty array and negative psId marks PS batch start
                  ps.push(-psId, Array())
                }

                // We start a new Thread to avoid blocking the answers to pulls.
                // Otherwise the work would only start when all the pulls are sent (for all iterations).
                workerThread = new Thread(new Runnable {
                  override def run(): Unit = {
                    log.debug(s"Batch pull thread started @ worker $workerId")

                    // wait for pull limit to go down to one
                    psLock.lock()
                    try {
                      while (pullCounter > 0) {
                        canPull.await()
                      }
                    } finally {
                      psLock.unlock()
                    }
                    log.info(s"Worker $workerId changed training status from BatchInit to Batch")

                    // start batch pulls
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
                    log.info(s"All pulls finished for batch training @ worker $workerId")
                  }
                })
                workerThread.start()
            }

          case rating@(u, i, r) =>
            // new data arrival
            onlinePullQueue.enqueue(rating)

            currentTrainingStatus match {
              case Batch() | BatchInit() =>
                ()
              case Online() =>
                rs.append((u, i, r))
                trySendingPulls(ps)
            }
        }
      }

      def trySendingPulls(ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {
        while (pullCounter < pullLimitOnline && onlinePullQueue.nonEmpty) {
          val (u, i, r) = onlinePullQueue.dequeue()
          pullCounter += 1
          log.debug(s"pull inc: $pullCounter")

          itemRatings.getOrElseUpdate(i, mutable.Queue[(UserId, Double)]())
            .enqueue((u, r))

          ps.pull(i)
        }
      }

      def vectorUpdateAndPush(item: ItemId,
                              itemVec: Vector,
                              ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {
        val (user, rating) = itemRatings(item).dequeue()
        val userVec = userVectors.getOrElseUpdate(user, factorInit.nextFactor(user))
        val (deltaUserVec, deltaItemVec) = factorUpdate.delta(rating, userVec, itemVec)
        userVectors(user) = userVec.zip(deltaUserVec).map(x => x._1 + x._2)
        ps.push(item, deltaItemVec)

        ps.output(user, userVec.zip(deltaItemVec).map(x => x._1 + x._2))

        pullCounter -= 1
        log.debug(s"pull dec: $pullCounter")
      }

      override def onPullRecv(item: ItemId,
                              itemVec: Vector,
                              ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {
        currentTrainingStatus match {
          case Online() =>
            // do the actual update and push
            vectorUpdateAndPush(item, itemVec, ps)
            // try sending pulls
            trySendingPulls(ps)
          case BatchInit() =>
            // throw away the pull answer, we need to start running batch ASAP
            psLock.lock()
            try {
              pullCounter -= 1
              currentTrainingStatus = Batch()
              log.info(s"Batch training started @ worker.")
              if (pullCounter == 0) {
                canPull.signal()
              }
            } finally {
              psLock.unlock()
            }
          case Batch() =>
            // Pulling runs in another thread and we assume that the PS client is not thread safe,
            // so we need to sync when we use it.
            if (workerThread.isAlive) {
              // pulling is still running
              psLock.lock()
              try {
                vectorUpdateAndPush(item, itemVec, ps)
                canPull.signal()
              } finally {
                psLock.unlock()
              }
            } else {
              // pulling has finished
              vectorUpdateAndPush(item, itemVec, ps)

              if (pullCounter == 0) {
                // finished batch training

                // notifying PS that online training has started
                for (psId <- 0 until psParallelism) {
                  // null array and negative psId marks PS batch finish
                  ps.push(-psId, Array(-1.0))
                }

                // adding received ratings to history
                rs ++= onlinePullQueue

                // we can continue with the online training
                currentTrainingStatus = Online()
                trySendingPulls(ps)
              }
            }
        }
      }

      override def close(): Unit = {
      }
    }

    val serverLogic = new ParameterServerLogic[Vector, (Int, Vector)] {
      val params = new mutable.HashMap[Int, Vector]()

      @transient lazy val factorInit: FactorInitializer = factorInitDesc.open()
      @transient lazy val init: (Int) => Vector =
        id => factorInit.nextFactor(id)
      @transient lazy val update: (Vector, Vector) => Vector = {
        case (xs, ys) => xs.zip(ys).map { case (x, y) => x + y }
      }

      var trainingStatus: TrainingStatus = Online()

      override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[Vector, (Int, Vector)]): Unit = {
        trainingStatus match {
          case Online() | Batch() =>
            ps.answerPull(id, params.getOrElseUpdate(id, init(id)), workerPartitionIndex)
          case BatchInit() =>
            // ignore pulls from workers still training online
            if (workerHasStartedBatch(workerPartitionIndex)) {
              ps.answerPull(id, params.getOrElseUpdate(id, init(id)), workerPartitionIndex)
            }
        }
      }

      val workerHasFinishedBatch = new mutable.BitSet(workerParallelism)
      var finishedBatchSignsReceived = 0

      def batchFinishedSign(workerId: Int): Unit = {
        workerHasFinishedBatch.add(workerId)
        finishedBatchSignsReceived += 1
        if (finishedBatchSignsReceived == workerParallelism) {
          // every worker started online training
          finishedBatchSignsReceived = 0
          workerHasFinishedBatch.clear()
          trainingStatus = Online()
          log.info(s"Batch finished @ PS")
        }
      }

      val workerHasStartedBatch = new mutable.BitSet(workerParallelism)
      var startedBatchSignsReceived = 0

      def batchStartedSign(workerId: Int): Unit = {
        workerHasStartedBatch.add(workerId)
        startedBatchSignsReceived += 1
        if (startedBatchSignsReceived == workerParallelism) {
          // every worker started batch training
          startedBatchSignsReceived = 0
          workerHasStartedBatch.clear()
          trainingStatus = Batch()
          log.info(s"Batch started @ PS")
        }
      }

      override def onPushRecv(id: Int, deltaUpdate: Vector, workerPartitionIndex: Int, ps: ParameterServer[Vector,
        (Int, Vector)]): Unit
      = {
        (id, deltaUpdate) match {
          case (minusWorkerId, Array()) if minusWorkerId <= 0 =>
            // batch started sign
            trainingStatus match {
              case Batch() =>
                throw new IllegalStateException("Should not have received batch started sign at batch.")
              case BatchInit() =>
                batchStartedSign(-minusWorkerId)
              case Online() =>
                trainingStatus = BatchInit()

                log.info(s"Batch init @ PS.")
                params.clear()
                batchStartedSign(-minusWorkerId)
            }
          case (minusWorkerId, Array(-1.0)) if minusWorkerId <= 0 =>
            // batch training finished
            trainingStatus match {
              case BatchInit() | Online() =>
                throw new IllegalStateException("Should not have received batch finished sign at online.")
              case Batch() =>
                batchFinishedSign(workerPartitionIndex)
            }
          case _ =>
            // normal push
            def normalUpdate(): Unit = {
              val updatedVector = params.get(id) match {
                case Some(param) =>
                  update(param, deltaUpdate)
                case None =>
                  throw new IllegalStateException("Trying to add delta but vector is not initialized." +
                    "A pull should have come before.")
              }

              ps.output((id, updatedVector))
            }

            trainingStatus match {
              case Batch() =>
                // normal update
                val c = params.get(id) match {
                  case Some(q) =>
                    update(q, deltaUpdate)
                  case None =>
                    throw new IllegalStateException("Trying to add delta but vector is not initialized." +
                      "A pull should have come before.")
                }
                params += ((id, c))
              case BatchInit() =>
                // ignore workers that has not started batch
                if (workerHasStartedBatch(workerPartitionIndex)) {
                  normalUpdate()
                }
              case Online() =>
                normalUpdate()
            }
        }
      }
    }

    val paramPartitioner: WorkerOut[Array[Double]] => Int = {
      case WorkerOut(partitionId, msg) => msg match {
        case Left(paramId) => Math.abs(paramId) % psParallelism
        case Right((psIndex, Array())) if psIndex <= 0 => -psIndex // this marks the start of batch training
        case Right((psIndex, Array(-1.0))) if psIndex <= 0 => -psIndex // this marks the end of batch training
        case Right((paramId, delta)) => Math.abs(paramId) % psParallelism
      }
    }

    val wInPartition: WorkerIn[Array[Double]] => Int = {
      case WorkerIn(id, workerPartitionIndex, msg) => workerPartitionIndex
    }

    ratings.partitionCustom(new Partitioner[Rating] {
      override def partition(rating: Rating, numPartitions: Int): Int = {
        rating match {
          case (workerId, _, -1.0) =>
            workerId
          case (u, i, r) =>
            Math.abs(u) % numPartitions
        }
      }
    }, x => x)

    val trigger = batchTrainingTrigger.flatMap(x => for (w <- 0 until workerParallelism) yield (w, -1, -1.0))

    val modelUpdates = FlinkPS.psTransform(ratings.union(trigger), workerLogic, serverLogic,
      new SimpleClientReceiver[Array[Double]](),
      new SimpleClientSender[Array[Double]](),
      new SimplePSReceiver[Array[Double]](),
      new SimplePSSender[Array[Double]](),
      paramPartitioner = paramPartitioner,
      wInPartition = wInPartition,
      workerParallelism,
      psParallelism,
      iterationWaitTime)

    modelUpdates
  }

}
