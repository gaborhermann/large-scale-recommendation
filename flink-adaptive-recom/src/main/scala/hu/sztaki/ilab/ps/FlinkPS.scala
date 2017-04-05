package hu.sztaki.ilab.ps

import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/* PS worker message flow */

trait ParameterServerClient[P, WOut] extends Serializable {

  def pull(id: Int): Unit

  def push(id: Int, deltaUpdate: P): Unit

  def output(out: WOut): Unit
}

trait ClientReceiver[IN, P] extends Serializable {
  def onPullAnswerRecv(msg: IN, pullHandler: (Int, P) => Unit)
}

trait ClientSender[OUT, P] extends Serializable {
  def onPull(id: Int, collectAnswerMsg: OUT => Unit, partitionId: Int)

  def onPush(id: Int, deltaUpdate: P, collectAnswerMsg: OUT => Unit, partitionId: Int)
}

trait WorkerLogic[T, P, WOut] extends Serializable {

  def onRecv(data: T, ps: ParameterServerClient[P, WOut]): Unit

  def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit

  def close(): Unit = ()
}

class MessagingPSClient[IN, OUT, P, WOut](sender: ClientSender[OUT, P],
                                          partitionId: Int,
                                          collector: Collector[Either[OUT, WOut]]) extends ParameterServerClient[P, WOut] {

  def collectAnswerMsg(msg: OUT): Unit = {
    collector.collect(Left(msg))
  }

  override def pull(id: Int): Unit =
    sender.onPull(id, collectAnswerMsg, partitionId)

  override def push(id: Int, deltaUpdate: P): Unit =
    sender.onPush(id, deltaUpdate, collectAnswerMsg, partitionId)

  override def output(out: WOut): Unit = {
    collector.collect(Right(out))
  }
}

/* PS message flow */

trait PSReceiver[WorkerOUT, P] extends Serializable {
  def onWorkerMsg(msg: WorkerOUT,
                  onPullRecv: (Int, Int) => Unit,
                  onPushRecv: (Int, P, Int) => Unit)
}

trait ParameterServerLogic[P, PSOut] extends Serializable {

  def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, PSOut]): Unit

  def onPushRecv(id: Int, deltaUpdate: P, workerPartitionIndex: Int, ps: ParameterServer[P, PSOut]): Unit
}

trait ParameterServer[P, PSOut] extends Serializable {
  def answerPull(id: Int, value: P, workerPartitionIndex: Int): Unit

  def output(out: PSOut): Unit
}

trait PSSender[WorkerIn, P] extends Serializable {
  def onPullAnswer(id: Int,
                   value: P,
                   workerPartitionIndex: Int,
                   collectAnswerMsg: WorkerIn => Unit)
}

class MessagingPS[WorkerIn, WorkerOut, P, PSOut](psSender: PSSender[WorkerIn, P],
                                                 collector: Collector[Either[WorkerIn, PSOut]])
  extends ParameterServer[P, PSOut] {

  def collectAnswerMsg(msg: WorkerIn): Unit = {
    collector.collect(Left(msg))
  }

  override def answerPull(id: Int, value: P, workerPartitionIndex: Int): Unit = {
    psSender.onPullAnswer(id, value, workerPartitionIndex, collectAnswerMsg)
  }

  override def output(out: PSOut): Unit = {
    collector.collect(Right(out))
  }
}

class FlinkPS {

}

object FlinkPS {

  private val log = LoggerFactory.getLogger(classOf[FlinkPS])

  def psTransform[T, P, PSOut, WOut, WorkerIn, WorkerOut](xs: DataStream[T],
                                                          workerLogic: WorkerLogic[T, P, WOut],
                                                          psLogic: ParameterServerLogic[P, PSOut],
                                                          clientReceiver: ClientReceiver[WorkerIn, P],
                                                          clientSender: ClientSender[WorkerOut, P],
                                                          psReceiver: PSReceiver[WorkerOut, P],
                                                          psSender: PSSender[WorkerIn, P],
                                                          paramPartitioner: WorkerOut => Int,
                                                          wInPartition: WorkerIn => Int,
                                                          workerParallelism: Int,
                                                          psParallelism: Int,
                                                          iterationWaitTime: Long = 10000)
                                                         (implicit
                                                          tiT: TypeInformation[T],
                                                          tiP: TypeInformation[P],
                                                          tiPSOut: TypeInformation[PSOut],
                                                          tiWOut: TypeInformation[WOut],
                                                          tiWorkerIn: TypeInformation[WorkerIn],
                                                          tiWorkerOut: TypeInformation[WorkerOut]
                                                         ): DataStream[Either[WOut, PSOut]] = {
    def stepFunc(workerIn: ConnectedStreams[T, WorkerIn]):
    (DataStream[WorkerIn], DataStream[Either[WOut, PSOut]]) = {

      val worker = workerIn
        .flatMap(
          new RichCoFlatMapFunction[T, WorkerIn, Either[WorkerOut, WOut]] {

            val receiver: ClientReceiver[WorkerIn, P] = clientReceiver
            val sender: ClientSender[WorkerOut, P] = clientSender
            val logic: WorkerLogic[T, P, WOut] = workerLogic

            // incoming answer from PS
            override def flatMap2(msg: WorkerIn, out: Collector[Either[WorkerOut, WOut]]): Unit = {
              log.debug(s"Pull answer: $msg")
              val psClient =
                new MessagingPSClient[WorkerIn, WorkerOut, P, WOut](
                  sender,
                  getRuntimeContext.getIndexOfThisSubtask,
                  out)
              receiver.onPullAnswerRecv(msg, {
                case (id, value) => logic.onPullRecv(id, value, psClient)
              })
            }

            // incoming data
            override def flatMap1(data: T, out: Collector[Either[WorkerOut, WOut]]): Unit = {
              val psClient =
                new MessagingPSClient[WorkerIn, WorkerOut, P, WOut](
                  sender,
                  getRuntimeContext.getIndexOfThisSubtask,
                  out)

              log.debug(s"Incoming data: $data")
              logic.onRecv(data, psClient)
            }

            override def close(): Unit = {
              logic.close()
            }
          }
        )
        .setParallelism(workerParallelism)

      val wOut = worker.flatMap(x => x match {
        case Right(out) => Some(out)
        case _ => None
      })

      val ps = worker
        .flatMap(x => x match {
          case Left(workerOut) => Some(workerOut)
          case _ => None
        })
        .partitionCustom(new Partitioner[Int]() {
          override def partition(key: Int, numPartitions: Int): Int = {
            key % numPartitions
          }
        }, paramPartitioner)
        .flatMap(new RichFlatMapFunction[WorkerOut, Either[WorkerIn, PSOut]] {

          val logic: ParameterServerLogic[P, PSOut] = psLogic
          val receiver: PSReceiver[WorkerOut, P] = psReceiver
          val sender: PSSender[WorkerIn, P] = psSender

          override def flatMap(msg: WorkerOut, out: Collector[Either[WorkerIn, PSOut]]): Unit = {
            log.debug(s"Pull request or push msg @ PS: $msg")

            val ps = new MessagingPS[WorkerIn, WorkerOut, P, PSOut](sender, out)
            receiver.onWorkerMsg(msg,
              (pullId, workerPartitionIndex) => logic.onPullRecv(pullId, workerPartitionIndex, ps), {
                case (pushId, deltaUpdate, workerPartitionIndex) =>
                  logic.onPushRecv(pushId, deltaUpdate, workerPartitionIndex, ps)
              }
            )
          }
        })
        .setParallelism(psParallelism)

      val psToWorker = ps
        .flatMap(_ match {
          case Left(x) => Some(x)
          case _ => None
        })
        .setParallelism(psParallelism)
        .map(x => x).setParallelism(workerParallelism)
        .partitionCustom(new Partitioner[Int]() {
          override def partition(key: Int, numPartitions: Int): Int = {
            if (0 <= key && key < numPartitions) {
              key
            } else {
              throw new RuntimeException("Pull answer key should be the partition ID itself!")
            }
          }
        }, wInPartition)

      val psToOut = ps.flatMap(_ match {
        case Right(x) => Some(x)
        case _ => None
      })
        .setParallelism(psParallelism)

      val wOutEither: DataStream[Either[WOut, PSOut]] = wOut.map(x => Left(x))
      val psOutEither: DataStream[Either[WOut, PSOut]] = psToOut.map(x => Right(x))

      (psToWorker, wOutEither.union(psOutEither))
    }

    xs
      .map(x => x)
      .setParallelism(workerParallelism)
      .iterate((x: ConnectedStreams[T, WorkerIn]) => stepFunc(x), iterationWaitTime)
  }
}

