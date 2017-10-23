package hu.sztaki.ilab.recom.spark

import hu.sztaki.ilab.recom.core.{PseudoRandomFactorInitializerDescriptor, Rating, SGDUpdater}
import hu.sztaki.ilab.recom.spark.SparkExample.data
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.time.{Minutes, Seconds, Span}

import scala.collection.mutable

class suiteOnline extends FunSuite with Matchers with Logging {
  test("Basic online Spark test.") {
    val data = for (user <- 1 to 100; item <- 1 to (101 - user)) yield (user, item, 5.0)

    val nFactors = 4
    val batchDuration = 5000
    val checkpointEvery = 40

    val conf = new SparkConf()
      .setAppName("Basic online Spark test").setMaster("local[10]")
    val ssc = new StreamingContext(conf, Milliseconds(batchDuration))
    val sc = ssc.sparkContext

    val batches = data.sliding(1000, 1000).map(slice => sc.makeRDD(slice))
    val coldData = batches.take(1).next()

    val ratings: DStream[Rating[Int, Int]] = ssc.queueStream(
      (mutable.Queue() ++ batches.drop(1)).map(_.map(r => Rating.fromTuple[Int, Int](r))),
      oneAtATime = true
    )

    val factorInit = PseudoRandomFactorInitializerDescriptor[Int](nFactors)
    val factorUpdate = new SGDUpdater(0.01)

    val model = new Online[Int, Int](coldData.map(r => Rating.fromTuple[Int, Int](r)))()

    val updatedVectors =
      model.buildModelWithMap(
        ratings, factorInit, factorInit, factorUpdate, checkpointEvery)

    updatedVectors.foreachRDD(_.foreach(println))

    ssc.start()

    Thread.sleep(30000)

    val user = 100
    val items = model ? (List(user), () => sc.makeRDD(data.map(_._2)).map(_ -> true), 5, 0.001)
    items.flatMap {
      _._2
    }.foreach {
      r =>
        println(r._1 + "-" + r._2)
    }

    ssc.stop()
  }

  test("Basic online Spark test with query stream.") {
    val data = for (user <- 1 to 100; item <- 1 to (101 - user))
               yield (user.toString, item.toString, 5.0)

    val nFactors = 4
    val batchDuration = 1000
    val checkpointEvery = 40

    val conf = new SparkConf()
      .setAppName("Basic online Spark test").setMaster("local[10]")
    val ssc = new StreamingContext(conf, Milliseconds(batchDuration))
    ssc.checkpoint("/tmp")
    val sc = ssc.sparkContext

    val batches = data.sliding(1000, 1000).map(slice => sc.makeRDD(slice))
    val coldData = batches.take(1).next()

    val ratings: DStream[Rating[String, String]] = ssc.queueStream(
      (mutable.Queue() ++ batches.drop(1)).map(_.map(r => Rating.fromTuple[String, String](r))),
      oneAtATime = true
    ).repartition(21)

    val factorInit = PseudoRandomFactorInitializerDescriptor[String](nFactors)
    val factorUpdate = new SGDUpdater(0.01)

    val model = new Online[String, String](
      coldData.map(r => Rating.fromTuple[String, String](r)).repartition(5)
    )()

    val updatedVectors =
      model.buildModelWithMap(
        ratings, factorInit, factorInit, factorUpdate, checkpointEvery)

    updatedVectors.print()

    val queryQueue = mutable.Queue[RDD[String]]()
    val queries = ssc.queueStream(
      queryQueue,
      oneAtATime = true
    ).cache()
    val filter = () => sc.makeRDD(data.map(_._2).distinct).map(_ -> true).filter(p => p._1.toInt > 20)

    queries.print()
    (model ? (queries, filter, 5, 0.001))
      .join(queries.map((_, null)))
      .print()

    ssc.start()

    logInfo("Waiting for ranking snapshots to be computed.")
    Eventually.eventually(Timeout(Span(4, Minutes)), Interval(Span(1, Seconds))) {
      model.snapshotsComputed should be >= 1
    }
    logInfo("First ranking snapshot has been computed.")

    val user = 100.toString
    queryQueue += sc.makeRDD(Seq(user))

    Thread.sleep(30000)

    ssc.stop()
  }
}
