package hu.sztaki.ilab.recom.spark

import hu.sztaki.ilab.recom.core.{PseudoRandomFactorInitializerDescriptor, Rating, SGDUpdater}
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
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

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

    model.buildModelWithMap(
      ratings, factorInit, factorInit, factorUpdate)

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

    model.buildModelWithMap(
      ratings, factorInit, factorInit, factorUpdate)

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

  test("Matrix validation from ratings stream.") {
    val sampleTastes = for (user <- 0 to 9; item <- 0 to 99)
      yield (user.toString, item.toString, math.random * 5)

    val similarUsers = sampleTastes
      .groupBy(_._1)
      .mapValues(_.toList)
      .flatMap {
        case (_, ratings: List[(String, String, Double)]) =>
          /**
            * Create 100 similar users to this user.
            */
          (1 to 100).flatMap {
            newUser => ratings.map {
              case (u, i, r) =>
                (newUser + ".similar-to." + u, i, r + ((math.random - 0.5) / 100.0))
            /**
              * Remove 30% of ratings.
              */
            }.filter {
              _ => math.random > 0.3
            }
          }
      }

    val data = (similarUsers ++ sampleTastes).map(d => math.random -> d)
      .toSeq.sortBy(-_._1).map(_._2)

    val nFactors = 20
    val batchDuration = 1000

    @transient val conf = new SparkConf()
      .setAppName("Basic online Spark test").setMaster("local[10]")
    @transient val ssc = new StreamingContext(conf, Milliseconds(batchDuration))
    ssc.checkpoint("/tmp")
    @transient val sc = ssc.sparkContext

    val batches = data.sliding(8000, 8000).map(slice => sc.makeRDD(slice)).toList

    logInfo(s"There are a total of [${batches.size}] batches.")

    val coldData = batches.head

    val ratings: DStream[Rating[String, String]] = ssc.queueStream(
      (mutable.Queue() ++ batches.drop(1)).map(_.map(r => Rating.fromTuple[String, String](r))),
      oneAtATime = true
    ).repartition(20)

    val factorInit = PseudoRandomFactorInitializerDescriptor[String](nFactors)
    val factorUpdate = new SGDUpdater(0.08)

    val model = new Online[String, String](
      coldData.map(r => Rating.fromTuple[String, String](r)).repartition(5)
    )(nPartitions = 5, parallelism = 5, iterations = 10)

    model.buildModelWithMap(
      ratings, factorInit, factorInit, factorUpdate)

    val queryQueue = mutable.Queue[RDD[String]]()
    val queries = ssc.queueStream(
      queryQueue,
      oneAtATime = true
    ).cache()
    @transient val filter = sc.makeRDD(data.map(_._2).distinct).map(_ -> true).cache()
    filter.count()

    queries.print()
    (model ? (queries, () => filter, 200, 0.001))
      .join(queries.map((_, null)))
      .print()

    ssc.start()

    logInfo("Waiting for ranking snapshots to be computed.")
    Eventually.eventually(Timeout(Span(60, Minutes)), Interval(Span(10, Seconds))) {
      model.snapshotsComputed should be >= 3
    }
    logInfo("First ranking snapshot has been computed.")

    var referenceUser = sampleTastes.filter(_._1 == "5")
    var user = "50.similar-to.5"

    var items = (model ? (List(user), () => filter, 200, 0.001)).head._2.toList
    logInfo(s"Got [${items.length}] items.")
    logInfo(s"Reference user is [${referenceUser.mkString(", ")}].")
    items.foreach {
      r =>
        println(r._1 + " -> " + r._2)
    }

    logInfo(s"Total error is [${
      math.sqrt(referenceUser.map {
        case (_, i, r) =>
          math.pow(items.find(_._1 == i).map(_._2 - r).getOrElse(0.0), 2)
      }.sum)
    }].")

    logInfo(s"Random's error is [${
      math.sqrt(referenceUser.map {
        case (_, i, r) =>
          math.pow(math.random - r, 2)
      }.sum)
    }].")


    referenceUser = sampleTastes.filter(_._1 == "8")
    user = "20.similar-to.8"

    items = (model ? (List(user), () => filter, 200, 0.001)).head._2.toList
    logInfo(s"Got [${items.length}] items.")
    logInfo(s"Reference user is [${referenceUser.mkString(", ")}].")
    items.foreach {
      r =>
        println(r._1 + " -> " + r._2)
    }

    logInfo(s"Total error is [${
      math.sqrt(referenceUser.map {
        case (_, i, r) =>
          math.pow(items.find(_._1 == i).map(_._2 - r).getOrElse(0.0), 2)
      }.sum)
    }].")

    logInfo(s"Random's error is [${
      math.sqrt(referenceUser.map {
        case (_, i, r) =>
          math.pow(math.random - r, 2)
      }.sum)
    }].")

    ssc.stop()
  }
}
