package hu.sztaki.ilab.recom.spark

import hu.sztaki.ilab.recom.core.{PseudoRandomFactorInitializerDescriptor, Rating, SGDUpdater}
import hu.sztaki.ilab.recom.spark.SparkExample.data
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.FunSuite

import scala.collection.mutable

class suiteOnline extends FunSuite {
  test("Basic online Spark test.") {
    val data = (for (user <- 1 to 100; item <- 1 to (101 - user)) yield (user, item, 5.0))

    val nFactors = 4
    val batchDuration = 5000
    val checkpointEvery = 40

    val conf = new SparkConf()
      .setAppName("Basic online Spark test").setMaster("local[10]")
    val ssc = new StreamingContext(conf, Milliseconds(batchDuration))
    val sc = ssc.sparkContext

    val batches = sc.makeRDD(data.take(25)) ::
      sc.makeRDD(data.slice(25, 75)) ::
      sc.makeRDD(data.drop(75)) :: Nil

    val ratings: DStream[Rating[Int, Int]] = ssc.queueStream(
      (mutable.Queue() ++ batches).map(_.map(r => Rating.fromTuple[Int, Int](r))),
      oneAtATime = true
    )

    val factorInit = PseudoRandomFactorInitializerDescriptor[Int](nFactors)
    val factorUpdate = new SGDUpdater(0.01)

    val model = new Online(ratings)()

    val updatedVectors =
      model.buildModelWithMap(
        ratings, factorInit, factorInit, factorUpdate, Map(), checkpointEvery)

    updatedVectors.foreachRDD(_.foreach(println))

    ssc.start()

    Thread.sleep(30000)

    val user = 100
    val items = model ? (List(user), 5, 0.001)
    items.flatMap {
      _._2
    }.foreach {
      r =>
        println(r._1 + "-" + r._2)
    }

    ssc.stop()
  }
}