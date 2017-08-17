package hu.sztaki.ilab.recom.spark

import java.util.Random

import hu.sztaki.ilab.recom.core.{PseudoRandomFactorInitializerDescriptor, Rating, SGDUpdater}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.mutable
import scala.util

object SparkExample {
  def main(args: Array[String]): Unit = {
    val numFactors = 4
    val batchDuration = 5000
    val offlineEvery = -1 // -1 means no batch training, only online
    val checkpointEvery = 40
    val offlineAlgorithm = "ALS" // or DSGD

    val conf = new SparkConf().setAppName("SparkMFExample")
    val ssc = new StreamingContext(conf, Milliseconds(batchDuration))
    val sc = ssc.sparkContext

    val batch1 = sc.makeRDD(data.take(20))
    val batch2 = sc.makeRDD(data.slice(20, 30))
    val batch3 = sc.makeRDD(data.drop(30))

    val ratings: DStream[Rating[Int, Int]] = ssc.queueStream(
      mutable.Queue(batch1, batch2, batch3).map(_.map(Rating.fromTuple)),
      oneAtATime = true)

    val factorInit = PseudoRandomFactorInitializerDescriptor[Int](numFactors)
    val factorUpdate = new SGDUpdater(0.01)

    val model = new Online(ratings)()

    val updatedVectors =
      if (offlineEvery == -1) {
        model.buildModelWithMap(
          ratings, factorInit, factorInit, factorUpdate, Map(), checkpointEvery)
      } else {
        model.buildModelCombineOffline(
          ratings, factorInit, factorInit, factorUpdate, Map(), checkpointEvery,
          offlineEvery, 10, offlineAlgorithm, numFactors)
      }

    updatedVectors.foreachRDD(_.foreach(println))

    ssc.start()

    Thread.sleep(30000)

    val user = data(util.Random.nextInt(data.size))
    val items = model ? (List(user).map(_._1), 5, 0.001)
    items.flatMap {
        _._2
    }.foreach {
      r =>
        println(r._1 + "-" + r._2)
    }

    ssc.stop()
  }

  val data: Seq[(Int, Int, Double)] = {
    Seq(
      (2,13,534.3937734561154),
      (6,14,509.63176469621936),
      (4,14,515.8246770897443),
      (7,3,495.05234565105),
      (2,3,532.3281786219485),
      (5,3,497.1906356844367),
      (3,3,512.0640508585093),
      (10,3,500.2906742233019),
      (1,4,521.9189079662882),
      (2,4,515.0734651491396),
      (1,7,522.7532725967008),
      (8,4,492.65683825096403),
      (4,8,492.65683825096403),
      (10,8,507.03319667905413),
      (7,1,522.7532725967008),
      (1,1,572.2230209271174),
      (2,1,563.5849190220224),
      (6,1,518.4844061038742),
      (9,1,529.2443732217674),
      (8,1,543.3202505434103),
      (7,2,516.0188923307859),
      (1,2,563.5849190220224),
      (1,11,515.1023793011227),
      (8,2,536.8571133978352),
      (2,11,507.90776961762225),
      (3,2,532.3281786219485),
      (5,11,476.24185144363304),
      (4,2,515.0734651491396),
      (4,11,469.92049343738233),
      (3,12,509.4713776280098),
      (4,12,494.6533165132021),
      (7,5,482.2907867916308),
      (6,5,477.5940040923741),
      (4,5,480.9040684364228),
      (1,6,518.4844061038742),
      (6,6,470.6605085832807),
      (8,6,489.6360564705307),
      (4,6,472.74052954447046),
      (7,9,482.5837650471611),
      (5,9,487.00175463269863),
      (9,9,500.69514584780944),
      (4,9,477.71644808419325),
      (7,10,485.3852917539852),
      (8,10,507.03319667905413),
      (3,10,500.2906742233019),
      (5,15,488.08215944254437),
      (6,15,480.16929757607346)
    )
  }
}
