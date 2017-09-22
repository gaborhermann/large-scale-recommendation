package hu.sztaki.ilab.recom.spark

import hu.sztaki.ilab.recom.spark.OfflineSpark.ShiftedIntHasher
import org.apache.spark.internal.Logging
import org.scalatest.{FunSuite, Matchers}

class suiteHash extends FunSuite with Matchers with Logging {
  test("Test.") {
    val h = ShiftedIntHasher(10, _.hashCode(), 0)

    h.getPartition("101acdf49b463c384ffc434582e10806.62b11353c5441d8fa64ea09339cc8879") should be >= 0
    h.getPartition("101b362de479d8f6ebe3b8f5481e1194.241d8391665374cbde344cbf5b3afd8c") should be >= 0
    h.getPartition("1019549ac795e580b204ef7c7286d9b.321fe2c21610b995c08964fc91569895") should be >= 0
  }
}
