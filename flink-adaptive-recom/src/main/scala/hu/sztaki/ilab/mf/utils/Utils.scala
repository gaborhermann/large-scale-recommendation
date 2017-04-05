package hu.sztaki.ilab.mf.utils

import org.apache.flink.api.common.functions.Partitioner

object Utils {

  class CustomHasher extends Partitioner[Int] with Serializable {
    override def partition(key: Int, numPartitions: Int): Int = {
      Math.abs(key.hashCode()) % numPartitions
    }
  }
}
