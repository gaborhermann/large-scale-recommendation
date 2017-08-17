package hu.sztaki.ilab.recom

import org.apache.spark.rdd.RDD

package object spark {
  implicit def toVectorRDD[I](d: RDD[core.FactorVector[I]]): RDD[spark.OfflineSpark.Vector[I]] = {
    d.map(fv => (fv.ID, fv.vector))
  }
  implicit def toFactorRDD[I](d: RDD[spark.OfflineSpark.Vector[I]]): RDD[core.FactorVector[I]] = {
    d.map(v => core.FactorVector[I](v._1, v._2))
  }
}
