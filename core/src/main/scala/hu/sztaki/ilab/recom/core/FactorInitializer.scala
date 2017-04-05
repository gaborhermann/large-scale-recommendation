package hu.sztaki.ilab.recom.core

import scala.util.Random

trait FactorInitializer {
  def nextFactor(id: Int): Array[Double]
}

trait FactorInitializerDescriptor {
  def open(): FactorInitializer
}

object FactorInitializerDescriptor {

  def apply(init: Int => Array[Double]): FactorInitializerDescriptor =
    new FactorInitializerDescriptor() {
      override def open(): FactorInitializer = new FactorInitializer {
        override def nextFactor(id: Int): Array[Double] = init(id)
      }
    }
}

class RandomFactorInitializer(random: Random, numFactors: Int)
  extends FactorInitializer {
  override def nextFactor(id: Int): Array[Double] = {
    Array.fill(numFactors)(random.nextDouble)
  }
}

class PseudoRandomFactorInitializer(numFactors: Int)
  extends FactorInitializer {
  override def nextFactor(id: Int): Array[Double] = {
    val random = new Random(id)
    Array.fill(numFactors)(random.nextDouble)
  }
}

case class RandomFactorInitializerDescriptor(numFactors: Int)
  extends FactorInitializerDescriptor {

  override def open(): FactorInitializer =
    new RandomFactorInitializer(scala.util.Random, numFactors)
}

case class PseudoRandomFactorInitializerDescriptor(numFactors: Int)
  extends FactorInitializerDescriptor {

  override def open(): FactorInitializer =
    new PseudoRandomFactorInitializer(numFactors)
}
