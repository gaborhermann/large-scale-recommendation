package hu.sztaki.ilab.recom.core

import scala.util.Random

trait FactorInitializer[I] {
  def nextFactor(ID: I): Array[Double]
}

trait FactorInitializerDescriptor[I] {
  def open(): FactorInitializer[I]
}

object FactorInitializerDescriptor {
  def apply[I](init: I => Array[Double]): FactorInitializerDescriptor[I] =
    new FactorInitializerDescriptor[I]() {
      override def open(): FactorInitializer[I] = new FactorInitializer[I] {
        override def nextFactor(ID: I): Array[Double] = init(ID)
      }
    }
}

class RandomFactorInitializer[I](random: Random, numFactors: Int)
  extends FactorInitializer[I] {
  override def nextFactor(ID: I): Array[Double] = {
    Array.fill(numFactors)(random.nextDouble)
  }
}

class PseudoRandomFactorInitializer[I](numFactors: Int)
  extends FactorInitializer[I] {
  override def nextFactor(ID: I): Array[Double] = {
    val random = new Random()
    Array.fill(numFactors)(random.nextDouble)
  }
}

case class RandomFactorInitializerDescriptor[I](numFactors: Int)
  extends FactorInitializerDescriptor[I] {

  override def open(): FactorInitializer[I] =
    new RandomFactorInitializer[I](scala.util.Random, numFactors)
}

case class PseudoRandomFactorInitializerDescriptor[I](numFactors: Int)
  extends FactorInitializerDescriptor[I] {

  override def open(): FactorInitializer[I] =
    new PseudoRandomFactorInitializer[I](numFactors)
}
