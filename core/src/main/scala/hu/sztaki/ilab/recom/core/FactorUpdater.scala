package hu.sztaki.ilab.recom.core

trait FactorUpdater extends Serializable {
  def nextFactors(rating: Double,
                  user: Array[Double],
                  item: Array[Double]): (Array[Double], Array[Double])

  /**
    *
    * @param rating
    * @param user
    * @param item
    * @return
    *         Vector to add to user and item vector respectively.
    */
  def delta(rating: Double,
            user: Array[Double],
            item: Array[Double]): (Array[Double], Array[Double])
}

class MockFactorUpdater extends FactorUpdater {
  override def nextFactors(rating: Double,
                           user: Array[Double],
                           item: Array[Double]): (Array[Double], Array[Double]) = {
    (user, item)
  }

  override def delta(rating: Double,
                     user: Array[Double],
                     item: Array[Double]): (Array[Double], Array[Double]) = {
    (user, item)
  }
}

class SGDUpdater(learningRate: Double) extends FactorUpdater {

  override def nextFactors(rating: Double,
                           user: Array[Double],
                           item: Array[Double]): (Array[Double], Array[Double]) = {

    val e = rating - user.zip(item).map { case (x, y) => x * y }.sum
    val userItem = user.zip(item)
    (userItem.map { case (u, i) => u + learningRate * e * i },
      userItem.map { case (u, i) => i + learningRate * e * u })
  }

  override def delta(rating: Double, user: Array[Double], item: Array[Double]): (Array[Double], Array[Double]) = {
    val e = rating - user.zip(item).map { case (x, y) => x * y }.sum
    val userItem = user.zip(item)

    (item.map(i => learningRate * e * i),
     user.map(u => learningRate * e * u))
  }
}
