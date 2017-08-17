package hu.sztaki.ilab.recom

package object core {
  sealed trait VectorUpdate
  case class UserUpdate[QI](v: FactorVector[QI]) extends VectorUpdate
  case class ItemUpdate[PI](v: ItemUpdate[PI]) extends VectorUpdate

  case class Rating[QI, PI](user: QI, item: PI, rating: Double)

  object Rating {
    def fromTuple[QI, PI](t: (QI, PI, Double)): Rating[QI, PI] = Rating(t._1, t._2, t._3)
  }

  case class FactorVector[I](ID: I, vector: Array[Double]) {
    override def toString: String = s"FactorVector($ID, [${vector.mkString(",")}])"
  }
}
