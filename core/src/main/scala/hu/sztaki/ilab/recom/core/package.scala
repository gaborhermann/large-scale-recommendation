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

  import org.slf4j.LoggerFactory

  trait Logger {
    private val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(
      this.getClass.getName.stripSuffix("$")
    ))

    protected def logInfo(message: => String): Unit = logger.info(message)
    protected def logInfo(message: => String,
                          throwable: Throwable): Unit = logger.info(message, throwable)

    protected def logWarning(message: => String): Unit = logger.warn(message)
    protected def logWarning(message: => String,
                             throwable: Throwable): Unit = logger.warn(message, throwable)

    protected def logError(message: => String): Unit = logger.error(message)
    protected def logError(message: => String,
                           throwable: Throwable): Unit = logger.error(message, throwable)

    protected def logDebug(message: => String): Unit = logger.debug(message)
    protected def logDebug(message: => String,
                           throwable: Throwable): Unit = logger.debug(message, throwable)

    protected def logTrace(message: => String): Unit = logger.trace(message)
    protected def logTrace(message: => String,
                           throwable: Throwable): Unit = logger.trace(message, throwable)
  }
}
