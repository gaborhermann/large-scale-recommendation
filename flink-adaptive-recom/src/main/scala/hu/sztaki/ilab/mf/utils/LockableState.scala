package hu.sztaki.ilab.mf.utils

import hu.sztaki.ilab.recom.core.Rating
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Random

trait LockableState[IN, S] {
  def getStateOrCacheValue(input: IN): Option[(IN, S)]

  def setStateAndSendNext(newState: S): Option[(IN, S)]
}

class LockableStateWithQueue[IN, S](val initState: S) extends LockableState[IN, S] {

  private var state = initState

  private val log = LoggerFactory.getLogger(classOf[LockableStateWithQueue[IN, S]])

  private var locked: Boolean = false
  private val cacheQueue: mutable.Queue[IN] = mutable.Queue.empty

  override def getStateOrCacheValue(input: IN): Option[(IN, S)] = {
    if (locked) {
      cacheQueue.enqueue(input)
      None
    } else {
      locked = true
      Some((input, state))
    }
  }

  override def setStateAndSendNext(newState: S): Option[(IN, S)] = {
    if (locked) {

      state = newState

      val headOpt = cacheQueue.headOption.map((_, newState))

      if (headOpt.isEmpty) {
        locked = false
      } else {
        cacheQueue.dequeue()
      }

      headOpt
    } else {
      throw new RuntimeException("IMPOSSIBLE! Vector should have been locked!")
    }
  }

}
