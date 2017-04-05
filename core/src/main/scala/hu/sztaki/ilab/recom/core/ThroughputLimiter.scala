package hu.sztaki.ilab.recom.core

class ThroughputLimiter(letThrough: Int, perMillisec: Long) {

  var batchStart: Long = -1
  var cnt = 0

  def emitOrWait[A](in: A): A = {
    if (batchStart == -1) {
      batchStart = System.currentTimeMillis()
    }

    cnt += 1
    if (cnt > letThrough) {
      val currentTime = System.currentTimeMillis()
      val waitTime = batchStart + perMillisec - currentTime
      if (waitTime > 0) {
        Thread.sleep(waitTime)
      }
      batchStart = currentTime
      cnt = 0
    }
    in
  }
}
