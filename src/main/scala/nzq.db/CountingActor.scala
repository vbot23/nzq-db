package nzq.db

import akka.actor.Actor
import akka.event.Logging

class CountingActor extends Actor {
  val log = Logging(context.system, this)
  var counter = 0

  override def receive: Receive = {
    case IncreaseCounter =>
      counter += 1
      log.info(s"total number of update messages sent is $counter")
    case _ => log.warning("counting actor receives invalid message")
  }
}
