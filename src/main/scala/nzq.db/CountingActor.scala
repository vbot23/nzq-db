package nzq.db

import akka.actor.{Actor, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberUp
import akka.event.Logging
import com.typesafe.config.ConfigFactory

class CountingActor extends Actor {
  val log = Logging(context.system, this)
  var counter = 0

  val cluster = Cluster(context.system)
//  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])
  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = {
    case IncreaseCounter =>
      counter += 1
      log.info(s"total number of update messages sent is $counter")
    case _ => log.warning("counting actor receives invalid message")
  }

}
object CountingActor {
  def main(args: Array[String]): Unit = {
    val port = args(0)
    val name = args(1)
    val config = ConfigFactory.parseString(
      s"""
      akka.remote.netty.tcp.port=$port
      akka.remote.artery.canonical.port=$port
      """)
      .withFallback(ConfigFactory.load())

    val system = ActorSystem("nzq", config)
    system.actorOf(Props[CountingActor], name)
  }
}
