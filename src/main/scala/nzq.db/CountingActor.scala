package nzq.db

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.event.Logging
import com.typesafe.config.ConfigFactory

class CountingActor(numOfMachines: Int) extends Actor {
  val log = Logging(context.system, this)
  var counter = 0
  var ready = 0

  val cluster = Cluster(context.system)
  val mediator = DistributedPubSub(context.system).mediator
  val topic = "discovery"
  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = {
    case IncreaseCounter =>
      counter += 1
      log.info(s"total number of update messages sent is $counter")
    case Ready =>
      ready += 1
      log.info(s"received $sender, ready is $ready")
      if (ready == numOfMachines) {
        Thread.sleep(20000)
        mediator ! Publish(topic, AllUp)
        log.info("published allup!!!")
      }
    case m => log.warning(s"counting actor receives invalid message $m")
  }

}
object CountingActor {
  def create(port: Int, name: String, numOfMachines: Int): ActorRef = {
    val config = ConfigFactory.parseString(
      s"""
      akka.remote.netty.tcp.port=$port
      akka.remote.artery.canonical.port=$port
      """)
      .withFallback(ConfigFactory.load())

    val system = ActorSystem("nzq", config)
    system.actorOf(Props(new CountingActor(numOfMachines)), name)
  }
}
