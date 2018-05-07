//package nzq.db.ycsb
//
//import java.net.InetSocketAddress
//
//import akka.actor.{Actor, ActorSystem, Props}
//import akka.io.{IO, Tcp}
//import akka.util.ByteString
//import com.typesafe.config.ConfigFactory
//import nzq.db.{Client, CountingActor}
//
//
//class YCSBHandler extends Actor with akka.actor.ActorLogging {
//  import Tcp._
//  val system = ActorSystem("nzq")
//  private val conf = ConfigFactory.load()
//  private val numOfNodes = conf.getInt("numOfNodes")
//  private var countActor = system.actorOf(Props(new CountingActor(numOfNodes)))
//  private var clients = Array[Client]()
//
//  override def receive: Receive = {
//    case Received(data: ByteString) => {
//      data.toString.trim.split(",") match {
//        case (k, v) => sender() ! client.write()
//        case k => sender() ! client.read(k)
//      }
//    }
//  }
//}
//
//
//class YCSBServer extends Actor  with akka.actor.ActorLogging {
//  import Tcp._
//  import context.system
//
//
//
//
//
//
//
//
//  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", 2552))
//
//  def receive = {
//    case b @ Bound(localAddress) ⇒
//      context.parent ! b
//      log.info(s"running on $localAddress")
//
//    case CommandFailed(_: Bind) ⇒ context stop self
//
//    case c @ Connected(remote, local) ⇒
//      val connection = sender()
//      connection ! Register(context.actorOf(Props[YCSBHandler]))
//      log.info(s"connedted to $connection")
//    case "hehe" =>
//      log.info(s"received hehe from ${sender()}")
//  }
//
//}
//
//object YCSBRunner {
//  def main(args: Array[String]): Unit = {
//    val system = ActorSystem("System")
//    system.actorOf(Props[YCSBServer], name = "ycsb-server")
//  }
//}
