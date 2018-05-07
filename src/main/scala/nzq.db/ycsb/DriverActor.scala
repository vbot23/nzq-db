//package nzq.db.ycsb
//
//import java.net.InetSocketAddress
//
//import akka.actor.{Actor, ActorSystem, Props}
//import akka.io.{IO, Tcp}
//import akka.util.ByteString
//
//
//
//
//
//object DriverActor {
//  def props(remote: InetSocketAddress) =
//    Props(classOf[DriverActor], remote)
//}
//
//class DriverActor(remote: InetSocketAddress) extends Actor with akka.actor.ActorLogging {
//
//  import Tcp._
//  import context.system
//
//
//  IO(Tcp) ! Connect(remote)
//
//  def receive = {
//    case CommandFailed(_: Connect) ⇒
//      context stop self
//
//    case c @ Connected(remote, local) ⇒
//      val connection = sender()
//      log.info(s"connedted to $connection")
//      connection ! Register(self)
//      context become {
//        case data: ByteString ⇒
//          connection ! Write(data)
//        case Received(data) ⇒
//          log.info(s"received $data")
//        case "close" ⇒
//          connection ! Close
//        case _: ConnectionClosed ⇒
//          context stop self
//      }
//  }
//}
//
//
//object DriverRunner {
//  def main(args: Array[String]): Unit = {
//
//  }
//}