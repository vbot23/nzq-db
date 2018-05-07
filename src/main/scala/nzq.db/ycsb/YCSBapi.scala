//package nzq.db.ycsb
//
//import java.net.InetSocketAddress
//
//import akka.actor.{ActorSystem, Props}
//
//class YCSBapi {
//  val system = ActorSystem("ycsb")
//  val driver = system.actorOf(Props(new DriverActor(new InetSocketAddress("localhost", 2552))), name = "driver")
//
//  def write(k: String, v: Int): Unit = {
//    driver
//  }
//
//  def read(k: String): Int = {
//
//  }
//}
