package nzq.db
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object DBRunner {
  private val conf = ConfigFactory.load()
  val system = ActorSystem("nzq")
  val numOfNodes = conf.getInt("numOfNodes")
  var countActor = system.actorOf(Props[CountingActor])
  var clients = Array[Client]()

  /**
    * create node
    */
  def initNodes(): Unit = {
    for (i <- 0 until numOfNodes) {
      val keys = conf.getStringList (s"n$i.keys").asScala
      var values = conf.getIntList (s"n$i.values").asScala.map(_.toInt).toList
      val node = StorageNodeActor.create(keys.toList, values, i, numOfNodes, s"n$i", countActor)
      clients = clients :+ new Client(node)
    }
  }
  /**
    * interative shell
    */
  def shell(): Unit = {
    while (true) {
      val line = scala.io.StdIn.readLine("nzq db >")
      val list = line.trim.split(" ")
      list match {
        case Array("r", i, k) => clients(i.toInt).get(k)
        case Array("w", i, k, v) => clients(i.toInt).update(k, v.toInt)
        case _ => println("invalid command, try again")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    CountingActor.main(Seq("2551", "Counter").toArray) // cluster seed
    initNodes()
    shell()
  }

}

