package nzq.db
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object DBRunner {
  private val conf = ConfigFactory.load()
  val system = ActorSystem("nzq")
  private val allKeys = initAllKeys()

  def initAllKeys() = {
    var ak = List[List[String]]()
    for (i <- 0 until conf.getInt("numOfNodes")) {
      val keys = conf.getStringList(s"n$i.keys").asScala
      ak = ak :+ keys.toList
    }
    ak
  }

  /**
    * load data and send neighbors info
    */
  def initNodes(nodes: Array[ActorRef], clients: ListBuffer[Client]): Unit = {
    for (i <- nodes.indices) {
      val keys = conf.getStringList(s"n$i.keys").asScala
      val values = conf.getIntList(s"n$i.values").asScala
      for ((k, v) <- keys zip values) {
        clients(i).write(k, v)
      }
      nodes(i) ! UpdateAllRefs(nodes)
    }

  }


  def createNodesAndClients() = {
    var nodes = Array[ActorRef]()
    var clients = ListBuffer[Client]()
    var countActor = system.actorOf(Props[CountingActor])
    for (i <- 0 until conf.getInt("numOfNodes")) {

      val node = system.actorOf(Props(
        new StorageNodeActor(i,
          conf.getInt("numOfNodes"),
          getNeighborIdx(i),
          nodes,
          allKeys,
          countActor))
        , s"node$i")

      val client = new Client(node)
      nodes = nodes :+ node
      clients += client
    }
    (nodes, clients)
  }


  def getNeighborIdx(i: Int): List[Int] = {
    var localKeys = allKeys(i)
    var neighborsIdx = List[Int]()
    for (j <- 0 until conf.getInt("numOfNodes")) {
      if (j != i && localKeys.intersect(allKeys(j)).nonEmpty) {
        neighborsIdx = neighborsIdx :+ j
      }
    }
    neighborsIdx
  }

  def shell(nodes: Array[ActorRef], clients: ListBuffer[Client]): Unit = {
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
    val (nodes, clients) = createNodesAndClients()
    initNodes(nodes, clients)
    shell(nodes, clients)
  }

}

