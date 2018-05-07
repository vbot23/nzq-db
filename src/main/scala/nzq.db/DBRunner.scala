package nzq.db

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
import util.control.Breaks._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object DBRunner {
  private val conf = ConfigFactory.load()
  val system = ActorSystem("nzq")
  val numOfNodes = conf.getInt("numOfNodes")
  var countActor = system.actorOf(Props(new CountingActor(numOfNodes)))
  var clients = Array[Client]()
  val random = new scala.util.Random


  /**
    * create nodes from conf file
    */
  def initNodes(): Unit = {
    for (i <- 0 until numOfNodes) {
      val keys = conf.getStringList(s"n$i.keys").asScala
      var values = conf.getIntList(s"n$i.values").asScala.map(_.toInt).toList
      val node = StorageNodeActor.create(keys.toList, values, i, numOfNodes, s"n$i", countActor)
      clients = clients :+ new Client(node, keys.toList)
    }
  }

  def initNodesBenchMark(numOfMachines: Int, numOfKeys: Int, maxCliqueSize: Int): Unit = {
    var keySet = Array[String]()
    val values = mutable.HashMap.empty[String, Int]
    val machines = (0 until numOfMachines).toList
    val assignments = mutable.HashMap.empty[Int, List[String]]
    var mat = Array.ofDim[Int](numOfMachines, numOfMachines)
    mat = mat.map(_ => Array.fill(numOfMachines)(0))


    for (i <- 0 until numOfKeys) {
      keySet = keySet :+ ('a'.toInt + i).toChar.toString
      values(('a'.toInt + i).toChar.toString) = 'a'.toInt + i
    }
    for (i <- 0 until numOfMachines) {
      assignments(i) = List()
    }
    for (k <- keySet) {
      val cliqueSize = random.nextInt(maxCliqueSize) + 1
      val clique = random.shuffle(machines).take(cliqueSize)
      // build adjacency matrix
      val pairs = for (x <- clique; y <- clique) yield (x, y)
      for ((idx1, idx2) <- pairs) {
        if (idx1 != idx2) {
          mat(idx1)(idx2) = 1
        }
      }

      for (m <- clique) {
        assignments(m) = assignments(m) :+ k
      }
    }
    for (i <- 0 until numOfMachines) {
      val nodeVals = assignments(i).map(k => values(k))
      val node = StorageNodeActor.create(assignments(i), nodeVals, i, numOfMachines, s"n$i", countActor)
      clients = clients :+ new Client(node, assignments(i))
    }
    printAjacencyMat(mat)
    println(s"db content is $assignments")
    calProbabilityOfAnEdge(numOfMachines, maxCliqueSize, numOfKeys)
  }

  def calProbabilityOfAnEdge(numOfMachines: Int, maxCliqueSize: Int, numOfKeys: Int): Double = {
    val expectedClique = (maxCliqueSize + 1) / 2.0
    val probabilityOfEdgeNotAppear = expectedClique * (expectedClique - 1) / (numOfMachines * (numOfMachines - 1)).toFloat
    val overall = 1 - scala.math.pow(1 - probabilityOfEdgeNotAppear, numOfKeys)
    println(s"an edge in the shared graph has $overall to appear")
    overall
  }

  def printAjacencyMat(mat: Array[Array[Int]]): Unit = {
    for (i <- mat.indices) {
      for (j <- mat(i).indices) {
        if (j < mat(i).length - 1) {
          print(mat(i)(j) + ", ")
        }
        else println(mat(i)(j))
      }
    }
  }

  /**
    * randomly select a machine and send update
    */
  def benchmarking(times: Int): Unit = {
    println("starts benchmarking.........")
    var i = 0
    while (i < times) {
      breakable {
        val idx = random.nextInt(clients.length)
        if (clients(idx).getKeys.isEmpty) {
          break
        }
        val k = random.shuffle(clients(idx).getKeys).take(1).head
        println(s"writing $k with value $i to machine $i.")
        clients(idx).write(k, i)
        i += 1
      }
    }
  }

  /**
    * interative shell
    */
  def shell(): Unit = {
    while (true) {
      val line = scala.io.StdIn.readLine("nzq db > ")
      val list = line.trim.split(" ")
      list match {
        case Array("r", i, k) => clients(i.toInt).get(k)
        case Array("w", i, k, v) => clients(i.toInt).write(k, v.toInt)
        case _ => println("invalid command, try again")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    CountingActor.create(2551, "Counter", numOfNodes) // cluster seed
    initNodesBenchMark(5, 5,5)
    // ensure every node is up
    Thread.sleep(30000)
    println(
      """
        |
        | __          __  _                            _                           _____  ____
        | \ \        / / | |                          | |                         |  __ \|  _ \
        |  \ \  /\  / /__| | ___ ___  _ __ ___   ___  | |_ ___    _ __  ______ _  | |  | | |_) |
        |   \ \/  \/ / _ \ |/ __/ _ \| '_ ` _ \ / _ \ | __/ _ \  | '_ \|_  / _` | | |  | |  _ <
        |    \  /\  /  __/ | (_| (_) | | | | | |  __/ | || (_) | | | | |/ / (_| | | |__| | |_) |
        |     \/  \/ \___|_|\___\___/|_| |_| |_|\___|  \__\___/  |_| |_/___\__, | |_____/|____/
        |                                                                     | |
        |                                                                     |_|
        |
      """.stripMargin)
    benchmarking(500)
    //    initNodes()
    shell()
  }

}

