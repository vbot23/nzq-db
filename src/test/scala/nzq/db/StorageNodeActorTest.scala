package nzq.db

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestActors, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest._

// test has to be run one at a time due to the reason of
class StorageNodeActorTest extends TestKit(ActorSystem("nzq")) with FlatSpecLike
    with ImplicitSender
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
{

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)


  "storage node actor" should "be loaded with correct keys" in {
    val counter = CountingActor.create(2551, "Counter", 1)
    val n0Ref = TestActorRef(new StorageNodeActor(List("a", "b"), List(1, 1), 0, 4, counter))
    val n0 = n0Ref.underlyingActor
    n0.getDBkeys().toSet should be(Set("a", "b"))
  }

  it should "have complete graph" in {
    val counter = CountingActor.create(2551, "Counter", 10)
    val n0Ref = TestActorRef(new StorageNodeActor(List("a", "b"), List(1, 1), 0, 10, counter))
    for (i <- 1 until 10) {
      TestActorRef(new StorageNodeActor(List("a", "b"), List(1, 1), i, 10, counter))
    }
    val n0 = n0Ref.underlyingActor
    Thread.sleep(30000)
    n0.getNeighborsOfKey("a").toSet should be((1 until 10).toSet)
    n0.getNeighborsOfKey("b").toSet should be((1 until 10).toSet)
  }

  it should "not be connected" in {
    val counter = CountingActor.create(2551, "Counter", 10)
    val n0Ref = TestActorRef(new StorageNodeActor(List("a"), List(1), 0, 10, counter), "n0")
    val n0 = n0Ref.underlyingActor
    for (i <- 1 until 10) {
      val key = ('a' + i).toChar.toString
      TestActorRef(new StorageNodeActor(List(key), List(1, 1), i, 10, counter))
    }
    Thread.sleep(30000)
    n0.getNeighborsOfKey("a").toSet should be(Set())
  }

  it should "be partially connected" in {
    val counter = CountingActor.create(2551, "Counter", 5)
    val n0ref = TestActorRef(new StorageNodeActor(List("a", "b"), List(1, 1), 0, 5, counter), "n0")
    val n1ref = TestActorRef(new StorageNodeActor(List("a", "c"), List(1, 1), 1,5, counter), "n1")
    val n2ref = TestActorRef(new StorageNodeActor(List("c", "d"), List(1, 1), 2,5, counter), "n2")
    val n3ref = TestActorRef(new StorageNodeActor(List("b", "d"), List(1, 1), 3,5, counter), "n3")
    val n4ref = TestActorRef(new StorageNodeActor(List("a", "b"), List(1, 1), 4,5, counter), "n4")
    val n0 = n0ref.underlyingActor
    val n1 = n1ref.underlyingActor
    val n2 = n2ref.underlyingActor
    val n3 = n3ref.underlyingActor
    val n4 = n4ref.underlyingActor
    Thread.sleep(30000)
    n0.getNeighborsOfKey("a").toSet should be(List(1, 4).toSet)
    n0.getNeighborsOfKey("b").toSet should be(List(3, 4).toSet)
    n1.getNeighborsOfKey("a").toSet should be(List(0, 4).toSet)
    n1.getNeighborsOfKey("c").toSet should be(List(2).toSet)
    n2.getNeighborsOfKey("c").toSet should be(List(1).toSet)
    n2.getNeighborsOfKey("d").toSet should be(List(3).toSet)
    n3.getNeighborsOfKey("b").toSet should be(List(0, 4).toSet)
    n3.getNeighborsOfKey("d").toSet should be(List(2).toSet)
    n4.getNeighborsOfKey("a").toSet should be(List(0, 1).toSet)
    n4.getNeighborsOfKey("b").toSet should be(List(0, 3).toSet)
  }
}
