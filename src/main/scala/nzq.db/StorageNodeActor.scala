package nzq.db


import akka.actor.{Actor, ActorRef, ActorSystem, Props, RootActorPath}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.pubsub.DistributedPubSub
import akka.event.Logging
import com.typesafe.config.ConfigFactory
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe, SubscribeAck}

import scala.collection.mutable

class StorageNodeActor(keys: List[String],
                       values: List[Int],
                       localIdx: Int,
                       numOfMachines: Int,
                       countingActor: ActorRef)
  extends Actor {

  val cluster = Cluster(context.system)
  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])
  override def postStop(): Unit = cluster.unsubscribe(self)
  val mediator = DistributedPubSub(context.system).mediator
  val log = Logging(context.system, this)
  val topic = "discovery"
  mediator ! Subscribe(topic, self)


  private var db = mutable.HashMap.empty[String, Int]
  private var neighborRef = mutable.HashMap.empty[String, List[ActorRef]]  // store neighbors [key -> ref]
  private var neighborIdx = mutable.HashMap.empty[String, List[Int]]  // store neighbors [key -> ref]
  private var pendings = mutable.MutableList.empty[UpdateRequest]
  (keys zip values) foreach {case (k, v) => db(k) = v}



  // init vector clock
  private val localVc = new VectorClock(numOfMachines, localIdx)
  neighborIdx foreach {case (_, neighbors) => localVc.init(neighbors)}
  Thread.sleep(5000)
  mediator ! Publish(topic, Hi(db.keys.toList, localIdx, self))

  override def receive: Receive = {
    case ur: UpdateRequest => putUpdateRequest(ur)
    case cw: ClientWrite => putClientWrite(cw)
    case cwi: ClientInitWrite => putClientWriteNoProp(cwi)
    case rw: ReadRequest => get(rw)
    case hi: Hi => handshake(hi)
    case SubscribeAck(_) => log.info(s"$self subscribed to the topic")
    case MemberUp(m) => log.info(s"$m is up!!!")
  }

  def handshake(hi: Hi): Unit = {
    if (hi.newMember == self) return
    log.info(s"$self received $hi from $sender()")
    for (k <- hi.keys) {
      if (db.contains(k)) {
        if (!neighborRef.contains(k)) {
          neighborRef(k) = List()
          neighborIdx(k) = List()
        }
        neighborRef(k) = neighborRef(k) :+ sender()
        neighborIdx(k) = neighborIdx(k) :+ hi.idx
      }
    }
  }

  def canUpdate(vc: VectorClock): Boolean = {
    localVc.inCausalPast(vc)
  }

  /**
    * TODO: create a pending actor (checking pending task concurrently)
    */
  def updateBulkUpdates(updates: mutable.MutableList[UpdateRequest]): Unit = {
    for (update <- updates) {
      db(update.key) = update.value
      localVc.merge(update.vc)
    }
  }
  /**
    * TODO: create a pending actor (checking pending task concurrently)
    */
  def checkPendings(): Unit = {
    var updates = mutable.MutableList.empty[UpdateRequest]
    do {
      updates = pendings.filter(ur => canUpdate(ur.vc))
      pendings = pendings.filterNot(ur => canUpdate(ur.vc))
      updateBulkUpdates(updates)
    } while(updates.nonEmpty)
  }

  /**
    * main version write from client
    * 1. update db
    * 2. advance vc
    * 3. propagate to neighbors that have the same key
    * 4. send write ACK to client
    */
  def putClientWrite(cw: ClientWrite): Unit = {
    log.info(s"receive update: $cw")
    db(cw.key) = cw.value
    val needToUpdateNeighbors = neighborIdx.getOrElse(cw.key, List())
    log.info(s"need to update neighors are $needToUpdateNeighbors")
    localVc.advance(needToUpdateNeighbors)
    sender() ! WACK(cw)
    propagate(UpdateRequest(cw.key, cw.value, localVc), neighborRef.getOrElse(cw.key, List()))
  }

  /**
    * for init use
    */
  def putClientWriteNoProp(cw: ClientInitWrite): Unit = {
    log.info(s"receive client write $cw")
    db(cw.key) = cw.value
    sender() ! WACK(cw)
  }

  /**
    * handles update request propagated by neighbors.
    */
  def putUpdateRequest(ur: UpdateRequest): Unit = {
    log.info(s"receive update request $ur")
    if (canUpdate(ur.vc)) {
      db(ur.key) = ur.value
      localVc.merge(ur.vc)
      checkPendings()
    }
    else {
      pendings += ur
    }
  }

  /**
    *  propagate to neighbors and update counter
    */
  def propagate(ur: UpdateRequest, neighbors: List[ActorRef]): Unit = {
    for (machine <- neighbors) {
      machine ! ur
      countingActor ! IncreaseCounter
    }
  }

  /**
    * fulfill client get
    */
  def get(rw: ReadRequest): Unit = {
    log.info(s"received read request, $rw")
    sender() ! RACK(db.get(rw.key))
  }
}

object StorageNodeActor {
  def props(keys: List[String],
            values: List[Int],
            localIdx: Int,
            numOfMachines: Int,
            countingActor: ActorRef)
  = Props(new StorageNodeActor(keys: List[String], values, localIdx, numOfMachines, countingActor))


  /**
    * arg[0]: port
    * arg[1]: name
    */
  def create(keys: List[String], values: List[Int], localIdx: Int, numOfMachines: Int, name: String, countingActor: ActorRef): ActorRef = {
    val port = 0
    val config = ConfigFactory.parseString(s"""
        akka.remote.netty.tcp.port=$port
        akka.remote.artery.canonical.port=$port
        """)
      .withFallback(ConfigFactory.load())

    val system = ActorSystem("nzq", config)
    system.actorOf(Props(new StorageNodeActor(keys, values, localIdx, numOfMachines, countingActor)), name)
  }
}