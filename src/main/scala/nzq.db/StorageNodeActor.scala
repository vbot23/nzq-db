package nzq.db


import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging

import scala.collection.mutable

class StorageNodeActor(localIdx: Int,
                       numOfMachines: Int,
                       neighborsIdx: List[Int],
                       var allNodesRef: Array[ActorRef],
                       val allKeys: List[List[String]],
                       countingActor: ActorRef)
  extends Actor {

  private var db = mutable.HashMap.empty[String, Int]
  private var pendings = mutable.MutableList.empty[UpdateRequest]
  val log = Logging(context.system, this)
  private val localVc = new VectorClock(numOfMachines, localIdx, neighborsIdx, allKeys)

  override def receive: Receive = {
    case ur: UpdateRequest => putUpdateRequest(ur)
    case cw: ClientWrite => putClientWrite(cw)
    case cwi: ClientInitWrite => putClientWriteNoProp(cwi)
    case rw: ReadRequest => get(rw)
    case UpdateAllRefs(nrefs) => setNeighborsRef(nrefs)
  }

  def canUpdate(vc: VectorClock): Boolean = {
    localVc.larger(vc)
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
    */
  def putClientWrite(cw: ClientWrite): Unit = {
    log.info(s"receive update: $cw")
    db(cw.key) = cw.value
    val needToUpdateNeighbors = neighborsIdx.filter(j => j != localIdx && allKeys(j).contains(cw.key))
    println(needToUpdateNeighbors)
    localVc.advance(needToUpdateNeighbors)
    sender() ! WACK(cw)
    propagate(UpdateRequest(cw.key, cw.value, localVc), needToUpdateNeighbors)
  }

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
  def propagate(ur: UpdateRequest, neighbors: List[Int]): Unit = {
    for (machine <- neighbors) {
      log.info(s"propagate write to ${allNodesRef(machine)}")
      allNodesRef(machine) ! ur
      countingActor ! IncreaseCounter
    }
  }

  def get(rw: ReadRequest): Unit = {
    log.info(s"received read request, $rw")
    sender() ! RACK(db.get(rw.key))
  }

  def setNeighborsRef(nrefs: Array[ActorRef]): Unit = {
    log.info(s"received all refs")
    allNodesRef = nrefs
  }

}

object StorageNodeActor {
  def props(localIdx: Int, numOfMachines: Int, neighborsIdx: List[Int], allRefs: Array[ActorRef], allKeys: List[List[String]], countingActor: ActorRef): Props
                                                  = Props(new StorageNodeActor(localIdx, numOfMachines, neighborsIdx, allRefs, allKeys, countingActor))
}