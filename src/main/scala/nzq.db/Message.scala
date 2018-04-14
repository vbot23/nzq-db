package nzq.db

import akka.actor.ActorRef

import scala.collection.mutable.ListBuffer

case class UpdateRequest(key: String, value: Int, vc: VectorClock)
case class ReadRequest(key: String)

abstract class Write()
case class ClientInitWrite(key: String, value: Int) extends Write
case class ClientWrite(key: String, value: Int) extends Write

case class NotFoundKey(key: String)
case class WACK(clientWrite: Write)
case class UACK(updateRequest: UpdateRequest)
case class RACK(option: Option[Int])

case object IncreaseCounter
case class UpdateAllRefs(nrefs: Array[ActorRef])
