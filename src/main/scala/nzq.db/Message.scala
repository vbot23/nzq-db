package nzq.db

import akka.actor.ActorRef

import scala.collection.mutable.ListBuffer

case class UpdateRequest(key: String, value: Int, vc: VectorClock)
case class ReadRequest(key: String)

abstract class Write()
case class ClientWrite(key: String, value: Int) extends Write

case class WACK(clientWrite: Write)
case class UACK(updateRequest: UpdateRequest)
case class RACK(option: Option[Int])

case object IncreaseCounter
// indicate can publish
case object AllUp
case object Ready

// discovery
case class Hi(keys: List[String], idx: Int, newMember: ActorRef)