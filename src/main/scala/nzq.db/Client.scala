package nzq.db

import akka.actor.{Actor, ActorRef}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Future
import scala.util.{Failure, Success}

class Client(node: ActorRef) {
  def get(k: String): Future[Any] = {
    implicit val timeout: Timeout = Timeout(1 seconds)
    val f = node ? ReadRequest(k)
    var ret: Option[Int] = None
    f onComplete {
      case Success(RACK(r)) =>
        println(s"received RACK $r from $node")
        ret = r
      case Failure(e) => println(e)
    }
    f
  }

  def write(k: String, v: Int): Future[Any] = {
    implicit val timeout: Timeout = Timeout(1 seconds)
    val f = node ? ClientInitWrite(k, v)
    f onComplete {
      case Success((WACK(cw))) => println(s"received WACK $cw from $node")
      case Failure(e) => println(e)
    }
    f
  }

  def update(k: String, v: Int): Future[Any] = {
    implicit val timeout: Timeout = Timeout(1 seconds)
    val f = node ? ClientWrite(k, v)
    f onComplete {
      case Success((WACK(cw))) => println(s"received WACK $cw from $node")
      case Failure(e) => println(e)
    }
    f
  }
}
