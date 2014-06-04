/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence

import scala.annotation.tailrec
import scala.collection.breakOut
import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import akka.actor.Actor
import akka.actor.ActorPath

object ReliableRedelivery {

  @SerialVersionUID(1L)
  case class ReliableRedeliverySnapshot(currentSeqNr: Long, unconfirmedDeliveries: immutable.Seq[UnconfirmedDelivery]) {

    // FIXME Java api
    // FIXME protobuf serialization

  }

  case class UnconfirmedDelivery(seqNr: Long, destination: ActorPath, msg: Any)

  object Internal {
    case class Delivery(destination: ActorPath, msg: Any, timestamp: Long, attempt: Int)
    case object RedeliveryTick
  }

}

trait ReliableRedelivery extends PersistentActor {
  import ReliableRedelivery._
  import ReliableRedelivery.Internal._

  def redeliverInterval: FiniteDuration

  private var redeliverTask = {
    import context.dispatcher
    val interval = redeliverInterval / 2
    context.system.scheduler.schedule(interval, interval, self, RedeliveryTick)
  }

  private var deliverySequenceNr = 0L
  private var unconfirmed = immutable.SortedMap.empty[Long, Delivery]

  private def nextDeliverySequenceNr(): Long = {
    // FIXME one counter sequence per destination?
    deliverySequenceNr += 1
    deliverySequenceNr
  }

  def deliver(destination: ActorPath, seqNrToMessage: Long => Any): Unit = {
    val seqNr = nextDeliverySequenceNr()
    val d = Delivery(destination, seqNrToMessage(seqNr), System.nanoTime(), attempt = 0)
    if (recoveryRunning)
      unconfirmed = unconfirmed.updated(seqNr, d)
    else
      send(seqNr, d)
  }

  // FIXME Java API for deliver

  def confirmDelivery(seqNr: Long): Boolean = {
    if (unconfirmed.contains(seqNr)) {
      unconfirmed -= seqNr
      true
    } else false
  }

  private def redeliverOverdue(): Unit = {
    val iter = unconfirmed.iterator
    val deadline = System.nanoTime() - redeliverInterval.toNanos
    @tailrec def loop(): Unit = {
      if (iter.hasNext) {
        val (seqNr, delivery) = iter.next()
        // skip all after deadline, since they are added in seqNo order 
        if (delivery.timestamp <= deadline) {
          send(seqNr, delivery)
          loop()
        }
      }
    }
    loop()
  }

  private def send(seqNr: Long, d: Delivery): Unit = {
    context.actorSelection(d.destination) ! d.msg
    unconfirmed = unconfirmed.updated(seqNr, d.copy(timestamp = System.nanoTime(), attempt = d.attempt + 1))
  }

  def getDeliverySnapshot: ReliableRedeliverySnapshot =
    ReliableRedeliverySnapshot(deliverySequenceNr,
      unconfirmed.map { case (seqNr, d) => UnconfirmedDelivery(seqNr, d.destination, d.msg) }(breakOut))

  def setDeliverySnapshot(snapshot: ReliableRedeliverySnapshot): Unit = {
    deliverySequenceNr = snapshot.currentSeqNr
    val now = System.nanoTime()
    unconfirmed = snapshot.unconfirmedDeliveries.map(d =>
      d.seqNr -> Delivery(d.destination, d.msg, now, 0))(breakOut)
  }

  override protected[akka] def aroundPreRestart(reason: Throwable, message: Option[Any]): Unit = {
    redeliverTask.cancel()
    super.aroundPreRestart(reason, message)
  }

  override protected[akka] def aroundPostStop(): Unit = {
    redeliverTask.cancel()
    super.aroundPostStop()
  }

  override protected[akka] def aroundReceive(receive: Receive, message: Any): Unit =
    message match {
      case RedeliveryTick ⇒ redeliverOverdue()
      case _              ⇒ super.aroundReceive(receive, message)
    }

  // FIXME max redelivery attempts?
  // FIXME redelivery failure listener?

}
