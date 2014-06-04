/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import akka.actor.Actor
import akka.actor.ActorPath

/**
 * INTERNAL API
 */
private[akka] object ReliableRedelivery {

  case class Delivery(seqNr: Long, destination: ActorPath, msg: Any, timestamp: Long, attempt: Int)
  case object RedeliveryTick

}

trait ReliableRedelivery extends PersistentActor {
  import ReliableRedelivery._

  def redeliverInterval: FiniteDuration

  private var redeliverTask = {
    import context.dispatcher
    val interval = redeliverInterval / 2
    context.system.scheduler.schedule(interval, interval, self, RedeliveryTick)
  }

  private var deliverySequenceNr = 0L
  private var unconfirmed = immutable.SortedMap.empty[Long, Delivery]

  def nextDeliverySequenceNr(destination: ActorPath): Long = {
    // FIXME one counter sequence per destination
    deliverySequenceNr += 1
    deliverySequenceNr
  }

  def deliver(deliverySequenceNr: Long, destination: ActorPath, msg: Any): Unit = {
    require(!unconfirmed.contains(deliverySequenceNr), s"deliverySequenceNr [$deliverySequenceNr] already in use")
    val d = Delivery(deliverySequenceNr, destination, msg, System.nanoTime(), attempt = 0)
    if (recoveryRunning)
      unconfirmed = unconfirmed.updated(deliverySequenceNr, d)
    else
      send(d)
  }

  def confirmDelivery(deliverySequenceNr: Long): Boolean = {
    if (unconfirmed.contains(deliverySequenceNr)) {
      unconfirmed -= deliverySequenceNr
      true
    } else false
  }

  private def redeliverOverdue(): Unit = {
    val iter = unconfirmed.iterator
    val deadline = System.nanoTime() - redeliverInterval.toNanos
    @tailrec def loop(): Unit = {
      if (iter.hasNext) {
        val (_, delivery) = iter.next()
        // skip all after deadline, since they are added in seqNo order 
        if (delivery.timestamp <= deadline) {
          send(delivery)
          loop()
        }
      }
    }
    loop()
  }

  private def send(d: Delivery): Unit = {
    context.actorSelection(d.destination) ! d.msg
    unconfirmed = unconfirmed.updated(d.seqNr, d.copy(timestamp = System.nanoTime(), attempt = d.attempt + 1))
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
