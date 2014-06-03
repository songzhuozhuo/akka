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
  case object ReliveryTick

}

trait ReliableRedelivery extends Actor {
  import ReliableRedelivery._

  def redeliverInterval: FiniteDuration

  private var redeliverTask = {
    import context.dispatcher
    val interval = redeliverInterval / 2
    context.system.scheduler.schedule(interval, interval, self, ReliveryTick)
  }

  private var deliverySequenceNr = 0L
  private var unconfirmed = immutable.SortedMap.empty[Long, Delivery]
  private var added = List.empty[Delivery]

  def nextDeliverySequenceNr(destination: ActorPath): Long = {
    // FIXME one counter sequence per destination
    deliverySequenceNr += 1
    deliverySequenceNr
  }

  def addDelivery(deliverySequenceNr: Long, destination: ActorPath, msg: Any): Unit = {
    val d = Delivery(deliverySequenceNr, destination, msg, System.nanoTime(), attempt = 0)
    added = d :: added
  }

  def confirm(deliverySequenceNr: Long): Boolean = {
    moveAddedToUnconfirmed()
    if (unconfirmed.contains(deliverySequenceNr)) {
      unconfirmed -= deliverySequenceNr
      true
    } else false
  }

  def sendAddedDeliveries(): Unit = {
    added.reverse foreach send
    moveAddedToUnconfirmed()
  }

  private def redeliverOverdue(): Unit = {
    moveAddedToUnconfirmed()

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

  private def moveAddedToUnconfirmed(): Unit = {
    if (added.nonEmpty) {
      unconfirmed ++= added.map { d ⇒ d.seqNr -> d }
      added = Nil
    }
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
      case ReliveryTick ⇒
        // FIXME this doesn't work, because aroundReceive is first handled by the Processor
        //       and I can't change the mixin order beacuse of final aroundPreRestart and friends.
        //       Perhaps this should not be an Actor mixin at all? The user could schedule the 
        //       ticks and call redeliverOverdue(). That would also allow for more flexible
        //       backoff strategies. WDYT?
        redeliverOverdue()
      case _ ⇒
        super.aroundReceive(receive, message)
    }

  // FIXME see above aroundReceive
  override def unhandled(message: Any): Unit = message match {
    case ReliveryTick ⇒ redeliverOverdue()
    case _            ⇒ super.unhandled(message)
  }

  // FIXME max redelivery attempts?
  // FIXME redelivery failure listener?

}
