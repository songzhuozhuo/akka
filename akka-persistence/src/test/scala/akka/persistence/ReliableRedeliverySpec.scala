/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence

import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import com.typesafe.config._
import akka.actor._
import akka.testkit._
import akka.persistence.ReliableRedelivery.ReliableRedeliverySnapshot

object ReliableRedeliverySpec {

  case class Req(payload: String)
  case object ReqAck
  case object InvalidReq

  sealed trait Evt
  case class AcceptedReq(payload: String, destination: ActorPath) extends Evt
  case class ReqDone(id: Long) extends Evt

  case class Action(id: Long, payload: String)
  case class ActionAck(id: Long)
  case object Boom
  case object SaveSnap
  case class Snap(deliverySnapshot: ReliableRedeliverySnapshot) // typically includes some user data as well

  def sndProps(name: String, redeliverInterval: FiniteDuration, destinations: Map[String, ActorPath]): Props =
    Props(new Sender(name, redeliverInterval, destinations))

  class Sender(name: String, override val redeliverInterval: FiniteDuration, destinations: Map[String, ActorPath])
    extends PersistentActor with ReliableRedelivery {

    override def processorId: String = name

    def updateState(evt: Evt): Unit = evt match {
      case AcceptedReq(payload, destination) ⇒
        deliver(destination, seqNr => Action(seqNr, payload))
      case ReqDone(id) ⇒
        confirmDelivery(id)
    }

    val receiveCommand: Receive = {
      case Req(payload) ⇒
        if (payload.isEmpty)
          sender() ! InvalidReq
        else {
          val destination = destinations(payload.take(1).toUpperCase)
          persist(AcceptedReq(payload, destination)) { evt ⇒
            updateState(evt)
            sender() ! ReqAck
          }
        }

      case ActionAck(id) ⇒
        if (confirmDelivery(id))
          persist(ReqDone(id)) { evt ⇒
            updateState(evt)
          }

      case Boom ⇒
        throw new RuntimeException("boom") with NoStackTrace

      case SaveSnap =>
        saveSnapshot(Snap(getDeliverySnapshot))

    }

    def receiveRecover: Receive = {
      case evt: Evt ⇒ updateState(evt)
      case SnapshotOffer(_, Snap(deliverySnapshot)) =>
        println("# got snap: " + deliverySnapshot)
        setDeliverySnapshot(deliverySnapshot)

    }
  }

  def destinationProps(testActor: ActorRef): Props =
    Props(new Destination(testActor))

  class Destination(testActor: ActorRef) extends Actor {

    var allReceived = Set.empty[Long]

    def receive = {
      case a @ Action(id, payload) ⇒
        // discard duplicates (naive impl)
        if (!allReceived.contains(id)) {
          testActor ! a
          allReceived += id
        }
        sender() ! ActionAck(id)
    }
  }

  def unreliableProps(dropMod: Int, target: ActorRef): Props =
    Props(new Unreliable(dropMod, target))

  class Unreliable(dropMod: Int, target: ActorRef) extends Actor {
    var count = 0
    def receive = {
      case msg ⇒
        count += 1
        if (count % dropMod != 0)
          target forward msg
    }
  }

}

abstract class ReliableRedeliverySpec(config: Config) extends AkkaSpec(config) with PersistenceSpec with ImplicitSender {
  import ReliableRedeliverySpec._

  "ReliableRedelivery" must {
    "must deliver messages in order when nothing is lost" in {
      val probeA = TestProbe()
      val destinations = Map("A" -> system.actorOf(destinationProps(probeA.ref)).path)
      val snd = system.actorOf(sndProps(name, 500.millis, destinations), name)
      snd ! Req("a")
      expectMsg(ReqAck)
      probeA.expectMsg(Action(1, "a"))
      probeA.expectNoMsg(1.second)
    }

    "must re-deliver lost messages" in {
      val probeA = TestProbe()
      val dst = system.actorOf(destinationProps(probeA.ref))
      val destinations = Map("A" -> system.actorOf(unreliableProps(3, dst)).path)
      val snd = system.actorOf(sndProps(name, 500.millis, destinations), name)
      snd ! Req("a-1")
      expectMsg(ReqAck)
      probeA.expectMsg(Action(1, "a-1"))

      snd ! Req("a-2")
      expectMsg(ReqAck)
      probeA.expectMsg(Action(2, "a-2"))

      snd ! Req("a-3")
      snd ! Req("a-4")
      expectMsg(ReqAck)
      expectMsg(ReqAck)
      // a-3 was lost
      probeA.expectMsg(Action(4, "a-4"))
      // and then re-delivered
      probeA.expectMsg(Action(3, "a-3"))
      probeA.expectNoMsg(1.second)
    }

    "must re-deliver lost messages after restart" in {
      val probeA = TestProbe()
      val dst = system.actorOf(destinationProps(probeA.ref))
      val destinations = Map("A" -> system.actorOf(unreliableProps(3, dst)).path)
      val snd = system.actorOf(sndProps(name, 500.millis, destinations), name)
      snd ! Req("a-1")
      expectMsg(ReqAck)
      probeA.expectMsg(Action(1, "a-1"))

      snd ! Req("a-2")
      expectMsg(ReqAck)
      probeA.expectMsg(Action(2, "a-2"))

      snd ! Req("a-3")
      snd ! Req("a-4")
      expectMsg(ReqAck)
      expectMsg(ReqAck)
      // a-3 was lost
      probeA.expectMsg(Action(4, "a-4"))

      // trigger restart
      snd ! Boom

      // and then re-delivered
      probeA.expectMsg(Action(3, "a-3"))

      snd ! Req("a-5")
      expectMsg(ReqAck)
      probeA.expectMsg(Action(5, "a-5"))

      probeA.expectNoMsg(1.second)
    }

    "must restore state from snapshot" in {
      val probeA = TestProbe()
      val dst = system.actorOf(destinationProps(probeA.ref))
      val destinations = Map("A" -> system.actorOf(unreliableProps(3, dst)).path)
      val snd = system.actorOf(sndProps(name, 500.millis, destinations), name)
      snd ! Req("a-1")
      expectMsg(ReqAck)
      probeA.expectMsg(Action(1, "a-1"))

      snd ! Req("a-2")
      expectMsg(ReqAck)
      probeA.expectMsg(Action(2, "a-2"))

      snd ! Req("a-3")
      snd ! Req("a-4")
      snd ! SaveSnap
      expectMsg(ReqAck)
      expectMsg(ReqAck)
      // a-3 was lost
      probeA.expectMsg(Action(4, "a-4"))

      // trigger restart
      snd ! Boom

      // and then re-delivered
      probeA.expectMsg(Action(3, "a-3"))

      snd ! Req("a-5")
      expectMsg(ReqAck)
      probeA.expectMsg(Action(5, "a-5"))

      probeA.expectNoMsg(1.second)
    }

    "must re-deliver many lost messages" in {
      val probeA = TestProbe()
      val probeB = TestProbe()
      val probeC = TestProbe()
      val dstA = system.actorOf(destinationProps(probeA.ref))
      val dstB = system.actorOf(destinationProps(probeB.ref))
      val dstC = system.actorOf(destinationProps(probeC.ref))
      val destinations = Map(
        "A" -> system.actorOf(unreliableProps(2, dstA)).path,
        "B" -> system.actorOf(unreliableProps(5, dstB)).path,
        "C" -> system.actorOf(unreliableProps(3, dstC)).path)
      val snd = system.actorOf(sndProps(name, 500.millis, destinations), name)
      val N = 100
      for (n ← 1 to N) {
        snd ! Req("a-" + n)
      }
      for (n ← 1 to N) {
        snd ! Req("b-" + n)
      }
      for (n ← 1 to N) {
        snd ! Req("c-" + n)
      }
      probeA.receiveN(N).map { case a: Action ⇒ a.payload }.toSet should be((1 to N).map(n ⇒ "a-" + n).toSet)
      probeB.receiveN(N).map { case a: Action ⇒ a.payload }.toSet should be((1 to N).map(n ⇒ "b-" + n).toSet)
      probeC.receiveN(N).map { case a: Action ⇒ a.payload }.toSet should be((1 to N).map(n ⇒ "c-" + n).toSet)
    }

  }
}

class LeveldbReliableRedeliverySpec extends ReliableRedeliverySpec(PersistenceSpec.config("leveldb", "ReliableRedeliverySpec"))
@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class InmemReliableRedeliverySpec extends ReliableRedeliverySpec(PersistenceSpec.config("inmem", "ReliableRedeliverySpec"))
