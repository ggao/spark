package org.apache.spark.deploy.yarn.server

import akka.actor.ActorRef
import org.apache.spark.SparkContext
import org.apache.spark.deploy.yarn.server.ApplicationContext._
import org.apache.spark.deploy.yarn.server.ChannelProtocols._

import scala.language.postfixOps

/**
 * This class will be run inside the Yarn Cluster
 */


object ChannelProtocols {
  val AKKA = "akka"
  val NETTY = "netty"
  val WEBSOCKET ="websocket"
}


sealed trait ChannelMessenger {
  val protocol: String
  val messenger: Option[Any]

  def sendMessage(sc: SparkContext, message: Any)
}

case class ActorMessenger(messenger:Option[ActorRef]) extends ChannelMessenger {
  val protocol = ChannelProtocols.AKKA

  def sendMessage(sc: SparkContext, message: Any)   = {
    implicit val actorSystem = sc.env.actorSystem
    messenger.foreach(_ ! message)
  }

}
case class NettyMessenger(messenger:Option[Any]) extends ChannelMessenger {
  val protocol = ChannelProtocols.NETTY

  def sendMessage(sc: SparkContext, message: Any) = ???

}
case class WebSocketMessenger(messenger:Option[Any]) extends ChannelMessenger {
  val protocol = ChannelProtocols.WEBSOCKET

  def sendMessage(sc: SparkContext, message: Any) = ???
}

object CommunicationHelper {
  def stopRelayMessenger(sparkCtx: Option[SparkContext], channelMessenger: ChannelMessenger) {
    sparkCtx.map { sc =>
      channelMessenger.protocol match {
        case AKKA =>
          channelMessenger.messenger.asInstanceOf[Option[ActorRef]].map(sc.env.actorSystem.stop)
        case NETTY =>
        case WEBSOCKET =>
        case _  =>
      }
    }
  }
/*

  def verifyRemoteActorSystem(sc: SparkContext, relayMessenger: ActorRef) {
    implicit val actorSystem = sc.env.actorSystem
    implicit val timeout = new Timeout(2 seconds)
    val f = relayMessenger.ask(Ping)
    val response = Await.result(f, timeout.duration)
    println(s"response = $response")
  }
*/


  def createRelayMessenger(appCtx: ApplicationContext): ChannelMessenger = {
    val protocol = appCtx.conf.get(SPARK_APP_CHANNEL_PROTOCOL, AKKA)
    protocol match {
      case AKKA =>  AkkaChannelUtils.createRelayMessenger(appCtx)
      case NETTY => NettyMessenger(None)
      case WEBSOCKET => WebSocketMessenger(None)
      case _ => ActorMessenger(None)
    }

  }

}
