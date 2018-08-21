package test.actor

import scala.actors._
import scala.actors.remote.RemoteActor._
import scala.actors.remote._

/**
  * Created by baidu on 2018/8/20.
  */
case class Msg(msg:String)
object ActorRemoteClient {


  class MyClientActor extends Actor {

    override  def exceptionHandler: PartialFunction[Exception, Unit] = {
      case e:Exception=>println(e)
    }

    def act() {

      val myRemoteActor = select(Node("127.0.0.1", 9000), 'myActor)
      myRemoteActor ! Msg("Hello!")
      receive {
        case response:String => println("Response: " + response)
        case r:MyMsg=> println("Response: " + r.msg)
        case _=>println(">>>>>")
      }
//      myRemoteActor !? "What is the meaning of life?" match {
//        case 42 => println("Success")
//        case oops:String => println("Failed: " + oops)
//        case _=>println(">>>>>")
//      }
      //    val future = myRemoteActor !! "What is the last digit of PI?"

    }
  }

  def main(args: Array[String]): Unit = {

    new MyClientActor().start()

    println("shutdown")

  }

}
