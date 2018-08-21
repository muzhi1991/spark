package test.actor

import scala.actors.Actor
import scala.actors.Actor._
import scala.actors.remote.RemoteActor._

/**
  * Created by baidu on 2018/8/20.
  */


case class MyMsg(msg:String,code:Int)

class MyActor extends Actor {
//  RemoteActor.classLoader= getClass().getClassLoader()
  def act() {


    alive(9000)
    register('myActor, self)
    // ...
    loop{
      react{
        case msg:String=>sender ! MyMsg(msg,42)
        case msg:Msg=>sender ! MyMsg(msg.msg,42)
      }
    }


  }
}


object ActorRemoteServer {

  def main(args: Array[String]): Unit = {
    new MyActor().start()
  }

}
