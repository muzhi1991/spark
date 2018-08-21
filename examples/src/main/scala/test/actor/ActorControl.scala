package test.actor

import scala.actors.Actor
import scala.actors.Actor._

/**
  * Created by baidu on 2018/8/14.
  */
//case object Ping
//case object Pong
//case object Stop




object ActorControl {

  class Ping(count: Int, pong: Actor) extends Actor {
    /**
      * act方法是每个Actor实际运行的方法
      */
    @Override
    def act() {
      var pingsLeft = count - 1
      // 感叹号是发送消息的方法
      pong ! Ping
      loop {
        react {
          case Pong =>
            Console.println("Ping: pong", self)
            if (pingsLeft > 0) {
              pong ! Ping
              pingsLeft -= 1
            } else {
              Console.println("Ping: stop")
              pong ! Stop
              exit()
            }
        }
      }
    }
  }



  class Pong extends Actor {

    def act() {
      var pongCount = 0
      loop {
        (react {
          case Ping =>
            Console.println("Pong: ping "+pongCount,self)
            sender ! Pong
            pongCount = pongCount + 1
            // 这里的continue理解成for语句里面的continue，react里面的不会执行，进入下个循环。但是andThen会执行
//            continue()
//            println("continue")
          case Stop =>
            Console.println("Pong: stop")
            exit()
        }:Unit) andThen{
          println("test")
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val pong = new Pong
    val ping = new Ping(5, pong)
    ping.start
    pong.start






  }
}
