package test.actor

/**
  * 老版本才使用这个scala.actors.xxx API
  * Created by baidu on 2018/8/9.
  */
import java.net.{InetAddress, UnknownHostException}

import scala.actors.Actor
import scala.actors.Actor.self

case object Ping
case object Pong
case object Stop

class Ping(count: Int, pong: Actor) extends Actor {
  /**
    * act方法是每个Actor实际运行的方法
    */
  @Override
  def act() {
    var pingsLeft = count - 1
    // 感叹号是发送消息的方法
    pong ! Ping
    while (true) {

     val t = receive {
        case Pong =>
          if (pingsLeft % 1000 == 0)
            Console.println("Ping: pong")
//            "ennnn"
          if (pingsLeft > 0) {
//            Console.println("Ping: running......")
            pong ! Ping
            pingsLeft -= 1
            "exe"
          } else {
//            Console.println("Ping: stop")
            pong ! Stop
            exit()
            "stop"
          }
      }
      println("running......"+t)
    }

//    loop {
//      react {
//        case Pong =>
//          if (pingsLeft % 1000 == 0)
//            Console.println("Ping: pong",self)
//          if (pingsLeft > 0) {
//            pong ! Ping
//            pingsLeft -= 1
//          } else {
//            Console.println("Ping: stop")
//            pong ! Stop
//            exit()
//          }
//      }
//    }
  }
}

class Pong extends Actor {

  /**
    * 可以自定义错误处理的函数
    * @return
    */
  override def exceptionHandler: PartialFunction[Exception, Unit] = {
    case x:Exception=>println("I catch Exception")
  }
  def act() {
    var pongCount = 0
//    while (true) {
//      receive {
//        case Ping =>
//          if (pongCount % 1000 == 0)
//            Console.println("Pong: ping "+pongCount)
//          // sender  获得最后一条接收的消息的发送者（就是这条？）
//          sender ! Pong
//          pongCount = pongCount + 1
//        case Stop =>
//          Console.println("Pong: stop")
//          exit()
//      }
//    }
    loop {
      println("dddd"+getState)

      react {
        case Ping =>
          if (pongCount % 1000 == 0)
            Console.println("Pong: ping "+pongCount,self)
          // sender  获得最后一条接收的消息的发送者（就是这条？）
          sender ! Pong
          pongCount = pongCount + 1
        case Stop =>
          Console.println("Pong: stop1")
//          throw new Exception("cannot process data")
//          Console.println("Pong: stop2")
          exit()
          Console.println("Pong: stop3")
      }
    }


  }
}

object TestActor extends Actor {
  override def act(): Unit = {
    println("11111")
    loop {
      react {
        case t: String =>
          println("here",self)
          sender ! getIp(t)
        case _ =>
          println("what????")
      }

    }

    println("22222")
  }

  def getIp(name: String): Option[InetAddress] = {
    try {
      // 解析的结果返回Some
       Some(InetAddress.getByName(name));
    } catch {
      // 抛出异常，返回None
      case _: UnknownHostException => None;
    }

  }

}

object ActorTest {




  def main(args: Array[String]): Unit = {
//
    val pong = new Pong
    val ping = new Ping(5, pong)
    println(ping.getState,pong.getState)
    ping.start
    println(ping.getState,pong.getState)
    pong.start
    println(ping.getState,pong.getState)
    Thread.sleep(500)
    println(ping.getState,pong.getState)





//    TestActor.start()
//    TestActor ! "www.baidu.com"
//    TestActor ! "heelo2"

//    Actor.actor {
//      TestActor ! "www.baidu.com"
//      TestActor ! "heelo2"
//      self.loop {
//        self.react {
//          case msg => println("msg==>" + msg, self)
//        }
//      }
//    }
//
//
//    TestActor ! "heelo2"

//    self.receive {
//      case msg => println("msg==>" + msg)
//    }

//    val sactor3 = Actor.actor{
//      var work = true;
//      while(true){
//        Actor.receive{
//          case x :Int =>println("got an Int: "+x)
//          case _ =>println("not an Int");
//        }
//      }
//    }

  }


}
