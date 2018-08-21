package test.actor

import scala.actors.Actor._
import scala.actors.{Channel, OutputChannel}
/**
  * Created by baidu on 2018/8/15.
  */
object ActorChannel {

  /**
    * channel与actor最大的不同就是他可以固定接收、发送的数据类型
    * channel内部其实封装了一个Actor（名字是receiver），他本质上就是实现了一个数据类型的转换（InputChannel&&OutputChannel接口中定义的方法）
    * 因此，在功能上相同(Actor实现了InputChannel&&OutputChannel(父类Reactor中))
    */



  def main(args: Array[String]): Unit = {

    // case1:直接共享channel
    actor {
      // Channel用来接收发送固定数据类型，这里与外层actor绑定了
      val channel = new Channel[String] // 构建channel，查看实现里面用了self，即这个Actor
      val child = actor {
        react {
          case "go" => channel ! "hello"
        }
      }
//      out = channel
      child ! "go"
      channel.receive {
        case msg => println("case1:"+msg.length)
      }
    }


    // case2:通过消息分享channel
    case class ReplyTo(out: OutputChannel[String])

    val child = actor {
      react {
        case ReplyTo(out) => out ! "hello world"
      }
    }

    actor {
      val channel = new Channel[String]
      child ! ReplyTo(channel)
      channel.receive {
        case msg => println("case2:"+msg.length)
      }
    }

  }
}
