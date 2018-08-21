package test.actor

import scala.actors.Actor._
import scala.actors.Futures

/**
  * Created by baidu on 2018/8/15.
  */
object ActorFuture {

  val testActor=actor{
    loop{
      react{
        case name:String=>reply("hello:"+name)
      }
    }
  }

  def main(args: Array[String]): Unit = {

    /**
      * 使用actor的函数获得future
      */
    val f=testActor!!"muz"
    // 同步获得
    println(f())
    // 轮训检测
//    while(!f.isSet){
//      println("runing....")
//    }
//    println(f())

    // error :需要在actor上下文中定义
//    f.inputChannel.react{case i:String=>println(i)}

    /**
      * 直接定义future
      */
    val fut = Futures.future{ println("dddd") ;1}
    println(fut())



    /**
      * 在actor中使用future的inputChannel来异步获得
      */
    actor{
      val fut = Futures.future{ println("dddd") ;1}
      fut.inputChannel.react{case i:Int=>println(i)}
    }
  }

}
