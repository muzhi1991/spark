package test.actor

/**
  * Created by baidu on 2018/8/14.
  */

import scala.actors.Actor._
import scala.actors.Exit


object ActorLink {

  def main(args: Array[String]): Unit = {

    val divide = actor {
      // The other half of error handling
      self.trapExit = true // 标记为trapExit的actor会收到Exit消息，可以在这里错误处理，否则错误会向外发散

      def newLinkedWorker = {
        // 实际执行运算的actor
        val worker = actor {
          loop {
            react {
              case (x: Int, y: Int) => println(x + " / " + y + " = " + (x / y))
            }
          }
        }
        // 连接worker与外面的actor
        self link worker
        worker
      }

      // Message handler loop 提供消息处理的循环服务
//      def run(worker: Actor): Nothing = react {
//        case (x: Int, y: Int) =>
//          worker ! (x, y)
//          run(worker)
//        case Exit(deadWorker, reason) =>
//          println("Worker 挂了，我们捕获到了，错误原因是: " + reason)
//          run(newLinkedWorker)
//      }
//      run(newLinkedWorker)

      val worker= newLinkedWorker
      loop{
       println("runing....")

        react {
          case (x: Int, y: Int) =>
            println("Worker state:"+worker.getState)
            worker ! (x, y)
          case Exit(deadWorker, reason) =>
            println("Worker 挂了，我们捕获到了，错误原因是: " + reason)
            println("Worker state:"+worker.getState)
            println("Worker restart")
            // 重新启动（使用旧的）
//            worker.restart()
            exit()
            println("Worker state:"+worker.getState)
          case _=>println("??????")
        }
      }


    }

    divide !(1,2)
    divide !(1,0)
    divide !(1,2)


  }

}
