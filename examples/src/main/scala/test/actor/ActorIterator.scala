package test.actor

import scala.actors.Actor
import scala.actors.Actor._

/**
  * Created by baidu on 2018/8/13.
  */
object ActorIterator {

  case class Tree(elem: Int, left: Tree, right: Tree)

  case object Next


  abstract class Producer[T] {
    protected def produceValues: Unit

    protected def produce(x: T) {
      coordinator ! Some(x)
      receive { case Next => }
    }

    private val producer: Actor = actor {
      receive {
        case Next =>
          produceValues
          coordinator ! None
      }
    }

    private val coordinator: Actor = actor {
      loop {
        react {
          case Next =>
            producer ! Next
            reply {
              receive {
                case x: Option[_] => x
              }
            }
          case Stop => exit('stop)
        }
      }
    }

    private val Undefined = new Object

    def iterator = new Iterator[T] {
      private var current: Any = Undefined
      private def lookAhead = {
        if (current == Undefined) current = coordinator !? Next
        current
      }

      def hasNext: Boolean = lookAhead match {
        case Some(x) => true
        case None => { coordinator ! Stop; false }
      }

      def next: T = lookAhead match {
        case Some(x) => current = Undefined; x.asInstanceOf[T]
      }
    }
  }

  class PreOrder(n: Tree) extends Producer[Int] {
    def produceValues = traverse(n)

    def traverse(n: Tree) {
      if (n != null) {
        produce(n.elem)
        traverse(n.left)
        traverse(n.right)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val t=Tree(0,Tree(1,null,null),Tree(2,Tree(1,null,null),null))

    val p=new PreOrder(t)
    val iter=p.iterator
    iter.foreach(x=>println(x))


  }


}
