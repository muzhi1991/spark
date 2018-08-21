package test.actor

/**
  * Created by baidu on 2018/8/15.
  */
object ActorScheduler {
  /**
    * scheduler用于执行一个Reactor实例
    * 学习源码了解Actor中schedule结构
    * IScheduler接口
    *   - DelegatingScheduler
    *     - Scheduler 一个单例object
    *     - DaemonScheduler
    *   - ForkJoinScheduler（默认） -- 使用ForkJoinPool执行任务 了解ForkJoin模型底层原理
    *   - ResizableThreadPoolScheduler/ExecutorScheduler/SingleThreadedScheduler 其他方式执行任务：各种线程池
    *   - SchedulerAdapter -- 自定义Scheduler时使用，使用上面的单例Scheduler
    *
    * ManagedBlocker原理：优化forkJoinPool中的io操作--某个线程阻塞时，启用新线程（Actor中在阻塞调用之前总会使用他通知ForkJoinPool）
    *
    * 调度对象：
    * ForkJoinTask接口
    *   - RecursiveTask
    *   - RecursiveAction
    *     - ReactorTask（actor控制的核心实现 -- Control异常的处理）
    *       - ReplayReactorTask
    *         - ActorTask
    *           - Reaction
    * @return
    */
//  def scheduler: IScheduler
  def main(args: Array[String]): Unit = {

  }

}
