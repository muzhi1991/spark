package spark

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue, Map}

/**
 * A task created by the DAG scheduler. Knows its stage ID and map ouput tracker generation.
 */
abstract class DAGTask[T](val runId: Int, val stageId: Int) extends Task[T] {
  val gen = SparkEnv.get.mapOutputTracker.getGeneration
  override def generation: Option[Long] = Some(gen)
}

/**
 * A completion event passed by the underlying task scheduler to the DAG scheduler.
 */
case class CompletionEvent(
    task: DAGTask[_],
    reason: TaskEndReason,
    result: Any,
    accumUpdates: Map[Long, Any])

/**
 * Various possible reasons why a DAG task ended. The underlying scheduler is supposed to retry
 * tasks several times for "ephemeral" failures, and only report back failures that require some
 * old stages to be resubmitted, such as shuffle map fetch failures.
 */
sealed trait TaskEndReason
case object Success extends TaskEndReason
case class FetchFailed(serverUri: String, shuffleId: Int, mapId: Int, reduceId: Int) extends TaskEndReason
case class ExceptionFailure(exception: Throwable) extends TaskEndReason
case class OtherFailure(message: String) extends TaskEndReason

/**
 * A Scheduler subclass that implements stage-oriented scheduling. It computes a DAG of stages for 
 * each job, keeps track of which RDDs and stage outputs are materialized, and computes a minimal 
 * schedule to run the job. Subclasses only need to implement the code to send a task to the cluster
 * and to report fetch failures (the submitTasks method, and code to add CompletionEvents).
 */
private trait DAGScheduler extends Scheduler with Logging {
  // Must be implemented by subclasses to start running a set of tasks. The subclass should also
  // attempt to run different sets of tasks in the order given by runId (lower values first).
  def submitTasks(tasks: Seq[Task[_]], runId: Int): Unit

  // Must be called by subclasses to report task completions or failures.
  def taskEnded(task: Task[_], reason: TaskEndReason, result: Any, accumUpdates: Map[Long, Any]) {
    lock.synchronized {
      val dagTask = task.asInstanceOf[DAGTask[_]]
      eventQueues.get(dagTask.runId) match {
        case Some(queue) =>
          queue += CompletionEvent(dagTask, reason, result, accumUpdates)
          lock.notifyAll()
        case None =>
          logInfo("Ignoring completion event for DAG job " + dagTask.runId + " because it's gone")
      }
    }
  }

  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 2000L

  // The time, in millis, to wake up between polls of the completion queue in order to potentially
  // resubmit failed stages
  val POLL_TIMEOUT = 500L

  private val lock = new Object          // Used for access to the entire DAGScheduler

  private val eventQueues = new HashMap[Int, Queue[CompletionEvent]]   // Indexed by run ID

  val nextRunId = new AtomicInteger(0)

  val nextStageId = new AtomicInteger(0)

  val idToStage = new HashMap[Int, Stage]

  val shuffleToMapStage = new HashMap[Int, Stage]

  var cacheLocs = new HashMap[Int, Array[List[String]]]

  val env = SparkEnv.get
  val cacheTracker = env.cacheTracker
  val mapOutputTracker = env.mapOutputTracker

  def getCacheLocs(rdd: RDD[_]): Array[List[String]] = {
    cacheLocs(rdd.id)
  }
  
  def updateCacheLocs() {
    cacheLocs = cacheTracker.getLocationsSnapshot()
  }

  def getShuffleMapStage(shuf: ShuffleDependency[_,_,_]): Stage = {
    shuffleToMapStage.get(shuf.shuffleId) match {
      case Some(stage) => stage
      case None =>
        val stage = newStage(shuf.rdd, Some(shuf))
        shuffleToMapStage(shuf.shuffleId) = stage
        stage
    }
  }

  def newStage(rdd: RDD[_], shuffleDep: Option[ShuffleDependency[_,_,_]]): Stage = {
    // Kind of ugly: need to register RDDs with the cache and map output tracker here
    // since we can't do it in the RDD constructor because # of splits is unknown
    cacheTracker.registerRDD(rdd.id, rdd.splits.size)
    if (shuffleDep != None) {
      mapOutputTracker.registerShuffle(shuffleDep.get.shuffleId, rdd.splits.size)
    }
    val id = nextStageId.getAndIncrement()
    val stage = new Stage(id, rdd, shuffleDep, getParentStages(rdd)) // my:这里形成了链式的stage结构
    idToStage(id) = stage
    stage
  }

  def getParentStages(rdd: RDD[_]): List[Stage] = {
    val parents = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of splits is unknown
        cacheTracker.registerRDD(r.id, r.splits.size)
        for (dep <- r.dependencies) {
          dep match { // my:找出rdd中的shuffler依赖（宽依赖）
            case shufDep: ShuffleDependency[_,_,_] =>
              parents += getShuffleMapStage(shufDep) // my:构建了shuffleStage，注意他的结构包含了该Stage的最后一个RDD，和接着的Dependency
            case _ =>
              visit(dep.rdd)
          }
        }
      }
    }
    visit(rdd)
    parents.toList
  }

  def getMissingParentStages(stage: Stage): List[Stage] = { // my:核心函数：向上寻找没有执行的最近的parent-stage，注意逻辑忽略窄依赖，他不能构成stage
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        val locs = getCacheLocs(rdd) // my:update缓存，可能某些rdd的partition完成了缓存，需要更新snapshot
        for (p <- 0 until rdd.splits.size) {
          if (locs(p) == Nil) { // my:注意：这里不是说明运算过的stage的最后task都会cache，而是判断缓存里如果cache了某个rdd就不用向前递归了。
            for (dep <- rdd.dependencies) {
              dep match {
                case shufDep: ShuffleDependency[_,_,_] =>
                  val stage = getShuffleMapStage(shufDep)
                  if (!stage.isAvailable) { // my:核心逻辑，isAvailable函数判断了shuffle的stage是否已经完成了
                    missing += stage
                  }
                case narrowDep: NarrowDependency[_] =>
                  visit(narrowDep.rdd)
              }
            }
          }
        }
      }
    }
    visit(stage.rdd)
    missing.toList
  }

  override def runJob[T, U]( // my:核心的调度逻辑入口
      finalRdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean)
      (implicit m: ClassManifest[U]): Array[U] = {
    lock.synchronized {
      val runId = nextRunId.getAndIncrement()
      
      val outputParts = partitions.toArray
      val numOutputParts: Int = partitions.size
      val finalStage = newStage(finalRdd, None)
      val results = new Array[U](numOutputParts)
      val finished = new Array[Boolean](numOutputParts)
      var numFinished = 0
  
      val waiting = new HashSet[Stage] // stages we need to run whose parents aren't done
      val running = new HashSet[Stage] // stages we are running right now
      val failed = new HashSet[Stage]  // stages that must be resubmitted due to fetch failures
      val pendingTasks = new HashMap[Stage, HashSet[Task[_]]] // missing tasks from each stage
      var lastFetchFailureTime: Long = 0  // used to wait a bit to avoid repeated resubmits
  
      SparkEnv.set(env)
  
      updateCacheLocs()
      
      logInfo("Final stage: " + finalStage)
      logInfo("Parents of final stage: " + finalStage.parents)
      logInfo("Missing parents: " + getMissingParentStages(finalStage))
  
      // Optimization for short actions like first() and take() that can be computed locally
      // without shipping tasks to the cluster.
      if (allowLocal && finalStage.parents.size == 0 && numOutputParts == 1) {
        logInfo("Computing the requested partition locally")
        val split = finalRdd.splits(outputParts(0))
        val taskContext = new TaskContext(finalStage.id, outputParts(0), 0)
        return Array(func(taskContext, finalRdd.iterator(split)))
      }

      // Register the job ID so that we can get completion events for it
      eventQueues(runId) = new Queue[CompletionEvent]
  
      def submitStage(stage: Stage) { // my:核心函数：找出最开始的没有执行的stage并提交，其他stage放入waiting中等待执行
        if (!waiting(stage) && !running(stage)) {
          val missing = getMissingParentStages(stage) // my:向上寻找没有执行的最近的parent-stage
          if (missing == Nil) { // my:全都执行完了，执行本stage
            logInfo("Submitting " + stage + ", which has no missing parents")
            submitMissingTasks(stage)
            running += stage
          } else { // my:存在没有执行的stage，继续向上递归寻找
            for (parent <- missing) {
              submitStage(parent)
            }
            waiting += stage
          }
        }
      }
  
      def submitMissingTasks(stage: Stage) { // my:提交某个stage的所有task（一个分区一个task）
        // Get our pending tasks and remember them in our pendingTasks entry
        val myPending = pendingTasks.getOrElseUpdate(stage, new HashSet)
        var tasks = ArrayBuffer[Task[_]]()
        if (stage == finalStage) { // my:这里比较特殊，提交的job会为最后的stage特殊处理用ResultTask启动任务
          for (id <- 0 until numOutputParts if (!finished(id))) {
            val part = outputParts(id)
            val locs = getPreferredLocs(finalRdd, part)
            tasks += new ResultTask(runId, finalStage.id, finalRdd, func, part, locs, id)
          }
        } else { // my:非最后stage都是ShuffleMapTask，原因很简单：只有shuffle才会是宽依赖生成stage
          for (p <- 0 until stage.numPartitions if stage.outputLocs(p) == Nil) {
            val locs = getPreferredLocs(stage.rdd, p)
            tasks += new ShuffleMapTask(runId, stage.id, stage.rdd, stage.shuffleDep.get, p, locs)
          }
        }
        myPending ++= tasks
        submitTasks(tasks, runId) // my:实际提交task的函数！！
      }
  
      submitStage(finalStage) // my:开始执行
  
      while (numFinished != numOutputParts) {
        val eventOption = waitForEvent(runId, POLL_TIMEOUT) // my:挂起等待（用了wait+事件queue）
        val time = System.currentTimeMillis // TODO: use a pluggable clock for testability
  
        // If we got an event off the queue, mark the task done or react to a fetch failure
        if (eventOption != None) { // my:一般是非空，如果是none，表示wait超时了也没收到新消息，没关系，循环继续
          val evt = eventOption.get
          val stage = idToStage(evt.task.stageId)
          pendingTasks(stage) -= evt.task
          if (evt.reason == Success) { // my:返回的事件队列中的事件是执行成功了
            // A task ended
            logInfo("Completed " + evt.task)
            Accumulators.add(evt.accumUpdates) // my:累加器逻辑
            evt.task match {
              case rt: ResultTask[_, _] => // my:最终结果
                results(rt.outputId) = evt.result.asInstanceOf[U]
                finished(rt.outputId) = true
                numFinished += 1
              case smt: ShuffleMapTask => // my:shuffle结果
                val stage = idToStage(smt.stageId)
                stage.addOutputLoc(smt.partition, evt.result.asInstanceOf[String]) // my:记录某个shuffle task的结果（result是一个文件路径，会被后面的新提交的下游任务读取）
                if (running.contains(stage) && pendingTasks(stage).isEmpty) { // my:pendingTasks(stage)表示某个stage的所有的task，那么stage完成。
                  logInfo(stage + " finished; looking for newly runnable stages")
                  running -= stage
                  if (stage.shuffleDep != None) { // my:应该是必然成立的
                    mapOutputTracker.registerMapOutputs(
                      stage.shuffleDep.get.shuffleId,
                      stage.outputLocs.map(_.head).toArray) // my:表示这个stage的shuffle完成（所有shuffle task完成）！！！，把所有输出登记到mapOutputTracker中
                  }
                  updateCacheLocs() // my:很重要，更新cache，下面的getMissingParentStages才会返回新结果。
                  val newlyRunnable = new ArrayBuffer[Stage]
                  for (stage <- waiting if getMissingParentStages(stage) == Nil) { // my:重要逻辑，从waiting中拿出『新的下一个』且『父依赖都生成了的』stage，继续下个执行周期
                    newlyRunnable += stage
                  }
                  waiting --= newlyRunnable
                  running ++= newlyRunnable
                  for (stage <- newlyRunnable) {
                    submitMissingTasks(stage) // my:开始执行下个stage
                  }
                }
            }
          } else { // my: 返回的事件队列中的事件是执行失败, 注意在localScheduler的情况下似乎有bug，他只有ExceptionFailure（虽然里面有FetchFailed）。local遇到fetch直接退出了
            evt.reason match {
              case FetchFailed(serverUri, shuffleId, mapId, reduceId) => // my:Eexcutor中出现了错误，一定注意这个ShuffleId是出错的那个下游RDD指向的上游ShuffleId
                // Mark the stage that the reducer was in as unrunnable
                val failedStage = idToStage(evt.task.stageId)
                running -= failedStage
                failed += failedStage
                // TODO: Cancel running tasks in the stage
                logInfo("Marking " + failedStage + " for resubmision due to a fetch failure")
                // Mark the map whose fetch failed as broken in the map stage
                val mapStage = shuffleToMapStage(shuffleId)
                mapStage.removeOutputLoc(mapId, serverUri) // my:一个重要的逻辑：失败后移除上游shuffle输出，导致submitStage后的stage.isAvailable不成立，使得**上游重算**
                mapOutputTracker.unregisterMapOutput(shuffleId, mapId, serverUri)
                logInfo("The failed fetch was from " + mapStage + "; marking it for resubmission")
                failed += mapStage
                // Remember that a fetch failed now; this is used to resubmit the broken
                // stages later, after a small wait (to give other tasks the chance to fail)
                lastFetchFailureTime = time
                // TODO: If there are a lot of fetch failures on the same node, maybe mark all
                // outputs on the node as dead.
              case _ =>
                // Non-fetch failure -- probably a bug in the job, so bail out
                eventQueues -= runId
                throw new SparkException("Task failed: " + evt.task + ", reason: " + evt.reason)
                // TODO: Cancel all tasks that are still running
            }
          }
        } // end if (evt != null)
  
        // If fetches have failed recently and we've waited for the right timeout,
        // resubmit all the failed stages
        if (failed.size > 0 && time > lastFetchFailureTime + RESUBMIT_TIMEOUT) { // my:重试提交，这里要求间隔大于2s，注意这里无限次重试，只有没有fetch成功就不会跳出
          logInfo("Resubmitting failed stages")
          updateCacheLocs()
          for (stage <- failed) {
            submitStage(stage)
          }
          failed.clear()
        }
      }
  
      eventQueues -= runId
      return results
    }
  }

  def getPreferredLocs(rdd: RDD[_], partition: Int): List[String] = {
    // If the partition is cached, return the cache locations
    val cached = getCacheLocs(rdd)(partition)
    if (cached != Nil) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    val rddPrefs = rdd.preferredLocations(rdd.splits(partition)).toList
    if (rddPrefs != Nil) {
      return rddPrefs
    }
    // If the RDD has narrow dependencies, pick the first partition of the first narrow dep
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    rdd.dependencies.foreach(_ match { // my:本地运行原理，从下往上寻找依赖链上的本地位置
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocs(n.rdd, inPart)
          if (locs != Nil)
            return locs;
        }
      case _ =>
    })
    return Nil
  }

  // Assumes that lock is held on entrance, but will release it to wait for the next event.
  def waitForEvent(runId: Int, timeout: Long): Option[CompletionEvent] = {
    val endTime = System.currentTimeMillis() + timeout   // TODO: Use pluggable clock for testing
    while (eventQueues(runId).isEmpty) {
      val time = System.currentTimeMillis()
      if (time >= endTime) {
        return None
      } else {
        lock.wait(endTime - time)
      }
    }
    return Some(eventQueues(runId).dequeue())
  }
}
