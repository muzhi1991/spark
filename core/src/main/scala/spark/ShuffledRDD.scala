package spark

import java.util.{HashMap => JHashMap}

class ShuffledRDDSplit(val idx: Int) extends Split {
  override val index = idx
  override def hashCode(): Int = idx
}

class ShuffledRDD[K, V, C]( // my:作为shuffle后的第一个RDD
    parent: RDD[(K, V)],
    aggregator: Aggregator[K, V, C],
    part : Partitioner) 
  extends RDD[(K, C)](parent.context) {
  //override val partitioner = Some(part)
  override val partitioner = Some(part)
  
  @transient
  val splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))

  override def splits = splits_
  
  override def preferredLocations(split: Split) = Nil
  
  val dep = new ShuffleDependency(context.newShuffleId, parent, aggregator, part)
  override val dependencies = List(dep)

  override def compute(split: Split): Iterator[(K, C)] = {
    val combiners = new JHashMap[K, C]
    def mergePair(k: K, c: C) {
      val oldC = combiners.get(k)
      if (oldC == null) {
        combiners.put(k, c)
      } else {
        combiners.put(k, aggregator.mergeCombiners(oldC, c)) // my:这里只有C值，只用了mergeCombiners，因为这里本质上是shuffle之后获得了，V->C发生在之前ShuffleMapTask:createCombiner,mergeValue
      }
    }
    val fetcher = SparkEnv.get.shuffleFetcher
    fetcher.fetch[K, C](dep.shuffleId, split.index, mergePair) // my:注意fetch的时候只有C值，传入了合并函数，结果在combiners里面
    return new Iterator[(K, C)] {
      var iter = combiners.entrySet().iterator() // my:注意这里纯存储存储数据，如果c很大，如groupByKey的时候，内存可能爆了

      def hasNext(): Boolean = iter.hasNext()

      def next(): (K, C) = {
        val entry = iter.next()
        (entry.getKey, entry.getValue)
      }
    }
  }
}
