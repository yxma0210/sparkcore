# sparkcore
  
## RDD的创建方式
  1. 从内存中创建
  ```
  val sparkConf =
    new SparkConf().setMaster("local[*]").setAppName("spark") 
  val sc= new SparkContext(sparkConf)
  val rdd1 = sc.parallelize( List(1,2,3,4)
  )
  val rdd2 = sc.makeRDD( List(1,2,3,4) //从底层代码实现来讲，makeRDD 方法其实就是parallelize 方法
  )
  rdd1.collect().foreach(println)
```
  2. 从外部文件创建

## RDD转换算子
  ### Value类型
  
  1. map
  ```
  def map[U: ClassTag](f: T => U): RDD[U]
  
  // 将处理的数据逐条进行映射转换，这里的转换可以是类型的转换，也可以是值的转换。rdd的计算一个分区内的数据是一个一个执行逻辑， 
  // 只有前面一个数据全部的逻辑执行完毕后，才会执行下一个数据。分区内数据的执行是有序的。 不同分区数据计算是无序的。
  ```
  2. mapPartitions
  ```
  def mapPartitions[U: 
    ClassTag]( f: Iterator[T] => 
    Iterator[U],
    preservesPartitioning: Boolean = false): RDD[U]
    
  // 将待处理的数据以分区为单位发送到计算节点进行处理，但是会将整个分区的数据加载到内存进行引用，如果处理完的数据是不会被释放掉，
  // 存在对象的引用，在内存较小，数据量较大的场合下，容易出现内存溢出。
  ```
  3. mapPartitionsWithIndex
  ```
  def mapPartitionsWithIndex[U: 
    ClassTag]( f: (Int, Iterator[T]) => 
    Iterator[U],
    preservesPartitioning: Boolean = false): RDD[U]
    
  // 将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处理，哪怕是过滤数据，在处理时同时可以获取当前分区索引
  ```
  4. flatMap
  ```
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U]
  // 将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射
  ```
  5. glom
  ```
  def glom(): RDD[Array[T]]
  //将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
  ```
  6. groupBy
  ```
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])]
  // 将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样的操作称之为shuffle。极限情况下，数据可能
  // 被分在同一个分区中，一个组的数据在一个分区中，但是并不是说一个分区中只有一个组
  ```
  7. filter
  ```
  def filter(f: T => Boolean): RDD[T]
  // 将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。当数据进行筛选过滤后，分区不变，但是分区内的数据
  // 可能不均衡，生产环境下，可能会出现数据倾斜。
  ```
   8. sample
   ```
   def sample( 
    withReplacement:  Boolean, 
    fraction: Double,
    seed: Long = Utils.random.nextLong): RDD[T])
   // 根据指定的规则从数据集中抽取数据
   ```
   9. distinct
   ```
   def distinct()(implicit ord: Ordering[T] = null): RDD[T]
   // 将数据集中重复的数据去重，实现原理
   ```
   10. coalesce
   ```
   def coalesce(
    numPartitions: Int, 
    shuffle: Boolean = false,
    partitionCoalescer: Option[PartitionCoalescer] = Option.empty) 
    (implicit ord: Ordering[T] = null)
  : RDD[T]
  // 根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率当 spark 程序中，存在过多的小任务的时候，
  // 可以通过 coalesce 方法，收缩合并分区，减少分区的个数，减小任务调度成本
  ```
  11. repatition
  ```
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T]
  // 该操作内部其实执行的是coalesce操作，参数 shuffle 的默认值为 true。无论是将分区数多的RDD转换为分区数少的RDD，
  // 还是将分区数少的RDD转换为分区数多的RDD，repartition 操作都可以完成，因为无论如何都会经 shuffle 过程
  ```
  12. sortBy
  ```
  def sortBy[K](
    f: (T) => K,
    ascending: Boolean = true,
    numPartitions: Int = this.partitions.length)
    (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
  // 该操作用于排序数据。在排序之前，可以将数据通过 f 函数进行处理，之后按照 f 函数处理的结果进行排序，默认为升序
  // 排列，排序后新产生的 RDD 的分区数与原 RDD 的分区数一致，中间存在 shuffle 的过程。第二个参数可以改变排序的方式。
  ```
 ### 双Value类型
 1. intersection
 ```
 def intersection(other: RDD[T]): RDD[T]
 // 对源 RDD 和参数 RDD 求交集后返回一个新的 RDD，要求两个数据源数据类型保持一致
 ```
 2. union
 ```
 def union(other: RDD[T]): RDD[T]
 // 对源 RDD 和参数 RDD 求并集后返回一个新的 RDD。要求两个数据源数据类型保持一致
 ```
 3. subtract
 ```
 def subtract(other: RDD[T]): RDD[T]
 // 以一个RDD元素为主，去除两个RDD中重复元素，将其他元素保留下来，求差集。要求两个数据源数据类型保持一致
 ```
 4. zip
 ```
 def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)]
 // 将两个 RDD 中的元素，以键值对的形式进行合并。其中，键值对中的Key为第 1个RDD中的元素，Value为第 2个RDD中的相同位置
 // 的元素。拉链操作两个数据源的类型可以不一致，两个数据源要求分区数量要保持一致，两个数据源要求分区中数据数量保持一致
 ```
 
 ### Key-Value类型
 1. partitionBy
 ```
 def partitionBy(partitioner: Partitioner): RDD[(K, V)]
 // 将数据按照指定 Partitioner 重新进行分区。Spark 默认的分区器是 HashPartitioner
 ```
 2. reduceByKey
 ```
 def reduceByKey(func: (V, V) => V): RDD[(K, V)]
 def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)]
 // 可以将数据按照相同的 Key 对 Value 进行聚合
 ```
 3. groupByKey
 ```
 def groupByKey(): RDD[(K, Iterable[V])]
 def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])]
 def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])]
 // 将数据源的数据根据 key 对 value 进行分组
 ```
 4. aggregateByKey
 ```
 def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
    combOp: (U, U) => U): RDD[(K, U)]
 // 将数据根据不同的规则进行分区内计算和分区间计算
 ```
 5. foldByKey
 ```
 def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
 // 当分区内计算规则和分区间计算规则相同时，aggregateByKey 就可以简化为 foldByKey
 ```
 6. combineByKey
 ```
 def combineByKey[C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C): RDD[(K, C)]
 // 最通用的对key-value型rdd进行聚集操作的聚集函数（aggregation function）。
 // 类似aggregate()，combineByKey()允许用户返回值的类型与输入不一致。
 // 第一个参数表示：将相同key的第一个数据进行结构的转换，实现操作
 // 第二个参数表示：分区内的计算规则
 // 第三个参数表示：分区间的计算规则
 ```
 7. sortByKey
 ```
 def sortByKey(ascending: Boolean = true, 
    numPartitions: Int = self.partitions.length): RDD[(K, V)]
 // 在一个(K,V)的 RDD 上调用，K 必须实现 Ordered 接口(特质)，返回一个按照 key 进行排序的RDD。
 ```
 8. join
 ```
 def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
 // 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素连接在一起的(K,(V,W))的 RDD
 ```
 9. leftOuterJoin
 ```
 def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))]
 // 类似于 SQL 语句的左外连接
 ```
 10. cogroup
 ```
 def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))]
 // 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的 RDD
 ```
## RDD行动算子
1. reduce
```
def reduce(f: (T, T) => T): T
// 聚集RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据
```
2. collect
```
def collect(): Array[T]
// 在驱动程序中，以数组Array 的形式返回数据集的所有元素
```
3. count
```
def count(): Long
// 返回RDD 中元素的个数
```
4. first
```
def first(): T
// 返回RDD 中的第一个元素
```
5. take
```
def take(num: Int): Array[T]
// 返回一个由RDD 的前 n 个元素组成的数组
```
6. takeOrdered
```
def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T]
// 返回该 RDD 排序后的前 n 个元素组成的数组
```
7. aggregate
```
def aggregate[U: ClassTag](
    zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
// 分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
```
8. fold
```
def fold(zeroValue: T)(op: (T, T) => T): T
// 折叠操作，aggregate 的简化版操作，分区内和分区间操作相同
```
9. countByKey
```
def countByKey(): Map[K, Long]
// 统计每种 key 的个数
```
10. save相关算子
 ```
 def saveAsTextFile(path: String): Unit 
 def saveAsObjectFile(path: String): Unit 
 def saveAsSequenceFile(
    path: String,
    codec: Option[Class[_ <: CompressionCodec]] = None): Unit
 // 将数据保存到不同格式的文件中
 ```
 11. foreach
 ```
 def foreach(f: T => Unit): Unit = withScope {
     val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
 }
 // 分布式遍历RDD 中的每一个元素，调用指定函数
 ```
