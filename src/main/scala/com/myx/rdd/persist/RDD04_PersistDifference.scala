package com.myx.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * @Description: cache,persist 和 checkpoint的区别
 * @author mayx
 * @date 2021/9/29 0:38
 */
object RDD04_PersistDifference {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")
    // 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello World", "Hi World", "Hello Every", "Hi HaHa"))
    // 扁平化操作，将List集合展开
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    // 转换数据结构
    val mapRDD: RDD[(String, Int)] = flatRDD.map(
      word => {
        println("=" * 20)
        (word,1)
      }
    )

    // cache : 将数据临时存储在内存中进行数据重用
    //         会在血缘关系中添加新的依赖。一旦，出现问题，可以重头读取数据
    // persist : 将数据临时存储在磁盘文件中进行数据重用
    //           涉及到磁盘IO，性能较低，但是数据安全
    //           如果作业执行完毕，临时保存的数据文件就会丢失
    // checkpoint : 将数据长久地保存在磁盘文件中进行数据重用
    //           涉及到磁盘IO，性能较低，但是数据安全
    //           为了保证数据安全，所以一般情况下，会独立执行作业
    //           为了能够提高效率，一般情况下，是需要和cache联合使用
    //           执行过程中，会切断血缘关系。重新建立新的血缘关系
    //           checkpoint等同于改变数据源
    mapRDD.cache()
    mapRDD.checkpoint()
    // 根据相同的key聚合
    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceByKeyRDD.collect().foreach(println)
    println("*" * 40)
    val groupByRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy(_._1)
    groupByRDD.collect().foreach(println)
    // 关闭资源
    sc.stop()
  }
}
