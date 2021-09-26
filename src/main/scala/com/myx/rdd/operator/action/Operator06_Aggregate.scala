package com.myx.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: aggregate:分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
 * @author mayx
 * @date 2021/9/22 23:41
 */
object Operator06_Aggregate {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 2), ("a", 3)
    ),2)
    println(rdd.aggregate(0)((v, t) => v + t._2, (x, y) => x + y))
    sc.stop()
  }
}
