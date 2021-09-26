package com.myx.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: takeOrdered: 返回该 RDD 排序后的前 n 个元素组成的数组
 * @author mayx
 * @date 2021/9/22 23:38
 */
object Operator05_TakeOrdered {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(4, 1, 3, 5,2))
    // 排序后取前3个元素
    println(rdd.takeOrdered(3).mkString(","))
    sc.stop()
  }
}
