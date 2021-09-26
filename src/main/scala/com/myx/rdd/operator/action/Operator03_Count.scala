package com.myx.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: count:返回 RDD 中元素的个数
 * @author mayx
 * @date 2021/9/22 23:32
 */
object Operator03_Count {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // 返回RDD中元素的个数
    println(rdd.count())
    sc.stop()
  }
}
