package com.myx.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: reduce:聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据
 * @author mayx
 * @date 2021/9/22 23:19
 */
object Operator01_Reduce {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val reduceRDD: Int = rdd.reduce(
      (x, y) => {
        x + y
      }
    )
    println(reduceRDD)
    sc.stop()
  }
}
