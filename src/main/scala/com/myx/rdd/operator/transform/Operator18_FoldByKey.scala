package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: foldByKey: 当分区内计算规则和分区间计算规则相同时，aggregateByKey 就可以简化为 foldByKey
 * @author mayx
 * @date 2021/9/22 0:35
 */
object Operator18_FoldByKey {
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

    // 当分区内计算规则和分区间计算规则相同时，aggregateByKey 就可以简化为 foldByKey
    val foldByKeyRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
    foldByKeyRDD.collect().foreach(println)
    sc.stop()
  }
}
