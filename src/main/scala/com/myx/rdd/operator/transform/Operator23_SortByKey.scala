package com.myx.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Operator23_SortByKey {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建rdd
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 3), ("b", 2), ("c", 5), ("b", 1)))
    rdd.sortByKey(false).collect().foreach(println)
    println("--------------")
    rdd.sortByKey(true).collect().foreach(println)
    sc.stop()
  }
}
