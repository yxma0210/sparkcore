package com.myx.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @Description: glom:将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
 * @author mayx
 * @date 2021/9/19 23:14
 */
object Operator05_Glom {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 4, 5), 2)
    // 将同一个分区的数据直接转换为相同类型的内存数组
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    // 遍历打印每个数组
    glomRDD.collect().foreach(data=> println(data.mkString(",")))
    sc.stop()
  }
}
