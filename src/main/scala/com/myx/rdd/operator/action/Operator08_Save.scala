package com.myx.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description:
 * @author mayx
 * @date 2021/9/23 0:01
 */
object Operator08_Save {
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
    // 保存成 Text 文件
    rdd.saveAsTextFile("text")
    // 序列化成对象保存到文件
    rdd.saveAsObjectFile("object")
    // 保存成 Sequencefile 文件,saveAsSequenceFile方法要求数据的格式必须为K-V类型
    rdd.saveAsSequenceFile("sequence")
    sc.stop()
  }
}
