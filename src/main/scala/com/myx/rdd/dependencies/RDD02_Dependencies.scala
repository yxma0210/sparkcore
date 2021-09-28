package com.myx.rdd.dependencies

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: 窄依赖与宽依赖
 * @author mayx
 * @date 2021/9/28 23:55
 */
object RDD02_Dependencies {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello World", "Hi World", "Hello Every", "Hi HaHa"))

    println("*" * 20)
    println(rdd.toDebugString)
    // 扁平化操作，将List集合展开
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    println("*" * 20)
    println(flatRDD.dependencies)
    // 转换数据结构
    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
    println("*" * 20)
    println(mapRDD.dependencies)
    // 根据相同的key聚合
    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    println("*" * 20)
    println(resultRDD.dependencies)

    resultRDD.collect().foreach(println)

    // 关闭资源
    sc.stop()
  }
}
