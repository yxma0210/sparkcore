package com.myx.rdd.dependencies

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: 打印RDD的血缘
 * @author mayx
 * @date 2021/9/28 23:38
 */
object RDD01_Lineage {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List(("Hello World"), ("Hi World"), ("Hello Every"), ("Hi HaHa")))
    println("*" * 20)
    println(rdd.toDebugString)
    // 扁平化操作，将List集合展开
    val flatRDD: RDD[String] = rdd.flatMap(
      str => str.split(" ")
    )
    println("*" * 20)
    println(flatRDD.toDebugString)
    // 转换数据结构
    val mapRDD: RDD[(String, Int)] = flatRDD.map(
      word => (word, 1)
    )
    println("*" * 20)
    println(mapRDD.toDebugString)
    // 根据相同的key聚合
    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(
      (x, y) => {
        x + y
      }
    )
    println("*" * 20)
    println(resultRDD.toDebugString)

    resultRDD.collect().foreach(println)

    // 关闭资源
    sc.stop()
  }
}
