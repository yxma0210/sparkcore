package com.myx.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: RDD算子只存储计算逻辑，不存储数据
 * @author mayx
 * @date 2021/9/29 0:04
 */
object RDD01_NoPersist {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello World", "Hi World", "Hello Every", "Hi HaHa"))
    // 扁平化操作，将List集合展开
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    // 转换数据结构
    val mapRDD: RDD[(String, Int)] = flatRDD.map(
      word => {
        println("=" * 20)
        (word,1)
      }
    )
    // 根据相同的key聚合
    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceByKeyRDD.collect().foreach(println)
    println("*" * 40)
    val groupByRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy(_._1)
    groupByRDD.collect().foreach(println)
    // 关闭资源
    sc.stop()
  }
}
