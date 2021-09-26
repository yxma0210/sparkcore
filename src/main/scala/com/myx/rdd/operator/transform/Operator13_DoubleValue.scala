package com.myx.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: intersection-交集  union-并集  subtract-差集  zip-拉链
 * @author mayx
 * @date 2021/9/20 15:51
 */
object Operator13_DoubleValue {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 3, 5, 7, 9))
    val rdd2: RDD[Int] = sc.makeRDD(List(2, 4, 6, 8, 9))
    val rdd3: RDD[String] = sc.makeRDD(List("a", "b", "c", "d","e"))
    // 交集，并集和差集要求两个数据源数据类型保持一致
    // 交集 intersection
    val intersectionRDD: RDD[Int] = rdd1.intersection(rdd2)
    println(intersectionRDD.collect().mkString(","))
    // 并集 union
    val unionRDD: RDD[Int] = rdd1.union(rdd2)
    println(unionRDD.collect().mkString(","))
    // 差集 subtract
    val subtractRDD: RDD[Int] = rdd1.subtract(rdd2)
    println(subtractRDD.collect().mkString(","))
    // 拉链操作两个数据源的类型可以不一致
    // Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    // 两个数据源要求分区数量要保持一致
    // Can only zip RDDs with same number of elements in each partition
    // 两个数据源要求分区中数据数量保持一致
    val zipRDD: RDD[(Int, String)] = rdd1.zip(rdd3)
    println(zipRDD.collect().mkString(","))
    sc.stop()
  }
}
