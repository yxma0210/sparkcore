package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: filter:将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
 *               当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出
 *               现数据倾斜
 * @author mayx
 * @date 2021/9/20 11:25
 */
object Operator07_Filter {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    // 保留奇数
    val filterRDD: RDD[Int] = rdd.filter(
      num =>{ num %2 !=0}
    )
    filterRDD.collect().foreach(println)
    sc.stop()
  }
}
