package com.myx.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: 获取每个分区的最大值
 * @author mayx
 * @date 2021/9/18 10:44
 */
object Operator02_MapPartitionsTest {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddPartitions")
    // 创建spark运行时环境对象
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(2, 5, 7, 36,6,8,12,56,15), 2)
    // 获取每个分区的最大值
    val maxRdd: RDD[Int] = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
    maxRdd.collect().foreach(println)
    sc.stop()
  }
}
