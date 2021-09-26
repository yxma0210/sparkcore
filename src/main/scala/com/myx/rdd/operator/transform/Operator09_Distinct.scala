package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: distinct: 将数据集中重复的数据去重
 * @author mayx
 * @date 2021/9/20 11:52
 */
object Operator09_Distinct {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 2, 3, 4, 4, 1))
    // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    // 将重复元素去重
    val distinctRDD: RDD[Int] = rdd.distinct()
    println(distinctRDD.collect().mkString(","))
    sc.stop()
  }
}
