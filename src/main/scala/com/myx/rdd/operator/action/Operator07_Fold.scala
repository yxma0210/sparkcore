package com.myx.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: fold:折叠操作，aggregate 的简化版操作
 * @author mayx
 * @date 2021/9/22 23:49
 */
object Operator07_Fold {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(4, 1, 3, 5,2))
    println(rdd.fold(0)(_ + _))
    sc.stop()
  }
}
