package com.myx.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description:
 * @author mayx
 * @date 2021/9/16 23:14
 */
object RDD_File {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("created RDD Parallelize")
    // 创建spark运行时环境对象
    val sc = new SparkContext(sparkConf)

    // 外部存储创建RDD
    val rdd: RDD[String] = sc.textFile("src\\date\\word")
    rdd.collect().foreach(print)
    //关闭环境
    sc.stop()
  }

}
