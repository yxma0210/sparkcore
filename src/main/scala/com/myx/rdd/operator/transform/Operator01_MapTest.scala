package com.myx.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: 从服务器日志数据 apache.log 中获取用户请求 URL 资源路径
 * @author mayx
 * @date 2021/9/17 0:33
 */
object Operator01_MapTest {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    // 创建spark运行时环境对象
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[String] = sc.textFile("src\\data\\apach.log")
    // 从服务器日志数据 apache.log 中获取用户请求 URL 资源路径
    // 66.249.73.135 - - 20/05/2015:20:05:48 +0000 GET /blog/tags/smart
    val strRdd: RDD[String] = rdd.map(
      (line: String) => {
        val str: Array[String] = line.split(" ")
        str(6)
      }
    )
    strRdd.collect().foreach(println)
    // 关闭环境
    sc.stop()
  }
}
