package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: 从服务器日志数据 apache.log 中获取 2015 年 5 月 17 日的请求路径
 * @author mayx
 * @date 2021/9/20 11:32
 */
object Operator07_FilterTest {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[String] = sc.textFile("src\\date\\apach.log")
    // 66.249.73.135 - - 20/05/2015:20:05:48 +0000 GET /blog/tags/smart
    val filterRDD: RDD[String] = rdd.filter(
      line => {
        val str: Array[String] = line.split(" ")
        val time: String = str(3)
        time.startsWith("17/05/2015")
      }
    )
    filterRDD.collect().foreach(println)
    sc.stop()
  }

}
