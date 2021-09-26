package com.myx.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: 从服务器日志数据 apache.log 中获取每个时间段访问量。
 * @author mayx
 * @date 2021/9/20 0:27
 */
object Operator06_GroupByTest02 {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[String] = sc.textFile("src\\data\\apach.log")
    // 66.249.73.135 - - 20/05/2015:20:05:48 +0000 GET /blog/tags/smart
    val mapRDD: RDD[(String, Int)] = rdd.map(
      line => {
        val str: Array[String] = line.split(" ")
        val time = str(3)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date: Date = sdf.parse(time)
        val formatHH = new SimpleDateFormat("HH")
        val hour: String = formatHH.format(date)
        (hour, 1)
      }
    )
    // 使用reduceBykey计算每个时间段访问量
    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey(
      (x, y) => {
        x + y
      }
    )
    reduceByKeyRDD.collect().foreach(println)

    println("==================================")
    // 使用groupBy计算每个时间段访问量
    val groupByRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy(_._1)
    val count: RDD[(String, Int)] = groupByRDD.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }
    count.collect().foreach(println)
    sc.stop()
  }
}
