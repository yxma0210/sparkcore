package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: 按照首字母分组
 * @author mayx
 * @date 2021/9/20 0:09
 */
object Operator06_GroupByTest01 {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello", "hi", "scala", "spark", "sorry"), 2)
    val groupByRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(
      str => {
        str.charAt(0)
      }
    )
    groupByRDD.collect().foreach(println)
    sc.stop()
  }

}
