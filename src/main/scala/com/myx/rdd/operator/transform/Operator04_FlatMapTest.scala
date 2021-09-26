package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: 将 List(List(1,2,5),9,List(6,7)) 进行扁平化操作
 * @author mayx
 * @date 2021/9/19 23:14
 */
object Operator04_FlatMapTest {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 将 List(List(1,2,5),9,List(6,7)) 进行扁平化操作
    // 创建RDD
    val rdd: RDD[Any] = sc.makeRDD(List(List(1, 2, 5), 3, List(6, 7)))
    val flatMapRDD: RDD[Any] = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case num => List(num)
        }
      }
    )
    println(flatMapRDD.collect().mkString(","))
    sc.stop()
  }
}
