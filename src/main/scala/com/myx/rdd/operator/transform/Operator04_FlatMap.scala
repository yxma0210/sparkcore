package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: flatMap: 将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射
 * @author mayx
 * @date 2021/9/19 23:08
 */
object Operator04_FlatMap {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2, 5), List(6, 7)))
    // flatMap: 将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射
    val rddFlatMap: RDD[Int] = rdd.flatMap(
      list => {
        list
      }
    )

    rddFlatMap.collect().foreach(println)
    sc.stop()
  }

}
