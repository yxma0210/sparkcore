package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: 获取第二个数据分区的数据
 * @author mayx
 * @date 2021/9/19 22:49
 */
object Operator03_MapPartitionsWithIndexTest {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mapPartitionsWithIndexTest")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 4, 5), 2)
    // 练习：获取第二个数据分区的数据
    val rddWithIndex: RDD[Int] = rdd.mapPartitionsWithIndex(
      (index, datas) => {
        if (index == 1) {
          datas
        } else {
          Nil.iterator
        }
      }
    )
    rddWithIndex.collect().foreach(println)
    sc.stop()
  }
}
