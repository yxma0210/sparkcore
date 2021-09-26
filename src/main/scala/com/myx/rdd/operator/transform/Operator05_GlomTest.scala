package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
 * @author mayx
 * @date 2021/9/19 23:52
 */
object Operator05_GlomTest {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 4, 5), 2)
    // 将同一个分区的数据直接转换为相同类型的内存数组
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    // 取出每个数组的最大值
    val maxArray: RDD[Int] = glomRDD.map(
      array => {
        array.max
      }
    )
    // 求每个分区最大值的和
    val sum: Int = maxArray.collect().sum
    println(sum)
    sc.stop()
  }

}
