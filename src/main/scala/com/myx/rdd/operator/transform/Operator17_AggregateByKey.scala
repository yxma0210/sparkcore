package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: aggregateByKey:将数据根据不同的规则进行分区内计算和分区间计算
 * @author mayx
 * @date 2021/9/22 0:25
 */
object Operator17_AggregateByKey {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 2), ("a", 3)
    ),2)
    // aggregateByKey存在函数柯里化，有两个参数列表
    // 第一个参数列表,需要传递一个参数，表示为初始值
    //       主要用于当碰见第一个key的时候，和value进行分区内计算
    // 第二个参数列表需要传递2个参数
    //      第一个参数表示分区内计算规则
    //      第二个参数表示分区间计算规则
    // 取出每个分区内相同 key 的最大值然后分区间相加
    rdd.aggregateByKey(0)(
      (x,y) => math.max(x,y),
      (x,y) => x + y
    ).collect().foreach(println)
    sc.stop()
  }

}
