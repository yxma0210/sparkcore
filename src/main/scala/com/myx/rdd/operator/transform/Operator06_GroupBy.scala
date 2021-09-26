package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: groupBy: 将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样
 *               的操作称之为 shuffle。极限情况下，数据可能被分在同一个分区中
 *               一个组的数据在一个分区中，但是并不是说一个分区中只有一个组
 * @author mayx
 * @date 2021/9/19 23:59
 */
object Operator06_GroupBy {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 4, 5), 2)
    // 按照奇偶数分组
    val groupByRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)
    println(groupByRDD.collect().mkString(","))
    sc.stop()
  }

}
