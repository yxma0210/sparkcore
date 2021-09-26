package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: sample:根据指定的规则从数据集中抽取数据,sample算子需要传递三个参数
 *               1. 第一个参数表示，抽取数据后是否将数据返回 true（放回），false（丢弃）
 *               2. 第二个参数表示，
 *                       如果抽取不放回的场合：数据源中每条数据被抽取的概率
 *                       如果抽取放回的场合：表示数据源中的每条数据被抽取的可能次数
 *               3. 第三个参数表示，抽取数据时随机算法的种子
 *                               如果不传递第三个参数，那么使用的是当前系统时间
 * @author mayx
 * @date 2021/9/20 11:40
 */
object Operator08_Sample {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    // 抽取数据不放回（伯努利算法）
    // 伯努利算法：又叫 0、1 分布。例如扔硬币，要么正面，要么反面。
    // 具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
    // 第一个参数：抽取的数据是否放回，false：不放回
    // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
    // 第三个参数：随机数种子
    val sampleFalseRDD: RDD[Int] = rdd.sample(
      false,
      0.4,
      1
    )
    // 抽取数据放回（泊松算法）
    // 第一个参数：抽取的数据是否放回，true：放回；false：不放回
    // 第二个参数：重复数据的几率，范围大于等于 0.表示每一个元素被期望抽取到的次数
    // 第三个参数：随机数种子
    val sampleTrueRDD: RDD[Int] = rdd.sample(
      true,
      3,
      2
    )
    println(sampleFalseRDD.collect().mkString(","))
    println("=======================")
    println(sampleTrueRDD.collect().mkString(","))
    sc.stop()
  }

}
