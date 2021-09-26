package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: sort:该操作用于排序数据。在排序之前，可以将数据通过 f 函数进行处理，之后按照 f 函数处理
 *               的结果进行排序，默认为升序排列。排序后新产生的 RDD 的分区数与原 RDD 的分区数一
 *               致。中间存在 shuffle 的过程
 * @author mayx
 * @date 2021/9/20 12:23
 */
object Operator12_SortBy {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(5, 8, 3, 2, 1, 9))
    // sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式
    // sortBy默认情况下，不会改变分区。但是中间存在shuffle操作
    val sortRDD: RDD[Int] = rdd.sortBy(num => num)
    sortRDD.collect().foreach(println)
  }
}
