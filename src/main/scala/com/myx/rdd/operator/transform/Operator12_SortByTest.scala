package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: List(("LiBai",1),("WuKong",2),("BaiLi"),3) 按照字母降序排序
 * @author mayx
 * @date 2021/9/20 12:29
 */
object Operator12_SortByTest {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("LiBai", 1), ("WuKong", 2), ("BaiLi", 3)), 2)
    // sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式
    // sortBy默认情况下，不会改变分区。但是中间存在shuffle操作
    val sortRDD: RDD[(String, Int)] = rdd.sortBy(t => t._1, false)
    println(sortRDD.collect().mkString(","))
    sc.stop()
  }

}
