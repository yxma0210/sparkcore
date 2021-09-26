package com.myx.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: groupByKey:将数据源的数据根据 key 对 value 进行分组
 * @author mayx
 * @date 2021/9/22 0:20
 */
object Operator16_GroupByKey {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 3), ("a", 2), ("b", 3)))
    // groupByKey : 将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
    //              元组中的第一个元素就是key，
    //              元组中的第二个元素就是相同key的value的集合
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey(new HashPartitioner(2))
    groupByKeyRDD.collect().foreach(println)
    sc.stop()
  }
}
