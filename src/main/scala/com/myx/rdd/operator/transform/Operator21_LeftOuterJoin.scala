package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: leftOutJoin:类似于 SQL 语句的左外连接
 * @author mayx
 * @date 2021/9/22 1:26
 */
object Operator21_LeftOuterJoin {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("c", 3)
    ))

    val rdd2 = sc.makeRDD(List(
      ("a", 5), ("c", 6),("a", 4)
    ))
    rdd1.leftOuterJoin(rdd2).collect().foreach(println)
  }
}
