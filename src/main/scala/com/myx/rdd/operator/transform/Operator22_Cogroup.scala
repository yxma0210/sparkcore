package com.myx.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: cogroup:在类型为(K,V)和(K,W)的 RDD 上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的 RDD
 * @author mayx
 * @date 2021/9/22 1:28
 */
object Operator22_Cogroup {
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
    // cogroup : connect + group (分组，连接)
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    cogroupRDD.collect().foreach(println)
    sc.stop()
  }
}
