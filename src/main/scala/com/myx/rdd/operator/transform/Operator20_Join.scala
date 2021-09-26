package com.myx.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: join:在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素连接在一起的
 *               (K,(V,W))的 RDD
 * @author mayx
 * @date 2021/9/22 1:23
 */
object Operator20_Join {
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

    // join : 两个不同数据源的数据，相同的key的value会连接在一起，形成元组
    //        如果两个数据源中key没有匹配上，那么数据不会出现在结果中
    //        如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔乘积，数据量会几何性增长，会导致性能降低。
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    joinRDD.collect().foreach(println)
    sc.stop()
  }
}
