package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: reduceByKey:可以将数据按照相同的 Key对Value进行聚合
 * @author mayx
 * @date 2021/9/22 0:11
 */
object Operator15_ReduceByKey {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 3), ("a", 2), ("b", 3)))
    // reduceByKey : 相同的key的数据进行value数据的聚合操作
    // scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
    // 例如 a：[1，3，2]
    //        [4，2]
    //        [6]
    // reduceByKey中如果key的数据只有一个，是不会参与运算的。
    val reduceByKeyRDD: RDD[(String, Int)] = rdd.reduceByKey(
      (x, y) => {
        println(s"x = ${x}, y = ${y}")
        x + y
      }
    )
    reduceByKeyRDD.collect().foreach(println)
    sc.stop()
  }
}
