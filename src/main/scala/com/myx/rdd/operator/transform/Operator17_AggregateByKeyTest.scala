package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description:
 * @author mayx
 * @date 2021/9/22 0:43
 */
object Operator17_AggregateByKeyTest {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 2), ("a", 3)
    ), 2)
    // 获取相同key的数据的平均值 => (a, 2),(b, 3)
    val aggregateByKeyRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    val average: RDD[(String, Int)] = aggregateByKeyRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    average.collect().foreach(println)
    sc.stop()
  }
}
