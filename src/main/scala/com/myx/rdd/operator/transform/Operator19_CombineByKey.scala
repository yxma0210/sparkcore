package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: combineByKey:最通用的对 key-value 型 rdd 进行聚集操作的聚集函数（aggregation function）。类似于
 *               aggregate()，combineByKey()允许用户返回值的类型与输入不一致
 * @author mayx
 * @date 2021/9/22 0:38
 */
object Operator19_CombineByKey {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 2), ("a", 3)
    ),2)
    // combineByKey : 方法需要三个参数
    // 第一个参数表示：将相同key的第一个数据进行结构的转换，实现操作
    // 第二个参数表示：分区内的计算规则
    // 第三个参数表示：分区间的计算规则
    // 获取相同key的数据的平均值
    val combineByKeyRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (x: (Int, Int), y: (Int, Int)) => {
        (x._1 + y._1, x._2 + y._2)
      }
    )
    val averageRDD: RDD[(String, Int)] = combineByKeyRDD.mapValues {
      case (num, cnt) => num / cnt
    }
    averageRDD.collect().foreach(println)
    sc.stop()
  }
}
