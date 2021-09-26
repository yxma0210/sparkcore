package com.myx.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: partitionBy: 将数据按照指定 Partitioner 重新进行分区。Spark 默认的分区器是 HashPartitioner
 * @author mayx
 * @date 2021/9/20 16:47
 */
object Operator14_PartitionBy {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 4, 5), 2)
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
    val partitionRDD: RDD[(Int, Int)] = mapRDD.partitionBy(new HashPartitioner(2))
    partitionRDD.saveAsTextFile("partitionBy")
  }
}
