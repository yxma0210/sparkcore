package com.myx.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: mapPartitions : 可以以分区为单位进行数据转换操作
 * @author mayx
 * @date 2021/9/18 10:30
 */
object Operator02_MapPartitions {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddPartitions")
    // 创建spark运行时环境对象
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 4, 5, 2), 2)
    // mapPartitions : 可以以分区为单位进行数据转换操作
    //                 但是会将整个分区的数据加载到内存进行引用
    //                 如果处理完的数据是不会被释放掉，存在对象的引用。
    //                 在内存较小，数据量较大的场合下，容易出现内存溢出。
    val mapPartitionsRDD: RDD[Int] = rdd.mapPartitions(
      iter => {
        println("=======================")
        iter.map(_ * 2)
      }
    )
    mapPartitionsRDD.collect().foreach(println)
    sc.stop()
  }
}
