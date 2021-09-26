package com.myx.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: mapPartitionsWithIndex
 *              将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处
 *              理，哪怕是过滤数据，在处理时同时可以获取当前分区索引。
 * @author mayx
 * @date 2021/9/19 22:33
 */
object Operator03_MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mapPartitionsWithIndex")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 4, 5), 2)
    // 获取当前分区并输出数据
    val rddWithIndex: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(
          num => {
            (index, num)
          }
        )
      }
    )
    rddWithIndex.collect().foreach(println)
    sc.stop()
  }

}
