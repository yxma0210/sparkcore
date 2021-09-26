package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: 测试分区中数据的执行顺序
 * @author mayx
 * @date 2021/9/17 0:52
 */
object Operator01_Map_Parallelize {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    // 创建spark运行时环境对象
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    // map:将处理的数据逐条进行映射转换，这里的转换可以是类型的转换，也可以是值的转换。
    // 1. rdd的计算一个分区内的数据是一个一个执行逻辑
    //    只有前面一个数据全部的逻辑执行完毕后，才会执行下一个数据。分区内数据的执行是有序的。
    // 2. 不同分区数据计算是无序的。
    val mapRDD = rdd.map(
      num => {
        println(">>>>>>>> " + num)
        num
      }
    )
    val mapRDD1 = mapRDD.map(
      num => {
        println("######" + num)
        num
      }
    )
    mapRDD1.collect()
    sc.stop()
  }

}
