package com.myx.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description:
 * @author mayx
 * @date 2021/9/16 22:49
 */
object RDD_Memory {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("created RDD Parallelize")
    // 创建spark运行时环境对象
    val sc = new SparkContext(sparkConf)
    // 创建集合
    val seq: Seq[Int] = Seq[Int](1, 2, 3, 5, 7)
    // 从内存中创建RDD，将内存中集合的数据作为处理的数据源

    // 方法一：makeRDD
    // makeRDD方法在底层实现时其实就是调用了rdd对象的parallelize方法。
    // val rdd: RDD[Int] = sc.makeRDD(seq)

    // 方法二：parallelize
    val rdd: RDD[Int] = sc.parallelize(seq)
    rdd.collect().foreach(println)
    //关闭环境
    sc.stop()
  }

}
