package com.myx.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: collect:在驱动程序中，以数组 Array 的形式返回数据集的所有元素
 * @author mayx
 * @date 2021/9/22 23:29
 */
object Operator02_Collect {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // 所谓的行动算子，其实就是触发作业(Job)执行的方法
    // 底层代码调用的是环境对象的runJob方法
    // 底层代码中会创建ActiveJob，并提交执行。
    rdd.collect().foreach(println)
    sc.stop()
  }
}
