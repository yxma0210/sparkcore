package com.myx.rdd.accumulator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: 累加器用来把 Executor端变量信息聚合到 Driver端。在Driver程序中定义的变量，在
 *               Executor端的每个 Task 都会得到这个变量的一份新的副本，每个 task 更新这些副本的值后，
 *               传回 Driver 端进行 merge。
 * @author mayx
 * @date 2021/9/30 0:11
 */
object Acc01_ForeachSum {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 获取系统累加器
    // Spark默认就提供了简单数据聚合的累加器
    val sumAcc = sc.longAccumulator("sum")

   rdd.foreach(
      num => {
        // 使用累加器
        sumAcc.add(num)
      }
    )
    println(sumAcc.value)


    // 少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    // 多加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    // 一般情况下，累加器会放置在行动算子进行操作
    // 获取累加器的值
    println(sumAcc)
    sc.stop()
  }
}
