package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: repartition:该操作内部其实执行的是 coalesce 操作，参数 shuffle 的默认值为 true。无论是将分区数多的
 *               RDD 转换为分区数少的 RDD，还是将分区数少的 RDD 转换为分区数多的 RDD，repartition
 *               操作都可以完成，因为无论如何都会经 shuffle 过程。
 * @author mayx
 * @date 2021/9/20 12:17
 */
object Operator11_Repartition {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    // 增加分区
    // coalesce算子可以扩大分区的，但是如果不进行shuffle操作，是没有意义，不起作用。
    // 所以如果想要实现扩大分区的效果，需要使用shuffle操作
    // spark提供了一个简化的操作
    // 缩减分区：coalesce，如果想要数据均衡，可以采用shuffle
    // 扩大分区：repartition, 底层代码调用的就是coalesce，而且肯定采用shuffle
    val repartitionRDD: RDD[Int] = rdd.repartition(4)
    repartitionRDD.saveAsTextFile("repartition")
    sc.stop()
  }
}
