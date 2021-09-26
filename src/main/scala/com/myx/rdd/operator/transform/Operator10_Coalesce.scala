package com.myx.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: coalesce:根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
 *               当 spark 程序中，存在过多的小任务的时候，可以通过 coalesce 方法，收缩合并分区，减少
 *               分区的个数，减小任务调度成本
 * @author mayx
 * @date 2021/9/20 12:03
 */
object Operator10_Coalesce {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6),3)
    rdd.saveAsTextFile("src\\\\data")
    // coalesce方法默认情况下不会将分区的数据打乱重新组合
    // 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    // 如果想要让数据均衡，可以进行shuffle处理
    val coalesceFalse: RDD[Int] = rdd.coalesce(2)
    coalesceFalse.saveAsTextFile("coalesceFalse")
    // 将第二个参数设置为true，进行shuffle时数据均匀分配
    val coalesceTrue: RDD[Int] = rdd.coalesce(2, true)
    coalesceTrue.saveAsTextFile("coalesceTrue")
    sc.stop()
  }
}
