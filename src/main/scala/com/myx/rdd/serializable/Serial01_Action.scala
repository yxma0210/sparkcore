package com.myx.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: 算子以外的代码都是在 Driver 端执行, 算子里面的代码都是在 Executor端执行
 * @author mayx
 * @date 2021/9/27 23:57
 */
object Serial01_Action {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // foreach 其实是Driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)
    println("* "*20)
    // foreach 其实是Executor端内存数据打印
    rdd.foreach(println)
    // 算子：Operator（操作）
    //      RDD的方法和Scala集合对象的方法不一样，集合对象的方法都是在同一个节点的内存中完成的。
    //      RDD的方法可以将计算逻辑发送到Executor端（分布式节点）执行
    //      为了区分不同的处理效果，所以将RDD的方法称之为算子。
    //      RDD的方法外部的操作都是在Driver端执行的，而方法内部的逻辑代码是在Executor端执行。
    // 关闭资源
    sc.stop()
  }
}
