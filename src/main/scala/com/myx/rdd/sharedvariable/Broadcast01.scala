package com.myx.rdd.sharedvariable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object Broadcast01 {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 5)))
    val map = mutable.Map(("a",2),("b",1),("c",3))
    // 封装广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    // (a,(1,2))
    rdd.map{
      case (w, c) => {
        val l: Int = bc.value.getOrElse(w, 0)
        (w, (c, l))
      }
    }.collect().foreach(println)
    sc.stop()
  }
}
