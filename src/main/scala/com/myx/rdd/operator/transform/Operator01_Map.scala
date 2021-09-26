package com.myx.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: map: 将处理的数据逐条进行映射转换，这里的转换可以是类型的转换，也可以是值的转换
 * @author mayx
 * @date 2021/9/16 23:28
 */
object Operator01_Map {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    // 创建spark运行时环境对象
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 4, 5))
    //转换函数
    def mapFunc(num:Int): Int ={
      num * 2
    }
    // val mapRDD: RDD[Int] = rdd.map(mapFunc)
    // 匿名函数
    // val mapRDD: RDD[Int] = rdd.map((num: Int) => { num * 2 })
    /* 简写
      1、return可以省略，scala会使用函数体的最后一行代码作为返回值，函数体只有一行，大括号可以省略
      val mapRDD: RDD[Int] = rdd.map((num: Int) => num * 2 )
      2、参数的类型可以省略，会根据形参进行自动推到
      val mapRDD: RDD[Int] = rdd.map((num) => num * 2 )
      3、类型省略之后，发现只有一个参数，则圆括号可以省略；其他情况：没有参数和参数超过1的永远不能省略圆括号
      val mapRDD: RDD[Int] = rdd.map(num => num * 2 )
      4、如果参数只出现一次，则参数省略且后面参数可以用_代替
      val mapRDD: RDD[Int] = rdd.map(_ * 2)
     */
    val mapRDD: RDD[Int] = rdd.map(_ * 2)
    mapRDD.collect().foreach(println)
    // 关闭环境
    sc.stop()
  }


}
