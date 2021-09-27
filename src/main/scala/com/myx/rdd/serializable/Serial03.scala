package com.myx.rdd.serializable

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: 函数与属性序列化
 * @author mayx
 * @date 2021/9/28 0:22
 */
object Serial03 {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[String] = sc.makeRDD(Array("Hello World","Hello Spark","Hi Every"))

    val search =  Search("E")
    search.getMatch1(rdd).collect().foreach(println)
    println("=" * 20)
    search.getMatch2(rdd).collect().foreach(println)
    // 关闭资源
    sc.stop()
  }
  // 查询对象
  // 类的构造参数其实是类的属性, 构造参数需要进行闭包检测，其实就等同于类进行闭包检测
  case class Search(query:String){
    def isMatch(s: String): Boolean = {
      s.contains(this.query)
    }

    // 函数序列化案例
    def getMatch1 (rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      val s = query // 将类的属性赋值给s，s不需要序列化就可以在网络中传递
      rdd.filter(x => x.contains(s))
    }
  }
}

