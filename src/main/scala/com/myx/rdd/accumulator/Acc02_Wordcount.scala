package com.myx.rdd.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Acc02_Wordcount {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Spark", "Hello", "Scala"))
    // 创建累加器对象
    val wcArr = new MyAccumulator()
    sc.register(wcArr,"wordcountAcc")
    rdd.foreach(
      word => {
        // 使用累加器进行数据的累加
        wcArr.add(word)
      }
    )
    println(wcArr.value)
    sc.stop()
  }

  //
  class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Long]]{
    // 创建map集合，用于接收word
    private var wcMap = mutable.Map[String,Long]()

    // 判断是否初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    // 累加word
    override def add(v: String): Unit = {
      val count = wcMap.getOrElse(v,0L) + 1
      wcMap.update(v,count)
    }

    // 在Driver端合并Executor间的累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach{
        case (word,count) =>{
          val newCount = map1.getOrElse(word,0L) + count
          map1.update(word,newCount)
        }
      }
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }

}
