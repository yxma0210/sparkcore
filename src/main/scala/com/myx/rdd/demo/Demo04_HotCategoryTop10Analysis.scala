package com.myx.rdd.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * @Description: Top10热门品类,
 *               分别统计每个品类点击的次数，下单的次数和支付的次数：
 *               （品类，点击总数）（品类，下单总数）（品类，支付总数）
 * @author: mayx
 * @date: 2021/10/8 17:32
 */
object Demo04_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("HotCategoryTop10Analysis")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 1、读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("src/data/user_visit_action.txt")

  }

  case class HotCategory(cid:String,var clickCount:Int,var orderCount:Int,var payCount:Int)
  /**
   * 自定义累加器
   * 1. 继承AccumulatorV2，定义泛型
   *    IN : ( 品类ID, 行为类型 )
   *    OUT : mutable.Map[String, HotCategory]
   * 2. 重写方法（6）
   */
  class HotCategoryAccumulator extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]]{
    // 创建map集合，用于接收点击流数据
    private val hcMap = mutable.Map[String,HotCategory]()

    override def isZero: Boolean = ???

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = ???

    override def reset(): Unit = ???

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actionType = v._2
      val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if(actionType == "click"){
        category.clickCount += 1
      } else if(actionType == "order") {
        category.orderCount += 1
      } else if(actionType == "pay") {
        category.payCount += 1
      }
      hcMap.update(cid,category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {

    }

    override def value: mutable.Map[String, HotCategory] = ???
  }
}
