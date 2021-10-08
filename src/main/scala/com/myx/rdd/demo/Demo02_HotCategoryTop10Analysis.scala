package com.myx.rdd.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: Top10热门品类,
 *               分别统计每个品类点击的次数，下单的次数和支付的次数：
 *               （品类，点击总数）（品类，下单总数）（品类，支付总数）
 * @author: mayx
 * @date: 2021/10/8 14:21
 */
object Demo02_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 1、读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("src/data/user_visit_action.txt")

    actionRDD.cache()

    // 2、统计品类点击数量（品类ID，点击数量）
    // 过滤出点击数据
    val clickActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(6) != "-1"
      }
    )
    // 统计品类点击数据
    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map {
      click => {
        val datas: Array[String] = click.split("_")
        (datas(6), 1)
      }
    }.reduceByKey(_ + _)

    // 3、统计品类下单数量（品类ID，下单数量）
    val orderActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(8) != "null"
      }
    )
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
      order => {
        val datas: Array[String] = order.split("_")
        val cid = datas(8)
        val cids: Array[String] = cid.split(",")
        cids.map((_,1))
      }
    ).reduceByKey(_ + _)

    // 4、统计品类支付数量（品类ID，支付数量）
    val payActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(10) != "null"
      }
    )
    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(
      pay => {
        val datas: Array[String] = pay.split("_")
        val cid: String = datas(10)
        val cids: Array[String] = cid.split(",")
        cids.map((_, 1))
      }
    ).reduceByKey(_ + _)

    // 6、转换
    // (品类ID, 点击数量) => (品类ID, (点击数量, 0, 0))
    val newClickCountRDD: RDD[(String, (Int, Int, Int))] = clickCountRDD.map {
      case (cid, count) => {
        (cid, (count, 0, 0))
      }
    }

    // (品类ID, 下单数量) => (品类ID, (0, 下单数量, 0))
    val newOrderCountRDD: RDD[(String, (Int, Int, Int))] = orderCountRDD.map {
      case (cid, count) => {
        (cid, (0, count, 0))
      }
    }
    // (品类ID, 支付数量) => (品类ID, (0, 0, 支付数量))
    val newPayCountRDD: RDD[(String, (Int, Int, Int))] = payCountRDD.map {
      case (cid, count) => {
        (cid, (0, 0, count))
      }
    }

    // 7、将三个数据源合并在一起，统一进行聚合计算
    val unionRDD: RDD[(String, (Int, Int, Int))] =
        newClickCountRDD.union(newOrderCountRDD).union(newPayCountRDD)

    val reduceByKeyRDD: RDD[(String, (Int, Int, Int))] = unionRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    // 8、排序，取前10
    val hotCategoryTop10: Array[(String, (Int, Int, Int))] =
        reduceByKeyRDD.sortBy(_._2, false).take(10)

    // 9、打印输出到控制台
    hotCategoryTop10.foreach(println)

    // 10、关闭资源
    sc.stop()
  }
}
