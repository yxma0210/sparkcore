package com.myx.rdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: Top10热门品类,
 *               分别统计每个品类点击的次数，下单的次数和支付的次数：
 *               （品类，点击总数）（品类，下单总数）（品类，支付总数）
 * @author: mayx
 * @date: 2021/9/30 16:36
 */
object Demo01_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 1、读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("src/data/user_visit_action.txt")

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

    // 5. 将品类进行排序，并且取前10名
    //    点击数量排序，下单数量排序，支付数量排序
    //    元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
    //    ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
          clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    // 将Iterable[Int] 转为Int
    val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clinkCount = 0
        val click = clickIter.iterator
        while (click.hasNext) {
          clinkCount = click.next()
        }

        var orderCount = 0
        val order = orderIter.iterator
        while (order.hasNext) {
          orderCount = order.next()
        }

        var payCount = 0
        val pay = payIter.iterator
        while (pay.hasNext) {
          payCount = pay.next()
        }
        (clinkCount, orderCount, payCount)
      }
    }
    val hotCategorytop10: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
    hotCategorytop10.foreach(println)
    sc.stop()
  }
}
