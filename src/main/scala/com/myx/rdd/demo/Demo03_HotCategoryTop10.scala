package com.myx.rdd.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: Top10热门品类,
 *               分别统计每个品类点击的次数，下单的次数和支付的次数：
 *               （品类，点击总数）（品类，下单总数）（品类，支付总数）
 * @author: mayx
 * @date: 2021/10/8 14:54
 */
object Demo03_HotCategoryTop10 {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf()
                       .setMaster("local[*]")
                       .setAppName("HotCategoryTop10Analysis")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 1、读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("src/data/user_visit_action.txt")

    // 2. 将数据转换结构
    //    点击: ( 品类ID，( 1, 0, 0 ) )
    //    下单: ( 品类ID，( 0, 1, 0 ) )
    //    支付: ( 品类ID，( 0, 0, 1 ) )
    val flatMapRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          // 点击
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          // 下单
          val ids: Array[String] = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          // 支付
          val ids: Array[String] = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    // 3. 将相同的品类ID的数据进行分组聚合
    //    ( 品类ID，( 点击数量, 下单数量, 支付数量 ) )
    val reduceByKeyRDD: RDD[(String, (Int, Int, Int))] = flatMapRDD.reduceByKey(
      (t1, t2) => {
        ( t1._1+t2._1, t1._2 + t2._2, t1._3 + t2._3 )
      }
    )
    // 4. 将统计结果根据数量进行降序处理，取前10名
    val hotCategoryTop10: Array[(String, (Int, Int, Int))] =
          reduceByKeyRDD.sortBy(_._2, false).take(10)
    // 5、打印输出
    hotCategoryTop10.foreach(println)

    // 6、关闭资源
    sc.stop()
  }
}
