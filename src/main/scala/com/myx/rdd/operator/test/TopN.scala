package com.myx.rdd.operator.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
 *              统计出每一个省份每个广告被点击数量排行的 Top3
 * @author mayx
 * @date 2021/9/23 0:14
 */
object TopN {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[String] = sc.textFile("src\\data\\agent.log")
    // 时间戳 省份 城市 用户 广告 =>(省份,广告) 1
    val provinceAdvert: RDD[((String, String), Int)] = rdd.map(
      line => {
        val str: Array[String] = line.split(" ")
        val province: String = str(1)
        val advert: String = str(4)
        ((province, advert), 1)
      }
    )
    // ((省份,广告),1) => ((省份,广告) num)
    val provinceAdvertNum: RDD[((String, String), Int)] = provinceAdvert.reduceByKey(_ + _)
    // ((省份,广告),num) => (省份,(广告,num))
    val mapProvinceAdvert: RDD[(String, (String, Int))] = provinceAdvertNum.map {
      case ((province, advert), num) => {
        (province, (advert, num))
      }
    }
    val groupByKeyRDD: RDD[(String, Iterable[(String, Int)])] = mapProvinceAdvert.groupByKey()
    // 取前3名
    val top3RDD: RDD[(String, List[(String, Int)])] = groupByKeyRDD.mapValues(
      iter => iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    )
    top3RDD.collect().foreach(println)
    sc.stop()
  }
}
