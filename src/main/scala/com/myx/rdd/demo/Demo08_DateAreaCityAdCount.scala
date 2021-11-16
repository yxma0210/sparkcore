package com.myx.rdd.demo

import java.text.SimpleDateFormat

import com.myx.rdd.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @Description: 广告点击量实时统计
 *               实时统计每天各地区各城市各广告的点击总流量，并将其存入 MySQL。
 * @author: mayx
 * @date: 2021/11/16 16:21
 */
object Demo08_DateAreaCityAdCount {
  def main(args: Array[String]): Unit = {
    // 1.创建 SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BlackList")
    // 2.创建 StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop201:9092,hadoop202:9092,hadoop203:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "adverting",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // LocationStrategies：根据给定的主题和集群地址创建 consumer
    // LocationStrategies.PreferConsistent：持续的在所有 Executor 之间分配分区
    // ConsumerStrategies：选择如何在 Driver 和 Executor 上创建和配置 Kafka Consumer
    // ConsumerStrategies.Subscribe：订阅一系列主题
    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("adverting"), kafkaPara)
    )
   /* val dd: DStream[((String, String, String, String, String), Int)] = kafkaDataDS.map(
      kafkaData => {
        val str = kafkaData.value()
        val datas = str.split(" ")
        ((datas(0), datas(1), datas(2), datas(3), datas(4)), 1)
      }
    ).reduceByKey(_ + _)*/


    // 将sparkStreaming消费到的kafka数据进行拆分转换
    val adClickData: DStream[AdClickData] = kafkaDataDS.map(
      kafkaData => {
        // 获取kafka中的值
        val data: String = kafkaData.value()
        // 根据空格进行拆分
        val datas: Array[String] = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    val reduceDS: DStream[((String, String, String, String), Int)] = adClickData.map(
      data => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val day = sdf.format(new java.util.Date(data.ts.toLong))
        val area = data.area
        val city = data.city
        val ad = data.ad
        ((day, area, city, ad), 1)
      }
    ).reduceByKey(_ + _)

    reduceDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val sql =
              """
                | insert into area_city_ad_count ( dt, area, city, adid, count )
                | values ( ?, ?, ?, ?, ? )
                | on DUPLICATE KEY
                | UPDATE count = count + ?
                |""".stripMargin
            val conn = JDBCUtil.getConnection
            val pstat = conn.prepareStatement(sql)
            iter.foreach{
              case ((day,area,city,ad),sum) => {
                pstat.setString(1,day)
                pstat.setString(2,area)
                pstat.setString(3,city)
                pstat.setString(4,ad)
                pstat.setInt(5,sum)
                pstat.setInt(6,sum)
                pstat.executeUpdate()
              }
            }
            pstat.close()
            conn.close()
          }
        )
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
  // 广告点击数据
  case class AdClickData( ts:String, area:String, city:String, user:String, ad:String )
}
