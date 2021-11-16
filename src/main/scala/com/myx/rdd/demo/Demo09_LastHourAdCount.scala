package com.myx.rdd.demo

import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @Description:
 * @author: mayx
 * @date: 2021/11/16 17:52
 */
object Demo09_LastHourAdCount {
  def main(args: Array[String]): Unit = {
    // 1.创建 SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BlackList")
    // 2.创建 StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

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

    val adClickData: DStream[AdClickData] = kafkaDataDS.map(
      kafkaData => {
        val data: String = kafkaData.value()
        val datas = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    // 最近一分钟，每10秒计算一次
    // 使用窗口函数计算
    val reduceDS: DStream[(String, Int)] = adClickData.map(
      data => {
        val ts = data.ts.toLong
        val newTs = ts / 10000 * 10000
        val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(
          new java.util.Date(newTs.toLong))
        (date, 1)
      }
    ).reduceByKeyAndWindow((x:Int,y:Int)=>(x+y), Seconds(60), Seconds(10))

    val sortDS: DStream[(String, Int)] = reduceDS.transform(
      rdd => {
        rdd.sortBy(_._1)
      }
    )

    sortDS.print()
    ssc.start()
    ssc.awaitTermination()
  }

  // 广告点击数据
  case class AdClickData( ts:String, area:String, city:String, user:String, ad:String )
}
