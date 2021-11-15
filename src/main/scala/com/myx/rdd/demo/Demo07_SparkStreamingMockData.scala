package com.myx.rdd.demo

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * @Description:
 * @author: mayx
 * @date: 2021/11/15 17:52
 */
object Demo07_SparkStreamingMockData {
  def main(args: Array[String]): Unit = {
    // 生成模拟数据
    // 格式：timestamp area city userid adid
    // 含义： 时间戳   区域  城市 用户 广告

    val prop = new Properties()
    // 添加配置
    // broker配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop201:9092")
    // 序列类型
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // 创建kafkaProducer
    val producer = new KafkaProducer[String, String](prop)

    while ( true ) {
      mockdata().foreach(
        data => {
          // 向Kafka中生成数据
          val record = new ProducerRecord[String, String]("adverting", data)
          producer.send(record)
          println(data)
        }
      )

      Thread.sleep(2000)
    }
  }


  def mockdata() = {
    val list = ListBuffer[String]()
    val areaList = ListBuffer[String]("华东","华南","华北","华中")
    val cityList = ListBuffer[String]("上海","深圳","北京","武汉")

    for (i <- 1 to new Random().nextInt(50)) {
      val area = areaList(new Random().nextInt(4))
      val city = cityList(new Random().nextInt(4))
      val userid = new Random().nextInt(6) + 1
      val adid = new Random().nextInt(6) + 1

      list.append(s"${System.currentTimeMillis()} ${area},${city},${userid}," +
        s"${adid}")
    }
    list
  }
}
