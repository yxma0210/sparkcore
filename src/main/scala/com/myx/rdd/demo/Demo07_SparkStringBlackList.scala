package com.myx.rdd.demo

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat

import com.myx.rdd.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * @Description: 黑名单
 * @author: mayx
 * @date: 2021/11/16 11:10
 */
object Demo07_SparkStringBlackList {
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


    val ds: DStream[((String, String, String), Int)] = adClickData.transform(
      rdd => {
        // 通过JDBC周期性获取黑名单
        val blackList = ListBuffer[String]()
        val conn: Connection = JDBCUtil.getConnection
        val pstat: PreparedStatement = conn.prepareStatement("select userid from black_list")
        val rs: ResultSet = pstat.executeQuery()
        while (rs.next()) {
          blackList.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        conn.close()

        // 判断点击用户是否在黑名单中
        val filterRDD: RDD[AdClickData] = rdd.filter(
          data => {
            !blackList.contains(data.user)
          }
        )
        // 如果用户不在黑名单中，那么进行统计数量（每个采集周期）
        filterRDD.map(
          data => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day = sdf.format(new java.util.Date(data.ts.toLong))
            val user = data.user
            val ad = data.ad
            ((day, user, ad), 1)
          }
        ).reduceByKey(_ + _)
      }
    )

    ds.foreachRDD(
      rdd => {
        // rdd. foreach方法会每一条数据创建连接
        // foreach方法是RDD的算子，算子之外的代码是在Driver端执行，算子内的代码是在Executor端执行
        // 这样就会涉及闭包操作，Driver端的数据就需要传递到Executor端，需要将数据进行序列化
        // 数据库的连接对象是不能序列化的。

        // RDD提供了一个算子可以有效提升效率 : foreachPartition
        // 可以一个分区创建一个连接对象，这样可以大幅度减少连接对象的数量，提升效率
        rdd.foreachPartition(
          iter => {
            val conn = JDBCUtil.getConnection
            iter.foreach{
              case((day,user,ad),count) => {
                println(s"${day} ${user} ${ad} ${count}")
                if (count >= 20){
                  // 如果统计数量超过点击阈值(20)，那么将用户拉入到黑名单
                  val sql = """
                              |insert into black_list (userid) values (?)
                              |on DUPLICATE KEY
                              |UPDATE userid = ?
                              |""".stripMargin
                  JDBCUtil.executeUpdate(conn,sql,Array(user,user))
                } else {
                  val sql = """
                              | select
                              |     *
                              | from user_ad_count
                              | where dt = ? and userid = ? and adid = ?
                                      """.stripMargin
                  val flag = JDBCUtil.isExist(conn, sql, Array( day, user, ad ))
                  // 查询统计表数据
                  if (flag) {
                    // 如果数据存在，就更新
                    val sql1 = """
                                 | update user_ad_count
                                 | set count = count + ?
                                 | where dt = ? and userid = ? and adid = ?
                                           """.stripMargin
                    JDBCUtil.executeUpdate(conn, sql1, Array(count, day, user, ad))
                    // 判断更新后的点击数据是否超过阈值，如果超过，那么将用户拉入到黑名单。
                    val sql2 = """
                                 |select
                                 |    *
                                 |from user_ad_count
                                 |where dt = ? and userid = ? and adid = ? and count >= 30
                                           """.stripMargin
                    val flg1 = JDBCUtil.isExist(conn, sql2, Array( day, user, ad ))
                    if ( flg1 ) {
                        val sql3 = """
                                     |insert into black_list (userid) values (?)
                                     |on DUPLICATE KEY
                                     |UPDATE userid = ?
                                                """.stripMargin
                        JDBCUtil.executeUpdate(conn, sql3, Array( user, user ))
                      }
                    } else {
                      val sql4 = """
                                   | insert into user_ad_count ( dt, userid, adid, count )
                                   | values ( ?, ?, ?, ? )
                                  """.stripMargin
                      JDBCUtil.executeUpdate(conn, sql4, Array( day, user, ad, count ))
                    }
                  }
                }
              }
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
