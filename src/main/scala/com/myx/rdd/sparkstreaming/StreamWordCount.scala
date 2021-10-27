package com.myx.rdd.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Description: SparkSteaming 计算WordCount
 * @author mayx
 * @date 2021/10/27 22:51
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1、初始化Spark的配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SteamWordCount")
    // 2、初始化SparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 3、监视端口创建DSteam，读取的数据为一行行
    val linesStreams: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // 4、将每一行数据做切分，形参一个个单词
    val wordStreams: DStream[String] = linesStreams.flatMap(_.split(" "))
    // 5、将单词映射成元组
    val wordAndOneStreams: DStream[(String, Int)] = wordStreams.map((_, 1))
    // 6、将相同单词次数做统计
    val wordCountStreams: DStream[(String, Int)] = wordAndOneStreams.reduceByKey(_ + _)
    // 7、打印到屏幕
    wordCountStreams.print()
    // 8、启动SparkStreamingContext
    // 由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
    // 如果main方法执行完毕，应用程序也会自动结束。所以不能让main执行完毕
    // 启动采集器
    ssc.start()
    // 等待采集器的关闭
    ssc.awaitTermination()
  }
}
