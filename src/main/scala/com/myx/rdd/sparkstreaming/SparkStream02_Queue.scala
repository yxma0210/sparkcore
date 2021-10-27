package com.myx.rdd.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @Description: RDD队列
 * @author mayx
 * @date 2021/10/27 23:23
 */
object SparkStream02_Queue {
  def main(args: Array[String]): Unit = {
    // 1、初始化Spark的配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Queue")
    // 2、初始化StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    // 3、创建RDD队列
    val queue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]
    // 4、创建QueueInputStream
    val inputDStream: InputDStream[Int] = ssc.queueStream(queue, false)

    val mapDStream: DStream[(Int, Int)] = inputDStream.map((_, 1))
    val reduceDStream: DStream[(Int, Int)] = mapDStream.reduceByKey(_ + _)
    reduceDStream.print()

    // 启动任务
    ssc.start()
    // 循环创建并向RDD队列中放入RDD
    for (i <- 1 to 5) {
      queue += ssc.sparkContext.makeRDD(1 to 100)
      Thread.sleep(2000)
    }
    // 等待采集器关闭
    ssc.awaitTermination()
  }
}
