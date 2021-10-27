package com.myx.rdd.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * @Description: 自定义数据源
 * @author mayx
 * @date 2021/10/27 23:44
 */
object SparkStream03_DefineReceiver {
  def main(args: Array[String]): Unit = {
    // 1、初始化Spark的配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DefineReceiver")
    // 2、初始化StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(4))
    // 3、创建数据源
    val receiverInputDStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())
    receiverInputDStream.print()

    // 4、启动
    ssc.start()
    // 5、等待关闭
    ssc.awaitTermination()
  }

  /*
   自定义数据采集器
   1. 继承Receiver，定义泛型, 传递参数
   2. 重写方法
    */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flag = true
    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while ( flag ) {
            val message = "采集的数据为：" + new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()
    }
    override def onStop(): Unit = {
      flag = false
    }
  }
}



