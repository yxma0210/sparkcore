package com.myx.rdd.serializable

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: 序列化的方法
 * @author mayx
 * @date 2021/9/28 0:08
 */
object Serial02 {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3))

    // Exception in thread "main" org.apache.spark.SparkException: Task not serializable
    // Caused by: java.io.NotSerializableException: com.myx.rdd.serializable.User

    // RDD算子中传递的函数是会包含闭包操作，那么就会进行检测功能
    // 闭包检测
    val user = new User()
    rdd.foreach(
      num => {
        // 算子内部在Executor节点中执行，引用到Driver节点的user对象，所以需要序列化
        println("age= " + (user.age + num))
      }
    )

    // 关闭资源
    sc.stop()
  }
  // 序列化方法：
  // 1、继承Serializable
  // 2、使用样例类，样例类在编译时，会自动混入序列化特质（实现可序列化接口）
  class User extends Serializable {
    var age: Int = 30;
  }
}


