package com.myx.rdd.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: 页面单跳转换率统计
 * @author mayx
 * @date 2021/10/26 23:39
 */
object Demo06_PageflowAnalysis {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("PageflowAnalysis")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 1、读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("src/data/user_visit_action.txt")

    val actionDataRDD: RDD[UserVisitAction] = actionRDD.map {
      action => {
        val datas: Array[String] = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    }
    actionDataRDD.cache()

    // 对指定的页面连续跳转进行统计
    val ids: List[Long] = List[Long](1, 2, 3, 4, 5, 6, 7) // 用于过滤分母
    val folowIds: List[(Long, Long)] = ids.zip(ids.tail)  // 用于过滤分子

    // 计算分母
    val pageidCount: Map[Long, Long] = actionDataRDD.filter(
      action => {
        ids.init.contains(action.page_id)
      }
    ).map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap

    // 计算分子
    // 根据sessionId进行分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)
    // 分组后根据访问时间排序
    val mapValuesRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
        val flowIds: List[Long] = sortList.map(_.page_id)
        val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)
        // 过滤不合法的页面跳转
        pageflowIds.filter(
          t => {
            folowIds.contains(t)
          }
        ).map(
          t => {
            (t, 1)
          }
        )
      }
    )

    val flatRDD: RDD[((Long, Long), Int)] = mapValuesRDD.map(_._2).flatMap(list => list)
    val dataRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey(_ + _)

    // 计算单跳转换率
    dataRDD.foreach{
      case ((pageid1,pageid2),sum) => {
        val lon: Long = pageidCount.getOrElse(pageid1, 0)
        println(s"页面${pageid1}跳转到${pageid2}的单跳转换率为:" + (sum.toDouble/lon))
      }
    }

    // 关闭资源
    sc.stop()
  }

  //用户访问动作表
  case class UserVisitAction(
                              date: String,//用户点击行为的日期
                              user_id: Long,//用户的ID
                              session_id: String,//Session的ID
                              page_id: Long,//某个页面的ID
                              action_time: String,//动作的时间点
                              search_keyword: String,//用户搜索的关键词
                              click_category_id: Long,//某一个商品品类的ID
                              click_product_id: Long,//某一个商品的ID
                              order_category_ids: String,//一次订单中所有品类的ID集合
                              order_product_ids: String,//一次订单中所有商品的ID集合
                              pay_category_ids: String,//一次支付中所有品类的ID集合
                              pay_product_ids: String,//一次支付中所有商品的ID集合
                              city_id: Long  //城市 id
                            )
}
