package com.myx.rdd.demo
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Description: Top10热门品类中每个品类的的Top10活跃Session
 * @author mayx
 * @date 2021/10/20 23:44
 */
object Demo05_HotCategoryTop10Session {
  def main(args: Array[String]): Unit = {
    // 创建运行时对象
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("HotCategoryTop10Analysis")
    // 创建spark运行时环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 1、读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("src/data/user_visit_action.txt")
    actionRDD.cache()
    val top10Ids: Array[String] = top10Category(actionRDD)

    // 1. 过滤原始数据,保留点击和前10品类ID
    val filterActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          top10Ids.contains(datas(6))
        } else {
          false
        }
      }
    )

    // 2. 根据品类ID和sessionid进行点击量的统计
    val reduceByKeyRDD: RDD[((String, String), Int)] = filterActionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    // 3. 将统计的结果进行结构的转换
    //  (（ 品类ID，sessionId ）,sum) => ( 品类ID，（sessionId, sum） )
    val mapRDD: RDD[(String, (String, Int))] = reduceByKeyRDD.map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }

    // 4. 相同的品类进行分组
    val groupByKeyRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    // 5. 将分组后的数据进行点击量的排序，取前10名
    val resultRDD = groupByKeyRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )
    resultRDD.collect().foreach(println)
    sc.stop()
  }

  def top10Category(actionRDD: RDD[String]) = {
    val flatMapRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids: Array[String] = datas(8).split(",")
          ids.map(id => {
            (id, (0, 1, 0))
          })
        } else if (datas(10) != "null") {
          val ids: Array[String] = datas(10).split(",")
          ids.map(
            id => {
              (id, (0, 0, 1))
            }
          )
        } else {
          Nil
        }
      }
    )

    val reduceRDD: RDD[(String, (Int, Int, Int))] = flatMapRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t2._1 + t1._2, t1._3 + t2._3)
      }
    )
    reduceRDD.sortBy(_._2,false).take(10).map(_._1)
  }
}
