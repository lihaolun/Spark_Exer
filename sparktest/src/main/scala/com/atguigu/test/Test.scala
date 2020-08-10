package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  //需求：统计出每一个省份广告被点击次数的TOP3
  //1516609143869 1 7 87 12
  def main(args: Array[String]): Unit = {
    //1,初始化spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    //2.获取sc
    val sparkContext = new SparkContext(sparkConf)

    //3.读取数据
    val line: RDD[String] = sparkContext.textFile("G:\\TestData\\input\\spark")
    val fields = line.map { x =>
      val strings: Array[String] = x.split(" ")
      ((strings(1), strings(4)), 1)
    }
    val value: RDD[((String, String), Int)] = fields.reduceByKey(_ + _)
    //转换格式
    val res1: RDD[(String, (String, Int))] = fields.map((x) => (x._1._1, (x._1._2, x._2)))
    val group: RDD[(String, Iterable[(String, Int)])] = res1.groupByKey()

    val mapValue = group.mapValues {
      x => x.toList.sortBy(_._2)
        .take(3)
    }

    mapValue.collect().foreach(println)

    sparkContext.stop

  }
}
