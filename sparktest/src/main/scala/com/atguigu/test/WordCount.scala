package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建配置
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount")
    //创建sc
    val sparkContext = new SparkContext(sparkConf)
    //读取文件
    val line: RDD[String] = sparkContext.textFile(args(0))
    //压平返回数组
    val word: RDD[String] = line.flatMap(_.split(" "))
    //形成元组
    val wordTuple: RDD[(String, Int)] = word.map((_, 1))
    //统计
    val wordAndCount: RDD[(String, Int)] = wordTuple.reduceByKey(_ + _)
    //打印
    //wordAndCount.collect().foreach(println)
    //写入文件
    wordAndCount.saveAsTextFile(args(1))
    //连接关闭
    sparkContext.stop()
  }
}
