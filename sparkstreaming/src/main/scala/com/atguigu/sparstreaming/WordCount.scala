package com.atguigu.sparstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkStream来完成wordCount
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //2.初始化StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    //3.通过监控端口创建DStream，读进来的数据为一行行
    val lineStream = ssc.socketTextStream("hadoop101", 9999)
    //切分
    val wordStream: DStream[String] = lineStream.flatMap(_.split(" "))
    //映射元组
    val wordAndOneStream: DStream[(String, Int)] = wordStream.map((_, 1))
    //统计求和
    val wordAndCountStream: DStream[(String, Int)] = wordAndOneStream.reduceByKey(_ + _)
    //打印
    wordAndCountStream.print()
    //流式计算，需要启动
    ssc.start()
    ssc.awaitTermination()

  }
}
