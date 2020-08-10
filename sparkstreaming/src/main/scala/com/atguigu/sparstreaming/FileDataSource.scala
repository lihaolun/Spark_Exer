package com.atguigu.sparstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileDataSource {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FileDataSource")

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //监控hdfs上文件夹
    val hdfsStream: DStream[String] = ssc.textFileStream("hdfs://hadoop101:9000/spark/input")

    val wordStream = hdfsStream.flatMap(_.split(" "))

    wordStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
