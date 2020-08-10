package com.atguigu.sparstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object QueueStream {
  /**
    * 循环创建几个RDD，将RDD放入队列。通过SparkStream创建Dstream，
    * 计算WordCount
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //创建rddQueue(数据集队列)
    val rddQueue = new mutable.Queue[RDD[Int]]()
    //创建QueueInputStream
    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, false)
    //处理队列中的RDD数据
    val mapedStream: DStream[(Int, Int)] = inputStream.map((_, 1))
    val reducedStream: DStream[(Int, Int)] = mapedStream.reduceByKey(_ + _)
    //打印结果
    reducedStream.print()
    ssc.start()

    for(i<-1 to 6){
        rddQueue += ssc.sparkContext.makeRDD(1 to 20,5)
    }
    ssc.awaitTermination()

  }
}
