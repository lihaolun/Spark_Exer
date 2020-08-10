package com.atguigu.sparstreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Add {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Application")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5))
    val acc: LongAccumulator = sc.longAccumulator("acc")
    rdd.map(
      item =>acc.add(item)
    ).collect()
    println(acc.value)

    sc.stop()
  }
}
