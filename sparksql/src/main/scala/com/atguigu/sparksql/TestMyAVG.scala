package com.atguigu.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestMyAVG {
  def main(args: Array[String]): Unit = {
      //创建SparkSession
    val sparkSession: SparkSession = SparkSession.
      builder()
      .master("local[*]")
      .appName("TestMyAVG")
      .getOrCreate()
    //创建一个DF
    val dataFrame: DataFrame = sparkSession.read.json("G:\\TestData\\input\\spark\\people.json")
    //创建临时表
    dataFrame.createTempView("people")

    //注册自定义函数
    sparkSession.udf.register("MyAvg",MyAVG)
    //函数应用
    sparkSession.sql("select MyAvg(age) from people").show()

    sparkSession.stop()
  }
}
