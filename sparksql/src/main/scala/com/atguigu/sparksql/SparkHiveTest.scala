package com.atguigu.sparksql

import org.apache.spark.sql.SparkSession

object SparkHiveTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkHiveTest")
      .getOrCreate()
    spark.sql("show tables").show()
    spark.stop()
  }
}
