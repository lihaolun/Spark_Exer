package com.atguigu.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

object UdfFunctionTest {
  def main(args: Array[String]): Unit = {
    //创建SparkConf()并设置App名称
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkSqlExample")
      .getOrCreate()

    //导入隐式转换
    import sparkSession.implicits._
    //读取本地文件，创建DataFrame
    val df: DataFrame = sparkSession.read.json("G:\\TestData\\input\\spark\\people.json")
    df.show()

    //DSL风格：查询年龄在21岁以上的
    df.filter($"age" > 21).show()

    //创建临时表
    df.createTempView("persons")

    //sql风格：查询年龄在21岁以上的
    sparkSession.sql("SELECT * FROM persons where age > 21").show()

    sparkSession.stop()
  }
}
