package com.atguigu.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SqlTest {
  def main(args: Array[String]): Unit = {
    //构建SparkSession
     val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("sqlTest")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //隐式转换
    //import spark.implicits._
    //创建一个RDD
    val rddInt: RDD[Int] = sc.parallelize(Array(1,2,3,4))
    //转换Rdd(Int)为Rdd(Row)
    val rowRdd: RDD[Row] = rddInt.map(x=>{
      Row(x)
    })
    rowRdd.collect().foreach(println)

    //构建元数据信息
    val structType = new StructType
    val structType1: StructType = structType.add(StructField("id",IntegerType))

    //转换为dataframe
    val dataFrame: DataFrame = spark.createDataFrame(rowRdd,structType1)
    //DSL风格
    dataFrame.show()
    //sql风格
    dataFrame.createTempView("people")
    //查询数据
    spark.sql("select * from people").show()
    //资源关闭
    spark.stop()
  }
}
