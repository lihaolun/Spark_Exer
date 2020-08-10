package com.atguigu.sparksql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object MyAVG extends UserDefinedAggregateFunction {
  //输入数据的类型
  override def inputSchema: StructType = {
    StructType(StructField("input", LongType) :: Nil)
  }

  //缓存类型
  override def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  //输出数据的类型
  override def dataType: DataType = {
    DoubleType
  }

  //函数的稳定参数（一般都为true）
  override def deterministic: Boolean = true

  //初始化缓存的值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //在Executor内
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    //次数
    buffer(1) = buffer.getLong(1) + 1L
  }

  //在Executor之间
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //汇总
  override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble / buffer.getLong(1)
}
