package com.atguigu.sparstreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Add {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Application")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5))
    val myacc = new Myacc()
    sc.register(myacc,"myacc")
   /* val acc: LongAccumulator = sc.longAccumulator("acc")*/
    rdd.map(
      item =>myacc.add(item)
    ).collect()
    println(myacc.value)

    sc.stop()
  }
}

class Myacc extends AccumulatorV2[Int,Int]{
  var sum = 0
  override def isZero: Boolean =sum==0

  override def copy(): AccumulatorV2[Int, Int] = {
    val myacc = new Myacc()
    myacc.synchronized{
      myacc.sum = 0
      myacc
    }
  }


  override def reset(): Unit = sum = 0

  override def add(v: Int): Unit = sum+v

  override def merge(other: AccumulatorV2[Int, Int]): Unit = sum+other.value

  override def value: Int = sum
}
