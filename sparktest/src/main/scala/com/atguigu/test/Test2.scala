package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("TEST2").setMaster("local[*]")
        val sparkContext = new SparkContext(sparkConf)


        val rdd = sparkContext.parallelize(1 to 16,4)

        rdd.glom().collect()

        rdd.collect()
    }
}
