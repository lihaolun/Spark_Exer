package com.atguigu

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HBaseWriteTest {

  def main(args: Array[String]): Unit = {

    //创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HBaseWriteTest")

    //创建SparkContext
    val sc = new SparkContext(sparkConf)

    //创建JobConf
    val jobConf = new JobConf()
    jobConf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103")
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "fruit_mr")

    //定义往Hbase插入数据的方法
    def convert(triple: (Int, String, Int)): (ImmutableBytesWritable, Put) = {
      val put = new Put(Bytes.toBytes(triple._1))
      put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(triple._2))
      put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("price"), Bytes.toBytes(triple._3))
      (new ImmutableBytesWritable, put)
    }

    //创建一个RDD
    val initialRDD: RDD[(Int, String, Int)] = sc.parallelize(List((1, "apple", 11), (2, "banana", 12), (3, "pear", 13)))

    //转换RDD
    val hbaseWriteRDD: RDD[(ImmutableBytesWritable, Put)] = initialRDD.map(convert)

    //写入HBase
    hbaseWriteRDD.saveAsHadoopDataset(jobConf)

    //关闭连接
    sc.stop()
  }

}
