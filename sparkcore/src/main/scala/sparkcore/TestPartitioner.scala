package sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestPartitioner {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyPartitioner")

    //创建sc
    val sc = new SparkContext(sparkConf)

    //创建rdd
    val rdd: RDD[Int] = sc.parallelize(Array(1,2,3,4,5))

    //改变元素为元组
    val wordAndOne: RDD[(Int, Int)] = rdd.map((_,1))

    //使用自定义分区
    val partitioned: RDD[(Int, Int)] = wordAndOne.partitionBy(new MyPartitioner(2))

    //查看重新分区后的分区分布情况
    val indexAndData: RDD[(Int, (Int, Int))] = partitioned.mapPartitionsWithIndex((i,t) => t.map((i,_)))

    indexAndData.collect().foreach(println)

    //断开连接
    sc.stop()
  }
}
