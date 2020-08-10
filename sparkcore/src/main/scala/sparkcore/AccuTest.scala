package sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AccuTest {
  def main(args: Array[String]): Unit = {
    //创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TransFormTest")

    //创建SC
    val sc = new SparkContext(sparkConf)

    val accumulator = new MyAccumulator

    sc.register(accumulator)
    //创建rdd
    val rdd:RDD[Int] = sc.parallelize(Array(1,2,3,4))

    val wordAndOne: RDD[(Int, Int)] = rdd.map(x => {
      accumulator.add(x)
      (x, 1)
    })
    wordAndOne.collect().foreach(println)
    println(accumulator.value)
    sc.stop()
  }
}
