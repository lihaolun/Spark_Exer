package sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//如果不是本地端执行，应该继承serializer
class Search(query: String) {
  //定义函数判断是否包含字符串
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD，RDD的函数传递
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains("aa"))
  }
}

object SearchTest {
  def main(args: Array[String]): Unit = {
    //创建sc
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestSearch")
    val sc = new SparkContext(sparkConf)
    //逻辑
    //创建Search对象及RDD
    /*val rdd: RDD[String] = sc.parallelize(Array("aabc", "bbac", "ccab"))*/
    //测试
  /*  val search = new Search("a")

    val matchWord: RDD[String] = search.getMatch2(rdd)

    matchWord.collect().foreach(println)*/

    val rdd = sc.parallelize(1 to 16,4)

    rdd.glom().collect()
    sc.stop()

  }
}

