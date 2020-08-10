package sparkcore

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object HBaseTest {
  def main(args: Array[String]): Unit = {
    //创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HBaseTest")
    //创建sc
    val sc = new SparkContext(sparkConf)
    //构建HBase配置信息
    val configuration: Configuration = HBaseConfiguration.create()

    configuration.set("hbase.zookeeper.quorum","hadoop101,hadoop102,hadoop103")
    configuration.set(TableInputFormat.INPUT_TABLE,"fruit")

    //读取HBase数据
    val hbaseRDD = new NewHadoopRDD(
      sc,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result],
      configuration)

    val rowKey: RDD[String] = hbaseRDD.map((x =>Bytes.toString(x._2.getRow)))

    rowKey.collect().foreach(println)

    sc.stop()

  }
}
