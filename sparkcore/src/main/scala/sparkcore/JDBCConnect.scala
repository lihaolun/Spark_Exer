package sparkcore

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

//一般用sparksql来连接，这里只是练习用sparkcore来连接
object JDBCConnect {
  def main(args: Array[String]): Unit = {
    //创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JDBCConnect")
    //创建sc
    val sc = new SparkContext(sparkConf)

    //定义JDBC所需的参数信息
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop101:3306/ct"
    val username = "root"
    val password = "hehe1233"
    //创建JDBCRDD
    val jdbcRDD = new JdbcRDD[(Int, String)](sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, username, password)
    },
      "select id,cid from t_call where ?<id and id<?",
      0,
      10,
      1,
      x => {
        (x.getInt(1), x.getString(2))
      })

    //打印RDD
    jdbcRDD.collect().foreach(println)

    //关闭连接
    sc.stop()
  }
}
