package sparkcore

import org.apache.spark.util.AccumulatorV2

class MyAccumulator extends AccumulatorV2[Int, Int] {
  //累加必须给个初始值
  var sum = 0

  //判断是否为空
  override def isZero: Boolean = sum == 0

  //复制
  override def copy(): AccumulatorV2[Int, Int] = {
    val accu = new MyAccumulator
    accu.sum = this.sum
    accu
  }

  override def reset(): Unit = sum = 0

  override def add(v: Int): Unit = sum += v

  override def merge(other: AccumulatorV2[Int, Int]): Unit = sum += other.value

  override def value: Int = sum
}
