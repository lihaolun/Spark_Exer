package sparkcore

import org.apache.spark.Partitioner

class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
   /* hash分区
    val code: Int = key.toString.hashCode()
    code/partitions */
    0
  }
}
