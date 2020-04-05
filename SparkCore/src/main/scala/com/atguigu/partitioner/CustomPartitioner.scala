package com.atguigu.partitioner

import org.apache.spark.Partitioner

class CustomPartitioner(partitions: Int) extends Partitioner {
  // 获取分区数
  override def numPartitions: Int = partitions

  // 获取分区号
  override def getPartition(key: Any): Int = {
    key.toString.last.toInt % numPartitions
  }
}