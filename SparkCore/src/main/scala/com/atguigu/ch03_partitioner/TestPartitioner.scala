package com.atguigu.ch03_partitioner

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestPartitioner {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("TestPartitioner").setMaster("local[*]")
    conf.set("spark.driver.bindAddress", "127.0.0.1")

    // 创建SparkContext
    val sc = new SparkContext(conf)

    // 创建RDD
    val words: RDD[String] = sc.parallelize(Array("hello", "hello", "atguigu", "hahah"))

    // 创建PairRDD
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))

    // 查看当前分区情况
    val valueWithIndex: RDD[(Int, (String, Int))] = wordToOne.mapPartitionsWithIndex((i, items) => items.map((i, _)))

    valueWithIndex.foreach(println)

    // 重新分区
    val repartitioned: RDD[(String, Int)] = wordToOne.partitionBy(new CustomPartitioner(4))
    repartitioned.mapPartitionsWithIndex((i, items) => items.map((i, _))).foreach(println)

    sc.stop()
  }
}
