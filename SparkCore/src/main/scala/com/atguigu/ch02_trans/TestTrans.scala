package com.atguigu.ch02_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestTrans {

  //需求: 在 RDD 中查找出来包含 query 子字符串的元素
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SerDemo").setMaster("local[*]")
    conf.set("spark.driver.bindAddress", "127.0.0.1")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)
    val searcher = new Searcher("hello")
    val result: RDD[String] = searcher.getMatchedRDD3(rdd)
    result.collect.foreach(println)
    sc.stop()
  }
}
