package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象, 并设置 App名字, 并设置为 local 模式
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    sparkConf.set("spark.driver.bindAddress", "127.0.0.1")
    // 2. 创建SparkContext对象
    val sc = new SparkContext(sparkConf)

    // 3. 使用sc创建RDD并执行相应的transformation和action
    val line: RDD[String] = sc.textFile("SparkCore/src/main/resources/agent.log")
    val words = line.flatMap(_.split(""))

    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

    val wordAndCount: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    wordAndCount.saveAsTextFile("SparkCore/src/main/resources/output")

    // 4. 关闭连接
    sc.stop()
  }
}
