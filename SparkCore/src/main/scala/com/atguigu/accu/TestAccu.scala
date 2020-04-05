package com.atguigu.accu

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestAccu {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("TestAccu").setMaster("local[*]")
    conf.set("spark.driver.bindAddress", "127.0.0.1")

    // 创建SparkContext
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))

    var sum = 0
    val maped: RDD[Int] = value.map(x => {
      sum += x
      x
    })

    maped.collect.foreach(println)
    println(s"Diver sum: $sum")

    val accu_sum = sc.longAccumulator("sum")
    val maped_accu: RDD[Int] = value.map(x => {
      accu_sum.add(x)
      x
    })

    maped_accu.collect.foreach(println)
    println(s"Accu sum: ${accu_sum.value}")

    sc.stop()
  }
}
