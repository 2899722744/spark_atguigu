package com.atguigu.ch04_accu

import com.atguigu.ch04_accuaccu.CustomeAccu
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestCustomAccu {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("TestCustomAccu").setMaster("local[*]")
    conf.set("spark.driver.bindAddress", "127.0.0.1")

    // 创建SparkContext
    val sc = new SparkContext(conf)

    // 创建累加器
    val accu: CustomeAccu = new CustomeAccu

    // 注册累加器
    sc.register(accu, "sum")


    val value: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))
    val maped: RDD[Int] = value.map(x => {
      accu.add(x)
      x
    })

    maped.collect().foreach(println)
    println(s"Custom Accu: ${accu.value}")

    sc.stop()
  }
}
