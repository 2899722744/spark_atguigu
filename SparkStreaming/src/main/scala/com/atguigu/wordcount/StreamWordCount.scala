package com.atguigu.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象, 并设置 App名字, 并设置为 local 模式
    val sparkConf: SparkConf = new SparkConf().setAppName("StreamWordCount").setMaster("local[*]")
    sparkConf.set("spark.driver.bindAddress", "127.0.0.1")

    // 2.初始化SparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 3. 通过监控端口创建DStream，读进来的数据为一行行
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = wordDStream.map((_, 1))

    val wordAndCnt: DStream[(String, Int)] = wordAndOne.reduceByKey((_ + _))

    wordAndCnt.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
