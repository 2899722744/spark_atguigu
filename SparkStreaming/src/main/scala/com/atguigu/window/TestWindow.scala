package com.atguigu.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object TestWindow {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象, 并设置 App名字, 并设置为 local 模式
    val sparkConf: SparkConf = new SparkConf().setAppName("TestWindow").setMaster("local[*]")
    sparkConf.set("spark.driver.bindAddress", "127.0.0.1")

    // 2.初始化SparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 3. 通过监控端口创建DStream，读进来的数据为一行行
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 窗口操作
    // 不需要checkpoint
    val wordAndCnt: DStream[(String, Int)] = wordAndOne.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(10), Seconds(5))

    // 减去旧窗口操作，适合重合的高的窗口
    // 需要checkpoint
    // 含有旧数据信息：(a, 0), (b, 0)
    // ssc.checkpoint("./SparkStreaming/ck")
    //  val wordAndCnt: DStream[(String, Int)] = wordAndOne.reduceByKeyAndWindow((x: Int, y: Int) => x + y,
    //  (a: Int, b: Int) => a-b,
    //  Seconds(10), Seconds(5))

    wordAndCnt.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
