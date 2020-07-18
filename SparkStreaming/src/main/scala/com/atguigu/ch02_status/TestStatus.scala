package com.atguigu.ch02_status

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestStatus {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象, 并设置 App名字, 并设置为 local 模式
    val sparkConf: SparkConf = new SparkConf().setAppName("TestStatus").setMaster("local[*]")
    sparkConf.set("spark.driver.bindAddress", "127.0.0.1")

    // 2.初始化SparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 3. 通过监控端口创建DStream，读进来的数据为一行行
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 定义更新状态方法，参数values为当前批次单词频度，state为以往批次单词频度
    val updateFunc: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], state: Option[Int]) => {
      val sum: Int = values.sum
      val previousCnt = state.getOrElse(0)
      Some(sum + previousCnt)
    }

    // 有状态转换
    val wordAndCnt: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc)

    wordAndCnt.print()

    ssc.checkpoint("./SparkStreaming/ck")
    ssc.start()
    ssc.awaitTermination()
  }
}
