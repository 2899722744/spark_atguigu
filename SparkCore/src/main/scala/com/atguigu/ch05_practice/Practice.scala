package com.atguigu.ch05_practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Practice {
  def main(args: Array[String]): Unit = {
    // 1. 初始化spark配置信息, 并建立到spark的连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")
    sparkConf.set("spark.driver.bindAddress", "127.0.0.1")
    // 2. 从文件中读取数据, 得到 RDD. RDD中存储的是文件的中的每行数据
    val sc = new SparkContext(sparkConf)

    // 3. 读取数据创建RDD
    val lines: RDD[String] = sc.textFile("SparkCore/src/main/resources/agent.log")

    // 4. 切分取出省份和广告ID ((province, ad), 1)
    val provinceAndAdToOne: RDD[((String, String), Int)] = lines.map(x => {
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(4)), 4)
    })

    // 5. 计算每个省份每个广告被点击的总次数 ((province, ad), 16)
    val provinceAndAdToCnt: RDD[((String, String), Int)] = provinceAndAdToOne.reduceByKey(_+_)

    // 6. 维度转换 (province, (ad, 16))
    val provinceToAddAndCnt: RDD[(String, (String, Int))] = provinceAndAdToCnt.map(x => (x._1._1, (x._1._2, x._2)))

    // 7. 聚合同一省份的广告 (province1, (ad1, 16), (ad2, 18), ...)
    val provinceToAdGroup: RDD[(String, Iterable[(String, Int)])] = provinceToAddAndCnt.groupByKey()

    // 8. 排序取前三
    val top3: RDD[(String, List[(String, Int)])] = provinceToAdGroup.mapValues(x => {
      x.toList.sortWith((a, b) => a._2 > b._2).take(3)
    })

    // 9. 按照省份的升序展示最终结果
    top3.sortByKey().collect().foreach(println)

    sc.stop()
  }
}
