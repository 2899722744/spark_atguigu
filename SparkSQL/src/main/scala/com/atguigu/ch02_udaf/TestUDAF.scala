package com.atguigu.ch02_udaf

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestUDAF {
  def main(args: Array[String]): Unit = {
    // 创建Spark Session对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("TestSparkSQL")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 创建DataFrame
    val df: DataFrame = spark.read.json("SparkSQL/src/main/resources/people.json")

    // SQL风格
    df.createTempView("people")

    // 注册UDAF
    spark.udf.register("avg", CustomUDAF)

    // 使用UDAF
    spark.sql("select avg(age) from people").show()

    spark.stop()
  }
}
