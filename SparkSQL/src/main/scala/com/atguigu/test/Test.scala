package com.atguigu.test

import org.apache.spark.sql.{DataFrame, SparkSession}

object Test {
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

    // DSL风格
    df.filter($"age" > 20).show()

    // SQL风格
    df.createTempView("people")
    spark.sql("select * from people").show()

    spark.stop()
  }
}
