package com.atguigu.ch01_test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object CreateDF {
  def main(args: Array[String]): Unit = {
    // 创建Spark Session对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("CreateDF")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate

    // 导入隐式转换
    import spark.implicits._

    // 获取Spark Context
    val sc = spark.sparkContext
    // 1. 创建RDD
    val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))
    // 2. 转换为RDD[Row]
    val rddRow: RDD[Row] = rdd.map(x => Row(x))
    // 3. 创建StructType
    val structType: StructType = StructType(StructField("id", IntegerType) :: Nil)
    // 4. 创建DataFrame
    val df: DataFrame = spark.createDataFrame(rddRow, structType)
    df.show()

    spark.stop()
  }
}
