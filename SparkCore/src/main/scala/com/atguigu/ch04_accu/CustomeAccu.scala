package com.atguigu.ch04_accuaccu

import org.apache.spark.util.AccumulatorV2

class CustomeAccu extends AccumulatorV2[Int, Int] {

  var sum: Int = 0

  // 判断对象是否为空
  override def isZero: Boolean = sum == 0

  override def copy(): AccumulatorV2[Int, Int] = {
    val accu = new CustomeAccu
    accu.sum = this.sum
    accu
  }

  override def reset(): Unit = sum = 0

  override def add(v: Int): Unit = sum += v

  // 合并Executor端传回来的数据
  override def merge(other: AccumulatorV2[Int, Int]): Unit = this.sum += other.value

  // 返回值
  override def value: Int = sum
}
