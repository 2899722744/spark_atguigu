package com.atguigu.trans

import org.apache.spark.rdd.RDD

// query 为需要查找的子字符串
// extends Serializable
class Searcher(val query: String) {
  // 判断 s 中是否包括子字符串 query
  def isMatch(s : String) = {
    s.contains(query)
  }
  // 过滤出包含 query字符串的字符串组成的新的 RDD
  def getMatchedRDD1(rdd: RDD[String]) = {
    rdd.filter(isMatch)  //
  }
  // 过滤出包含 query字符串的字符串组成的新的 RDD
  def getMatchedRDD2(rdd: RDD[String]) = {
    rdd.filter(_.contains(query))
  }

  def getMatchedRDD3(rdd: RDD[String]): RDD[String] = {
    val str = this.query
    rdd.filter(x => x.contains(str))
  }
}