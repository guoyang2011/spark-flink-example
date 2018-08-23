package com.fastbird.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangguo on 2018/8/23.
  */
object JoinSuit {
  def main(args: Array[String]): Unit = {
    val config=new SparkConf()
    config.setMaster("local[4]")
    config.setAppName("join_suit")
    val sc=new SparkContext(config)
    val r1=sc.makeRDD((1 to 10).map(i=>i->i))
    val r2=sc.makeRDD(5 to 15).map(i=>i->i)
    println("join Result (K,(V1,V2)):")
    r1.join(r2).collect().foreach(println)
    println("leftJoin Result (K,(V1,Option[V2])):")
    r1.leftOuterJoin(r2).collect().foreach(println)
    println("FullOutJoin Result (K,(Option[V1],V2)):")
    r1.fullOuterJoin(r2).collect().foreach(println)
  }
}
