package com.demo.spark.structuredStreaming

import org.apache.spark.sql.SparkSession

object demo2 {
  /**
   * 读取kafka
   */

  def main(args: Array[String]): Unit = {

    /**
     * sparkcontext环境
     */
    val spark = SparkSession
      .builder
      .appName("kafkademo")
      .master("local")
      .getOrCreate()

    /**
     * 获取kafka 相关配置
     */
  val df=  spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "bdp71:9092")
      .option("subscribe", "spark")
//      .option("kafka.session.timeout.ms","100")
      .option("startingOffsets","earliest")
      .load()
    import spark.implicits._

    /**
     * 通过cast 函数将key和value转为可读的数据
     * kafka在存储时将字母和中文使用ascii码保存
     */
    df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()



  }

}
