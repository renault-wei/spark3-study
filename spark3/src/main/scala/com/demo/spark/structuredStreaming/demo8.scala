package com.demo.spark.structuredStreaming

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.spark.utils.sparkUtils.getSpark
import org.apache.spark.sql.functions._

/**
 * author Renault
 * date 2020/8/3 11:37
 * Description 
 */
object demo8 {
  /**
   * 聚合  窗口 + 水印
   */
  def main(args: Array[String]): Unit = {

    val spark = getSpark()
    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "bdp75")
      .option("port", 9999)
      .load()
    val words = lines.as[String]
      .selectExpr("value")
      .map(line => {
        val arr = line.mkString.split(",")
        WordBean(arr(0), getTime(arr(1)))
      })
      .withColumn("cn", lit(1))
      .withWatermark("timestamp", "2 minutes")
      .groupBy(
        window($"timestamp", "5 minutes", "3 minutes"),
        $"word"
      )
      .agg("cn" -> "sum").withColumnRenamed("sum(cn)", "wc")
    //    val query = words.writeStream
    //      .outputMode("update")
    //      .format("console")
    //      .option("truncate", false)
    //      .start()
    //    query.awaitTermination()
    //指定的字段需要转为timestamp格式
    //update 模式过期字段将被清除

    val query = words.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()
    query.awaitTermination()
    //append 滑动模式下输出会有延迟 会等到窗口所有数据到来后才会输出 也会等待下一个batch到来才会输出数据

  }

  case class WordBean(word: String, timestamp: Timestamp)

  def getTime(str: String) = {
    val sd = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss")

    new Timestamp(sd.parse(str).getTime)
  }

}
