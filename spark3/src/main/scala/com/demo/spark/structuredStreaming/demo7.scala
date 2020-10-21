package com.demo.spark.structuredStreaming

import com.spark.utils.sparkUtils.getSpark
import org.apache.spark.sql.functions._

object demo7 {
  def main(args: Array[String]): Unit = {
    /**
     * 聚合+ 窗口操作
     */
    val spark = getSpark()
    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "bdp75")
      .option("port", 9999)
      .load()

    val df = lines.as[String].flatMap(_.split(" "))
      .selectExpr("value as word")
      .withColumn("timestamp", current_timestamp())
      .withColumn("cn", lit(1))

   val wordCounts= df.groupBy(
      window($"timestamp", "2 minutes", "1 minutes"),
        $"word"
      ).agg("cn" -> "sum").withColumnRenamed("sum(cn)", "wc")
    // 开启 query，将数据输出到控制台
//    val query = wordCounts.writeStream
//      .outputMode("complete")
//      .format("console")
//      .option("truncate", false)
//      .start()
//    query.awaitTermination()
    //complete 模式窗口输出包含全部数据结果
    //update  模式窗口输出只包含新增的数据结果
    //append  不支持该模式
    val query = wordCounts.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", false)
      .start()
    query.awaitTermination()




  }

}
