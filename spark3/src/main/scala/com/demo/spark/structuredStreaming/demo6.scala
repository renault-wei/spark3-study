package com.demo.spark.structuredStreaming

import org.apache.spark.sql.functions._
import com.spark.utils.sparkUtils._
object demo6 {
  def main(args: Array[String]): Unit = {
    /**
     * wordcount  complete聚合
     **/

    val spark = getSpark()
    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "bdp75")
      .option("port", 9999)
      .load()
    // 数据行转 word
    val words = lines.as[String].flatMap(_.split(" ")).selectExpr("value as word")
      .withColumn("cn",lit(1))
    val wordCounts = words.groupBy(
      $"word"
    ).agg("cn" -> "sum").withColumnRenamed("sum(cn)", "wc")
    // 开启 query，将数据输出到控制台
    /**
     * complete 聚合
     */
    //    val query = wordCounts.writeStream
//      .outputMode("complete")
//      .format("console")
//      .option("truncate", false)
//      .start()
//    +-----+---+
//    |word |wc |
//    +-----+---+
//    |you  |14 |
//      |ni   |21 |
//      |lite |14 |
//      |liote|7  |
//      |l;ite|7  |
//      |tiam |7  |
//      |mno  |7  |
//      |time |14 |
//      |tioan|7  |
//      +-----+---+
//
//    -------------------------------------------
//    Batch: 2
//    -------------------------------------------
//    +-----+---+
//    |word |wc |
//    +-----+---+
//    |you  |26 |
//      |ni   |39 |
//      |lite |26 |
//      |liote|13 |
//      |l;ite|13 |
//      |tiam |13 |
//      |mno  |13 |
//      |time |26 |
//      |tioan|13 |
//      +-----+---+
    val query = wordCounts.writeStream
          .outputMode("update")
          .format("console")
          .option("truncate", false)
          .start()
//Batch: 0
    //-------------------------------------------
    //+----+---+
    //|word|wc |
    //+----+---+
    //+----+---+
    //
    //-------------------------------------------
    //Batch: 1
    //-------------------------------------------
    //+----+---+
    //|word|wc |
    //+----+---+
    //|time|2  |
    //+----+---+
    //
    //-------------------------------------------
    //Batch: 2
    //-------------------------------------------
    //+-----+---+
    //|word |wc |
    //+-----+---+
    //|you  |9  |
    //|ni   |4  |
    //|hello|2  |
    //|mno  |4  |
    //|time |9  |
    //+-----+---+
//        val query = wordCounts.writeStream
//              .outputMode("append")
//              .format("console")
//              .option("truncate", false)
//              .start()
    //Exception in thread "main" org.apache.spark.sql.AnalysisException: Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;;
    //没有水印 不支持该模式

    query.awaitTermination()


  }

}
