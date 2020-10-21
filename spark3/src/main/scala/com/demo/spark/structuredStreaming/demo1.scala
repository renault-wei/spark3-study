package com.demo.spark.structuredStreaming
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
/**
 * author Renault
 * date 2020/7/28 14:36
 * Description 
 */
object demo1 {



  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .getOrCreate()


    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "bdp71")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()
    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }


}
