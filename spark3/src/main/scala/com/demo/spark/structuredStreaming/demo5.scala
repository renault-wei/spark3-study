package com.demo.spark.structuredStreaming
import com.spark.utils.sparkUtils._
import org.apache.spark.sql.streaming.StreamingQuery
/**
 * author Renault
 * date 2020/7/31 14:10
 * Description 
 */
object demo5 {
  def main(args: Array[String]): Unit = {
    /**
     * kafkawithtable
     */
    val spark = getSpark()

    /**
     * 获取kafka数据
     */
    val kafkadf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "bdp71:9092")
      .option("subscribe", "spark")
      //      .option("kafka.session.timeout.ms","100")
      .option("startingOffsets","earliest")
      .load()
    import spark.implicits._

    var query: StreamingQuery = null

    try{
      query = kafkadf
          .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          .as[(String,String)]
          .writeStream
          .queryName("kafkatable")
          .outputMode("append")
          .format("memory")
          .start()

      query.awaitTermination(10000)

      spark.sql("select * from kafkatable").show()

    }
    finally {
      if (query != null) {
        query.stop()
      }
    }
  }

}
