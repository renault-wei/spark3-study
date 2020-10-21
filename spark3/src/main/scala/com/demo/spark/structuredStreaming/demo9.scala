package com.demo.spark.structuredStreaming

import com.spark.utils.sparkUtils.getSpark
import org.apache.spark.sql.streaming.StreamingQuery

/**
 * author Renault
 * date 2020/8/3 14:14
 * Description 
 */
object demo9 {
  /**
   * join with static data
   */
  def main(args: Array[String]): Unit = {
    val spark = getSpark()

    /**
     *step1
     */
    import spark.implicits._

    var query: StreamingQuery = null
    try{

      val kafkadf  =spark
      .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "bdp71:9092")
        .option("subscribe", "spark")
        //      .option("kafka.session.timeout.ms","100")
        .option("startingOffsets","earliest")
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String,String)]
        .map(line=>{
          val v =line._2.split(",")
          (v(0),v(1),v(2),v(3),v(4))
        }).toDF("")
      val scoredf = spark
        .read
        .textFile("data/score.txt")
        .map(line=>{
          val split= line.split(",")

        })
    }finally {
      if(query!=null){
        query.stop()
      }

    }
  }
}
