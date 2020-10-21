package com.demo.spark.structuredStreaming

import com.spark.utils.sparkUtils.getSpark
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType

/**
 * author Renault
 * date 2020/7/31 10:57
 * Description 
 */
object demo4 {
  def main(args: Array[String]): Unit = {
    /**
     * 测试csv 数据读取
     */
    val spark = getSpark()

    val userSchema = new StructType()
      .add("linenum","string")
      .add("starttime","string")
      .add("combflag","string")
      .add("durations","string")
      .add("phone","string")
      .add("imsi","string")
      .add("upunsignedintegertype","string")
      .add("visitflag","string")
      .add("pc_msc","string")
      .add("pc_hlr","string")
      .add("mscid","string")
      .add("location_visit","string")
      .add("location_home","string")
      .add("switchid","string")

    val csvdf = spark
      .readStream
      .option("sep",",")
      .schema(userSchema)
      .csv("data/")

    var query: StreamingQuery = null

    try{
       query =csvdf
           .select("linenum","pc_msc")
           .writeStream
           .option("checkpointLocation", "checkpoint/")
           .format("csv")
           .start("output/")

      query.awaitTermination(6000)

     }
     finally {
       if (query != null) {
         query.stop()
       }}

  }

}
