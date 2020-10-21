package com.demo.spark.structuredStreaming
import com.spark.utils.sparkUtils._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
/**
 * author Renault
 * date 2020/7/29 10:38
 * Description 
 */
object demo3 {
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
    println(csvdf.isStreaming)

    //将流实例化为表
//  val rs=  csvdf.select("linenum","switchid")
//      .writeStream
//      .format("parquet")
//      //必须指定checkpoint路径
//      //checkpointLocation must be specified either through option("checkpointLocation", ...)
//      // or SparkSession.conf.set("spark.sql.streaming.checkpointLocation
//      .option("checkpointLocation", "checkpoint/")
//      .option("path", "output/")
//      .outputMode("append")
//      .start()

    val rs = csvdf
      .select("linenum","switchid")
      .writeStream
      .queryName("testtable")
      .format("memory")
      .outputMode("append")
      .start()
    rs.awaitTermination()
 rs.stop()

//



  }

}
