package com.spark.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * author Renault
 * date 2020/7/29 14:56
 * Description 
 */
object sparkUtils {

  /**
   * 注册spark信息
   */
  def getSpark() = {
    SparkSession
      .builder()
//      .config(new SparkConf().setMaster(master))
      .master("local[1]")
      .appName(getClass.getName)
      .getOrCreate()
  }

  /**
   * 获取hdfspath 并注册成表
   */
  def createOrReplaceTempView(spark: SparkSession, path: String, schema: String, headFlag: String, fileType: String, tableName: String,delimiter:String) = {
    var df: DataFrame = null
    fileType match {
      case "csv" => {
        df = spark.read
          .option("header", headFlag) //在csv第一行有属性"true"，没有就是"false"
          .option("inferSchema", headFlag) //这是自动推断属性列的数据类型
          .option("delimiter", delimiter)
          .csv(path)
          .toDF(schema.split(","): _*)
      }
        df.createOrReplaceTempView(tableName)
    }
  }
    /**
     * 执行sql 输出结果到文件路径
     */
    def querySqlAndSinK(spark: SparkSession, sql: String, path: String,delimiter:String) = {
      spark
        .sql(sql)
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .option("delimiter", delimiter)
        .format("csv")
        .save(path)
    }

}
