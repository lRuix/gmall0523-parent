package com.atguigu.gmall.realtime.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._


/**
 * @Author iRuiX
 * @Date 2021/3/28 23:36
 * @Version 1.0
 * @Desc xxxxx
 */
object DataLoader {

  //vin,BMS_TempProbe,carTime,timestamp,BMS_HVBatCellTempAve,BMS_HVBatCellTempMax,BMS_HVBatCellTempMin
  //LL227409XJW111550,
  // [12.0,12.0,12.0,1],
  // 1614553699000,
  // 1614554554381,
  // 12.0,
  // 13.0,
  // 12.0

  //case class TempProbe(vin: String,BMS_TempProbe: Array[String], carTime: String,timestamp: String,BMS_HVBatCellTempAve: String,BMS_HVBatCellTempMax: String,BMS_HVBatCellTempMin: String)

  val DATA_PATH = "G:\\Project\\gmall0523-realtime\\src\\main\\resources\\vin,BMS_TempProbe,carTime,timestamp,BMS_.csv"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    import spark.implicits._



    val cDF = spark.read

      .option("sep","'")
      .option("inferSchema", "true")
      //指定一个指示空值的字符串
      .option("nullvalue", "null")
      //当设置为 true 时，第一行文件将被用来命名列，而不包含在数据中
      .option("header", "true")
      .csv(DATA_PATH)
      //.as[TempProbe]
      .toDF()
      .cache()

    val originDF = cDF.withColumn("carTime",col("carTime").cast(TimestampType))
        .withColumn("timestamp",col("timestamp").cast(TimestampType))
      .cache()



    val uniqueDF = originDF
      .select("vin","carTime","timestamp")
      .withColumn("rn",row_number over(Window partitionBy("vin","carTime") orderBy("timestamp")))
      .withColumn("isUnique",expr("rn=1"))

    val corrDF = originDF
      .withColumn("BMS_TempProbe",expr("parseLength2Array(BMS_TempProbe)"))
      .printSchema







  }

}
