package com.company.spark.recon.filetofile

import java.io.File

import com.company.spark.recon.filetofile.AppConfig._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, trim}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.util.Try
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object SparkWindowFunJob extends LazyLogging with Serializable {

  /**
   * Creates the spark session for future purpose.
   *
   * @return Df: DataFrame .
   */
  def buildSparkSession: SparkSession = {
    sparkMaster match {
      case "local" => {
        SparkSession.builder
          .appName("ETL")
          .master("local[*]")
          .config("spark.driver.bindAddress", "127.0.0.1")
          .config("spark.driver.memory", "1G")
          .getOrCreate()
      }
      case _ => {
        SparkSession.builder
          .appName("ETL")
          .config("spark.master", sparkMaster)
          .config("spark.driver.bindAddress", "127.0.0.1")
          .getOrCreate()
      }
    }
  }


  /**
   * Entry point to the job.
   */
  def main(args: Array[String]): Unit = {
    implicit val spark = buildSparkSession
    import spark.implicits._
    spark.sparkContext.setLogLevel("INFO")

    import org.apache.spark.sql.functions.{get_json_object, json_tuple}

    val topic = "pgkyoqvg-test"
    val username = "pgkyoqvg"
    val password = "DXeme8qcmIfdv2IWhnbpvW1wdgzoxMTY"

    var streamingInputDF =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "rocket-01.srvs.cloudkafka.com:9094,rocket-02.srvs.cloudkafka.com:9094,rocket-03.srvs.cloudkafka.com:9094")
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("minPartitions", "10")
        .option("failOnDataLoss", "true")
        .load()
//        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//        .as[(String, String)]

    import org.apache.spark.sql.streaming.ProcessingTime
    import org.apache.spark.sql.functions._

//    var streamingSelectDF =
//      streamingInputDF
//        .select($"value".cast("string"))
//        .groupBy($"value")
//        .count()

    import org.apache.spark.sql.functions.{explode, split}
    val df = streamingInputDF.select(explode(split($"value".cast("string"), "\\s+")).as("word"))
      .groupBy($"word")
      .count
      .select($"word", $"count")

    val query = df
        .writeStream
        .format("console")
        .outputMode(OutputMode.Complete())
        .trigger(ProcessingTime("25 seconds"))
        .start()
        .awaitTermination()

  }
}