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
   * Loads the data from postgres db.
   *
   * @return Df: DataFrame .
   */
  def loadMetadataTable(query: String)(spark: SparkSession): DataFrame = {
    spark.read
      .format(postgresConfigJdbcFormat)
      .option("driver", postgresDbDriver)
      .option("url", postgresDbUrl)
      .option("user", postgresDbUsername)
      .option("password", postgresDbPassword)
      .option("query", query)
      .load()
  }

  /**
   * Loads the data from excel sheet.
   *
   * @return Df: DataFrame .
   */
  def loadExcelSheet(sheetName: String, filePath: String)(spark: SparkSession): DataFrame = {
    spark.read.
      format("com.crealytics.spark.excel")
      .option("header", "true")
      .option("dataAddress", s"'$sheetName'!A1")
      .option("treatEmptyValuesAsNulls", "false")
      .option("inferSchema", "false")
      .option("addColorColumns", "false")
      .load("file:///" + filePath)
  }

  /**
   * Save the mismatched records into target table in database.
   *
   * @param targetDf : Dataframe to save.
   */
  def saveMismatchedResults(targetDf: DataFrame) = {
    targetDf.write.format(sqlServerJdbcFormat)
      .option("driver", sqlServerDbDriver)
      .option("url", sqlServerDbUrl)
      .option("user", sqlServerDbUsername)
      .option("password", sqlServerDbPassword)
      .option("dbtable", sqlServerDbTable)
      .option("stringtype", "unspecified")
      .mode("append")
      .save()
  }

  /**
   * Entry point to the job.
   */
  def main(args: Array[String]): Unit = {
    implicit val spark = buildSparkSession
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val df = Seq(
      ("a@gmail.com", 0.9, "2020-10-02 03:04:00", "2020-12-02", "tx-id-1"),
      ("a@gmail.com", 0.9, "2020-10-12 03:04:00", "2020-12-02", "tx-id-1"),
      ("a@gmail.com", 0.9, "2020-10-30 03:04:00", "2020-12-02", "tx-id-1"),
      ("a@gmail.com", 0.8, "2020-11-02 03:04:00", "2020-12-02", "tx-id-2"),
      ("a@gmail.com", 0.8, "2020-11-03 03:04:00", "2020-12-02", "tx-id-2"),
      ("a@gmail.com", 0.1, "2020-12-05 03:04:00", "2020-12-02", "tx-id-2")
    ).toDF("email", "pscore", "transaction_datetm", "transaction_date", "transaction_id")

//    3
//    4
//    4
//    3
//    2
//    1
//    val expectedDF = Seq(
//      ("a@gmail.com", "2020-12-02 03:04:00", "1", "tx-id-1")
//    ).toDF("email", "transaction_datetm", "vel_accountEmailAddress_1day_count", "transaction_id")
//      .orderBy($"transaction_id")
//      .select("transaction_id", "transaction_datetm", "email", "vel_accountEmailAddress_1day_count")

//    val overCategory = Window.partitionBy('transaction_id).orderBy('transaction_datetm).rowsBetween(
//      Window.currentRow, Window.unboundedFollowing)
//
//    val sudf = udf((str:Seq[String]) => TextParser.filterShortWords2(str))
//    df.printSchema()
//    val df2 = df.withColumn("transaction_datetm", to_timestamp(col("transaction_datetm"), "yyyy-MM-dd HH:mm:ss"))
//      .withColumn("dates", collect_list('transaction_datetm) over overCategory)
//      .withColumn("Coutry wise count", sudf('dates))
//
//    df2.show(false)


    val overCategory2 = Window.partitionBy('transaction_id).orderBy('transaction_datetm)
    val df3 = df.withColumn("dates", collect_list('transaction_datetm) over overCategory2)
      .withColumn("count", size('dates).cast("int"))
    df3.show(false)
    df3.filter('count.gt(1)).show()

  }
}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter

object TextParser extends Serializable {

  def filterShortWords2(record: Seq[String]) = {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val currTxnDate = DateTime.parse(record.head.toString, formatter)

    record.tail.filter( x => {
      val txnDate = DateTime.parse(x.toString, formatter)
      currTxnDate.plusDays(30).getMillis > txnDate.getMillis
    }).size
  }

  def filterShortWords(record: String) = {
    frequencyWords(record).filter(x => x.length >= 5)
  }

  def frequencyWords(record: String) = {
    val PATTERN = "\\s|,|;|:|\\.|\\'|\"|!|\\?|\\[|\\]|\\(|\\)|\\{|\\}|\\<|\\>"
    record.split(PATTERN).toList; //.map(x => Row(x, 1))
  }
}