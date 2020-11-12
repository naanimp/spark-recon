package com.company.spark.recon.filetodb

import java.io.File

import com.company.spark.recon.filetodb.FileToDbAppConfig._
import com.company.spark.recon.filetofile.AppConfig.{postgresConfigJdbcFormat, postgresDbDriver, postgresDbPassword, postgresDbUrl, postgresDbUsername}
import com.company.spark.recon.filetofile.FileInfo
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, trim}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import scala.util.Try

object ReconFileToDBJob extends LazyLogging with Serializable {

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

  /**q
   * Loads the data from postgres db.
   *
   * @return Df: DataFrame .
   */
  def loadDatabaseTable(conn: DbConnectionInfo, query: String)(spark: SparkSession): DataFrame = {
    spark.read
      .format(conn.jdbcFomrat)
      .option("driver", conn.driver)
      .option("url", conn.url)
      .option("user", conn.username)
      .option("password", conn.password)
      //      .option("dbtable", query)
      .option("query", if (conn.dbtableOrQuery == "") query else conn.dbtableOrQuery)
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
  def saveMismatchedResults(conn: DbConnectionInfo, targetDf: DataFrame) = {
    targetDf.write.format(conn.jdbcFomrat)
      .option("driver", conn.driver)
      .option("url", conn.driver)
      .option("user", conn.username)
      .option("password", conn.password)
      .option("query", conn.dbtableOrQuery)
      .option("stringtype", "unspecified")
      .mode("append")
      .save()
  }

  def getUnionedSheets(fileInfo: FileInfo, fileName: String)(spark: SparkSession) = {
    var targetDf: DataFrame = null
    fileInfo.sheets.foldLeft(targetDf)((fdf, item) => {
      val df = Try(loadExcelSheet(item, fileInfo.basePath + File.separator + fileName)(spark)).getOrElse(null)
      println(" item... "+ item)
      df.show()
      if (fdf == null) df else if (df != null) fdf.union(df) else fdf
    })
  }

  def reconFileToPostgres(srcDf: DataFrame, initTgtDF:DataFrame, reconConfig: FileToDBReconConfig, dbType: String)(spark: SparkSession) = {

    val targetDf = reconConfig.targetPGdb.reconColumns.foldLeft(initTgtDF)((df, item) => {
      df.withColumnRenamed(item, "t_" + item)
    })

    val column: Column = null

    def joinConditions = reconConfig.keyColumns.foldLeft(column)((cl, item) => {
      val tcnmae = if(dbType == "db2") item.db2 else item.pg
      val df = trim(srcDf.col(item.src)).equalTo(trim(targetDf.col("t_" + tcnmae)))
      if (cl == null) df else cl.and(trim(srcDf.col(item.src)).equalTo(trim(targetDf.col("t_" + tcnmae))))
    })

    //    def nullCheckConditions = reconConfig.keyColumns.foldLeft(column)((cl, item) => {
    //      val df = targetDf.col("t_" + item).isNull
    //      if (cl == null) df else cl.and(targetDf.col("t_" + item).isNull)
    //    })

    val resDf2 = srcDf.join(targetDf, joinConditions, "left_outer")
    //.filter(nullCheckConditions)
    resDf2.cache()

    val resDF3 = addDataDifferenceColumn(resDf2, reconConfig, dbType)

    val discrepancyCnt = resDF3.count()
    resDF3.printSchema()
    resDF3.show(100)

    if (discrepancyCnt > 0) {
      logger.warn(s"------------ Warning: Found - ${discrepancyCnt} discrepancies ---------- ")
      logger.info(s"------------ Inserting discrepancies into sql-server database ---------- ")
      //        saveMismatchedResults(resDf)
      logger.info(s"------------ Inserted all discrepancies into sql-server database ---------- ")
    } else logger.info("------------ Success: There is no discrepancies ---------- ")
  }

  def addDataDifferenceColumn(df: DataFrame, reconConfig: FileToDBReconConfig, dbType: String) = {
    df.withColumn("data_difference",
      explode(array(
        reconConfig.columnMappings.map(name => {
          val cname = s"t_${if (dbType == "db2") name.db2 else name.pg}"
          when(col(cname) =!= col(name.pg), lit(null)).otherwise(concat_ws(" != ", col(cname), col(name.pg)))
        }): _*
      ))).filter(col("data_difference").isNotNull)
  }


  /**
   * Entry point to the job.
   */
  def main(args: Array[String]): Unit = {
    implicit val spark = buildSparkSession
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val metadataDf = loadDatabaseTable(pgConnectionInfo, metadataQuery)(spark)
    metadataDf.show()

    //    if(args.length < 2){
    //      logger.error(s"Argument length mismatched, found ${args.length}, please provide required parameters")
    //      System.exit(1)
    //    }
    //    val srcPath = args(0)
    //    val targetPath = args(1)

    //    val srcPath = "C://dev//workspace//recon//src//test//resources//HL7.xlsx"
    //    val targetPath = "C://dev//workspace//recon//src//test//resources//HL7_target.xlsx"


    val fileList = metadataDf.collectAsList().asScala
    if (fileList.length > 1) {
      metadataDf.collectAsList().asScala.foreach(job => {
//        val srcFileName = "HL7.xlsx"
//        val targetFileName = "HL7_target.xlsx"
//        val exceptionFileName = "HL7_exception.xlsx"

        val reconConfig = fileToDBReconConfig.apply(0)

        //      val srcDf = getUnionedSheets(reconConfig.srcFileInfo, srcFileName)
        //        .select(reconConfig.reconColumns.map(name => col(name)): _*)
        val srcDf = loadDatabaseTable(pgConnectionInfo, metadataQuery)(spark)
          .select(reconConfig.srcFileInfo.reconColumns.map(name => col(name)): _*)

        val initPgDf = loadDatabaseTable(pgConnectionInfo, metadataQuery)(spark)
          .select(reconConfig.targetPGdb.reconColumns.map(name => col(name)): _*)

        reconFileToPostgres(srcDf, initPgDf, reconConfig, "pg")(spark)

        val initDb2Df = loadDatabaseTable(pgConnectionInfo, metadataQuery)(spark)
          .select(reconConfig.targetPGdb.reconColumns.map(name => col(name)): _*)

        reconFileToPostgres(srcDf, initDb2Df, reconConfig, "db2")(spark)

        System.exit(0)
      })
    }
  }
}
