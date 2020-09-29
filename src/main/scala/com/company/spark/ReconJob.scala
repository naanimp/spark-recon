package com.company.spark

import java.io.File

import com.company.spark.AppConfig._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col

import scala.collection.JavaConverters._
import scala.util.Try

object ReconJob extends LazyLogging with Serializable {

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

//    val metadataDf = loadMetadataTable(postgresDbQuery.replace("$CODE", args(0)))(spark)


    //    if(args.length < 2){
    //      logger.error(s"Argument length mismatched, found ${args.length}, please provide required parameters")
    //      System.exit(1)
    //    }
    //    val srcPath = args(0)
    //    val targetPath = args(1)

    //    val srcPath = "C://dev//workspace//recon//src//test//resources//HL7.xlsx"
    //    val targetPath = "C://dev//workspace//recon//src//test//resources//HL7_target.xlsx"
    def getUnionedSheets(fileInfo: FileInfo, fileName: String) = {
      var targetDf: DataFrame = null
      fileInfo.sheets.foldLeft(targetDf)((fdf, item) => {
        val df = Try(loadExcelSheet(item, fileInfo.basePath + File.separator + fileName)(spark)).getOrElse(null)
        if (fdf == null) df else if (df != null) fdf.union(df) else fdf
      })
    }

//    val fileList = metadataDf.collectAsList().asScala
//    if (fileList.length > 1) logger.error(s"Found multiple records for the code : ${args(0)}")
//    else
    {
      //    metadataDf.collectAsList().asScala.foreach(job => {
      val srcFileName = "HL7.xlsx"
      val targetFileName = "HL7_target.xlsx"
      val exceptionFileName = "HL7_exception.xlsx"

      val reconConfig = reconConfigList.apply(0)

      val srcDf = getUnionedSheets(reconConfig.srcFileInfo, srcFileName)
        .select(reconConfig.reconColumns.map(name => col(name)): _*)

      val targetDf = getUnionedSheets(reconConfig.targetFileInfo, targetFileName)
        .select(reconConfig.reconColumns.map(name => col(name)): _*)

      val exceptionDf = getUnionedSheets(reconConfig.exceptionFileInfo, exceptionFileName)
        .select(reconConfig.reconColumns.map(name => col(name)): _*)

      val finalTargetDf = targetDf.union(exceptionDf)

      val column: Column = null

      def joinConditions = reconConfig.reconColumns.foldLeft(column)((cl, item) => {
        val df = trim(srcDf.col(item)).equalTo(trim(finalTargetDf.col(item)))
        if (cl == null) df else cl.and(trim(srcDf.col(item)).equalTo(trim(finalTargetDf.col(item))))
      })

      def nullCheckConditions = reconConfig.reconColumns.foldLeft(column)((cl, item) => {
        val df = finalTargetDf.col(item).isNull
        if (cl == null) df else cl.and(finalTargetDf.col(item).isNull)
      })

      val resDf2 = srcDf.join(finalTargetDf, joinConditions, "left_outer")
        .filter(nullCheckConditions)

      resDf2.cache()

      val discrepancyCnt = resDf2.count()
      resDf2.show(100)

      if (discrepancyCnt > 0) {
        logger.warn(s"------------ Warning: Found - ${discrepancyCnt} discrepancies ---------- ")
        logger.info(s"------------ Inserting discrepancies into sql-server database ---------- ")
        //        saveMismatchedResults(resDf)
        logger.info(s"------------ Inserted all discrepancies into sql-server database ---------- ")
      } else logger.info("------------ Success: There is no discrepancies ---------- ")


      System.exit(0)
      //    })
    }
  }
}
