package com.company.spark.recon.filetofile

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._

case class FileInfo(sheets:List[String], basePath:String)
case class ReconConfig(files:List[String], reconColumns:List[String],  srcFileInfo: FileInfo, targetFileInfo: FileInfo, exceptionFileInfo: FileInfo)

object AppConfig {
  val config = ConfigFactory.load("application.conf")

  def getFileInfo(c:Config) = {
    FileInfo(c.getStringList("sheets").asScala.toList,
      c.getString("base-path"))
  }

  val reconConfigList = config.getConfigList("recon-config").asScala.map( x => {
    val files = x.getStringList("files").asScala.toList
    val reconColumns = x.getStringList("recon-columns").asScala.toList
    val src = getFileInfo(x.getConfig("src"))
    val target = getFileInfo(x.getConfig("target"))
    val exception = getFileInfo(x.getConfig("exception"))
    ReconConfig(files, reconColumns, src, target, exception)
  })

//  val reconConfig = config.getConfig("recon")
//  val keyColumns = reconConfig.getStringList("key-columns")
//  val aggrColumns = reconConfig.getStringList("aggr-columns")
//  val nullCheckColumns = reconConfig.getStringList("null-check-columns")

//  val excelSheetsConfig = config.getConfig("excel-sheets")
//  val srcSheet = excelSheetsConfig.getString("src-sheet")
//  val targetSheets = excelSheetsConfig.getStringList("target-sheets").asScala

  val sparkConfig = config.getConfig("spark")
  val appName = sparkConfig.getString("app-name")
  val sparkMaster = sparkConfig.getString("master")
  val sparkLoglevel = sparkConfig.getString("log-level")

  val postgresConfig = config.getConfig("postgres-file-metadata-table")
  val postgresConfigJdbcFormat = postgresConfig.getString("jdbc-format")
  val postgresDbTable = postgresConfig.getString("dbtable")
  val postgresDbQuery = postgresConfig.getString("query")
  val postgresDbUrl = postgresConfig.getString("url")
  val postgresDbUsername = postgresConfig.getString("username")
  val postgresDbPassword = postgresConfig.getString("password")
  val postgresDbDriver = postgresConfig.getString("driver")

  val sqlServerConfig = config.getConfig("sqlserver-mismatched-datatable")
  val sqlServerJdbcFormat = sqlServerConfig.getString("jdbc-format")
  val sqlServerDbTable = sqlServerConfig.getString("dbtable")
  val sqlServerDbUrl = sqlServerConfig.getString("url")
  val sqlServerDbUsername = sqlServerConfig.getString("username")
  val sqlServerDbPassword = sqlServerConfig.getString("password")
  val sqlServerDbDriver = sqlServerConfig.getString("driver")

}
