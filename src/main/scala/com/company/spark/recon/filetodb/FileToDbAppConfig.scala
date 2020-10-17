package com.company.spark.recon.filetodb

import com.typesafe.config.{Config, ConfigFactory, ConfigList}

import scala.collection.JavaConverters._

case class FileToDBFileInfo(sheets: List[String], basePath: String, reconColumns: List[String])

case class DBInfo(dbtableQuery: String, reconColumns: List[String])

case class FileToDBReconConfig(files: List[String], keyColumns: List[ComparableColumns], srcFileInfo: FileToDBFileInfo,
                               targetPGdb: DBInfo, targetDB2db: DBInfo, columnMappings:List[ComparableColumns])

case class DbConnectionInfo(url: String, driver: String, username: String, password: String,
                            jdbcFomrat: String, dbtableOrQuery: String = "")

case class ComparableColumns(src:String, pg:String, db2:String)

object FileToDbAppConfig {
  val config = ConfigFactory.load("application-file-to-db.conf")

  def getDBInfo(c: Config) = {
    val reconColumns = c.getStringList("recon-columns").asScala.toList
    DBInfo(c.getString("dbtable-query"), reconColumns)
  }

  def getFileInfo(c: Config) = {
    val reconColumns = c.getStringList("recon-columns").asScala.toList
    FileToDBFileInfo(c.getStringList("sheets").asScala.toList, c.getString("base-path"), reconColumns)
  }

  val sparkConfig = config.getConfig("spark")
  val appName = sparkConfig.getString("app-name")
  val sparkMaster = sparkConfig.getString("master")
  val sparkLoglevel = sparkConfig.getString("log-level")

  val metadataQuery = config.getString("pg-metadata-table-query")

  val fileToDBReconConfig = config.getConfigList("file-to-db-recon-config").asScala.map(x => {
    val files = x.getStringList("files").asScala.toList
    val keyColumns = x.getStringList("key-columns").asScala.toList
    val src = getFileInfo(x.getConfig("src"))
    val targetPGdb = getDBInfo(x.getConfig("target-pgdb"))
    val targetDB2db = getDBInfo(x.getConfig("target-db2db"))
    val columnMappings = x.getObject("column-mapping").asScala.map( x => {
      val dbColumns = x._2.asInstanceOf[ConfigList].asScala.toList.map( x=> {
        x.unwrapped().toString
      })
      ComparableColumns(x._1.toString, dbColumns.head, dbColumns.apply(1))
    }).toList
    val keyColumnMappings = x.getObject("key-column-mapping").asScala.map( x => {
      val dbColumns = x._2.asInstanceOf[ConfigList].asScala.toList.map( x=> {
        x.unwrapped().toString
      })
      ComparableColumns(x._1.toString, dbColumns.head, dbColumns.apply(1))
    }).toList
    FileToDBReconConfig(files, keyColumnMappings, src, targetPGdb, targetDB2db, columnMappings)
  })

  val pgConnectionInfo: DbConnectionInfo = {
    val postgresConfig = config.getConfig("postgres-connection-info")
    val postgresDbUrl = postgresConfig.getString("url")
    val postgresDbDriver = postgresConfig.getString("driver")
    val postgresDbUsername = postgresConfig.getString("username")
    val postgresDbPassword = postgresConfig.getString("password")
    val postgresConfigJdbcFormat = postgresConfig.getString("jdbc-format")
    DbConnectionInfo(postgresDbUrl, postgresDbDriver, postgresDbUsername, postgresDbPassword, postgresConfigJdbcFormat)
  }

  val db2ConnectionInfo: DbConnectionInfo = {
    val db2Config = config.getConfig("selfish-connection-info")
    val db2DbUrl = db2Config.getString("url")
    val db2DbDriver = db2Config.getString("driver")
    val db2DbUsername = db2Config.getString("username")
    val db2DbPassword = db2Config.getString("password")
    val db2ConfigJdbcFormat = db2Config.getString("jdbc-format")
    DbConnectionInfo(db2DbUrl, db2DbDriver, db2DbUsername, db2DbPassword, db2ConfigJdbcFormat)
  }

  val sqlServerConnectionInfo: DbConnectionInfo = {
    val sqlServerConfig = config.getConfig("sqlserver-mismatched-datatable")
    val sqlServerDbUrl = sqlServerConfig.getString("url")
    val sqlServerDbDriver = sqlServerConfig.getString("driver")
    val sqlServerDbUsername = sqlServerConfig.getString("username")
    val sqlServerDbPassword = sqlServerConfig.getString("password")
    val sqlServerJdbcFormat = sqlServerConfig.getString("jdbc-format")
    val sqlServerDbTable = sqlServerConfig.getString("dbtable")
    DbConnectionInfo(sqlServerDbUrl, sqlServerDbDriver, sqlServerDbUsername, sqlServerDbPassword, sqlServerJdbcFormat, sqlServerDbTable)
  }


}
