package com.company.spark

import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.sql.SQLException
import java.util

import com.company.spark.AppConfig._

object PgConnection {

  def getConnection() = {
    Class.forName(postgresDbDriver)
    DriverManager.getConnection(postgresDbUrl, postgresDbUsername, postgresDbPassword)
  }

  @throws[SQLException]
  def getAllPersons: util.ArrayList[Nothing] = {
    val array = new util.ArrayList[Nothing]
    val result = getConnection.prepareStatement("select * from target").executeQuery
    while (result.next) {
      println(result.getString("ODS_ID"))
    }
    result.close
    array
  }

//  @throws[SQLException]
//  def addPerson(person: Nothing): Unit = {
//    val sql = "insert into person(name, identity, birthday)" + "values (?,?,?)"
//    val ps = getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
//    ps.setString(1, person.getName)
//    ps.setString(2, person.getIdentity)
//    ps.setString(3, person.getBirthday)
//    //use execute update when the database return nothing
//    ps.executeUpdate
//    val generatedKeys = ps.getGeneratedKeys
//    if (generatedKeys.next) person.setId(generatedKeys.getInt(1))
//  }

//  def main(args: Array[String]): Unit = {
//    getAllPersons
//  }

}
