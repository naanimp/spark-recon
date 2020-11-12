package com.company.spark.recon

import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object JobTest extends Serializable {

//Start date : 1604754233, End date : 1605013432
  def main(args: Array[String]) {


    val spark: SparkSession = SparkSession
      .builder()
      .appName("JDBC")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      //      .config("spark.sql.warehouse.dir", "C:/Exp/")
      .getOrCreate();

    spark.sparkContext.setLogLevel("WARN")

    val arrayStructureData = Seq(
      Row(List(Row("James ","","Smith")),List("Cricket","Movies"),Map("hair"->"black","eye"->"brown")),
      Row(List(Row("Michael ","Rose","")),List("Tennis"),Map("hair"->"brown","eye"->"black")),
      Row(List(Row("Robert ","","Williams")),List("Cooking","Football"),Map("hair"->"red","eye"->"gray")),
      Row(List(Row("Maria ","Anne","Jones")),null,Map("hair"->"blond","eye"->"red")),
      Row(List(Row("Jen","Mary","Brown")),List("Blogging"),Map("white"->"black","eye"->"black"))
    )


    val arrayStructureSchema = new StructType()
      .add("name", ArrayType(new StructType()
        .add("firstname",StringType)
        .add("lastname",StringType)))
      .add("hobbies", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))

    val df5 = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
//    df5.printSchema()
//    df5.show()

    val str =
      """
        |{
        |"a" : "1",
        |"b" : [
        |{"c1": "c11", "c2": "c12"},
        |{"c2": "c22", "c1": "c21"}
        |]
        |}
        |""".stripMargin

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    val strSchema = new StructType()
      .add("a", StringType)
      .add("b", ArrayType(new StructType()
        .add("c2",StringType)))


    val res = df5.withColumn("sds", from_json(lit(str), strSchema) )
      .withColumn("sds", explode(col("sds.b")) )
    res.printSchema()
    res.show()

//    df.printSchema()
//    df.show()
  }
}
