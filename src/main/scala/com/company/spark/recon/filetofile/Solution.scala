package com.company.spark.recon.filetofile

import scala.collection.JavaConverters._

// you can write to stdout for debugging purposes, e.g.
// println("this is a debug message")

object Solution {
  def main(args: Array[String]): Unit = {
//    //N = 1041 the function should return 5
//    solution(1041)
//    //N = 1041 the function should return 5
//    solution(15)
    solution(32)
  }

  def solution(n: Int): Int = {
    // write your code in Scala 2.12
    val max = 2147483647
    val min: Int = 1

    if(n<=max && n >= min){
      val a = Integer.toBinaryString(n).split("1").filter(x => x.length>0 )
      println(a.length)
      println(a.mkString(","))
      println(if(a.length>0) a.map(x=>x.length).max else "0")
      if(a.length>0) a.map(x=>x.length).max else 1
    }
   0
  }
}
