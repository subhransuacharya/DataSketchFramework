package com.ljmu.subhransu.datagenerator

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.rdd.RDD

import java.lang.Double
import scala.math.BigDecimal.RoundingMode

object MockDataGenerator extends App {
  val sparkSession = SparkSession.builder().appName("DataSketch Framework").master("local[10]").getOrCreate()
  val sparkContext = sparkSession.sparkContext
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val rnd = new scala.util.Random
//  val upperlimit = 100000000
  val upperlimit: Int = 10000000
  import sparkSession.implicits._

  def randomString(length: Int) = {
    val r = new scala.util.Random
    val sb = new StringBuilder
    for (i <- 1 to length) {
      sb.append(r.nextPrintableChar)
    }
    sb.toString
  }

  def nextDouble(input: Int) = {
    BigDecimal.valueOf(input)
      .setScale(3, RoundingMode.HALF_UP)
      .doubleValue();
  }

//  (x, x%24, x/rnd.nextDouble(), rnd.nextString(4))
  val start = System.currentTimeMillis()
  val df = sparkContext
    .parallelize((1 to upperlimit).map(x => (x, x%24, x/rnd.nextDouble(), randomString(4))))
    .toDF("id","hourOfDay","value","password")
//  df.show(100)
    df.write.parquet("/demo/df")
  println(System.currentTimeMillis()-start)
}
