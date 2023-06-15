package com.ljmu.subhransu.example

import com.ljmu.subhransu.core.AbstractDistributedSketchFramework
import com.ljmu.subhransu.core.distict.DistinctValueSketch
import com.ljmu.subhransu.core.frequent.FrequentValueSketch
import com.ljmu.subhransu.core.histogram.HistogramSketch
import com.ljmu.subhransu.core.sampling.SamplingSketch
import org.apache.datasketches.frequencies.ItemsSketch
import org.apache.datasketches.theta.Sketch
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.catalyst.dsl.expressions

import scala.App
import org.apache.spark.sql.functions.{countDistinct, desc}

object BasicExample extends App {
  val sparkSession = SparkSession.builder().appName("DataSketch Framework").master("local[10]").getOrCreate()
  //    import spark.implicits._
  val sparkContext: SparkContext = sparkSession.sparkContext


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val simpleData = Seq(Row("James", "", "Smith", "36636", "M", 3000),
    Row("Michael", "Rose", "", "40288", "M", 4000),
    Row("Robert", "", "Williams", "42114", "M", 4000),
    Row("Maria", "Anne", "Jones", "39192", "F", 4000),
    Row("Jen", "Mary", "Brown", "", "F", -10000000)
  )

  val simpleSchema = StructType(Array(
    StructField("firstname", StringType, true),
    StructField("middlename", StringType, true),
    StructField("lastname", StringType, true),
    StructField("id", StringType, true),
    StructField("gender", StringType, true),
    StructField("salary", IntegerType, true)
  ))

  val df = sparkSession.createDataFrame(
    sparkSession.sparkContext.parallelize(simpleData), simpleSchema)
  import sparkSession.implicits._

  val actualData = sparkSession.read.parquet("/demo/df")
//  actualData.show(10)

  def basicApproach= {
    var start = System.currentTimeMillis()
    actualData.agg(countDistinct("password")).show()
    timeTaken(start, System.currentTimeMillis())


    start = System.currentTimeMillis()
    actualData.select("password").groupBy("password").count().orderBy(desc("count")).show()
    timeTaken(start, System.currentTimeMillis())


    start = System.currentTimeMillis()
    val histogram = actualData.select("hourOfDay").map(
      row => row.getInt(0)
    ).rdd.histogram(3)
    println()
    histogram._1.foreach(elem => print(elem + "\t "))
    println()
    histogram._2.foreach(elem => print(elem + "\t "))
    timeTaken(start, System.currentTimeMillis())
  }


  def dataSketchFrameWorkApproach = {

    var start = System.currentTimeMillis()
    val distinctValueSketch: Sketch = DistinctValueSketch.calculate(actualData, "password")
    timeTaken(start, System.currentTimeMillis())
    DistinctValueSketch.printReport(distinctValueSketch)

    start = System.currentTimeMillis()
    val frequentValueSketch: ItemsSketch[String] = FrequentValueSketch.calculate(actualData, "password")
    timeTaken(start, System.currentTimeMillis())
    FrequentValueSketch.printReport(frequentValueSketch)

    start = System.currentTimeMillis()
    val histogramSketch = HistogramSketch.calculate(actualData, "hourOfDay")
    timeTaken(start, System.currentTimeMillis())
    HistogramSketch.printReport(histogramSketch)

    start = System.currentTimeMillis()
    val samplingSketch = SamplingSketch.calculate(actualData, "hourOfDay")
    SamplingSketch.printReport(samplingSketch)
    timeTaken(start, System.currentTimeMillis())
  }

  basicApproach
  dataSketchFrameWorkApproach

  def timeTaken(start: Long, end: Long) = {
    println()
    println("Time Taken: " + (end - start) + "ms")
  }
}
