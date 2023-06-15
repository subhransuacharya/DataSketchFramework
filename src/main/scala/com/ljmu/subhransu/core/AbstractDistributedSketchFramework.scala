package com.ljmu.subhransu.core

import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

abstract class AbstractDistributedSketchFramework[T] {
  implicit val myObjEncoder: Encoder[Array[Byte]] = org.apache.spark.sql.Encoders.kryo[Array[Byte]]

  protected def processPartitionedSketches(dataFrame: DataFrame, column: String): Dataset[Array[Byte]]

  protected def reduceSketches(partiallyCalculatedDf: Dataset[Array[Byte]]): Array[Byte]

  protected def calculateSketch(finalByteArray: Array[Byte]): T

  def calculate(dataFrame: DataFrame, column: String): T = {
    val mappedData: Dataset[Array[Byte]] = processPartitionedSketches(dataFrame, column)
    val finalByteArray: Array[Byte] = reduceSketches(mappedData)
    val finalSketch: T = calculateSketch(finalByteArray)
    finalSketch
  }

  def printReport(sketch: T): Unit
}
