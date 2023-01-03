package com.ljmu.subhransu.core

import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

abstract class Processor {
  implicit val myObjEncoder: Encoder[Array[Byte]] = org.apache.spark.sql.Encoders.kryo[Array[Byte]]

  protected def processPartitionedSketches(dataFrame: DataFrame, column: String): Dataset[Array[Byte]]

  protected def reduceSketches(partiallyCalculatedDf: Dataset[Array[Byte]]): Array[Byte]

  protected def calculateSketch(finalByteArray: Array[Byte]): Any

  def calculate(dataFrame: DataFrame, column: String): Any = {
    val mappedData: Dataset[Array[Byte]] = processPartitionedSketches(dataFrame, column)
    val finalByteArray: Array[Byte] = reduceSketches(mappedData)
    val finalSketch = calculateSketch(finalByteArray)
    finalSketch
  }
}
