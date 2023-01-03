package com.ljmu.subhransu.core.histogram

import org.apache.datasketches.ArrayOfStringsSerDe
import org.apache.datasketches.memory.Memory
import org.apache.datasketches.quantiles.{DoublesSketch, DoublesUnion, ItemsSketch, ItemsUnion, UpdateDoublesSketch}
import org.apache.spark.sql.{DataFrame, Encoder}

import java.util.Comparator

case class HistogramSketch(){
  val k = 32768

  implicit val myObjEncoder: Encoder[Array[Byte]] = org.apache.spark.sql.Encoders.kryo[Array[Byte]]

  def calculateHistogram(data: DataFrame, column: String): String = {
    val mappedData = data.mapPartitions(iterator => {
      val doublesSketch: UpdateDoublesSketch = DoublesSketch.builder().build()
      iterator.foreach(
        row => {
          doublesSketch.update(String.valueOf(row.getAs[String](column)).toDouble)
        }
      )
      List(doublesSketch.toByteArray()).iterator
    })

    val finalSketchByteArray: Array[Byte] = mappedData.reduce((barray1, barray2) => {
      val doublesSketch1 = DoublesSketch.wrap(Memory.wrap(barray1))
      val doublesSketch2 = DoublesSketch.wrap(Memory.wrap(barray2))
      if (doublesSketch1 == null && doublesSketch2 == null) {
        DoublesSketch.builder().build().toByteArray()
      }

      if(doublesSketch1 == null)
        doublesSketch2.toByteArray()
      if(doublesSketch2 == null)
        doublesSketch1.toByteArray()

      val union: DoublesUnion = DoublesUnion.builder().build()

      union.update(doublesSketch1)
      union.update(doublesSketch2)
      union.toByteArray
    }
    )
    val finalUpdateSketch = DoublesSketch.wrap(Memory.wrap(finalSketchByteArray))
    println(finalUpdateSketch)

    println("Min, Skewness, Max Values")
    println(finalUpdateSketch.getQuantiles(Array(0.0, 0.5, 1.0)).mkString("Array(", ", ", ")"))

    println("Probability Histogram: Estimated Probability Mass in 4 bins (-inf, -2) (-2, 0) (0, 2) (2, inf)")
    println(finalUpdateSketch.getPMF(Array(-2, 0, 2)).mkString("Array(", ", ", ")"))

    println("Frequency Histogram: Estimated number of original values in same bins")
    val hist = finalUpdateSketch.getPMF(Array(-2, 0, 2))
    hist.foreach(d => println(">->" + (d*finalUpdateSketch.getN)))

    finalUpdateSketch.getMaxValue.toString
  }

}
