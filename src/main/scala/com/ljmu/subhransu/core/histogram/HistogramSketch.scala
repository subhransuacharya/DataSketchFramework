package com.ljmu.subhransu.core.frequent

import org.apache.commons.codec.StringEncoderComparator
import org.apache.datasketches.ArrayOfStringsSerDe
import org.apache.datasketches.memory.Memory
import org.apache.datasketches.quantiles.{ItemsSketch, ItemsUnion}
import org.apache.datasketches.theta.{SetOperation, Sketches, UpdateSketch}
import org.apache.spark.sql.{DataFrame, Encoder}

import java.util.Comparator

case class FrequentValueSketch(){
  val k = 32768

  implicit val myObjEncoder: Encoder[Array[Byte]] = org.apache.spark.sql.Encoders.kryo[Array[Byte]]

  def calculateFrequencyCount(data: DataFrame, column: String): String = {
    val mappedData = data.mapPartitions(iterator => {
      val itemsSketch: ItemsSketch[String] = ItemsSketch.getInstance(k, Comparator.naturalOrder[String])//new ItemsSketch(65536)
      iterator.foreach(
        row => {
          itemsSketch.update(String.valueOf(row.getAs[String](column)))
        }
      )
      println("x")
      List(itemsSketch.toByteArray(new ArrayOfStringsSerDe())).iterator
    })

    val finalSketchByteArray: Array[Byte] = mappedData.reduce((barray1, barray2) => {
//      val itemsSketch1 = ItemsSketch.getInstance(Memory.wrap(barray1), new ArrayOfStringsSerDe())
      val itemsSketch1 = ItemsSketch.getInstance(Memory.wrap(barray1), Comparator.naturalOrder[String](), new ArrayOfStringsSerDe())
//      val itemsSketch1 = ItemsSketch.ge
      val itemsSketch2 = ItemsSketch.getInstance(Memory.wrap(barray2), Comparator.naturalOrder[String](), new ArrayOfStringsSerDe())
      if (itemsSketch1 == null && itemsSketch2 == null) {
        ItemsSketch.getInstance(k, Comparator.naturalOrder[String]).toByteArray(new ArrayOfStringsSerDe)
      }

      if(itemsSketch1 == null)
        itemsSketch2.toByteArray(new ArrayOfStringsSerDe)
      if(itemsSketch2 == null)
        itemsSketch1.toByteArray(new ArrayOfStringsSerDe)

      val union: ItemsUnion[String] = ItemsUnion.getInstance(k, Comparator.naturalOrder())
      union.update(itemsSketch1)
      union.update(itemsSketch2)
      union.toByteArray(new ArrayOfStringsSerDe())
    }
    )
    val finalUpdateSketch = ItemsSketch.getInstance(Memory.wrap(finalSketchByteArray), Comparator.naturalOrder[String](), new ArrayOfStringsSerDe())
//      Sketches.wrapSketch(Memory.wrap(finalSketchByteArray))
    println(finalUpdateSketch)
    finalUpdateSketch.getMaxValue.toString
  }

}
