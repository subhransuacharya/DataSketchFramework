package com.ljmu.subhransu.core.frequent

import com.ljmu.subhransu.core.AbstractDistributedSketchFramework
import org.apache.datasketches.ArrayOfStringsSerDe
import org.apache.datasketches.frequencies.{ErrorType, ItemsSketch}
import org.apache.datasketches.memory.Memory
import org.apache.spark.sql.{DataFrame, Dataset}

import java.util.Comparator

object FrequentValueSketch extends AbstractDistributedSketchFramework[ItemsSketch[String]] {
  val k = 32768

  def calculateFrequencyCount(data: DataFrame, column: String): String = {
    val mappedData = data.mapPartitions(iterator => {
      val itemsSketch: ItemsSketch[String] = new ItemsSketch(k) //new ItemsSketch(65536)
      iterator.foreach(
        row => {
          itemsSketch.update(String.valueOf(row.getAs[String](column)))
        }
      )
      List(itemsSketch.toByteArray(new ArrayOfStringsSerDe())).iterator
    })

    val finalSketchByteArray: Array[Byte] = mappedData.reduce((barray1, barray2) => {
      //      val itemsSketch1 = ItemsSketch.getInstance(Memory.wrap(barray1), new ArrayOfStringsSerDe())
      val itemsSketch1 = ItemsSketch.getInstance(Memory.wrap(barray1), new ArrayOfStringsSerDe())
      //      val itemsSketch1 = ItemsSketch.ge
      val itemsSketch2 = ItemsSketch.getInstance(Memory.wrap(barray2), new ArrayOfStringsSerDe())
      if (itemsSketch1 == null && itemsSketch2 == null) {
        new ItemsSketch(k)
      }

      if (itemsSketch1 == null)
        itemsSketch2.toByteArray(new ArrayOfStringsSerDe)
      if (itemsSketch2 == null)
        itemsSketch1.toByteArray(new ArrayOfStringsSerDe)

      val union: ItemsSketch[String] = new ItemsSketch[String](k)
      union.merge(itemsSketch1)
      union.merge(itemsSketch2)
      union.toByteArray(new ArrayOfStringsSerDe())
    }
    )
    val finalUpdateSketch: ItemsSketch[String] = ItemsSketch.getInstance(Memory.wrap(finalSketchByteArray), new ArrayOfStringsSerDe())
    //      Sketches.wrapSketch(Memory.wrap(finalSketchByteArray))
    println(finalUpdateSketch)
    finalUpdateSketch.getCurrentMapCapacity.toString
  }

  override protected def processPartitionedSketches(dataFrame: DataFrame, column: String): Dataset[Array[Byte]] = {
    dataFrame.mapPartitions(iterator => {
      val itemsSketch: ItemsSketch[String] = new ItemsSketch(k) //new ItemsSketch(65536)
      iterator.foreach(
        row => {
          itemsSketch.update(String.valueOf(row.getAs[String](column)))
        }
      )
      List(itemsSketch.toByteArray(new ArrayOfStringsSerDe())).iterator
    })
  }

  override protected def reduceSketches(partiallyCalculatedDf: Dataset[Array[Byte]]): Array[Byte] = {
    partiallyCalculatedDf.reduce((barray1, barray2) => {
      //      val itemsSketch1 = ItemsSketch.getInstance(Memory.wrap(barray1), new ArrayOfStringsSerDe())
      val itemsSketch1 = ItemsSketch.getInstance(Memory.wrap(barray1), new ArrayOfStringsSerDe())
      //      val itemsSketch1 = ItemsSketch.ge
      val itemsSketch2 = ItemsSketch.getInstance(Memory.wrap(barray2), new ArrayOfStringsSerDe())
      if (itemsSketch1 == null && itemsSketch2 == null) {
        new ItemsSketch(k)
      }

      if (itemsSketch1 == null)
        itemsSketch2.toByteArray(new ArrayOfStringsSerDe)
      if (itemsSketch2 == null)
        itemsSketch1.toByteArray(new ArrayOfStringsSerDe)

      val union: ItemsSketch[String] = new ItemsSketch[String](k)
      union.merge(itemsSketch1)
      union.merge(itemsSketch2)
      union.toByteArray(new ArrayOfStringsSerDe())
    }
    )
  }

  override protected def calculateSketch(finalByteArray: Array[Byte]): ItemsSketch[String] = {
    ItemsSketch.getInstance(Memory.wrap(finalByteArray), new ArrayOfStringsSerDe())
  }

  override def printReport(sketch: ItemsSketch[String]): Unit = {
    println()
    println("FrequentValueSketch-------------")
    println()
    val frequentItems = sketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES).take(20)
    val sortedItems = frequentItems.sortBy(item => item.getEstimate)(Ordering[Long].reverse).take(20)
    println(ItemsSketch.Row.getRowHeader)
    sortedItems.foreach(
      item => println(item.toString)
    )
    println("================================")
  }
}
