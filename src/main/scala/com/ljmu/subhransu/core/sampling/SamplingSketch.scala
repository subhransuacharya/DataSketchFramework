package com.ljmu.subhransu.core.sampling

import com.ljmu.subhransu.core.AbstractDistributedSketchFramework
import org.apache.datasketches.ArrayOfStringsSerDe
import org.apache.datasketches.memory.Memory
import org.apache.datasketches.sampling.{ReservoirItemsSketch, ReservoirItemsUnion}
import org.apache.spark.sql.{DataFrame, Dataset}

object SamplingSketch extends AbstractDistributedSketchFramework[ReservoirItemsSketch[String]]{
  val k = 32768

  def calculateSamples(data: DataFrame, column: String): String = {
    val mappedData = data.mapPartitions(iterator => {
      val reservoirSketch: ReservoirItemsSketch[String] = ReservoirItemsSketch.newInstance(k)
      iterator.foreach(
        row => {
          reservoirSketch.update(String.valueOf(row.getAs[String](column)))
        }
      )
      List(reservoirSketch.toByteArray(new ArrayOfStringsSerDe)).iterator
    })

    val finalSketchByteArray: Array[Byte] = mappedData.reduce((barray1, barray2) => {
      val reservoirSketch1 = ReservoirItemsSketch.heapify(Memory.wrap(barray1), new ArrayOfStringsSerDe)
      val reservoirSketch2 = ReservoirItemsSketch.heapify(Memory.wrap(barray2), new ArrayOfStringsSerDe)
      if (reservoirSketch1 == null && reservoirSketch2 == null) {
        ReservoirItemsSketch.newInstance(k).toByteArray(new ArrayOfStringsSerDe)
      }

      if(reservoirSketch1 == null)
        reservoirSketch2.toByteArray(new ArrayOfStringsSerDe)
      if(reservoirSketch2 == null)
        reservoirSketch1.toByteArray(new ArrayOfStringsSerDe)

      val union: ReservoirItemsUnion[String] = ReservoirItemsUnion.newInstance(k)

      union.update(reservoirSketch1)
      union.update(reservoirSketch2)
      union.getResult.toByteArray(new ArrayOfStringsSerDe)
    }
    )
    val finalUpdateSketch: ReservoirItemsSketch[String] = ReservoirItemsSketch.heapify(Memory.wrap(finalSketchByteArray), new ArrayOfStringsSerDe)
    println(finalUpdateSketch)

    println("First 10 results in union")
    finalUpdateSketch.getSamples().take(10).foreach(
      sample => println(">->> " + sample)
    )

    finalUpdateSketch.getNumSamples.toString
  }

  override protected def processPartitionedSketches(dataFrame: DataFrame, column: String): Dataset[Array[Byte]] = {
    dataFrame.mapPartitions(iterator => {
      val reservoirSketch: ReservoirItemsSketch[String] = ReservoirItemsSketch.newInstance(k)
      iterator.foreach(
        row => {
          reservoirSketch.update(String.valueOf(row.getAs[String](column)))
        }
      )
      List(reservoirSketch.toByteArray(new ArrayOfStringsSerDe)).iterator
    })
  }

  override protected def reduceSketches(partiallyCalculatedDf: Dataset[Array[Byte]]): Array[Byte] = {
    partiallyCalculatedDf.reduce((barray1, barray2) => {
      val reservoirSketch1 = ReservoirItemsSketch.heapify(Memory.wrap(barray1), new ArrayOfStringsSerDe)
      val reservoirSketch2 = ReservoirItemsSketch.heapify(Memory.wrap(barray2), new ArrayOfStringsSerDe)
      if (reservoirSketch1 == null && reservoirSketch2 == null) {
        ReservoirItemsSketch.newInstance(k).toByteArray(new ArrayOfStringsSerDe)
      }

      if(reservoirSketch1 == null)
        reservoirSketch2.toByteArray(new ArrayOfStringsSerDe)
      if(reservoirSketch2 == null)
        reservoirSketch1.toByteArray(new ArrayOfStringsSerDe)

      val union: ReservoirItemsUnion[String] = ReservoirItemsUnion.newInstance(k)

      union.update(reservoirSketch1)
      union.update(reservoirSketch2)
      union.getResult.toByteArray(new ArrayOfStringsSerDe)
    }
    )
  }

  override protected def calculateSketch(finalByteArray: Array[Byte]): ReservoirItemsSketch[String] = {
    ReservoirItemsSketch.heapify(Memory.wrap(finalByteArray), new ArrayOfStringsSerDe)
  }

  override def printReport(sketch: ReservoirItemsSketch[String]): Unit = {
    println()
    println("SamplingSketch-------------")
    println("First 10 results in the sample pool")
    sketch.getSamples().take(10).foreach(
      sample => println(sample)
    )
    println("================================")
  }
}
