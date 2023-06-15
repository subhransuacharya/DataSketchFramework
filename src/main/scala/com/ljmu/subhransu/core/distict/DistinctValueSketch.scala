package com.ljmu.subhransu.core.distict

import com.ljmu.subhransu.core.AbstractDistributedSketchFramework
import org.apache.datasketches.memory.Memory
import org.apache.datasketches.theta.{SetOperation, Sketch, Sketches, UpdateSketch}
import org.apache.spark.sql.{DataFrame, Dataset}

object DistinctValueSketch extends AbstractDistributedSketchFramework[Sketch]{

  def calculateDistinctCount(data: DataFrame, column: String): Double = {
    val mappedData: Dataset[Array[Byte]] = data.mapPartitions(iterator => {
      val updateSketch = UpdateSketch.builder().build()
      iterator.foreach(
        row => {
          updateSketch.update(row.getAs[String](column))
        }
      )
      List(updateSketch.compact().toByteArray).iterator
    })

    val finalSketchByteArray: Array[Byte] = mappedData.reduce((barray1, barray2) => {
      val thetaSketch1 = Sketches.wrapSketch(Memory.wrap(barray1))
      val thetaSketch2 = Sketches.wrapSketch(Memory.wrap(barray2))
      if (thetaSketch1 == null && thetaSketch2 == null) {
        UpdateSketch.builder.build.compact().toByteArray
      }

      if(thetaSketch1 == null)
        thetaSketch2.compact().toByteArray
      if(thetaSketch2 == null)
        thetaSketch1.compact().toByteArray

      val union = SetOperation.builder().buildUnion()
      val sketch = union.union(thetaSketch1,thetaSketch2)
      sketch.compact().toByteArray
    }
    )
    val finalUpdateSketch: Sketch = Sketches.wrapSketch((Memory.wrap(finalSketchByteArray)))
    println(finalUpdateSketch)
    finalUpdateSketch.getEstimate
  }

  override protected def processPartitionedSketches(dataFrame: DataFrame, column: String): Dataset[Array[Byte]] = {
    dataFrame.mapPartitions(iterator => {
      val updateSketch = UpdateSketch.builder().build()
      iterator.foreach(
        row => {
          updateSketch.update(row.getAs[String](column))
        }
      )
      List(updateSketch.compact().toByteArray).iterator
    })
  }

  override protected def reduceSketches(partiallyCalculatedDf: Dataset[Array[Byte]]): Array[Byte] = {
    partiallyCalculatedDf.reduce((barray1, barray2) => {
      val thetaSketch1 = Sketches.wrapSketch(Memory.wrap(barray1))
      val thetaSketch2 = Sketches.wrapSketch(Memory.wrap(barray2))
      if (thetaSketch1 == null && thetaSketch2 == null) {
        UpdateSketch.builder.build.compact().toByteArray
      }

      if(thetaSketch1 == null)
        thetaSketch2.compact().toByteArray
      if(thetaSketch2 == null)
        thetaSketch1.compact().toByteArray

      val union = SetOperation.builder().buildUnion()
      val sketch = union.union(thetaSketch1,thetaSketch2)
      sketch.compact().toByteArray
    }
    )
  }

  override protected def calculateSketch(finalByteArray: Array[Byte]): Sketch = {
    Sketches.wrapSketch((Memory.wrap(finalByteArray)))
  }

  override def printReport(sketch: Sketch) = {
    println()
    println("DistinctValueSketch-------------")
    println("Number of estimated Distinct Values are: " + sketch.getEstimate)
    println("================================")
  }
}
