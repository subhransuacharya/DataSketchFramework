package com.ljmu.subhransu.core.distict

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.theta.{CompactSketch, SetOperation, Sketches, UpdateSketch}
import org.apache.spark.sql.{DataFrame, Encoder}

case class CustomSketchImpl() {

  implicit val myObjEncoder: Encoder[Array[Byte]] = org.apache.spark.sql.Encoders.kryo[Array[Byte]]

  def getDistinctCount(data: DataFrame, column: String): Double = {
    val mappedData = data.mapPartitions(iterator => {
      val updateSketch = UpdateSketch.builder().build()
      iterator.foreach(
        row => {
          updateSketch.update(row.getAs[String](column))
        }
      )
      List(updateSketch.compact().toByteArray).iterator
    })

    val finalSketchByteArray: Array[Byte] = mappedData.reduce((barray1, barray2) => {
      val tsk1 = Sketches.wrapSketch(Memory.wrap(barray1))
      val tsk2 = Sketches.wrapSketch(Memory.wrap(barray2))
      if (tsk1 == null && tsk2 == null) {
        UpdateSketch.builder.build.compact().toByteArray
      }

      if(tsk1 == null)
        tsk2.compact().toByteArray
      if(tsk2 == null)
        tsk1.compact().toByteArray

      val union = SetOperation.builder().buildUnion()
      val sketch = union.union(tsk1,tsk2)
      sketch.compact().toByteArray
    }
    )
    val finalUpdateSketch = Sketches.wrapSketch((Memory.wrap(finalSketchByteArray)))
    println(finalUpdateSketch)
    finalUpdateSketch.getEstimate
  }

}
