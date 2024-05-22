package com.spark.sql

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.roaringbitmap.RoaringBitmap
import com.spark.util.BitMapUtil.{deserializeBitMap, serializeBitMap}

object BitMapOrMergeUDAF extends Aggregator[Array[Byte], Array[Byte], Array[Byte]]{
  override def zero: Array[Byte] = {
    val bitmap: RoaringBitmap = RoaringBitmap.bitmapOf()
    serializeBitMap(bitmap)
  }

  override def reduce(b: Array[Byte], a: Array[Byte]): Array[Byte] = {
    val bitmap1: RoaringBitmap = deserializeBitMap(b)
    val bitmap2: RoaringBitmap = deserializeBitMap(a)
    bitmap1.or(bitmap2)
    serializeBitMap(bitmap1)
  }

  override def merge(b1: Array[Byte], b2: Array[Byte]): Array[Byte] = reduce(b1, b2)

  override def finish(reduction: Array[Byte]): Array[Byte] = reduction

  override def bufferEncoder: Encoder[Array[Byte]] = Encoders.BINARY

  override def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
}
