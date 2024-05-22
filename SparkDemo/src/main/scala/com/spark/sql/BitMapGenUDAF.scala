package com.spark.sql

import com.spark.util.BitMapUtil.serializeBitMap
import com.spark.util.BitMapUtil.deserializeBitMap
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.roaringbitmap.RoaringBitmap



/**
 * 定义一个自定义聚合函数（User-Defined Aggregator）用于生成位图。
 * 该聚合函数将用户id写入bitmap中，不会重复写入。
 * 因为需要两个编码器，RoaringBitmap没有对应的编码器，所以我们用了序列化后的字节数组来代替，用的时候需要反序列化。
 */
object BitMapGenUDAF extends Aggregator[Int, Array[Byte], Array[Byte]]{
  /**
   * 初始化函数，用于创建一个初始的位图数组,用来初始化中间缓存结构。
   * @return 返回一个初始的位图数组。
   */
  override def zero: Array[Byte] = {
    // 创建一个空的bitmap，并序列化为字节数组，交还给框架
    val bitmap: RoaringBitmap = RoaringBitmap.bitmapOf()
    serializeBitMap(bitmap)
  }

  /**
   * 局部聚合函数，用于将一个整数添加到位图数组中。
   * @param b 当前的位图数组。
   * @param a 需要添加到位图中的整数。
   * @return 返回更新后的位图数组。
   */
  override def reduce(b: Array[Byte], a: Int): Array[Byte] = {
    // 将序列化后的buff反序列化成bitmap
    val bitmap: RoaringBitmap = deserializeBitMap(b)
    // 将整数添加到bitmap中
    bitmap.add(a)
    // 序列化bitmap并返回
    serializeBitMap(bitmap)
  }

  /**
   * 全局聚合函数，用于将两个位图数组合并成一个。
   * @param b1 第一个位图数组。
   * @param b2 第二个位图数组。
   * @return 返回合并后的位图数组。
   */
  override def merge(b1: Array[Byte], b2: Array[Byte]): Array[Byte] = {
    val bitmap1: RoaringBitmap = deserializeBitMap(b1)
    val bitmap2: RoaringBitmap = deserializeBitMap(b2)
    // 将两个bitmap合并
    val bitmap: RoaringBitmap = RoaringBitmap.or(bitmap1, bitmap2)
    serializeBitMap(bitmap)
  }

  /**
   * 最终要的结果函数，用于将聚合后的位图数组处理成最终结果。
   * @param reduction 聚合后的位图数组。
   * @return 返回处理后的位图数组。
   */
  override def finish(reduction: Array[Byte]): Array[Byte] = {
    reduction
  }

  /**
   * 定义缓冲区编码器，用于内部迭代器的中间状态编码。
   * @return 返回数组的编码器实例。
   */
  override def bufferEncoder: Encoder[Array[Byte]] = Encoders.BINARY

  /**
   * 定义输出编码器，用于最终结果的编码。
   * @return 返回数组的编码器实例。
   */
  override def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
}

