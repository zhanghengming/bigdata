package com.spark.util

import org.roaringbitmap.RoaringBitmap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

object BitMapUtil {
  // 序列化bitmap为字节数组
  def serializeBitMap(bitMap: RoaringBitmap): Array[Byte] = {
    // 包装字节数组的流
    val byteArrayOutputStream = new ByteArrayOutputStream()
    // 包装成数据输出流
    val stream = new DataOutputStream(byteArrayOutputStream)
    // 创建一个空的bitmap，并序列化为字节数组，交还给框架
    // 参数需要一个流，将对象序列化成字节后写入到流中，所以需要一个字节流
    bitMap.serialize(stream)
    // 将字节流反序列化字节数组，得到一个空的bitmap
    byteArrayOutputStream.toByteArray
  }

  // 反序列化字节数组为bitmap
  def deserializeBitMap(bytes: Array[Byte]): RoaringBitmap = {
    val bitmap: RoaringBitmap = RoaringBitmap.bitmapOf()
    // 创建字节输入流，并反序列化字节数组
    val stream = new ByteArrayInputStream(bytes)
    val dataInputStream = new DataInputStream(stream)
    // 将字节数组反序列化为bitmap
    bitmap.deserialize(dataInputStream)
    bitmap
  }
}
