package com.spark.sql

import com.spark.util.BitMapUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, IntegerType, StringType, StructField, StructType}

object udaf_test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("udaf").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    val schema: StructType = StructType(Seq(
      // 1,江西省,九江市,天河区,10
      StructField("id", DataTypes.IntegerType),
      StructField("province", DataTypes.StringType),
      StructField("city", DataTypes.StringType),
      StructField("district", DataTypes.StringType),
      StructField("pv", DataTypes.IntegerType)
    ))
    val df: DataFrame = spark.read.schema(schema).csv("data/udaf/input.txt")
    df.createTempView("df")

    import org.apache.spark.sql.functions.udaf
    spark.udf.register("gen_bitmap",udaf(BitMapGenUDAF))
    spark.udf.register("bitmap_merge",udaf(BitMapOrMergeUDAF))

    // 将字节数组反序列化为bitmap 然后求1的个数
    val result = (arr: Array[Byte]) => {
      BitMapUtil.deserializeBitMap(arr).getCardinality
    }
    spark.udf.register("count_one", result)

    val frame: DataFrame = spark.sql(
      """
        |select
        | province,
        | city,
        | district,
        | sum(pv) as total_pv,
        | count_one(gen_bitmap(id)) as uv_count,
        | gen_bitmap(id) as id_bitmap
        |from df
        |group by province, city, district
        |""".stripMargin)
    frame.createTempView("temp")
    frame.show(100, false)

    spark.sql(
      """
        |select
        | province,
        | city,
        | sum(total_pv) as total_pv,
        | count_one(bitmap_merge(id_bitmap)) as uv_count,
        | bitmap_merge(id_bitmap) as id_bitmap
        |from temp
        |group by province, city
        |""".stripMargin).show(100, false)


    spark.close()
  }
}
