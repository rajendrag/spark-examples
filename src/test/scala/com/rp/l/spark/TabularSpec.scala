package com.rp.l.spark

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSpec


class TabularSpec extends FunSpec with SparkSessionWrapper with DataFrameComparer {

  import spark.implicits._

  describe("withGoodVibes") {
    it("appends a chi column to a DataFrame") {
      val sourceDF = List("RP", "PR").toDF("name")
      val actualDF = sourceDF.transform(Tabular.withGoodVibes())

      val expectedSchema = List(
        StructField("name", StringType),
        StructField("chi", StringType, false)
      )

      val expectedData = List(
        Row("RP", "happy"),
        Row("PR", "happy")
      )

      val expectedDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), StructType(expectedSchema))

      assertSmallDataFrameEquality(actualDF, expectedDataFrame)
    }
  }

}
