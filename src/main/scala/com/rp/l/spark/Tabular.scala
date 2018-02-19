package com.rp.l.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Tabular {

  def withGoodVibes()(df: DataFrame): DataFrame = {
    df.withColumn("chi", lit("happy"))
  }

}
