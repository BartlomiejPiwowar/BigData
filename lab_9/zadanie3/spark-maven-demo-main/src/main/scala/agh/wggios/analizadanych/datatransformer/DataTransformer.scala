package agh.wggios.analizadanych.datatransformer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class DataTransformer {
  def transform(df: DataFrame): DataFrame = {
    df.withColumn("processed_time", current_timestamp())
  }
}

