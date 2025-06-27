package agh.wggios.analizadanych.datawriter

import org.apache.spark.sql.DataFrame

class DataWriter {
  def write_parquet(df: DataFrame, path: String): Unit = {
    df.write.mode("overwrite").parquet(path)
  }
}

