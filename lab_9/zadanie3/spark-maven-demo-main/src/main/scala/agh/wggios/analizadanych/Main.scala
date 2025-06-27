package agh.wggios.analizadanych

import agh.wggios.analizadanych.datareader.DataReader
import agh.wggios.analizadanych.datatransformer.DataTransformer
import agh.wggios.analizadanych.datawriter.DataWriter

object Main extends SparkSessionProvider {
  LoggingUtils.setupLogging()

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Podaj ścieżkę wejściową i wyjściową jako argumenty!")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val df = new DataReader().read_csv(inputPath)
    val transformed = new DataTransformer().transform(df)
    new DataWriter().write_parquet(transformed, outputPath)

    logInfo("Pipeline zakończony pomyślnie.")
  }
}

