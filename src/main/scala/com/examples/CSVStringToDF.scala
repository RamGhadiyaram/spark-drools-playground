package com.examples

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

object CSVStringToDF extends App {
  val spark = SparkSession.builder().appName("CSVStringToDF").master("local").getOrCreate()

  import spark.implicits._

  val csvdata: Dataset[String] = spark.sparkContext.parallelize(
    """
      |2cbeb5cb-219c-4a84-b0b1-fa13de0cbbd4,abc,2019-03-22 17:24:17.484,xyz,some,N,{"a":"b0f44820-1b20-40cb-8bdf-0e70feb2f599","b":"02012019","c":"0000000003218","d":"M" }
      |154eec60-9f08-4f27-a738-a0805bda5377,abc,2019-03-22 17:24:17.484,xyz,some,N,{"a":"283f6465-7f9d-4bde-81b2-8038ad88e7ed","b":"02012019","c":"0000000029021","d":"M" }
    """.stripMargin.lines.toList).toDS()
  val frame: DataFrame = spark.read
    .option("header", false).option("inferSchema", true)
    .option("escape", "\\")
    // .option("quoteMode", "ALL")
    .csv(csvdata)
  val frame1 = frame.withColumn("jsoncolumn"
    , functions.concat(functions.col("_c6"), lit(","), functions.col("_c7"), lit(","), functions.col("_c8")
      , lit(","), functions.col("_c9")))
  frame1.show(false)
  frame1.printSchema()
}
