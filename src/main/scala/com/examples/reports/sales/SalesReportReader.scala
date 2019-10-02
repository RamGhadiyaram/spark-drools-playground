package com.examples.reports.sales

import com.examples.reports.Reader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

case object SalesReportReader extends Reader with Logging {

  def setup(a: Map[String, Object], sparksession: SparkSession): Map[String, Object] = {
  logInfo("in setup " + a)
    a
  }

  def read(a: Map[String, Object], spark: SparkSession): Seq[DataFrame] = {

    logInfo("in read ")

    import spark.implicits._
    val sales = Seq(
      ("Dallas", 2016, 100d),
      ("Dallas", 2017, 120d),
      ("Sanjose", 2017, 200d),
      ("Plano", 2015, 50d),
      ("Plano", 2016, 50d),
      ("Newyork", 2016, 150d),
      ("Toronto", 2017, 50d)

    ).toDF("city", "year", "saleAmount")
    sales.printSchema()
    Seq(sales)
  }

  def close(a: Map[String, Object], sparkSession: SparkSession): Unit = {
    logInfo("in close ")
  }

}
