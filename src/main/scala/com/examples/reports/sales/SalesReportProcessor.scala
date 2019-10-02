package com.examples.reports.sales

import com.examples.SparkExcelReport.{logInfo, sales}
import com.examples.reports.Processor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{lit, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
case object SalesReportProcessor extends Processor with Logging {

  def setup(a: Map[String, Object], sparksession: SparkSession): Map[String, Object] = {
    logInfo("in setup ")
    a
  }

  def process(a: Map[String, Object], sparkSession: SparkSession, seq: Seq[DataFrame]): Seq[DataFrame] = {
    logInfo("in process " + this.getClass.getSimpleName)
    import sparkSession.implicits._
    logInfo(
      """
        |
        | rollup multi-dimensional aggregate operator is an extension of groupBy operator
        | that calculates subtotals and a grand total across specified group of n + 1 dimensions
        | (with n being the number of columns as cols and col1 and 1 for where values become null, i.e. undefined).
        | rollup operator is commonly used for analysis over hierarchical data; e.g. total salary by department, division, and company-wide total.
      """.stripMargin)
    logInfo("Using Rollup (\"city\", \"year\")")
    val sales = seq(0)
    val first = sales
      .rollup("city", "year")
      .agg(sum("saleAmount") as "saleAmount")
      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)
    first.show

    logInfo("Using .groupBy(\"city\", \"year\")")

    // The above query is semantically equivalent to the following
    val second = sales
      .groupBy("city", "year") // <-- subtotals (city, year)
      .agg(sum("saleAmount") as "saleAmount")


    second.show
    logInfo("Using group by (\"city\")")

    val third = sales
      .groupBy("city") // <-- subtotals (city)
      .agg(sum("saleAmount") as "saleAmount")
      .select($"city", lit(null) as "year", $"saleAmount") // <-- year is null
    third.show


    val fourth = sales
      .groupBy() // <-- grand total
      .agg(sum("saleAmount") as "saleAmount")
      .select(lit(null) as "city", lit(null) as "year", $"saleAmount") // <-- city and year are null

    fourth.show
    val finalDF = second
      .union(third)
      .union(fourth)
      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)
    finalDF.show
    Seq(first,  second, third, fourth, finalDF)
  }

  def close(a: Map[String, Object], sparkSession: SparkSession): Unit = {
    logInfo("in close ")

  }

}
