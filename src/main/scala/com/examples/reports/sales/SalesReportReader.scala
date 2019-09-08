package com.examples.reports.sales

import com.examples.reports.Reader
import com.examples.reports.sales.SalesReportProcessor.logInfo
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

case object SalesReportReader extends Reader with Logging{

 def setup(a: Map[String, String], sparksession: SparkSession): Unit = {

 }

 def read(a: Map[String, String], spark: SparkSession): Seq[DataFrame] = {

   logInfo("in process " + this.getClass.getSimpleName)

   import spark.implicits._

   val df1 = Seq(
     ("2019-01-01 00:00:00", "7056589658"),
     ("2019-02-02 00:00:00", "7778965896")
   ).toDF("DATE_TIME", "PHONE_NUMBER")

   df1.show()

   val df2 = Seq(
     ("2019-01-01 01:00:00", "194.67.45.126"),
     ("2019-02-02 00:00:00", "102.85.62.100"),
     ("2019-03-03 03:00:00", "102.85.62.100")
   ).toDF("DATE_TIME", "IP")
   df2.show
   Seq(df1, df2)
 }

 def close(a: Map[String, String],sparkSession: SparkSession): Unit = {

 }

}
