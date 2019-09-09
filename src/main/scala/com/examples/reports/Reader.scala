package com.examples.reports

import org.apache.spark.sql.{DataFrame, SparkSession}

trait  Reader  extends  Serializable {

  def  setup(a:Map[String,String], sparkSession: SparkSession)

  def read(a:Map[String,String], spark:SparkSession) : Seq[DataFrame]

  def close(a:Map[String,String] , sparkSession: SparkSession)

}
