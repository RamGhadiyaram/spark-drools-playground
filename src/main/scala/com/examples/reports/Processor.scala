package com.examples.reports


import org.apache.spark.sql.{DataFrame, SparkSession}

trait Processor {

  def setup(a: Map[String, String], sparkSession: SparkSession)

  def process(a: Map[String, String], sparkSession: SparkSession, seq:Seq[DataFrame]):Seq[DataFrame]

  def close(a: Map[String, String], sparkSession: SparkSession)

}
