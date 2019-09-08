package com.examples.reports

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Writer {

  def setup(a: Map[String, String], sparkSession: SparkSession) // pre

  def write(a:Map[String,String],sparkSession: SparkSession, seq : Seq[DataFrame]): Boolean

  def close(a: Map[String, String], sparkSession: SparkSession) // post
}
