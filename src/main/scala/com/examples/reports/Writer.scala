package com.examples.reports

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Writer extends  Serializable {

  def setup(a: Map[String, Object], sparksession: SparkSession): Map[String, Object]

  def write(a:Map[String,Object],sparkSession: SparkSession, seq : Seq[DataFrame]): Boolean

  def close(a: Map[String, Object], sparkSession: SparkSession) // post
}
