package com.examples

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.examples.HdfsUtils._

import scala.collection.mutable
import com.examples.DataFrameUtil.SparkSessionSingleton
/** *
  * take small pegs and make a large peg
  * and coalesce it
  *
  * @author : Ram Ghadiyaram
  */
object ParquetPlay extends Logging {
 // Logger.getLogger("org").setLevel(Level.OFF)
 // Logger.getLogger("akka").setLevel(Level.OFF)


  //public FileStatus[] globStatus(Path pathPattern) throws IOException
  def main(args: Array[String]): Unit = {
    val appName = if (args.length >0) args(0) else this.getClass.getName
    val spark: SparkSession = SparkSession.builder
      .config("spark.master", "local[*]") //.config("spark.eventLog.enabled", "true")
      .appName(appName)
      .getOrCreate()
//val config = spark.sparkContext.hadoopConfiguration
  //  val fs = FileSystem.get(new Configuration())
    //new Path("./userdata*.parquet"))
    // val files = fs.globStatus(new Path("./userdata*.parquet")).map(_.getPath.toString)
    val files = getAllFiles("C:\\Users\\ashwini\\Downloads\\codebase\\spark-general-examples\\userdata*.parquet", spark.sparkContext)
    val dfSeq = mutable.MutableList[DataFrame]()
    println(dfSeq)
    println(files.length)
    files.foreach(x => println(x))
    val newDFs = files.map(dir => {
      dfSeq += spark.read.parquet(dir).toDF()
    }) //.reduce(_ union _)
    println(dfSeq.length)
    val finalDF = dfSeq.reduce(_ union _)
      .toDF
    finalDF.show(false)
    println(System.getProperty("java.io.tmpdir"))
    println(System.getProperties.toString)
    finalDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv(s"${System.getProperty("java.io.tmpdir")}/final.csv")
    //single.show(false)
    println("done")
  }
}