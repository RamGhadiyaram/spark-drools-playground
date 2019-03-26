package com.examples

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** *
  * take small pegs and make a large peg
  * and coalesce it
  *
  * @author : Ram Ghadiyaram
  */
object DataFrameTest extends Logging {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  //public FileStatus[] globStatus(Path pathPattern) throws IOException
  def main(args: Array[String]): Unit = {
    val appName = if (args.length >0) args(0) else this.getClass.getName
    val spark: SparkSession = SparkSession.builder
      .config("spark.master", "local") //.config("spark.eventLog.enabled", "true")
      .appName(appName)
      .getOrCreate()

//    val fs = FileSystem.get(new Configuration())
//    //new Path("./userdata*.parquet"))
//    val files = fs.globStatus(new Path("./userdata*.parquet")).map(_.getPath.toString)
//    val dfSeq = mutable.MutableList[DataFrame]()
//    println(dfSeq)
//    println(files.length)
//    files.foreach(x => println(x))
//    val newDFs = files.map(dir => {
//      dfSeq += spark.read.parquet(dir).toDF()
//    }) //.reduce(_ union _)
//    println(dfSeq.length)
//    val finalDF = dfSeq.reduce(_ union _)
//      .toDF
//    finalDF.show(false)
//    println(System.getProperty("java.io.tmpdir"))
//    println(System.getProperties.toString)
//    finalDF.coalesce(2)
//      .write
//      .mode(SaveMode.Overwrite)
//      .parquet(s"${System.getProperty("java.io.tmpdir")}/final.parquet")
    //single.show(false)
    import spark.implicits._
   // val list = Seq((0.0,0.4,0.4,0.0),(0.1,0.0,0.0,0.7),(0.0,0.2,0.0,0.3),(0.3,0.0,0.0,0.0))
   // val list1: Seq[(Int, String, Int)] = Seq((1,"A",10),(2,"A",5),(3,"B",56))
    val list1: Seq[String] = ListBuffer(Array("1","2","3"),Array("11","22","33")).map(x => x.mkString)
    val client: DataFrame = spark.sparkContext.parallelize(
list1    //  Seq(Array(1,"A",10).toSeq,Arra.y(2,"A",5).toSeq,Array(3,"B",56).toSeq)
    ).toDF//("ID","Categ","test")
    println(System.getProperty("java.io.tmpdir"))
    client.show(false)
    client.coalesce(1).write
      .mode(SaveMode.Overwrite)
      .csv(s"${System.getProperty("java.io.tmpdir")}/finalclient.csv")
    println("done")
  }
}