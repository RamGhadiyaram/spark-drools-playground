package com.examples


import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkContext

object HdfsUtils {
  def pathExists(path: String, sc: SparkContext): Boolean = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.exists(new Path(path))
  }

  def getFullPath(path:String, sc: SparkContext): String = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.getFileStatus(new Path(path)).getPath().toString
  }

  def getAllFiles(path:String, sc: SparkContext): Seq[String] = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val files: Array[FileStatus] = fs.globStatus(new Path(path))
    files.map(_.getPath().toString)
  }
}