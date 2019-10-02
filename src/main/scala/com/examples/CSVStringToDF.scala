package com.examples

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession


object CSVStringToDF extends App {

  ////
  ////  import spark.implicits._
  ////
  ////  val csvdata: Dataset[String] = spark.sparkContext.parallelize(
  ////    """
  ////      |2cbeb5cb-219c-4a84-b0b1-fa13de0cbbd4,abc,2019-03-22 17:24:17.484,xyz,some,N,{"a":"b0f44820-1b20-40cb-8bdf-0e70feb2f599","b":"02012019","c":"0000000003218","d":"M" }
  ////      |154eec60-9f08-4f27-a738-a0805bda5377,abc,2019-03-22 17:24:17.484,xyz,some,N,{"a":"283f6465-7f9d-4bde-81b2-8038ad88e7ed","b":"02012019","c":"0000000029021","d":"M" }
  ////    """.stripMargin.lines.toList).toDS()
  ////  val frame: DataFrame = spark.read
  ////    .option("header", false).option("inferSchema", true)
  ////    .option("escape", "\\")
  ////    // .option("quoteMode", "ALL")
  ////    .csv(csvdata)
  ////  val frame1 = frame.withColumn("jsoncolumn"
  ////    , functions.concat(functions.col("_c6"), lit(","), functions.col("_c7"), lit(","), functions.col("_c8")
  ////      , lit(","), functions.col("_c9")))
  ////
  ////  frame1.show(false)
  ////  frame1.printSchema()
  ////  Thread.sleep(1000)
  ////  spark.stop
  // once you collect the dataframe partitions you will get an array like this

  val x = Array(
    "year=2019/month=1/day=21",
    "year=2019/month=1/day=22",
    "year=2019/month=1/day=23",
    "year=2019/month=1/day=24",
    "year=2019/month=1/day=25",
    "year=2019/month=1/day=26",
    "year=2019/month=2/day=27"
  )
  val finalPartitions = listKeys()

  import org.joda.time.DateTime
  val mapWithMostRecentBusinessDate = finalPartitions.sortWith(
    (a, b) => a("businessdate").isAfter(b("businessdate"))
  ).head
  val latest: Option[DateTime] = mapWithMostRecentBusinessDate.get("businessdate")

  println(mapWithMostRecentBusinessDate)
  val year = latest.get.getYear();
  val month = latest.get.getMonthOfYear();
  val day = latest.get.getDayOfMonth();

  def listKeys(): Seq[Map[String, DateTime]] = {
    val keys: Seq[DateTime] = x.map(row => {
      println(s" Identified Key: ${row.toString()}")
      DateTime.parse(row.replaceAll("/", "")
        .replaceAll("year=", "")
        .replaceAll("month=", "-")
        .replaceAll("day=", "-")
        // .replaceAll("=", "-")
      )
    })
      .toSeq
    println(keys)
    println(s"Fetched ${keys.size} ")
    val myPartitions: Seq[Map[String, DateTime]] = keys.map(key => Map("businessdate" -> key))

    myPartitions
  }
  println("latest year " + year + "  latest month " + month + "  latest day  " + day)

  /**
    * recursively print file sizes
    *
    * @param filePath
    * @param fs
    * @return
    */
  def getDisplaysizesOfS3Files(filePath: org.apache.hadoop.fs.Path, fs: org.apache.hadoop.fs.FileSystem): scala.collection.mutable.ListBuffer[String] = {
    val fileList = new scala.collection.mutable.ListBuffer[String]
    val fileStatus = fs.listStatus(filePath)
    for (fileStat <- fileStatus) {
      println(s"file path Name : ${fileStat.getPath.toString} length is  ${fileStat.getLen}")
      if (fileStat.isDirectory) fileList ++= (getDisplaysizesOfS3Files(fileStat.getPath, fs))
      else if (fileStat.getLen > 0 && !fileStat.getPath.toString.isEmpty) {
        println("fileStat.getPath.toString" + fileStat.getPath.toString)
        fileList += fileStat.getPath.toString
        val size = fileStat.getLen
        val display = org.apache.commons.io.FileUtils.byteCountToDisplaySize(size)
        println(" length zero files \n " + fileStat)
        println("Name    = " + fileStat.getPath().getName());
        println("Size    = " + size);
        println("Display = " + display);
      } else if (fileStat.getLen == 0) {
        println(" length zero files \n " + fileStat)

      }
    }
    fileList
  }
 implicit val spark = SparkSession.builder().appName("CSVStringToDF").master("local").getOrCreate()
  /**
    * getDisplaysizesOfS3Files
    * @param path
    * @param spark
    */
  def getDisplaysizesOfS3Files(path: String)( implicit spark: org.apache.spark.sql.SparkSession): Unit = {
    val filePath = new org.apache.hadoop.fs.Path(path)
    val fileSystem = filePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val size = fileSystem.getContentSummary(filePath).getLength
    val display = org.apache.commons.io.FileUtils.byteCountToDisplaySize(size)
    println("path    = " + path);
    println("Size    = " + size);
    println("Display = " + display);
  }

}
