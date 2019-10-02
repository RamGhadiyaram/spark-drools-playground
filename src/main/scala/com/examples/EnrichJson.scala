  package com.examples

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.functions.{col, explode}
  import org.apache.spark.sql.{DataFrame, SparkSession}


  object EnrichJson extends App {

    private[this] implicit val spark = SparkSession.builder().master("local[*]").getOrCreate()
    Logger.getLogger("org").setLevel(Level.WARN)

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val rec1: String =
      """{
      "visitorId": "v1",
      "products": [{
           "id": "i1",
           "interest": 0.68
      }, {
           "id": "i2",
           "interest": 0.42
      }]
  }"""

    val rec2: String =
      """{
      "visitorId": "v2",
      "products": [{
           "id": "i1",
           "interest": 0.78
      }, {
           "id": "i3",
           "interest": 0.11
      }]
  }"""

    val visitsData: Seq[String] = Seq(rec1, rec2)
    val productIdToNameMap = Map("i1" -> "Nike Shoes", "i2" -> "Umbrella", "i3" -> "Jeans")
    val dictionary = productIdToNameMap.toSeq.toDF("id", "name")
    val rddData = spark.sparkContext.parallelize(visitsData)


    dictionary.printSchema()


    println("for spark version >2.2.0")

    var resultDF = spark.read.json(visitsData.toDS)
      .withColumn("products", explode(col("products")))
      .selectExpr("products.*", "visitorId")
      .join(dictionary, Seq("id"))
    resultDF.show
    resultDF.printSchema()
    convertJson(resultDF)

    println("for spark version <2.2.0")
    resultDF = spark.read.json(rddData)
      .withColumn("products", explode(col("products")))
      .selectExpr("products.*", "visitorId")
      .join(dictionary, Seq("id"))
    // .withColumn("products", explode(col("products")))
    resultDF.show
    resultDF.printSchema()
    convertJson(resultDF)
    import spark.implicits._
  // println("keyby approach " + resultDF.rdd.keyBy(_.getAs[String]("id")).collect.mkString)
    /**
      * convertJson : converts the data frame to json string
      * @param resultDF
      */
    private def convertJson(resultDF: DataFrame) = {
      import org.apache.spark.sql.functions.{collect_list, _}

      val x: DataFrame = resultDF
        .groupBy("visitorId")
        .agg(collect_list(struct("id", "interest", "name")).as("products"))
      x.show
      println(x.toJSON.collect.mkString)
    }
  }