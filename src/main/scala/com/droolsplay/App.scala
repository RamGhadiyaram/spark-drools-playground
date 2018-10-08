package com.droolsplay

import com.droolsplay.util.DroolUtil._
import com.droolsplay.util.SparkSessionSingleton
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._

/**
  * @author : Ram Ghadiyaram
  */
object App extends Logging {

  def main(args: Array[String]): Unit = {
    // prepare some funny input data
    val inputData = Seq(
      ApplicantForLoan(1, "Ram", "Ghadiyaram", 680, 680)
      , ApplicantForLoan(2, "Mohd", "Ismail", 12000, 679)
      , ApplicantForLoan(3, "Phani", "Ramavajjala", 100, 600)
      , ApplicantForLoan(4, "Trump", "Donald", 1000000, 788)
      , ApplicantForLoan(5, "Nick", "Suizo", 10, 788)
      , ApplicantForLoan(7, "Sreenath", "Mamilla", 10, 788)
      , ApplicantForLoan(8, "Naveed", "Farroqui", 10, 788)
      , ApplicantForLoan(9, "Ashish", "Anand", 1000, 788)
      , ApplicantForLoan(10, "Vasudha", "Nanduri", 1001, 788)
      , ApplicantForLoan(11, "Tathagatha", "das", 1002, 788)
      , ApplicantForLoan(12, "Sean", "Owen", 1003, 788)
      , ApplicantForLoan(13, "Sandy", "Raza", 1004, 788)
      , ApplicantForLoan(14, "Holden", "Karau", 1005, 788)
      , ApplicantForLoan(15, "Gobinathan", "SP", 1005, 7)
      , ApplicantForLoan(16, "Arindam", "SenGupta", 1005, 670)
      , ApplicantForLoan(17, "NIKHIL", "POTLAPALLY", 100, 670)
      , ApplicantForLoan(18, "Phanindra", "Ramavojjala", 100, 671)
    )

    val spark = SparkSessionSingleton.getInstance()
    val rules = loadRules
    val broadcastRules = spark.sparkContext.broadcast(rules)
    val applicants = spark.sparkContext.parallelize(inputData)
    logInfo("list of all applicants " + applicants.getClass.getName)


    import spark.implicits._

    val applicantsDS = applicants.toDF()
    applicantsDS.show(false)
    val df_size_in_bytes: Long = checkDFSize(spark, applicantsDS)

    logInfo("byteCountToDisplaySize - df_size_in_bytes " + df_size_in_bytes)
    logInfo(applicantsDS.rdd.toDebugString)

    val approvedguys = applicants.map {
      x => {
        logDebug(x.toString)
        applyRules(broadcastRules.value, x)
      }
    }.filter((a: ApplicantForLoan) => (a.isApproved == true))
    logInfo("approvedguys " + approvedguys.getClass.getName)
    approvedguys.toDS.withColumn("Remarks", lit("Good Going!! your credit score =>680 check your score in https://www.creditkarma.com")).show(false)


    var numApproved: Long = approvedguys.count
    logInfo("Number of applicants approved: " + numApproved)

    /** **
      * antoher way to do it with dataframes just an example not required to execute this code above rdd applicants
      * is sufficient to get isApproved == false
      */
    val notApprovedguys = applicantsDS.rdd.map { row =>
      //(id: Int, firstName: String, lastName: String, requestAmount: Int, creditScore: Int)
      applyRules(broadcastRules.value,
        ApplicantForLoan(
          row.getAs[Int]("id")
          , row.getAs[String]("firstName")
          , row.getAs[String]("lastName")
          , row.getAs[Int]("requestAmount")
          , row.getAs[Int]("creditScore"))
      )
    }.filter((a: ApplicantForLoan) => (a.isApproved == false))

    logInfo("notApprovedguys " + notApprovedguys.getClass.getName)

    notApprovedguys.toDS().withColumn("Remarks", lit("credit score <680 Need to improve your credit history!!!  check your score : https://www.creditkarma.com")).show(false)

    val numNotApproved: Long = notApprovedguys.count
    logInfo("Number of applicants NOT approved: " + numNotApproved)
  }
}
