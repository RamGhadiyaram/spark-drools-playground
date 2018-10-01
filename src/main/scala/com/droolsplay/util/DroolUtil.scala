package com.droolsplay.util

import com.droolsplay.ApplicantForLoan
import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kie.api.{KieBase, KieServices}
import org.kie.internal.command.CommandFactory

/**
  * @author : Ram Ghadiyaram
  */
object DroolUtil extends Logging {
  /**
    * loadRules.
    *
    * @return KieBase
    */
  def loadRules: KieBase = {
    val kieServices = KieServices.Factory.get
    val kieContainer = kieServices.getKieClasspathContainer
    kieContainer.getKieBase
  }

  /**
    * applyRules.
    *
    * @param base      see [[KieBase]]
    * @param applicant see [[ApplicantForLoan]]
    * @return ApplicantForLoan see [[ApplicantForLoan]]
    */
  def applyRules(base: KieBase, applicant: ApplicantForLoan): ApplicantForLoan = {
    val session = base.newStatelessKieSession
    session.execute(CommandFactory.newInsert(applicant))
    logTrace("applyrules ->" + applicant)
    applicant
  }

  /**
    * checkDFSize
    *
    * @param spark        [[SparkSession]]
    * @param applicantsDS [[DataFrame]]
    * @return Long
    */
  def checkDFSize(spark: SparkSession, applicantsDS: DataFrame): Long = {
    applicantsDS.cache.foreach(x => x)
    val catalyst_plan = applicantsDS.queryExecution.logical
    // just to check dataframe size
    val df_size_in_bytes = spark.sessionState.executePlan(
      catalyst_plan).optimizedPlan.stats.sizeInBytes.toLong
    df_size_in_bytes
  }
}

/** Lazily instantiated singleton instance of SparkSession */

object SparkSessionSingleton extends Logging {
  val logger = Logger.getLogger(this.getClass.getName)
  @transient private var instance: SparkSession = _

  def getInstance(appName: Option[String]): SparkSession = {
    logDebug(" instance " + instance)
    if (instance == null) {
      instance = SparkSession
        .builder
        .config("spark.master", "local") //.config("spark.eventLog.enabled", "true")
        .appName(appName.getOrElse("AppDroolsPlayGroundWithSpark"))
        .getOrCreate()
    }
    instance
  }
}
