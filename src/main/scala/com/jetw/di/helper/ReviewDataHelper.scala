package com.jetw.di.helper

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.scalalogging._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.io.Source

object ReviewDataHelper extends LazyLogging {

  /**
   * parse the JSON content on application properties.
   * @throws java.lang.Exception
   * @return Map[String, String]
   */
  @throws(classOf[Exception])
  def loadApplicationProperties(): Map[String, String] = {
    val json = Source.fromFile("src/main/config/pipeline_config.json")

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val parsedJson = mapper.readValue[Map[String, String]](json.reader())

    return parsedJson
  }

  /**
   * Method to load review metaData
   * @param sparkSession
   * @throws java.lang.Exception
   * @return DataFrame
   */
  @throws(classOf[Exception])
  def loadReviewMetaData(sparkSession: SparkSession): DataFrame = {
    val metaDataDF = sparkSession.read.option("multiline","true")
      .json("src/main/resources/meta_Movies_and_TV.json")
      .drop("categories", "related", "salesRank")
      .na.drop(Seq("asin"))
//    metaDataDF.printSchema()
    return metaDataDF
  }

}