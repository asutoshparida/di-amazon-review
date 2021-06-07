package com.jetw.di.util

import java.time.{DayOfWeek, LocalDate, LocalDateTime}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import java.sql.DriverManager
import scala.sys.process._

class GenericUtility extends LazyLogging {

  var indexCreationAttempts = 0

  /**
   * prepareDataFrameFromS3File
   * @param s3FilePath
   * @param separator
   * @param quote
   * @param context
   * @param data_schema
   * @return
   */
  def prepareDataFrameFromS3File(s3FilePath: String, separator: String, quote: String, context: org.apache.spark.sql.SQLContext, data_schema: org.apache.spark.sql.types.StructType): DataFrame = {

    val df: DataFrame = context.read
      .format("com.databricks.spark.csv")
      .option("delimiter", separator)
      .option("quote", quote)
      .schema(data_schema)
      .load(s3FilePath)

    return df;
  }

  def ensureEsIndexMapping(es_indexname: String, es_host: String, index_mapping: String) {
    indexCreationAttempts = 0
    val index_status_cmds = s"curl -i -HEAD -H 'Content-Type: application/json' '$es_host/$es_indexname' 2>&1 | grep 200"
    checkAndCreateESIndex(es_indexname, es_host, index_mapping)
    execCommands(index_status_cmds)
  }

  def checkAndCreateESIndex(es_indexname: String, es_host: String, index_mapping: String) {
    logger.info(s"GenericUtility # checkAndCreateESIndex # Checking status of the Index: $es_indexname")
    val index_status_cmd = s"curl -i -HEAD '$es_host/$es_indexname' | head -n 1"
    val statusCode = execCommands(index_status_cmd).trim
    while (indexCreationAttempts < 4) {
      indexCreationAttempts += 1
      if (!statusCode.contains("200")) createESIndexMapping(es_indexname, es_host, index_mapping)
      checkAndCreateESIndex(es_indexname, es_host, index_mapping)
    }
  }

  def execCommands(cmd: String): String = {
    return Seq("bash", "-c", cmd) !!
  }

  /**
   * createESIndexMapping
   * @param esIndex
   * @param esHost
   * @param indexMapping
   */
  def createESIndexMapping(esIndex: String, esHost: String, indexMapping: String) {

    logger.info(s"GenericUtility # createESIndexMapping #  Creating ES index mapping for: '$esIndex'")
    val create_mapping_cmd = s"curl -X PUT -H 'Content-Type: application/json' -d '${indexMapping}' '${esHost}/${esIndex}'"
    val status = execCommands(create_mapping_cmd)
    println(s"$getDateTime Created ES Index Mapping: '$esIndex' => Status: '${status.trim}'")
    logger.info(s"GenericUtility # createESIndexMapping #  Created ES Index mapping for: '$esIndex'")

  }

  /**
   * InsertUpdateESIndexDoc
   * @param es_indexname
   * @param es_nodes
   * @param es_port
   * @param df
   * @return
   */
  def insertUpdateESIndexDocument(es_indexname: String, es_mappingid: String, es_nodes: String, es_port: String, df: DataFrame) {
    val elasticFormat = "org.elasticsearch.spark.sql"
    val elasticIndex = es_indexname //name of the index
    val elasticMapping = "data" //document
    val elasticOptions = Map(
      "es.mapping.id" -> es_mappingid,
      "es.nodes" -> es_nodes,
      "es.port" -> es_port,
      "es.write.operation" -> "upsert",
      "es.index.auto.create" -> "no",
      "es.nodes.wan.only" -> "true")

    df.write.format(elasticFormat)
      .options(elasticOptions)
      .mode(SaveMode.Append)
      .save(s"$elasticIndex/$elasticMapping")
  }


  def getDateTime(): String = {
    try {
      return s"${LocalDateTime.now.getDayOfMonth()}/${LocalDateTime.now.getMonthValue()}/${LocalDateTime.now.getYear()} ${LocalDateTime.now.getHour()}:${LocalDateTime.now.getMinute()}:${LocalDateTime.now.getSecond()}"
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        return null
      }
    }
  }

  /**
   * Method to execute CUD operations on mysql
   * @param mysqlURL
   * @param username
   * @param password
   * @param query
   */
  def executeMYSQL(mysqlURL: String, username: String, password: String, query: String): Unit = {
    val connection = DriverManager.getConnection(mysqlURL, username, password)
    val statement = connection.createStatement
    statement.executeUpdate(query)
  }


  /**
   * Dump dataframe to mysql table
   * @param mysql_url
   * @param userName
   * @param password
   * @param dataFrame
   * @param tableName
   */
  @throws(classOf[Exception])
  def exportToMysql(mysql_url: String, userName: String, password: String, dataFrame: DataFrame, tableName: String): Unit = {
    val prop = new java.util.Properties
    prop.put("driver", "com.mysql.cj.jdbc.Driver")
    prop.put("user", userName)
    prop.put("password", password)
    try {
      logger.info(s"$getDateTime # GenericUtility # createESIndexMapping # Exporting record(s) to the table: $tableName")
      // dataFrame.show()
      dataFrame.write.mode("append").jdbc(mysql_url, tableName, prop)
      logger.info(s"$getDateTime # GenericUtility # createESIndexMapping # Exported record(s) to the table: $tableName")
    } catch {
      case ex: Throwable =>
        logger.error(s"GenericUtility # createESIndexMapping # exception in exporting... '$tableName' with ERROR : ${ex.getMessage}")
        ex.printStackTrace()
        throw ex
    }
  }

}