package com.jetw.di.service

import com.jetw.di.util.GenericUtility
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType


class ReviewDataService(utility: GenericUtility) extends LazyLogging {

  /**
   * Push data frame to elasticsearch index (upsert)
   *
   * Need to create a elsticsearch index "amazon_review"
   * PUT /amazon_review
   * {
   * "settings": {
   * "index.mapper.dynamic": false,
   * "number_of_replicas": 1,
   * "number_of_shards": 1
   * },
   * "mappings": {
   * "data": {
   * "dynamic": false,
   * "properties": {
   * "overall": {
   * "type": "double"
   * },
   * "price": {
   * "type": "double"
   * },
   * "asin": {
   * "type": "text",
   * "fields": {
   * "raw": {
   * "type": "keyword",
   * "ignore_above": 256
   * }
   * }
   * },
   * "requisition_id": {
   * "type": "text",
   * "fields": {
   * "raw": {
   * "type": "keyword",
   * "ignore_above": 256
   * }
   * }
   * },
   * "reviewerID": {
   * "type": "text",
   * "fields": {
   * "raw": {
   * "type": "keyword",
   * "ignore_above": 256
   * }
   * }
   * },
   * "job_category": {
   * "type": "text",
   * "fields": {
   * "raw": {
   * "type": "keyword",
   * "ignore_above": 256
   * }
   * }
   * },
   * "title": {
   * "type": "text",
   * "fields": {
   * "raw": {
   * "type": "keyword",
   * "ignore_above": 256
   * }
   * }
   * },
   * "description": {
   * "type": "text",
   * "fields": {
   * "raw": {
   * "type": "keyword",
   * "ignore_above": 256
   * }
   * }
   * },
   * "imUrl": {
   * "type": "text",
   * "fields": {
   * "raw": {
   * "type": "keyword",
   * "ignore_above": 256
   * }
   * }
   * },
   * "unixReviewTime": {
   * "type": "date",
   * "format": "yyyy/MM/dd HH:mm:ss||yyyy/MM/dd HH:mm:ss.SSS||yyyy/MM/dd||epoch_millis||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd"
   * }
   * }
   * }
   * }
   * }
   * As elasticsearch upserts will handle duplicates on "id" column of our
   * dataframe we will be able to handle duplicates while inserting data to elasticsearch.
   *
   * @param reviewDF  DataFrame
   * @param configMap Map[String, String]
   *
   */
  @throws(classOf[Exception])
  def pushReviewDataToElastic(reviewDF: DataFrame, configMap: Map[String, String]) = {

    if (reviewDF != null && configMap != null && !configMap.isEmpty) {
      val esIndex = configMap.getOrElse("esIndexName", null)
      val esNodes = configMap.getOrElse("esNodes", null)
      val esPort = configMap.getOrElse("esPort", null)
      val esHost = s"$esNodes:$esPort"

      logger.info(s"ReviewDataService# pushReviewDataToElastic # esIndex : ${esIndex}, esNodes: ${esNodes}, esPort: ${esPort} ")
      //        utility.ensureEsIndexMapping(esIndex, esHost, Review.amazon_review_mapping)
      utility.insertUpdateESIndexDocument(esIndex, "id", esNodes, esPort, reviewDF)
      logger.info(s"ReviewDataService# pushReviewDataToElastic # data successfully inserted to esIndex : ${esIndex} ")
    } else {
      logger.info(s"ReviewDataService# pushReviewDataToElastic # Data not available ")
    }
  }

  /**
   *
   * This def pushes data to mysql.
   * First we create stage_amazon_reviews, amazon_reviews tables if not exists
   * Then we dump the whole micro batch data to stage table (after deleting previous batch data).
   * Then we run update/insert queries to amazon_reviews.
   *
   * @param reviewDF
   * @param configMap
   * @throws java.lang.Exception
   */
  @throws(classOf[Exception])
  def pushReviewDataToMysql(reviewDF: DataFrame, configMap: Map[String, String]) = {
    if (reviewDF != null && configMap != null && !configMap.isEmpty) {
      val mysqlAnalyticsDb = configMap.getOrElse("mysqlMasterDBURL", null) + configMap.getOrElse("mysqlSchema", null)
      val mysqlUser = configMap.getOrElse("mysqlDBUser", null)
      val mysqlPass = configMap.getOrElse("mysqlDBPass", null)
      val executeCreateTableFlag = configMap.getOrElse("executeCreateTableFlag", null)

      if (executeCreateTableFlag.equalsIgnoreCase("Y")) {
        try {
          /** *
           *
           * CREATE TABLE IF NOT EXISTS `stage_amazon_reviews` (
           * `id` VARCHAR(200) NOT NULL,
           * `asin` VARCHAR(100) NOT NULL,
           * `reviewerID` VARCHAR(200) NOT NULL,
           * `reviewDateTime` TIMESTAMP NOT NULL,
           * `overall` DOUBLE NOT NULL,
           * `price` DOUBLE DEFAULT NULL,
           * `description` TEXT DEFAULT NULL,
           * `imUrl` VARCHAR(2000) DEFAULT NULL,
           * `title` VARCHAR(1000) DEFAULT NULL,
           * PRIMARY KEY (`id`)
           * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
           *
           * */

          val amazonReviewsStageDDL = s"CREATE TABLE IF NOT EXISTS `stage_amazon_reviews` ( `id` VARCHAR(200) NOT NULL, `asin` VARCHAR(100) NOT NULL, `reviewerID` VARCHAR(200) NOT NULL, `reviewDateTime` TIMESTAMP NOT NULL, `overall` DOUBLE NOT NULL, `price` DOUBLE DEFAULT NULL, `description` TEXT DEFAULT NULL, `imUrl` VARCHAR(2000) DEFAULT NULL, `title` VARCHAR(1000) DEFAULT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
          logger.info(s"ReviewDataService# pushReviewDataToMysql # Create stage_amazon_reviews table if not exists :" + amazonReviewsStageDDL)
          utility.executeMYSQL(s"$mysqlAnalyticsDb", mysqlUser, mysqlPass, amazonReviewsStageDDL)

          val amazonReviewsDDL = s"CREATE TABLE IF NOT EXISTS `amazon_reviews` ( `id` VARCHAR(200) NOT NULL, `asin` VARCHAR(100) NOT NULL, `reviewerID` VARCHAR(200) NOT NULL, `reviewDateTime` TIMESTAMP NOT NULL, `overall` DOUBLE NOT NULL, `price` DOUBLE DEFAULT NULL, `description` TEXT DEFAULT NULL, `imUrl` VARCHAR(2000) DEFAULT NULL, `title` VARCHAR(1000) DEFAULT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
          logger.info(s"ReviewDataService# pushReviewDataToMysql # Create amazon_reviews table if not exists :" + amazonReviewsDDL)
          utility.executeMYSQL(s"$mysqlAnalyticsDb", mysqlUser, mysqlPass, amazonReviewsDDL)
        } catch {
          case ex: Exception => {
            logger.error(s"Failed to create amazon_reviews tables :" + ex.getMessage)
            ex.printStackTrace();
          }
        }
      }

      try {
        val truncateQuery = s"DELETE FROM stage_amazon_reviews"
        logger.info("ReviewDataService# pushReviewDataToMysql # Truncating stage_amazon_reviews table :" + truncateQuery)
        utility.executeMYSQL(s"$mysqlAnalyticsDb", mysqlUser, mysqlPass, truncateQuery)
      } catch {
        case ex: Exception => {
          logger.error(s"ReviewDataService# pushReviewDataToMysql # Failed to truncate stage_amazon_reviews :" + ex.getMessage)
          ex.printStackTrace();
          throw ex
        }
      }

      val finalReviewDF: DataFrame = reviewDF.select(col("reviewerID"), col("id"), col("asin"), col("overall"),
        col("description"), col("imUrl"), col("price"), col("title"), col("reviewDateTime").cast(TimestampType))
      utility.exportToMysql(s"$mysqlAnalyticsDb", mysqlUser, mysqlPass, finalReviewDF, "stage_amazon_reviews");
      logger.info(s"ReviewDataService# pushReviewDataToMysql # Inserted reviews data to MySQL stage_amazon_reviews")

      try {
        val updateQuery =
          s"""UPDATE amazon_reviews A INNER JOIN (SELECT sar.id, sar.reviewerID, sar.asin, sar.overall, sar.reviewDateTime, sar.description, sar.imUrl, sar.price, sar.title
          FROM stage_amazon_reviews sar LEFT JOIN amazon_reviews ar ON sar.id = ar.id
          WHERE ar.id IS NOT NULL) B USING (id) SET A.overall = B.overall, A.reviewDateTime = B.reviewDateTime,
          A.description = B.description, A.imUrl = B.imUrl, A.price = B.price, A.title = B.title """
        utility.executeMYSQL(s"$mysqlAnalyticsDb", mysqlUser, mysqlPass, updateQuery)
        logger.info("ReviewDataService# pushReviewDataToMysql # Updated amazon_reviews table over:" + updateQuery)
      } catch {
        case ex: Exception => {
          logger.error(s"ReviewDataService# pushReviewDataToMysql # Failed to Update to amazon_reviews : Exp.: ${ex.getMessage}")
          throw ex
        }
      }

      try {
        val insertQuery =
          s"""INSERT INTO amazon_reviews (id, reviewerID, asin, overall, reviewDateTime, description, imUrl, price, title)
          SELECT id, reviewerID, asin, overall, reviewDateTime, description, imUrl, price, title FROM (
          SELECT sar.id, sar.reviewerID, sar.asin, sar.overall, sar.reviewDateTime, sar.description, sar.imUrl, sar.price, sar.title
          FROM stage_amazon_reviews sar LEFT JOIN amazon_reviews ar ON sar.id = ar.id
          WHERE ar.id IS NULL) a"""
        utility.executeMYSQL(s"$mysqlAnalyticsDb", mysqlUser, mysqlPass, insertQuery)
        logger.info("ReviewDataService# pushReviewDataToMysql # Inserted into amazon_reviews table over:" + insertQuery)
      } catch {
        case ex: Exception => {
          logger.error(s"ReviewDataService# pushReviewDataToMysql # Failed to Insert to amazon_reviews : Exp.: ${ex.getMessage}")
          throw ex
        }
      }
    }
  }

}
