package com.jetw.di.controller

import com.jetw.di.helper.ReviewDataHelper
import com.jetw.di.misc.Review
import com.jetw.di.service.ReviewDataService
import com.jetw.di.util.GenericUtility
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * ReviewDataController receives the kafka stream for amazon review
 * then aggregates the metadata and pushes final data to elasticseach.
 */
object ReviewDataController extends LazyLogging {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val pipelineName = "amazon-review"

  def main(args: Array[String]): Unit = {
    {
      val envType = args(0)
      val numPartitions = 1;
      var sparkSession: SparkSession = null
      // Config SparkSession depending on what ENV we are running on.
      if ((envType == "DEV") || (envType == "QA")) {
        sparkSession = SparkSession.builder.appName(pipelineName).master("local[*]").getOrCreate()
      } else {
        sparkSession = SparkSession.builder.appName(pipelineName).getOrCreate()
      }
      // load the application level config as a Map[String, String]
      val configMap = ReviewDataHelper.loadApplicationProperties()
      val sparkContext = sparkSession.sparkContext

      sparkContext.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      sparkContext.hadoopConfiguration.set("fs.s3a.access.key", configMap.getOrElse("awsAccessKeyId", null))
      sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", configMap.getOrElse("awsSecretAccessKey", null))
      val sqlContext = sparkSession.sqlContext
      logger.info(s"ReviewDataController# main # Application configuration is Over")

      val genericUtility: GenericUtility = new GenericUtility()
      val reviewDataService: ReviewDataService = new ReviewDataService(genericUtility)

      try {
        /**
         * Load review meta data from s3 (here I am reading from local context)
         * and persist the dataframe with partition [we can dynamically handle partition count from properties]
         */
        val reviewMetaDataPartitionDF = ReviewDataHelper.loadReviewMetaData(sparkSession).repartition(configMap.getOrElse("metaDataPartitionCount", null).toInt).persist(StorageLevel.MEMORY_AND_DISK_SER)

        /**
         * This spark consumer will read from topic each 60 seconds
         * and after the transaction is success we will commit the offset(Exactly Once Semantics).
         */
        logger.info(s"ReviewDataController# main # Streaming starts ")
        val sparkStreamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(60))

        val kafkaParams = Map[String, String](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> configMap.getOrElse("bootstrapServers", null),
          ConsumerConfig.GROUP_ID_CONFIG -> configMap.getOrElse("consumerGroup", null),
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
            "org.apache.kafka.common.serialization.StringDeserializer",
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
            "org.apache.kafka.common.serialization.StringDeserializer",
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
          ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
        )

        val topicSet = Set(configMap.getOrElse("topic", null))
        val reviewDStream = KafkaUtils.createDirectStream[String, String](sparkStreamingContext, PreferConsistent, Subscribe[String, String](topicSet, kafkaParams))

        /**
         * Logic For Multiple subscribers when load will be more in future
         */
        //        val streams = (1 to numPartitions) map { _ =>
        //          KafkaUtils.createDirectStream[String, String](sparkStreamingContext,
        //            PreferConsistent,
        //            Subscribe [String, String](topicSet, kafkaParams)
        //        }
        //        val reviewDStream = sparkStreamingContext.union(streams)

        reviewDStream.foreachRDD(reviewRDD => {

          // Extract Offset
          val offsetRanges = reviewRDD.asInstanceOf[HasOffsetRanges].offsetRanges
          offsetRanges.foreach(offRange => {
            logger.info("----------------ReviewDataController# main # fromOffset: " + offRange.fromOffset + " # untill Offset: " + offRange.untilOffset)
          })
          if (reviewRDD != null) {
            val reviewRawData: RDD[Review.Review] = reviewRDD.map(record => Review.parse(record.value()))
            /**
             * creted a new column id for elasticserach "_id" column value on which
             * we can do upsert using "es.write.operation" -> "upsert"
             */
            val reviewRawDF = sqlContext.createDataFrame(reviewRawData).withColumn("id", functions.concat(col("reviewerID"), col("asin")))
              .withColumn("reviewDateTime", functions.from_unixtime(col("unixReviewTime"), "yyyy-MM-dd HH:mm:ss"))
            // create data frame after removing any duplicates from reach RDD .
            val reviewMicroBatchDF = reviewRawDF.dropDuplicates("reviewerID", "asin", "unixReviewTime").withWatermark("reviewTime", "5 minutes")
            reviewMicroBatchDF.show()
            logger.info("ReviewDataController# main # reviewMicroBatchDF created ")

            /**
             * left join reviewMicroBatchDF with reviewMetaDataPartitionDF on "asin" column
             * left join because for a review we might not have any metadata.
             */
            val dataCount: Long = reviewMicroBatchDF.select(reviewMicroBatchDF.schema.fieldNames.head).count()
            if (dataCount > 0) {
              val reviewWithMetaDataDF: DataFrame = reviewMicroBatchDF.join(reviewMetaDataPartitionDF, reviewMicroBatchDF("asin") <=> reviewMetaDataPartitionDF("asin"), "left").drop(reviewMetaDataPartitionDF("asin")).drop(reviewMicroBatchDF("unixReviewTime"))
              reviewWithMetaDataDF.show()
              reviewWithMetaDataDF.printSchema()
              val pustToESFlag = configMap.getOrElse("pushDataToElastics", null)
              if(pustToESFlag != null && pustToESFlag.equalsIgnoreCase("Y")) {
                // push data into elasticsearch
                reviewDataService.pushReviewDataToElastic(reviewWithMetaDataDF, configMap)
              } else {
                // push data into mysql
                reviewDataService.pushReviewDataToMysql(reviewWithMetaDataDF, configMap)
              }
              // After all processing is done, offset is committed back to Kafka
              reviewDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
            }
          }
        })

        sparkStreamingContext.start()
        sparkStreamingContext.awaitTermination()

      }
      catch {
        case exp: Exception => {
          logger.error("\n\n" + "ReviewDataController# main # ERROR :" + exp.getMessage)
          exp.printStackTrace()
        }
      }
      finally {
        logger.info("\n\n ReviewDataController# main # Stopping sparkContext \n\n")
        sparkContext.stop()
      }
    }
  }

}


