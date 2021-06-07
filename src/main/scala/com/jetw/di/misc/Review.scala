package com.jetw.di.misc

import com.typesafe.scalalogging._

object Review extends LazyLogging {

  case class Review(reviewerID:String, asin:String, overall:Double, unixReviewTime:String)

  val amazon_review_mapping = "{ \"settings\": {\"index.mapper.dynamic\": false,\"number_of_replicas\": 1,\"number_of_shards\": 1 },\"mappings\": {\"data\": {  \"dynamic\": false,  \"properties\": {    \"overall\": {      \"type\": \"double\"    },    \"price\": {      \"type\": \"double\"    },    \"asin\": {      \"type\": \"text\",      \"fields\": {        \"raw\": {          \"type\": \"keyword\",          \"ignore_above\": 256        }      }    },    \"requisition_id\": {      \"type\": \"text\",      \"fields\": {        \"raw\": {          \"type\": \"keyword\",          \"ignore_above\": 256        }      }    },    \"reviewerID\": {      \"type\": \"text\",      \"fields\": {        \"raw\": {          \"type\": \"keyword\",          \"ignore_above\": 256        }      }    },    \"job_category\": {      \"type\": \"text\",      \"fields\": {        \"raw\": {          \"type\": \"keyword\",          \"ignore_above\": 256        }      }    },    \"title\": {      \"type\": \"text\",      \"fields\": {        \"raw\": {          \"type\": \"keyword\",          \"ignore_above\": 256        }      }    },    \"description\": {      \"type\": \"text\",      \"fields\": {        \"raw\": {          \"type\": \"keyword\",          \"ignore_above\": 256        }      }    },    \"imUrl\": {      \"type\": \"text\",      \"fields\": {        \"raw\": {          \"type\": \"keyword\",          \"ignore_above\": 256        }      }    },    \"reviewDateTime\": {      \"type\": \"date\",      \"format\": \"yyyy/MM/dd HH:mm:ss||yyyy/MM/dd HH:mm:ss.SSS||yyyy/MM/dd||epoch_millis||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd\"    }  }} }}"

  /**
   * This function splits the comma separated values & if not a valid review event
   * then returns a default Review "Review("NA", "NA", 0.0, "0000")"
   * @param transactionRecord
   * @return Review case class
   */
  def parse(transactionRecord: String) = {
    val defaultReview = Review("NA", "NA", 0.0, "0000")
    if(transactionRecord.indexOf(",") > 0) {
      try {
        val fields = transactionRecord.split(",")
        val reviewerID = fields(0)
        val asin = fields(1)
        val overall = fields(2).toDouble
        val unixReviewTime = fields(3)
        Review(reviewerID, asin, overall, unixReviewTime)
      } catch {
        case exp: Exception => {
          logger.error("\n\n" + "Review# parse # ERROR :" + exp.getMessage)
          exp.printStackTrace()
          defaultReview
        }
      }
    } else {
      defaultReview
    }
  }

}