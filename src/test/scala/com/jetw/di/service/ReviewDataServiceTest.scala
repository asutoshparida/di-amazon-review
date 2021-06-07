package com.jetw.di.service

import com.jetw.di.util.GenericUtility
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import scala.collection.mutable.Map
import org.scalatestplus.mockito.MockitoSugar
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}
import org.mockito._
import org.mockito.Matchers.any

class ReviewDataServiceTest {

  var sparkContext: SparkContext = _
  var sqlContext: SQLContext = _
  var genericUtility: GenericUtility = _
  var reviewDataService: ReviewDataService = _

  case class ReviewsSchema(id:String, reviewerID:String, asin:String, overall:Double, price:Double, reviewDateTime:String, description:String, imUrl:String, title:String)

  @Before
  def beforeExecuteTest(): Unit = {
    sparkContext = SparkContext.getOrCreate(new SparkConf().setAppName("ReviewDataServiceTest").setMaster("local[*]"))
    sqlContext = new SQLContext(sparkContext)
    genericUtility = MockitoSugar.mock[GenericUtility]
    reviewDataService = new ReviewDataService(genericUtility)

  }

  @Test
  def pushReviewDataToElasticTest(): Unit = {
    val reviewData = Seq(ReviewsSchema("A3VCKTRD24BG7K0000589012", "A3VCKTRD24BG7K", "0000589012",5.0,12.99, "2013-09-05 20:00:00", "", "", ""),
      ReviewsSchema("A3W3EB24TA5BSI0001526863", "A3W3EB24TA5BSI", "0001526863",4.0,0.0, "2013-08-10 20:00:00", "232434", "http://g-ecx.images-amazon.com/images/G/01/x-site/icons/no-img-sm._CB192198896_.gif", "Everyday Italian (with Giada de Laurentiis), Volume 1 (3 Pack): Italian Classics, Parties, Holidays"),
      ReviewsSchema("A5Y15SAOMX6XA0000589012", "A5Y15SAOMX6XA", "0000589012",3.0,0.9, "2011-06-06 20:00:00",  "", "", ""))
    val reviewDataDF: DataFrame = sqlContext.createDataFrame(reviewData)

    val configMap: Map[String, String] = Map(("esIndexName", "amazon_review"), ("esNodes","""https:\\test"""), ("esPort","443"))
    Mockito.doNothing().when(genericUtility).insertUpdateESIndexDocument(any[String], any[String], any[String], any[String], any[DataFrame])

    reviewDataService.pushReviewDataToElastic(reviewDataDF, configMap.toMap)
  }


}
