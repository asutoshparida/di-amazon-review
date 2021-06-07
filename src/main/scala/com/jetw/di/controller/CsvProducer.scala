package com.jetw.di.controller

import com.typesafe.scalalogging.LazyLogging
import java.io.{BufferedReader, FileReader}
import java.util.{Properties, Random}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class  CsvProducer(brokers:String, topic:String) extends LazyLogging {

  val props = createProducerConfig(brokers)
  val producer = new KafkaProducer[String, String](props)

  def createProducerConfig(brokers: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

 /**
  * This def creates a rondom time sleep &
  * then publish the csv lines to kafka topic.
  */
  @throws(classOf[Exception])
  def publish(file:String): Unit =  {

    val rand: Random = new Random
    val br = new BufferedReader(new FileReader(file))

    var line = br.readLine()
    logger.info(s"CsvProducer# publish# line: ${line}")
    while(line != null) {
      val producerRecord = new ProducerRecord[String, String](topic, line)
      producer.send(producerRecord)
      line = br.readLine()
      Thread.sleep(rand.nextInt(1000 - 500) + 100)
      logger.info(s"CsvProducer# publish# next line: ${line}")
    }
    br.close()
    producer.close()
  }
}

object CsvProducer extends LazyLogging{

  def main(args: Array[String]) {

    if (args.length != 1) {
      logger.info("Please provide command line arguments: topic and transaction file path")
      System.exit(-1)
    }
    val topic = args(0)

    val transactionfile = "/opt/SAP-2607/di-amazon-review/src/main/resources/ratings_Movies_and_TV.csv"
    val csvProducer = new CsvProducer("localhost:9092", topic)
    csvProducer.publish(transactionfile)

  }

}