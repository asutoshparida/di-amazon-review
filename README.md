# amazon-review-pipeline
This repository will be used for managing all the code related to pipeline for amazon movies review data.

-----

# Requirements

    JDK 1.7 or higher
    maven
    Scala 2.11.11
    Spark 2.3.3
    Apache kafka 2.11-2.4.1
    elasticsearch 5.X
    mysql
    *AWS Lambda
    *AWS EMR

    * If we want to run the job on AWS EMR Using AWS Lambda.
----

# Build
    
    *** Change your /config/pipeline_config.json entries 
    according to your configuration ***

    Go to project root dir
    > mvn clean install -U

------

# Run

    Can use /script/lambda/amazon_review_lambda.py to configure a lambda function on AWS lambda
    by changing your VPC & security grougp configuration.

    Or For dry run execute ReviewDataController.scala first and then CsvProducer.scala as scala application from intellij

------