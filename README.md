kafka-s3-consumer
=================

Store batched Kafka messages in S3.

Build
-----------------

  mvn package

Run
-----------------

  java -jar kafka-s3-consumer-1.0.jar <props>

  #in order to start after some time of inactivity (so there are logs stored,
  #but the gap is larger between last uploaded log end the first record in kafka

  java -jar kafka-s3-consumer-1.0.jar <props> clean