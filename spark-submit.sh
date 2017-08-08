#!/bin/bash

./bin/spark-submit \
	--class org.apache.spark.examples.streaming.DirectKafkaWordCount \
	--master spark://ip-10-204-99-241.dqa.capitalone.com:6066 \
	--deploy-mode cluster \
	--supervise /home/ucf347/spark/examples/target/spark-examples_2.11-2.3.0-SNAPSHOT-jar-with-dependencies.jar 10.204.99.230:9092 msg-topic-4 schema-topic-4
