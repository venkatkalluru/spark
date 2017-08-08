#!/bin/bash

./bin/spark-submit \
	--class org.apache.spark.examples.streaming.DirectKafkaWordCount \
	--master spark://10.204.99.241:6066 \
	--deploy-mode cluster \
	--jars /home/ucf347/spark/examples/target/spark-examples_2.11-2.3.0-SNAPSHOT-jar-with-dependencies.jar \
	--supervise \
	10.204.99.230:9092 msg-topic-4 schema-topic-4
