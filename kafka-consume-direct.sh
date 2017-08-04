#!/bin/bash

bin/run-example \
	org.apache.spark.examples.streaming.DirectKafkaWordCount 10.204.99.230:9092 msg-topic-1 schema-topic-1
