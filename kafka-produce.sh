#!/bin/bash

bin/run-example \
	org.apache.spark.examples.streaming.KafkaWordCountProducer 10.204.99.230:9092 \
	test_topic 5 20
