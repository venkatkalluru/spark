#!/bin/bash

bin/run-example \
	org.apache.spark.examples.streaming.KafkaWordCount 10.204.99.230:8181 \
	test_group test_topic 1
