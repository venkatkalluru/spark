#!/bin/bash

bin/run-example \
	org.apache.spark.examples.streaming.DirectKafkaWordCount 10.204.99.230:9092 bdglue-topic schema-topic
