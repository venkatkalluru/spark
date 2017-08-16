#!/bin/bash

bin/run-example \
	org.apache.spark.examples.streaming.DirectKafkaWordCount localhost:9094 varuntopic cloSchemaTopic
