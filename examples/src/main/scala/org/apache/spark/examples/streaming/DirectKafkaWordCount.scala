/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.streaming

import kafka.serializer.DefaultDecoder
import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.Decoder
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.io.DecoderFactory

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    topic1,topic2
 */
object DirectKafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(brokers, msgTopics, schemaTopics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("fs.s3a.server-side-encryption-algorithm", "AES256")
    
    val ssc = new StreamingContext(sc, Seconds(2))

    // Create direct kafka stream with brokers and topics to pull from schema stream
    val schemaTopicSet = schemaTopics.split(",").toSet
    val schemaKafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val schemaStrm = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, schemaKafkaParams, schemaTopicSet)
      
    val schemaCache = collection.mutable.Map[Int, String]()
    var myBroadcast = sc.broadcast(schemaCache)
      
    def processSchemas(msg: (String, String)): (Int, String) = {
      
      val (k, v) = msg
      val schemaStr = v.asInstanceOf[String]
      val hashCode = schemaStr.hashCode()
      println("Schema Hash Code " + schemaStr.hashCode() + " \n" + "Message is" + schemaStr + "\n")      
      
      return (hashCode, schemaStr)
    }
    
    //val schemas = schemaStrm.map(s => processSchemas(s))
    //schemas.print()
    
    schemaStrm.foreachRDD(rdd => {
      rdd.map(x => processSchemas(x)).collect().foreach(x => {
        //println(x)
        val (k, v) = x
        schemaCache.put(k, v)
        println("Schema Map size is " + schemaCache.size)
      })
    })    
             
    // Create direct kafka stream with brokers and topics to pull from message stream
    val msgTopicsSet = msgTopics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val msgStrm = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
      ssc, kafkaParams, msgTopicsSet)

      
    val source = scala.io.Source.fromFile("/tmp/schema.avsc")
    //val schemaStr = try source.mkString finally source.close()

    def printDecodeData(message: Array[Byte]): String = {

      //  Deserialize and create generic record
      /*
      println("Schema size is " + schemaCache.size)
      val schemaStr = schemaCache.get(-1120428818).get
      val schema = new Schema.Parser().parse(schemaStr);
      val reader = new SpecificDatumReader[GenericRecord](schema)
      val decoder = DecoderFactory.get().binaryDecoder(message, null)
      val userData = reader.read(null, decoder)
      println(userData)
      return userData.toString()
      * 
      */
      //println(message.toString())
      return "Nothing"
    }
    
    val messages = msgStrm.map(_._2)
    val decodedMsgs = messages.map(msg => printDecodeData(msg.asInstanceOf[Array[Byte]]))
    decodedMsgs.saveAsTextFiles("prefix", "suffix")
//    decodedMsgs.foreachRDD(rdd =>
//      rdd.saveAsTextFile("s3a://coafstatim/venkat-cdc-testing/")
//    )

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
