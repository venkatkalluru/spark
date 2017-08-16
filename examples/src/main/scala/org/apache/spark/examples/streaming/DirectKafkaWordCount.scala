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
import org.apache.avro.SchemaNormalization
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.Decoder
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.io.DecoderFactory
import org.apache.spark.broadcast.Broadcast
import java.nio.ByteBuffer

import java.io.EOFException

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

    val schemaCache = collection.mutable.Map[Long, String]()

    def processSchemas(msg: (String, String)): (Long, String) = {

      val (k, v) = msg
      val schemaStr = v.asInstanceOf[String]
      val schema = new Schema.Parser().parse(schemaStr);
      //      val hashCode = schemaStr.hashCode()
      val hashCode = SchemaNormalization.parsingFingerprint64(schema)
      //      println("Schema Hash Code " + schemaStr.hashCode() + " \n" + "Message is" + schemaStr + "\n")      

      return (hashCode, schemaStr)
    }

    schemaStrm.foreachRDD(rdd => {
      rdd.map(x => processSchemas(x)).collect().foreach(x => {
        println("Schemas is " + x)
        val (k, v) = x
        schemaCache.put(k, v)
      })
    })

    // Create direct kafka stream with brokers and topics to pull from message stream   
    val msgTopicsSet = msgTopics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    //    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val msgStrm = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
      ssc, kafkaParams, msgTopicsSet)

    val is = getClass.getResourceAsStream("/gg.avsc")
    val source = scala.io.Source.fromInputStream(is)
    val schemaStr = try source.mkString finally source.close()
    //println(schemaStr)

    def decodeOracleWrapper(message: Array[Byte]): collection.mutable.Map[Long, List[ByteBuffer]] = {

      var hashCodeToEvents = collection.mutable.Map[Long, List[ByteBuffer]]()

      //        TODO: These few lines of code can also be avoided by broadcasting the final decoder.  
      val schema = new Schema.Parser().parse(schemaStr);
      val reader = new SpecificDatumReader[GenericRecord](schema)
      val decoder = DecoderFactory.get().binaryDecoder(message, null)

      var data: GenericRecord = null
      try {
        data = reader.read(null, decoder)
        //        println(s"First data is $data")
      } catch {
        case ex: Exception => {
          println("Got incorrect oraclewrapped avro data") //TODO Remove this stmt later.
          println(ex)
        }
      }

      try {
        while (data != null) {
          val hashCode = data.get("schema_fingerprint").asInstanceOf[Long]
          if (hashCodeToEvents.contains(hashCode)) {
            var dataList = hashCodeToEvents(hashCode)
            dataList ::= data.get("payload").asInstanceOf[ByteBuffer]
            hashCodeToEvents.put(hashCode, dataList)
          } else {
            var newList = List[ByteBuffer](data.get("payload").asInstanceOf[ByteBuffer])
            hashCodeToEvents.put(hashCode, newList)
          }
          data = reader.read(null, decoder)
          //          println(s"Next data is $data")
        }
      } catch {
        case ex: EOFException => {
          println("Mostly end of the buffer or possible incorrect oraclewrapped avro data") //TODO Remove this stmt later.
        }
      }
      return hashCodeToEvents
    }

    def printEventData(inputData: collection.mutable.Map[Long, List[ByteBuffer]]) = {

      for ((k, v) <- inputData) {

        //        println(s"Key is $k and Value is $v")
        if (schemaCache.contains(k)) {

          val schemaStr = schemaCache apply k

          //          println(s"Retreive schema is $schemaStr")

          val schema = new Schema.Parser().parse(schemaStr);
          val reader = new SpecificDatumReader[GenericRecord](schema)

          for (bb <- v) {

            //            println(s"Byte buffer is $bb")
            val decoder = DecoderFactory.get().binaryDecoder(bb.array(), null)
            var data = reader.read(null, decoder)
            println(s"Deserialized message is $data")
          }
        }
      }
    }

    val messages = msgStrm.map(_._2)
    val decodedMsgs = messages.map(msg => decodeOracleWrapper(msg.asInstanceOf[Array[Byte]]))
    val msgs = decodedMsgs.map(x => printEventData(x))
    msgs.count()
    msgs.foreachRDD(rdd => { println("No. of decoded messages are " + rdd.count()) })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
