package Kafka

import HelperUtils.ObtainConfigReference
import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

object Consumer extends App {
  implicit val system: ActorSystem = ActorSystem("consumer-sys")
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val config = ObtainConfigReference("akka") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  val consumerConfig = config.getConfig("akka.kafka.consumer")
  val consumerSettings = ConsumerSettings(consumerConfig, new StringDeserializer, new StringDeserializer)

  val consume = Consumer
    .plainSource(consumerSettings, Subscriptions.topics(config.getString("akka.kafka.topic")))
    .runWith(
      Sink.foreach(record => println(s"Received log: ${record.value()}"))
    )

  consume onComplete  {
    case Success(_) => println("Done"); system.terminate()
    case Failure(err) => println(err.toString); system.terminate()
  }
}

//
//
//import com.ippontech.kafka.stores.OffsetsStore
//import com.typesafe.scalalogging.slf4j.LazyLogging
//import kafka.message.MessageAndMetadata
//import kafka.serializer.Decoder
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//
//import scala.reflect.ClassTag
//
//object KafkaSource extends LazyLogging {
//
//  def kafkaStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag]
//  (ssc: StreamingContext, kafkaParams: Map[String, String], offsetsStore: OffsetsStore, topic: String): InputDStream[(K, V)] = {
//
//    val topics = Set(topic)
//
//    val storedOffsets = offsetsStore.readOffsets(topic)
//    val kafkaStream = storedOffsets match {
//      case None =>
//        // start from the latest offsets
//        KafkaUtils.createDirectStream[K, V, KD, VD](ssc, kafkaParams, topics)
//      case Some(fromOffsets) =>
//        // start from previously saved offsets
//        val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
//        KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](ssc, kafkaParams, fromOffsets, messageHandler)
//    }
//
//    // save the offsets
//    kafkaStream.foreachRDD(rdd => offsetsStore.saveOffsets(topic, rdd))
//
//    kafkaStream
//  }
//
//  // Kafka input stream
//  def kafkaStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag]
//  (ssc: StreamingContext, brokers: String, offsetsStore: OffsetsStore, topic: String): InputDStream[(K, V)] =
//    kafkaStream(ssc, Map("metadata.broker.list" -> brokers), offsetsStore, topic)
//
//}