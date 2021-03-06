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
