package Kafka

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties
import scala.concurrent.duration._

object KafkStream {

  object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String

    case class Order(orderId: OrderId, user: UserId, products: List[Product], amount: Double)
    case class Discount(profile: Profile, amount: Double) // in percentage points
    case class Payment(orderId: OrderId, status: String)
  }

  object Topics {
    final val OrdersByUserTopic = "orders-by-user"
    final val DiscountProfilesByUserTopic = "discount-profiles-by-user"
    final val DiscountsTopic = "discounts"
    final val OrdersTopic = "orders"
    final val PaymentsTopic = "payments"
    final val PaidOrdersTopic = "paid-orders"
  }
  import Domain._
  implicit def serdeOrder[A >: Null : Decoder : Encoder]: Serde[A] = {
    val serializer = (a: A) => a.asJson.noSpaces.getBytes()
    val deserializer = (bytes: Array[Byte]) => {
      val string = new String(bytes)
      decode[A](string).toOption
    }

    Serdes.fromFn[A](serializer, deserializer)
  }
