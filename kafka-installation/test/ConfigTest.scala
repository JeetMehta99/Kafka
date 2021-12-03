package test

import org.scalatest.flatspec.AnyFlatSpec
import HelperUtils.ObtainConfigReference
import com.amazonaws.services.s3.model.{GetObjectRequest, PutObjectRequest}


class ConfigTest extends AnyFlatSpec {

  // Initializing config reader
  val config = ObtainConfigReference("akka") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  // Testing for bootstrap servers in config
  "kafka config" should "contain bootstrap servers" in {
    val address = config.getString("kafka.host")
    val expected_address = config.getString("akka.kafka.producer.kafka-clients.bootstrap.servers")
    assert(address == expected_address)
  }
}

class ConfigTest extends AnyFlatSpec {

  // Initializing config reader
  val config = ObtainConfigReference("randomLogGenerator") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  // Testing for Pattern in config
  "Pattern config" should "contain Pattern" in {
    val pattern = config.getString("randomLogGenerator.Frequency")
    val expected_pattern = config.getString("randomLogGenerator.Pattern")
    assert(endpoint == expected_pattern)
  }

}