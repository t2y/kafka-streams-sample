package kafka.streams.sample.serde;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kafka.streams.sample.avro.Order;

public class OrderSerde extends SpecificAvroSerde<Order> {}
