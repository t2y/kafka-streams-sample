package kafka.streams.sample.serde;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import kafka.streams.sample.avro.User;

public class UserSerde extends SpecificAvroSerde<User> {}
