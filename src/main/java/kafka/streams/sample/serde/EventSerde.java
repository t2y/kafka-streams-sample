package kafka.streams.sample.serde;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kafka.streams.sample.avro.Event;

public class EventSerde extends SpecificAvroSerde<Event> {}
