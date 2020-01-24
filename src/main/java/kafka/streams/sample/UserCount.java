package kafka.streams.sample;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import kafka.streams.sample.avro.User;
import kafka.streams.sample.serde.UserSerde;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserCount {

  private final Properties props;

  private static final UserSerde USER_SERDE = getUserSerde();

  private static UserSerde getUserSerde() {
    val serdeConfig =
        Collections.singletonMap(
            AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            UserService.SCHEMA_REGISTRY_URL);
    val userSerde = new UserSerde();
    userSerde.configure(serdeConfig, false);
    return userSerde;
  }

  private Properties createProperties() {
    val p = new Properties();
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-topic-stream");
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, UserService.BOOTSTRAP_SERVERS);
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, USER_SERDE.getClass());
    p.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, UserService.SCHEMA_REGISTRY_URL);
    return p;
  }

  public UserCount() {
    this.props = this.createProperties();
  }

  public void aggregate() {
    val builder = new StreamsBuilder();

    KStream<Long, User> source =
        builder.<Long, User>stream(UserService.TOPIC, Consumed.with(Serdes.Long(), USER_SERDE));

    source
        .mapValues(value -> value)
        .groupBy((key, value) -> value, Grouped.with(USER_SERDE, null))
        .count(Materialized.<User, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
        .toStream()
        .to("user-counts-output", Produced.with(USER_SERDE, Serdes.Long()));

    val topology = builder.build();
    log.info(topology.describe().toString());

    val streams = new KafkaStreams(topology, this.props);
    val latch = new CountDownLatch(1);

    // attach shutdown handler to catch control-c
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread("streams-shutdown-hook") {
              @Override
              public void run() {
                streams.close();
                latch.countDown();
              }
            });

    try {
      streams.start();
      latch.await();
    } catch (Exception e) {
      System.exit(1);
    }
    System.exit(0);
  }
}
