package kafka.streams.sample;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import kafka.streams.sample.avro.User;
import kafka.streams.sample.processor.PrintUserProcessor;
import kafka.streams.sample.serde.UserSerde;
import kafka.streams.sample.util.DateTimeUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserCount {

  private final Properties props;

  private static final UserSerde USER_SERDE = getUserSerde();

  private static final String COUNT_BY_USER_TOPIC = "user-topic-agg-count-by-user";

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
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
    p.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, UserService.SCHEMA_REGISTRY_URL);
    return p;
  }

  public UserCount() {
    this.props = this.createProperties();
  }

  private Map<String, String> changelogConfig =
      new HashMap<>() {
        private static final long serialVersionUID = 1L;

        {
          put("min.insync.replicas", "1"); // override min.insync.replicas
        }
      };

  private StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier =
      Stores.keyValueStoreBuilder(
              Stores.persistentKeyValueStore(UserService.STORE_COUNTS),
              Serdes.String(),
              Serdes.Long())
          .withLoggingEnabled(
              this.changelogConfig); // enable changelogging, with custom changelog settings

  private void aggregateUserCounts(StreamsBuilder builder) {
    val topicConsumed = Consumed.with(Serdes.Long(), USER_SERDE);
    val countByUserProduced = Produced.with(Serdes.String(), Serdes.Long());
    val source = builder.<Long, User>stream(UserService.USER_TOPIC, topicConsumed);
    source
        .mapValues(value -> value.getName())
        .groupBy((key, value) -> value, Grouped.with(Serdes.String(), null))
        .windowedBy(TimeWindows.of(Duration.ofSeconds(2)))
        .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("local-counts-store"))
        .toStream()
        .map(
            (k, v) -> {
              val key = k.key();
              val startEnd = DateTimeUtil.getWindowStartAndEnd(k.window());
              log.info(startEnd + ": " + key + ", " + String.valueOf(v));
              return new KeyValue<>(key, v);
            })
        .to(COUNT_BY_USER_TOPIC, countByUserProduced);
  }

  private void showUserCounts(Topology topology) {
    val sourceName = "source-" + COUNT_BY_USER_TOPIC;
    val processorName = PrintUserProcessor.class.getSimpleName();
    topology.addSource(
        sourceName,
        Serdes.String().deserializer(),
        Serdes.Long().deserializer(),
        COUNT_BY_USER_TOPIC);
    topology.addProcessor(processorName, PrintUserProcessor::new, sourceName);
    topology.addStateStore(countStoreSupplier, processorName);
  }

  public void aggregate() {
    val builder = new StreamsBuilder();
    this.aggregateUserCounts(builder);
    val topology = builder.build();
    this.showUserCounts(topology);

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
