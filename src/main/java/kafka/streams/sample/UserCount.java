package kafka.streams.sample;

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import kafka.streams.sample.avro.User;
import kafka.streams.sample.processor.PrintUserProcessor;
import kafka.streams.sample.util.DateTimeUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.internals.WindowingDefaults;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

@Slf4j
public class UserCount {

  @Getter private final Properties props;

  public UserCount(UserConfig config) {
    this.props = config.getProps();
  }

  private Map<String, String> changelogConfig =
      Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1");

  private final StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier =
      Stores.keyValueStoreBuilder(
              Stores.persistentKeyValueStore(UserService.STORE_COUNTS),
              Serdes.String(),
              Serdes.Long())
          .withLoggingEnabled(this.changelogConfig);

  private static final long WINDOW_SEC = 5;

  @VisibleForTesting
  void aggregateUserCounts(StreamsBuilder builder) {
    val topicConsumed = Consumed.with(Serdes.Long(), UserConfig.USER_SERDE);
    val countByUserProduced = Produced.with(Serdes.String(), Serdes.Long());
    val source = builder.<Long, User>stream(UserService.USER_TOPIC, topicConsumed);
    source
        .mapValues(User::getName)
        .groupBy((key, value) -> value, Grouped.with(Serdes.String(), null))
        .windowedBy(TimeWindows.of(Duration.ofSeconds(WINDOW_SEC)))
        .count(
            Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("local-counts-store")
                .withRetention(Duration.ofMillis(WindowingDefaults.DEFAULT_RETENTION_MS + 1)))
        .toStream()
        .map(
            (k, v) -> {
              val key = k.key();
              val startEnd = DateTimeUtil.getWindowStartAndEnd(k.window());
              log.info("{}: {}, {}", startEnd, key, v.toString());
              return new KeyValue<>(key, v);
            })
        .to(UserService.COUNT_BY_USER_TOPIC, countByUserProduced);
  }

  @VisibleForTesting
  void showUserCounts(Topology topology) {
    val sourceName = "source-" + UserService.COUNT_BY_USER_TOPIC;
    val processorName = PrintUserProcessor.class.getSimpleName();
    topology.addSource(
        sourceName,
        Serdes.String().deserializer(),
        Serdes.Long().deserializer(),
        UserService.COUNT_BY_USER_TOPIC);
    topology.addProcessor(processorName, PrintUserProcessor::new, sourceName);
    topology.addStateStore(countStoreSupplier, processorName);
  }

  @VisibleForTesting
  Topology createTopology() {
    val builder = new StreamsBuilder();
    this.aggregateUserCounts(builder);
    val topology = builder.build();
    this.showUserCounts(topology);
    log.info("\n{}", topology.describe());
    return topology;
  }

  public void aggregate() {
    val topology = this.createTopology();
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
