package kafka.streams.sample.stream.user;

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import kafka.streams.sample.avro.User;
import kafka.streams.sample.processor.PrintUserProcessor;
import kafka.streams.sample.stream.SampleStreams;
import kafka.streams.sample.util.DateTimeUtil;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
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
public class UserStreams implements SampleStreams {

  private static final String COUNT_BY_USER_TOPIC = "user-topic-agg-count-by-user";
  public static final String STORE_COUNTS = "user-store-counts";
  public static final String USER_TOPIC = "user-topic";

  private final Properties props;

  public UserStreams(UserConfig config) {
    this.props = config.getProps();
  }

  private Map<String, String> changelogConfig =
      Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1");

  private final StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier =
      Stores.keyValueStoreBuilder(
              Stores.persistentKeyValueStore(STORE_COUNTS), Serdes.String(), Serdes.Long())
          .withCachingDisabled()
          .withLoggingEnabled(this.changelogConfig);

  private static final long WINDOW_SEC = 5;

  @VisibleForTesting
  void aggregateUserCounts(StreamsBuilder builder) {
    val source =
        builder.<Long, User>stream(USER_TOPIC, Consumed.with(Serdes.Long(), UserConfig.USER_SERDE));
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
        .to(COUNT_BY_USER_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
  }

  @VisibleForTesting
  void showUserCounts(Topology topology) {
    val sourceName = "source-" + COUNT_BY_USER_TOPIC;
    val processorName = PrintUserProcessor.class.getSimpleName();
    topology.addSource(
        sourceName,
        Serdes.String().deserializer(),
        Serdes.Long().deserializer(),
        COUNT_BY_USER_TOPIC);
    topology.addProcessor(processorName, PrintUserProcessor::new, sourceName);
    topology.addStateStore(this.countStoreSupplier, processorName);
  }

  @Override
  public Topology createTopology() {
    val builder = new StreamsBuilder();
    this.aggregateUserCounts(builder);
    val topology = builder.build();
    this.showUserCounts(topology);
    log.info("\n{}", topology.describe());
    return topology;
  }

  @Override
  public Properties getProperties() {
    return this.props;
  }
}
