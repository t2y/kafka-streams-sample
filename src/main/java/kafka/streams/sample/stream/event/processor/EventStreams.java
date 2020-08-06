package kafka.streams.sample.stream.event.processor;

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.Properties;
import kafka.streams.sample.serde.MySerdes;
import kafka.streams.sample.stream.event.EventStreamsConfig;
import kafka.streams.sample.stream.event.Store;
import kafka.streams.sample.stream.event.Topic;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

@Slf4j
public class EventStreams {

  private final Properties props;
  private final StreamsBuilder builder;

  private StoreBuilder<WindowStore<String, Long>> chunkNumAggregationBuilder =
      Stores.windowStoreBuilder(
              Stores.persistentWindowStore(
                  Store.CHUNK_NUM_AGGREGATION.getName(),
                  Duration.ofDays(7),
                  Duration.ofMinutes(1),
                  false),
              Serdes.String(),
              Serdes.Long())
          .withCachingEnabled();

  private StoreBuilder<WindowStore<Long, Long>> userIdAggregationBuilder =
      Stores.windowStoreBuilder(
              Stores.persistentWindowStore(
                  Store.USER_ID_AGGREGATION.getName(),
                  Duration.ofDays(31),
                  Duration.ofDays(1),
                  false),
              Serdes.Long(),
              Serdes.Long())
          .withCachingEnabled();

  public EventStreams(EventStreamsConfig config) {
    this.props = config.getProps();
    this.builder = new StreamsBuilder();
    this.builder.addStateStore(chunkNumAggregationBuilder);
    this.builder.addStateStore(userIdAggregationBuilder);
  }

  @VisibleForTesting
  void addEventAggregation(Topology topology) {
    topology.addSource(
        Topic.MY_EVENT.getName(),
        Serdes.String().deserializer(),
        MySerdes.EVENT_SERDE.deserializer(),
        Topic.MY_EVENT.getName());

    val eventAggregation = EventAggregationProcessor.class.getSimpleName();
    topology.addProcessor(
        eventAggregation, EventAggregationProcessor::new, Topic.MY_EVENT.getName());
    topology.connectProcessorAndStateStores(
        eventAggregation, Store.CHUNK_NUM_AGGREGATION.getName());

    topology.addSink(
        Topic.MY_QUEUE.getName() + "-sink",
        Topic.MY_QUEUE.getName(),
        Serdes.String().serializer(),
        Serdes.Long().serializer(),
        eventAggregation);
  }

  @VisibleForTesting
  void addUserIdRepartition(Topology topology) {
    topology.addSource(
        Topic.MY_QUEUE.getName(),
        Serdes.String().deserializer(),
        Serdes.Long().deserializer(),
        Topic.MY_QUEUE.getName());

    val userIdRepartition = UserIdRepartitionProcessor.class.getSimpleName();
    topology.addProcessor(
        userIdRepartition, UserIdRepartitionProcessor::new, Topic.MY_QUEUE.getName());

    topology.addSink(
        Topic.MY_REPARTITION.getName() + "-sink",
        Topic.MY_REPARTITION.getName(),
        Serdes.Long().serializer(),
        Serdes.Long().serializer(),
        userIdRepartition);
  }

  @VisibleForTesting
  void addAggregationByUserId(Topology topology) {
    topology.addSource(
        Topic.MY_REPARTITION.getName(),
        Serdes.Long().deserializer(),
        Serdes.Long().deserializer(),
        Topic.MY_REPARTITION.getName());

    val aggregationByUserId = AggregationByUserIdProcessor.class.getSimpleName();
    topology.addProcessor(
        aggregationByUserId, AggregationByUserIdProcessor::new, Topic.MY_REPARTITION.getName());
    topology.connectProcessorAndStateStores(
        aggregationByUserId, Store.USER_ID_AGGREGATION.getName());

    topology.addSink(
        Topic.MY_AGGREGATION.getName() + "-sink",
        Topic.MY_AGGREGATION.getName(),
        Serdes.Long().serializer(),
        Serdes.Long().serializer(),
        aggregationByUserId);
  }

  @VisibleForTesting
  Topology createTopology() {
    val topology = builder.build(this.props);
    // Sub-topology: 0
    this.addEventAggregation(topology);
    // Sub-topology: 1
    this.addUserIdRepartition(topology);
    // Sub-topology: 2
    this.addAggregationByUserId(topology);
    return topology;
  }
}
