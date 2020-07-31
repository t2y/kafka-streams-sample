package kafka.streams.sample.stream.event;

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.Properties;
import kafka.streams.sample.avro.Event;
import kafka.streams.sample.serde.MySerdes;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;

@Slf4j
public class EventStreams {

  private final Properties props;
  private final StreamsBuilder builder;

  public EventStreams(EventStreamsConfig config) {
    this.props = config.getProps();
    this.builder = new StreamsBuilder();
  }

  @VisibleForTesting
  void buildEventAggregation(KStream<String, Event> source) {
    Materialized<String, Long, WindowStore<Bytes, byte[]>> materialized =
        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(
                Store.CHUNK_NUM_AGGREGATION.getName())
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.Long());

    KStream<String, Long> aggregated =
        source
            .groupBy(
                (key, value) -> {
                  val chunkNum = String.valueOf(value.getUserId() / 4);
                  return String.format("%d_%s", value.getUserId(), chunkNum);
                },
                Grouped.with(Serdes.String(), MySerdes.EVENT_SERDE))
            .windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
            .aggregate(
                () -> 0L,
                (key, value, aggregate) -> {
                  Long total = aggregate;
                  switch (value.getType()) {
                    case VIEW:
                    case STOCK:
                      total += 2;
                    case BUY:
                      total += 10;
                    default:
                      total += 1;
                      break;
                  }
                  return total;
                },
                materialized)
            .toStream((windowedKey, value) -> windowedKey.key());

    aggregated.process(MyQueueProcessor::new);
    aggregated.to(Topic.MY_QUEUE.getName(), Produced.with(Serdes.String(), Serdes.Long()));
  }

  @VisibleForTesting
  void buildAggregationByUserId(KStream<String, Long> source) {
    KStream<Long, Long> aggregated =
        source
            .groupBy(
                (key, value) -> Long.valueOf(key.split("_")[0]),
                Grouped.with(Serdes.Long(), Serdes.Long()))
            .windowedBy(TimeWindows.of(Duration.ofDays(1)))
            .count(Materialized.as(Store.USER_ID_AGGREGATION.getName()))
            .toStream((windowedKey, value) -> windowedKey.key());

    aggregated.to(Topic.MY_AGGREGATION.getName(), Produced.with(Serdes.Long(), Serdes.Long()));
  }

  @VisibleForTesting
  Topology createTopology() {
    KStream<String, Event> event =
        this.builder.stream(
            Topic.MY_EVENT.getName(),
            Consumed.with(Serdes.String(), MySerdes.EVENT_SERDE)
                .withName(Topic.MY_EVENT.getName()));

    KStream<String, Long> queue =
        this.builder.stream(
            Topic.MY_QUEUE.getName(),
            Consumed.with(Serdes.String(), Serdes.Long()).withName(Topic.MY_QUEUE.getName()));

    KStream<Long, Long> aggregation =
        this.builder.stream(
            Topic.MY_AGGREGATION.getName(),
            Consumed.with(Serdes.Long(), Serdes.Long()).withName(Topic.MY_AGGREGATION.getName()));

    this.buildEventAggregation(event);
    this.buildAggregationByUserId(queue);
    // aggregation.print(Printed.toSysOut());
    aggregation.process(MyAggregationProcessor::new);

    val topology = builder.build(this.props);
    return topology;
  }
}
