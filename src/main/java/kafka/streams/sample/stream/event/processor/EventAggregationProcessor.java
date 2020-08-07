package kafka.streams.sample.stream.event.processor;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import kafka.streams.sample.avro.Event;
import kafka.streams.sample.stream.event.Store;
import kafka.streams.sample.stream.event.Utils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.WindowStore;

@Slf4j
public class EventAggregationProcessor implements Processor<String, Event> {

  private ProcessorContext context;
  private WindowStore<String, Long> store;

  private static final int INTERVAL_SECONDS = 10;

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    this.store =
        (WindowStore<String, Long>) context.getStateStore(Store.CHUNK_NUM_AGGREGATION.getName());
    this.context = context;
    this.context.schedule(
        Duration.ofSeconds(INTERVAL_SECONDS),
        PunctuationType.STREAM_TIME,
        (timestamp) -> {
          log.info(
              "schedule every {} sec: {}", INTERVAL_SECONDS, Utils.getLocalDateTime(timestamp));
          val fromTime = Instant.ofEpochMilli(timestamp).minusSeconds(INTERVAL_SECONDS);
          val iter = this.store.fetchAll(fromTime.toEpochMilli(), timestamp);
          val aggregate = new HashMap<String, Long>();
          while (iter.hasNext()) {
            KeyValue<Windowed<String>, Long> entry = iter.next();
            log.info(
                "key: {}, value: {}, window: {}",
                entry.key.key(),
                entry.value,
                entry.key.window().startTime());
            val chunkNumKey = entry.key.key();
            val current = aggregate.getOrDefault(chunkNumKey, 0L);
            aggregate.put(chunkNumKey, current + entry.value);
          }
          iter.close();
          log.info("--------------------------------");
          log.info("aggregated event by chunk num key");
          for (val e : aggregate.entrySet()) {
            log.info("key: {}, value: {}", e.getKey(), e.getValue());
            this.context.forward(e.getKey(), e.getValue());
          }
          this.context.commit();
          log.info("--------------------------------");
        });
  }

  public Long countValue(Event value) {
    switch (value.getType()) {
      case VIEW:
      case STOCK:
        return 2L;
      case BUY:
        return 10L;
      default:
        return 1L;
    }
  }

  @Override
  public void process(String key, Event value) {
    log.info("got key: {}, value: {}", key, value);
    val chunkNum = value.getCustomId() % 2;
    val chunkNumKey = String.format("%d_%d", value.getUserId(), chunkNum);
    this.store.put(chunkNumKey, this.countValue(value), context.timestamp());
  }

  @Override
  public void close() {}
}
