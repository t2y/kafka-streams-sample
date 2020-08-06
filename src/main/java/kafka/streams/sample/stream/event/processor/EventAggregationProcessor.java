package kafka.streams.sample.stream.event.processor;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
        PunctuationType.WALL_CLOCK_TIME,
        (timestamp) -> {
          log.info(
              "schedule every {} sec: {}", INTERVAL_SECONDS, Utils.getLocalDateTime(timestamp));
          val fromTime = Instant.ofEpochMilli(timestamp).minusSeconds(INTERVAL_SECONDS);
          val iter = this.store.fetchAll(fromTime.toEpochMilli(), timestamp);
          while (iter.hasNext()) {
            KeyValue<Windowed<String>, Long> entry = iter.next();
            log.info(
                "key: {}, value: {}, window: {}",
                entry.key.key(),
                entry.value,
                entry.key.window().startTime());
            this.context.forward(entry.key.key(), entry.value);
          }
          iter.close();
          this.context.commit();
          log.info("--------------------------------");
        });
  }

  private Instant truncateSecond() {
    val timestamp = context.timestamp();
    val time = Instant.ofEpochMilli(timestamp).truncatedTo(ChronoUnit.SECONDS);
    val second = time.atZone(Utils.UTC).getSecond();
    return time.minusSeconds(second % INTERVAL_SECONDS);
  }

  @Override
  public void process(String key, Event value) {
    log.info("got key: {}, value: {}", key, value);
    val chunkNum = value.getCustomId() % 4;
    val chunkNumKey = String.format("%d_%d", value.getUserId(), chunkNum);
    val timestamp = this.truncateSecond().toEpochMilli();
    Long total = this.store.fetch(key, timestamp);
    if (total == null) {
      total = 0L;
    }
    switch (value.getType()) {
      case VIEW:
      case STOCK:
        total += 2;
        break;
      case BUY:
        total += 10;
        break;
      default:
        total += 1;
        break;
    }
    this.store.put(chunkNumKey, total, timestamp);
  }

  @Override
  public void close() {}
}
