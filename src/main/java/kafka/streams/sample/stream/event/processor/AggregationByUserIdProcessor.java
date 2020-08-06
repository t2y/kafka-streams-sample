package kafka.streams.sample.stream.event.processor;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
public class AggregationByUserIdProcessor implements Processor<Long, Long> {

  private ProcessorContext context;
  private WindowStore<Long, Long> store;

  private static final int INTERVAL_SECONDS = 30;

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    this.store =
        (WindowStore<Long, Long>) context.getStateStore(Store.USER_ID_AGGREGATION.getName());
    this.context = context;
    this.context.schedule(
        Duration.ofSeconds(INTERVAL_SECONDS),
        PunctuationType.WALL_CLOCK_TIME,
        (timestamp) -> {
          log.info(
              "schedule every {} sec: {}", INTERVAL_SECONDS, Utils.getLocalDateTime(timestamp));
          val fromTime = Instant.ofEpochMilli(timestamp).truncatedTo(ChronoUnit.DAYS);
          val iter = this.store.fetchAll(fromTime.toEpochMilli(), timestamp);
          while (iter.hasNext()) {
            KeyValue<Windowed<Long>, Long> entry = iter.next();
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

  @Override
  public void process(Long key, Long value) {
    log.info("key: {}, value: {}", key, value);
    val timestamp = this.context.timestamp();
    val date = Instant.ofEpochMilli(timestamp).truncatedTo(ChronoUnit.DAYS).toEpochMilli();
    val aggregate = this.store.fetch(key, date);
    this.store.put(key, aggregate + value, date);
  }

  @Override
  public void close() {}
}
