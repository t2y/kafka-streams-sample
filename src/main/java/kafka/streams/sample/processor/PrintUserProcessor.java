package kafka.streams.sample.processor;

import java.time.Duration;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import kafka.streams.sample.UserService;
import kafka.streams.sample.util.DateTimeUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PrintUserProcessor implements Processor<String, Long> {

  private static long SCHEDULE_DURATION_SEC = 10;

  private ProcessorContext context;
  private KeyValueStore<String, Long> store;

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    log.info("called init()");
    this.context = context;
    this.store = (KeyValueStore<String, Long>) this.context.getStateStore(UserService.STORE_COUNTS);

    this.context.schedule(
        Duration.ofSeconds(SCHEDULE_DURATION_SEC),
        PunctuationType.STREAM_TIME,
        timestamp -> {
          log.info("called context.schedule(): {}", DateTimeUtil.getLocalDateTime(timestamp));
          try (val iter = this.store.all()) {
            while (iter.hasNext()) {
              val kv = iter.next();
              log.info(" - " + kv.toString());
            }
          }
          this.context.commit();
        });
  }

  @Override
  public synchronized void process(String key, Long value) {
    log.info("called process(): {}, {}",key, value.toString());
    val count = this.store.putIfAbsent(key, 0L);
    this.store.put(key, count + value);
  }

  @Override
  public void close() {
    log.info("called close()");
  }
}
