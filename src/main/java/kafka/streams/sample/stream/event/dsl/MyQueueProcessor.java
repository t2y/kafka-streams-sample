package kafka.streams.sample.stream.event.dsl;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
public class MyQueueProcessor implements Processor<String, Long> {

  @Override
  public void init(ProcessorContext context) {}

  @Override
  public void process(String key, Long value) {
    log.info("withChunk: {}, aggregatedValue: {}", key, value);
  }

  @Override
  public void close() {}
}
