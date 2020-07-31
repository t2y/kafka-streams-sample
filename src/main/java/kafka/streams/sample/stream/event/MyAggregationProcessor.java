package kafka.streams.sample.stream.event;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
public class MyAggregationProcessor implements Processor<Long, Long> {

  @Override
  public void init(ProcessorContext context) {}

  @Override
  public void process(Long key, Long value) {
    log.info("userId: {}, aggregatedValue: {}", key, value);
  }

  @Override
  public void close() {}
}
