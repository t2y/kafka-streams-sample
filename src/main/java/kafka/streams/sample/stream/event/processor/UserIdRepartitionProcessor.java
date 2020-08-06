package kafka.streams.sample.stream.event.processor;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
public class UserIdRepartitionProcessor implements Processor<String, Long> {

  private ProcessorContext context;

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public void process(String key, Long value) {
    val userId = Long.valueOf(key.split("_")[0]);
    this.context.forward(userId, value);
  }

  @Override
  public void close() {}
}
