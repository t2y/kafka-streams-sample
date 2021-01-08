package kafka.streams.sample.processor;

import kafka.streams.sample.avro.User;
import kafka.streams.sample.stream.global.GlobalTableStreams;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class GlobalUserProcessor implements Processor<Long, User> {

  private ProcessorContext context;
  private KeyValueStore<Long, User> store;

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    this.context = context;
    this.store =
        (KeyValueStore<Long, User>)
            this.context.getStateStore(GlobalTableStreams.MY_GLOBAL_USERS_STORE);
  }

  @Override
  public synchronized void process(Long key, User value) {
    log.info("called process: key: {}, value: {}", key, value);
    this.store.put(key, value);
  }

  @Override
  public void close() {
    // nothing to do
  }
}
