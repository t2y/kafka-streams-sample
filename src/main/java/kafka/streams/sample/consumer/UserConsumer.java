package kafka.streams.sample.consumer;

import java.util.Collections;
import kafka.streams.sample.avro.User;
import kafka.streams.sample.stream.global.GlobalTableStreams;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@Slf4j
public class UserConsumer extends AbstractConsumer {

  public UserConsumer() {
    super();
    this.props.put(ConsumerConfig.GROUP_ID_CONFIG, "user-consumer");
    this.props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
  }

  private static final long TIMEOUT_MS = 1000L * 60 * 60 * 24; // 1 day

  @Override
  public void poll() {
    try (val consumer = new KafkaConsumer<Long, User>(this.props)) {
      consumer.subscribe(Collections.singletonList(GlobalTableStreams.MY_GLOBAL_USERS));
      while (true) {
        log.info("before pool");
        val records = consumer.poll(TIMEOUT_MS);
        for (val record : records) {
          log.info("key: " + record.key());
          val user = record.value();
          if (user == null) {
            log.info("user is NULL");
          } else {
            log.info("value: {}", user);
          }
          log.info("------------------------------------------------------------------------");
        }
        consumer.commitSync();
        log.info("after commitSync");
      }
    }
  }

  public static void main(String[] args) {
    val consumer = new UserConsumer();
    consumer.poll();
  }
}
