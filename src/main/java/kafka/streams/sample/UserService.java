package kafka.streams.sample;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserService {
  public static final String TOPIC = "user-topic";
  public static final String BOOTSTRAP_SERVERS = "localhost:9092";
  public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

  private static void publish() {
    val producer = new UserProducer();
    producer.publish();
  }

  private static void consume() {
    val consumer = new UserConsumer();
    consumer.retrieve();
  }

  public static void main(String[] args) {
    log.info("start");
    publish();
    consume();
    new UserCount().aggregate();
    log.info("end");
  }
}
