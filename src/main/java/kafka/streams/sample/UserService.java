package kafka.streams.sample;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserService {
  public static final String TOPIC = "user-topic";
  public static final String BOOTSTRAP_SERVERS = "localhost:9092";
  public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

  public static void main(String[] args) {
    log.info("start");
    new UserCount().aggregate();
    log.info("end");
  }
}
