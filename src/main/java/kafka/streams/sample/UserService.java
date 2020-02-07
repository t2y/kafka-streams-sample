package kafka.streams.sample;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserService {
  public static final String USER_TOPIC = "user-topic";
  public static final String BOOTSTRAP_SERVERS = "localhost:9092";
  public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

  public static final String STORE_COUNTS = "user-store-counts";

  public static void main(String[] args) {
    log.info("start");
    new UserCount(new UserConfig()).aggregate();
    log.info("end");
  }
}
