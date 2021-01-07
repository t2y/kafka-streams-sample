package kafka.streams.sample.stream;

public class Constant {

  private Constant() {
    throw new IllegalStateException("utility class");
  }

  public static final String BOOTSTRAP_SERVERS = "localhost:9092";
  public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
}
