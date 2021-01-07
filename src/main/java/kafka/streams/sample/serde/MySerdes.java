package kafka.streams.sample.serde;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Map;
import kafka.streams.sample.stream.Constant;
import lombok.val;

public class MySerdes {

  private MySerdes() {
    throw new IllegalStateException("utility class");
  }

  private static final Map<String, String> SERDE_CONFIG =
      Map.of(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Constant.SCHEMA_REGISTRY_URL);

  public static final EventSerde EVENT_SERDE = getEventSerde();

  private static EventSerde getEventSerde() {
    val userSerde = new EventSerde();
    userSerde.configure(SERDE_CONFIG, false);
    return userSerde;
  }
}
