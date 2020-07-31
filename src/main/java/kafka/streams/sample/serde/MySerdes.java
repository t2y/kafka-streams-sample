package kafka.streams.sample.serde;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.util.Map;
import kafka.streams.sample.stream.Constant;
import lombok.val;

public class MySerdes {

  private static final Map<String, String> SERDE_CONFIG =
      Map.of(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Constant.SCHEMA_REGISTRY_URL);

  public static final EventSerde EVENT_SERDE = getEventSerde();

  private static EventSerde getEventSerde() {
    val userSerde = new EventSerde();
    userSerde.configure(SERDE_CONFIG, false);
    return userSerde;
  }
}
