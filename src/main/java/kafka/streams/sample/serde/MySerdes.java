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
  public static final OrderSerde ORDER_SERDE = getOrderSerde();
  public static final UserSerde USER_SERDE = getUserSerde();

  private static EventSerde getEventSerde() {
    val eventSerde = new EventSerde();
    eventSerde.configure(SERDE_CONFIG, false);
    return eventSerde;
  }

  private static OrderSerde getOrderSerde() {
    val orderSerde = new OrderSerde();
    orderSerde.configure(SERDE_CONFIG, false);
    return orderSerde;
  }

  private static UserSerde getUserSerde() {
    val userSerde = new UserSerde();
    userSerde.configure(SERDE_CONFIG, false);
    return userSerde;
  }
}
