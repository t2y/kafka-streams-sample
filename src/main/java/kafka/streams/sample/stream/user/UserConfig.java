package kafka.streams.sample.stream.user;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.Collections;
import java.util.Properties;
import kafka.streams.sample.serde.UserSerde;
import kafka.streams.sample.stream.Constant;
import lombok.Getter;
import lombok.ToString;
import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

@Getter
@ToString
public class UserConfig {

  private static final String APPLICATION_ID = "user-streams";

  public static final UserSerde USER_SERDE = getUserSerde();

  private static UserSerde getUserSerde() {
    val serdeConfig =
        Collections.singletonMap(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            Constant.SCHEMA_REGISTRY_URL);
    val userSerde = new UserSerde();
    userSerde.configure(serdeConfig, false);
    return userSerde;
  }

  private final Properties props;

  private Properties createProperties() {
    val p = new Properties();
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.BOOTSTRAP_SERVERS);
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
    p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
    p.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Constant.SCHEMA_REGISTRY_URL);
    return p;
  }

  void setBootstrapServer(String servers) {
    this.props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
  }

  public UserConfig() {
    this.props = this.createProperties();
  }
}
