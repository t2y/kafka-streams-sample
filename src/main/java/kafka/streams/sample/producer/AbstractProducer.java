package kafka.streams.sample.producer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.security.SecureRandom;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import kafka.streams.sample.stream.Constant;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.ProducerConfig;

@Slf4j
public abstract class AbstractProducer {

  protected final Properties props;
  protected final Random rand;

  protected AbstractProducer() {
    this.props = this.createProperties();
    this.rand = new SecureRandom();
  }

  protected Properties createProperties() {
    val p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.BOOTSTRAP_SERVERS);
    p.put(ProducerConfig.ACKS_CONFIG, "all");
    p.put(ProducerConfig.RETRIES_CONFIG, 0);
    p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Constant.SCHEMA_REGISTRY_URL);
    return p;
  }

  protected String createKey() {
    val uuid = UUID.randomUUID();
    return uuid.toString();
  }

  public abstract void run();
}
