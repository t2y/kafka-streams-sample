package kafka.streams.sample.consumer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.Properties;
import kafka.streams.sample.stream.Constant;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;

@Slf4j
public abstract class AbstractConsumer {

  protected final Properties props;

  protected AbstractConsumer() {
    this.props = this.createProperties();
  }

  protected Properties createProperties() {
    val p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.BOOTSTRAP_SERVERS);
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    p.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Constant.SCHEMA_REGISTRY_URL);
    return p;
  }

  public abstract void poll();
}
