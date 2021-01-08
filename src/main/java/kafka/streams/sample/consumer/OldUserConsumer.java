package kafka.streams.sample.consumer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import kafka.streams.sample.avro.User;
import kafka.streams.sample.stream.Constant;
import kafka.streams.sample.stream.user.UserStreams;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

@Slf4j
public class OldUserConsumer {

  private final Properties props;

  private Properties createProperties() {
    val p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.BOOTSTRAP_SERVERS);
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "user-consumer");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    p.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Constant.SCHEMA_REGISTRY_URL);
    return p;
  }

  public OldUserConsumer() {
    this.props = this.createProperties();
  }

  private static final Duration DURATION_SEC = Duration.ofSeconds(3);
  private static final long TIMEOUT_MS = 1000L * 60 * 60 * 24; // 1 day

  @SuppressWarnings("deprecation")
  private ConsumerRecords<Long, User> poolInternal(
      KafkaConsumer<Long, User> consumer, boolean withAsync) {
    if (withAsync) {
      return consumer.poll(DURATION_SEC);
    }
    return consumer.poll(TIMEOUT_MS);
  }

  public void pool(boolean withAsync) {
    try (val consumer = new KafkaConsumer<Long, User>(this.props)) {
      consumer.subscribe(Collections.singletonList(UserStreams.USER_TOPIC));
      while (true) {
        log.info("before pool");
        val records = this.poolInternal(consumer, withAsync);
        for (val record : records) {
          log.info("key: " + record.key());
          val user = record.value();
          log.info("value: " + user.toString());
          log.info("------------------------------------------------------------------------");
        }
        consumer.commitSync();
        log.info("after commitSync");
      }
    }
  }
}
