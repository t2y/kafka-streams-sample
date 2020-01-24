package kafka.streams.sample;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import kafka.streams.sample.avro.User;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserConsumer {

  private final Properties props;

  private Properties createProperties() {
    val p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, UserService.BOOTSTRAP_SERVERS);
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "user-consumer");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    p.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, UserService.SCHEMA_REGISTRY_URL);
    return p;
  }

  public UserConsumer() {
    this.props = this.createProperties();
  }

  public void retrieve() {
    try (val consumer = new KafkaConsumer<Long, User>(this.props)) {
      consumer.subscribe(Collections.singletonList(UserService.TOPIC));
      final ConsumerRecords<Long, User> records = consumer.poll(Duration.ofSeconds(1));
      for (val record : records) {
        log.info("key: " + record.key());
        val user = record.value();
        log.info("value: " + user.toString());
        log.info("------------------------------------------------------------------------");
      }
      consumer.commitSync();
    }
  }
}
