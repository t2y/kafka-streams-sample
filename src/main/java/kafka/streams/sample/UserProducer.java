package kafka.streams.sample;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.LongSerializer;

import kafka.streams.sample.avro.User;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserProducer {
  private final Properties props;

  private Properties createProperties() {
    val p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, UserService.BOOTSTRAP_SERVERS);
    p.put(ProducerConfig.ACKS_CONFIG, "all");
    p.put(ProducerConfig.RETRIES_CONFIG, 0);
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, UserService.SCHEMA_REGISTRY_URL);
    return p;
  }

  public UserProducer() {
    this.props = this.createProperties();
  }

  private User createUser(long n) {
    val userId = n;
    val name = "user" + String.valueOf(userId);
    val ename = name + "@example.com";
    val loggedIn = userId % 2 == 0;
    val age = (int) ((Math.random() * ((50 - 10) + 1)) + 10);
    return new User(userId, name, ename, loggedIn, age);
  }

  public void publish() {
    try (val producer = new KafkaProducer<Long, User>(this.props)) {
      for (long i = 0; i < 3; i++) {
        val user = this.createUser(i);
        val record = new ProducerRecord<Long, User>(UserService.TOPIC, i, user);
        producer.send(record);
        log.info(String.format("send record, key: %d, value(name): %s", i, user.getName()));
        Thread.sleep(100L);
      }
      producer.flush();

      val message =
          String.format(
              "Successfully produced 10 messages to a topic called %s%n", UserService.TOPIC);
      log.info(message);
    } catch (SerializationException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
