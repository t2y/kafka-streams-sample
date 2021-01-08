package kafka.streams.sample.producer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import kafka.streams.sample.avro.User;
import kafka.streams.sample.stream.Constant;
import kafka.streams.sample.stream.user.UserStreams;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

@Slf4j
public class UserProducer {
  private final Properties props;
  private final Random rand;

  private Properties createProperties() {
    val p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.BOOTSTRAP_SERVERS);
    p.put(ProducerConfig.ACKS_CONFIG, "all");
    p.put(ProducerConfig.RETRIES_CONFIG, 0);
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Constant.SCHEMA_REGISTRY_URL);
    return p;
  }

  public UserProducer() {
    this.props = this.createProperties();
    this.rand = new SecureRandom();
  }

  private User createUser(long userId, String name, String email) {
    val loggedIn = userId % 2 == 0;
    val age = this.rand.nextInt(80);
    return new User(userId, name, email, loggedIn, age);
  }

  private List<User> createUsers(long n) {
    val users = new ArrayList<User>((int) n);
    for (long i = 0; i < n; i++) {
      val userId = i;
      val name = "user" + userId;
      val email = name + "@example.com";
      users.add(this.createUser(userId, name, email));
    }
    return users;
  }

  public void publishUserInternal(
      KafkaProducer<Long, User> producer, long userId, String name, String email)
      throws InterruptedException {
    val user = this.createUser(userId, name, email);
    val record = new ProducerRecord<Long, User>(UserStreams.USER_TOPIC, userId, user);
    producer.send(record);
    log.info(String.format("sent record: %s", user.toString()));
    Thread.sleep(100L);
    producer.flush();
  }

  public void publishUser(long userId, String name, String email) {
    try (val producer = new KafkaProducer<Long, User>(this.props)) {
      this.publishUserInternal(producer, userId, name, email);
    } catch (InterruptedException e) {
      log.warn("sleeping is interrupted: {}", e.getMessage());
      Thread.currentThread().interrupt();
    }
  }

  public void publishUsers(long n) {
    try (val producer = new KafkaProducer<Long, User>(this.props)) {
      for (val user : this.createUsers(n)) {
        val record = new ProducerRecord<Long, User>(UserStreams.USER_TOPIC, user.getId(), user);
        producer.send(record);
        log.info(String.format("sent record: %s", user.toString()));
      }
      Thread.sleep(100L);
      producer.flush();
      log.info(String.format("sent %d users", n));
    } catch (InterruptedException e) {
      log.warn("sleeping is interrupted: {}", e.getMessage());
      Thread.currentThread().interrupt();
    }
  }
}
