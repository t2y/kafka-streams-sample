package kafka.streams.sample.producer;

import kafka.streams.sample.avro.User;
import kafka.streams.sample.serde.MySerdes;
import kafka.streams.sample.stream.global.GlobalTableStreams;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

@Slf4j
public class UserProducer extends AbstractProducer {

  @Setter private int num = 10;
  @Setter private Long userId = -1L;

  public UserProducer() {
    super();
    this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    this.props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MySerdes.USER_SERDE.serializer().getClass());
  }

  private User createUser(Long userId) {
    val loggedIn = userId % 2 == 0;
    val name = "user" + userId;
    val email = name + "@example.com";
    val age = this.rand.nextInt(80);
    return User.newBuilder()
        .setId(userId)
        .setName(name)
        .setEmail(email)
        .setAge(age)
        .setLoggedIn(loggedIn)
        .build();
  }

  @Override
  public void run() {
    try (val producer = new KafkaProducer<Long, User>(this.props)) {
      for (var i = 0; i < this.num; i++) {
        val userId = Long.valueOf(i);
        val user = this.createUser(userId);
        val record =
            new ProducerRecord<Long, User>(GlobalTableStreams.MY_GLOBAL_USERS, userId, user);
        producer.send(record);
        producer.flush();
        log.info("sent record: {}", user);
        Thread.sleep(100L);
      }

      if (this.userId > 0) {
        // delete record
        val record =
            new ProducerRecord<Long, User>(GlobalTableStreams.MY_GLOBAL_USERS, this.userId, null);
        producer.send(record);
        producer.flush();
      }
    } catch (InterruptedException e) {
      log.warn("sleeping is interrupted: {}", e.getMessage());
      Thread.currentThread().interrupt();
    }
  }

  public static void main(String[] args) {
    val producer = new UserProducer();
    if (args.length > 0) {
      producer.setNum(Integer.valueOf(args[0]));
      if (args.length == 2) {
        producer.setUserId(Long.valueOf(args[1]));
      }
    }
    producer.run();
  }
}
