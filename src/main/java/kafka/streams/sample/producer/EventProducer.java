package kafka.streams.sample.producer;

import java.time.Instant;
import java.util.Random;
import kafka.streams.sample.avro.Event;
import kafka.streams.sample.avro.EventType;
import kafka.streams.sample.serde.MySerdes;
import kafka.streams.sample.stream.event.Topic;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class EventProducer extends AbstractProducer {

  public EventProducer() {
    super();
    this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    this.props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MySerdes.EVENT_SERDE.serializer().getClass());
  }

  private EventType getEventType(int id) {
    if (id % 3 == 0) {
      return EventType.VIEW;
    } else if (id % 3 == 1) {
      return EventType.STOCK;
    } else {
      return EventType.BUY;
    }
  }

  private Event createEvent() {
    val userId = new Random().nextInt(8);
    val customId = new Random().nextInt(1024);
    val type = this.getEventType(userId);
    return Event.newBuilder()
        .setUserId(userId)
        .setCustomId(customId)
        .setType(type)
        .setAction("some")
        .setCreatedAt(Instant.now())
        .build();
  }

  @Override
  public void run() {
    try (val producer = new KafkaProducer<String, Event>(this.props)) {
      while (true) {
        val key = this.createKey();
        val value = this.createEvent();
        val record = new ProducerRecord<String, Event>(Topic.MY_EVENT.getName(), key, value);
        producer.send(record);
        producer.flush();
        log.info("sent event: {}", value);
        Thread.sleep(1000L);
      }
    } catch (InterruptedException e) {
      log.warn("sleeping is interrupted: {}", e.getMessage());
      Thread.currentThread().interrupt();
    }
  }

  public static void main(String[] args) {
    val producer = new EventProducer();
    producer.run();
  }
}
