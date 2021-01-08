package kafka.streams.sample.producer;

import kafka.streams.sample.avro.Order;
import kafka.streams.sample.avro.OrderState;
import kafka.streams.sample.avro.Product;
import kafka.streams.sample.serde.MySerdes;
import kafka.streams.sample.stream.global.GlobalTableStreams;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class OrderProducer extends AbstractProducer {

  public OrderProducer() {
    super();
    this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    this.props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MySerdes.ORDER_SERDE.serializer().getClass());
  }

  private Order createOrder(String orderId) {
    val userId = this.rand.nextInt(10);
    return Order.newBuilder()
        .setId(orderId)
        .setUserId(userId)
        .setState(OrderState.CREATED)
        .setProduct(Product.JUMPERS)
        .setQuantity(1)
        .setPrice(100.0)
        .build();
  }

  @Override
  public void run() {
    try (val producer = new KafkaProducer<String, Order>(this.props)) {
      while (true) {
        val key = this.createKey();
        val value = this.createOrder(key);
        val record =
            new ProducerRecord<String, Order>(GlobalTableStreams.MY_ORDER_TOPIC, key, value);
        producer.send(record);
        producer.flush();
        log.info("sent record: {}", value);
        Thread.sleep(1000L);
      }
    } catch (InterruptedException e) {
      log.warn("sleeping is interrupted: {}", e.getMessage());
      Thread.currentThread().interrupt();
    }
  }

  public static void main(String[] args) {
    val producer = new OrderProducer();
    producer.run();
  }
}
