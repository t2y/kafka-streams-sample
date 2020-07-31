package kafka.streams.sample.stream.event;

import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.streams.KafkaStreams;

@Slf4j
public class EventStreamsMain {

  public static void main(String[] args) {
    log.info("start");

    val config = new EventStreamsConfig();
    val eventStreams = new EventStreams(config);
    val topology = eventStreams.createTopology();
    log.info("\n{}", topology.describe());
    val streams = new KafkaStreams(topology, config.getProps());

    val latch = new CountDownLatch(1);
    // attach shutdown handler to catch control-c
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread("streams-shutdown-hook") {
              @Override
              public void run() {
                streams.close();
                latch.countDown();
              }
            });

    try {
      streams.start();
      latch.await();
    } catch (Exception e) {
      System.exit(1);
    }

    log.info("end");
    System.exit(0);
  }
}
