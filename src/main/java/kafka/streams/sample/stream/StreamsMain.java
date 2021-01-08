package kafka.streams.sample.stream;

import java.util.concurrent.CountDownLatch;
import kafka.streams.sample.stream.user.UserConfig;
import kafka.streams.sample.stream.user.UserStreams;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.streams.KafkaStreams;

@Slf4j
public class StreamsMain {

  private static SampleStreams getStreams(String name) {
    if (name.equals("UserStreams")) {
      return new UserStreams(new UserConfig());
    } else {
      throw new IllegalArgumentException(name + " is not supported.");
    }
  }

  public static void main(String[] args) {
    log.info("start");
    if (args.length == 0) {
      log.error("require SampleStreams name as an argument.");
      return;
    }

    val streams = getStreams(args[0]);
    val topology = streams.createTopology();
    log.info("\n{}", topology.describe());
    val kafkaStreams = new KafkaStreams(topology, streams.getProperties());

    val latch = new CountDownLatch(1);
    // attach shutdown handler to catch control-c
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread("streams-shutdown-hook") {
              @Override
              public void run() {
                kafkaStreams.close();
                latch.countDown();
              }
            });
    try {
      kafkaStreams.start();
      latch.await();
    } catch (Exception e) {
      System.exit(1);
    }

    log.info("end");
    System.exit(0);
  }
}
