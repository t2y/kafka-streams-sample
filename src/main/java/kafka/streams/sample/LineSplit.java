package kafka.streams.sample;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LineSplit {
  public static void main(String[] args) {
    log.info("start");

    val props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    val builder = new StreamsBuilder();

    KStream<String, String> source = builder.stream("streams-plaintext-input");
    source
        .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
        .to("streams-linesplit-output");

    val topology = builder.build();
    log.info(topology.describe().toString());
   
    val streams = new KafkaStreams(topology, props);
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
    System.exit(0);

    log.info("end");
  }
}
