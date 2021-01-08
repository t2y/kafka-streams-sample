package kafka.streams.sample.stream;

import java.util.concurrent.CountDownLatch;
import kafka.streams.sample.avro.User;
import kafka.streams.sample.stream.global.GlobalTableConfig;
import kafka.streams.sample.stream.global.GlobalTableStreams;
import kafka.streams.sample.stream.user.UserConfig;
import kafka.streams.sample.stream.user.UserStreams;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

@Slf4j
public class StreamsMain {

  private static SampleStreams getStreams(String name) {
    if (name.equals("UserStreams")) {
      return new UserStreams(new UserConfig());
    } else if (name.equals("GlobalTableStreams")) {
      return new GlobalTableStreams(new GlobalTableConfig());
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

    kafkaStreams.setStateListener(
        (newState, oldState) -> {
          if (newState.isRunningOrRebalancing()) {
            try {
              ReadOnlyKeyValueStore<Long, ValueAndTimestamp<User>> users =
                  kafkaStreams.store(
                      StoreQueryParameters.fromNameAndType(
                          GlobalTableStreams.MY_GLOBAL_USERS_STORE,
                          QueryableStoreTypes.timestampedKeyValueStore()));
              val iter = users.all();
              log.info("start: read from Global table");
              while (iter.hasNext()) {
                log.info("{}", iter.next());
              }
              log.info("end:   read from Global table");
              log.info("users.get(3): {}", users.get(3L));
            } catch (InvalidStateStoreException ex) {
              log.error("cannot find {} store", GlobalTableStreams.MY_GLOBAL_USERS_STORE);
            }
          }

          if (!newState.isRunningOrRebalancing()) {
            latch.countDown();
          }
        });

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
