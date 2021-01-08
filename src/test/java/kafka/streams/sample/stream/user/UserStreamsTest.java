package kafka.streams.sample.stream.user;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.FutureTask;
import kafka.streams.sample.avro.User;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

@Slf4j
class UserStreamsTest {

  private TopologyTestDriver testDriver;

  @BeforeEach
  void setup() {
    val userConfig = new UserConfig();
    userConfig.setBootstrapServer("dummy:1234");
    val userStreams = new UserStreams(userConfig);
    val topology = userStreams.createTopology();
    this.testDriver = new TopologyTestDriver(topology, userStreams.getProperties());
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  private List<TestRecord<Long, User>> createUserTopicRecords() {
    val records = new ArrayList<TestRecord<Long, User>>();
    records.add(new TestRecord<>(1L, new User(1L, "user1", "user1@example.com", true, 35)));
    records.add(new TestRecord<>(2L, new User(2L, "user2", "user2@example.com", false, 28)));
    records.add(new TestRecord<>(3L, new User(3L, "user3", "user3@example.com", true, 41)));
    records.add(new TestRecord<>(2L, new User(2L, "user2", "user2@example.com", false, 28)));
    records.add(new TestRecord<>(2L, new User(2L, "user2", "user2@example.com", false, 28)));
    return records;
  }

  @Test
  void testAggregateUserCounts() {
    val topic =
        this.testDriver.createInputTopic(
            UserStreams.USER_TOPIC, Serdes.Long().serializer(), UserConfig.USER_SERDE.serializer());
    val inputRecords = this.createUserTopicRecords();
    for (val record : inputRecords) {
      topic.pipeInput(record.getKey(), record.getValue());
    }
    val output =
        this.testDriver.createOutputTopic(
            UserStreams.COUNT_BY_USER_TOPIC,
            Serdes.String().deserializer(),
            Serdes.Long().deserializer());

    val outputRecords = new ArrayList<TestRecord<String, Long>>();
    while (!output.isEmpty()) {
      val result = output.readRecord();
      log.info(result.toString());
      outputRecords.add(result);
    }

    assertEquals(inputRecords.size(), outputRecords.size());

    var i = 0;
    val counter = new HashMap<String, Long>();
    for (val input : inputRecords) {
      val name = input.getValue().getName();
      assertEquals(name, outputRecords.get(i).getKey());
      val value = counter.getOrDefault(name, 0L);
      counter.put(name, value + 1);
      i++;
    }
  }

  private List<TestRecord<String, Long>> createUserCountRecords() {
    val records = new ArrayList<TestRecord<String, Long>>();
    records.add(new TestRecord<String, Long>("user1", 1L));
    records.add(new TestRecord<String, Long>("user2", 5L));
    records.add(new TestRecord<String, Long>("user3", 3L));
    return records;
  }

  @Test
  void testPrintUser() {
    val topic =
        this.testDriver.createInputTopic(
            UserStreams.COUNT_BY_USER_TOPIC,
            Serdes.String().serializer(),
            Serdes.Long().serializer());
    val records = this.createUserCountRecords();
    val future =
        new FutureTask<Void>(
            () -> {
              for (val record : records) {
                topic.pipeInput(record);
              }
              return null;
            });
    future.run();
    assertDoesNotThrow((Executable) future::get);
  }
}
