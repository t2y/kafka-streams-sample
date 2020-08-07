package kafka.streams.sample.stream.event;

import org.apache.kafka.streams.processor.StreamPartitioner;

public class ChunkNumPartitioner implements StreamPartitioner<String, Long> {

  public Integer partition(String topic, String key, Long value, int numPartitions) {
    return Integer.valueOf(key.split("_")[1]) % numPartitions;
  }
}
