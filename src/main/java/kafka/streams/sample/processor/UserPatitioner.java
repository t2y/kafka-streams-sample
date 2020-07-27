package kafka.streams.sample.processor;

import kafka.streams.sample.avro.User;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class UserPatitioner implements StreamPartitioner<Long, User> {

  private static final Serializer<String> SERIALIZER = new StringSerializer();

  @Override
  public Integer partition(String topic, Long key, User value, int numPartitions) {
    final byte[] keyBytes = SERIALIZER.serialize(topic, value.getName());
    return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
  }
}
