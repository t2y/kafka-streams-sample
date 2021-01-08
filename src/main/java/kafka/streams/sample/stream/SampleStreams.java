package kafka.streams.sample.stream;

import java.util.Properties;
import org.apache.kafka.streams.Topology;

public interface SampleStreams {
  Topology createTopology();

  Properties getProperties();
}
