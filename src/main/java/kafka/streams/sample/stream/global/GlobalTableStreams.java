package kafka.streams.sample.stream.global;

import java.util.Properties;
import kafka.streams.sample.avro.Order;
import kafka.streams.sample.avro.User;
import kafka.streams.sample.serde.MySerdes;
import kafka.streams.sample.stream.SampleStreams;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class GlobalTableStreams implements SampleStreams {

  private final Properties props;
  private final StreamsBuilder builder;

  private static final String MY_USER_ORDER_TOPIC = "my-user-order";

  public static final String MY_GLOBAL_USERS = "my-global-users";
  public static final String MY_GLOBAL_USERS_STORE = "my-global-users-store";
  public static final String MY_ORDER_TOPIC = "my-order";

  public GlobalTableStreams(GlobalTableConfig config) {
    this.props = config.getProps();
    this.builder = new StreamsBuilder();
  }

  @Override
  public Topology createTopology() {
    final KStream<String, Order> orders =
        this.builder.stream(MY_ORDER_TOPIC, Consumed.with(Serdes.String(), MySerdes.ORDER_SERDE));
    final GlobalKTable<Long, User> users =
        this.builder.globalTable(
            MY_GLOBAL_USERS,
            Materialized.<Long, User, KeyValueStore<Bytes, byte[]>>as(MY_GLOBAL_USERS_STORE)
                .withKeySerde(Serdes.Long())
                .withValueSerde(MySerdes.USER_SERDE));
    final KStream<String, String> userOrders =
        orders.join(
            users,
            (orderId, order) -> order.getUserId(),
            (order, user) -> order.getState() + ":" + user.getName());

    userOrders.print(Printed.toSysOut());

    userOrders.to(MY_USER_ORDER_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    return this.builder.build(this.props);
  }

  @Override
  public Properties getProperties() {
    return this.props;
  }
}
