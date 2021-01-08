package kafka.streams.sample.cli;

import kafka.streams.sample.consumer.OldUserConsumer;
import kafka.streams.sample.env.EnvVar;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class ConsumerMain {

  public static void main(String[] args) {
    log.info("ConsumerMain start");
    val consumer = new OldUserConsumer();
    consumer.pool(EnvVar.WITH_ASYNC.getBoolValue());
    log.info("ConsumerMain end");
  }
}
