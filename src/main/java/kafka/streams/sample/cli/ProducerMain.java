package kafka.streams.sample.cli;

import kafka.streams.sample.env.EnvVar;
import kafka.streams.sample.producer.UserProducer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class ProducerMain {

  public static void main(String[] args) {
    log.info("ProducerMain start");
    val producer = new UserProducer();
    val userNums = EnvVar.USER_NUMS.getLongValue();
    if (userNums.longValue() == 0) {
      producer.publishUser(
          EnvVar.USER_ID.getLongValue(), EnvVar.USER_NAME.getValue(), EnvVar.USER_EMAIL.getValue());
    } else {
      producer.publishUsers(userNums);
    }
    log.info("ProducerMain end");
  }
}
