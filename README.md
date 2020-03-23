# kafka-streams-sample

Kafka Streams sample code

## How to build/run

Start kafka broker and schema registry servers on localhost.

```bash
$ ./gradlew run
```

## How to run producer

To send records into kafka, run like this.

```bash
$ ./gradlew runProducer
```

## How to run consumer

To receive records from kafka, run like this.

```bash
$ ./gradlew runConsumer
```

## Reference

* https://kafka.apache.org/documentation/streams/
* https://docs.confluent.io/current/schema-registry/index.html
* https://docs.confluent.io/current/streams/code-examples.html
* https://github.com/confluentinc/kafka-streams-examples
