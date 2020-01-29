# kafka-streams-sample

Kafka Streams sample code

## How to build

Install gradle-avro-plugin SHAPSHOT into local maven repository.

```bash
$ git clone git@github.com:davidmc24/gradle-avro-plugin.git
$ cd gradle-avro-plugin/
$ ./gradlew publishToMavenLocal
```

Start kafka broker and schema registry servers on localhost.

```bash
$ gradle run
```

## Reference

* https://kafka.apache.org/documentation/streams/
* https://docs.confluent.io/current/schema-registry/index.html
* https://docs.confluent.io/current/streams/code-examples.html
* https://github.com/confluentinc/kafka-streams-examples
