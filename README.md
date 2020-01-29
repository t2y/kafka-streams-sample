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
