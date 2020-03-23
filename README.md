# kafka-streams-sample

Kafka Streams sample code

## How to build/run

Start kafka broker and schema registry servers on localhost.

```bash
$ ./gradlew run
```

Topologies

```
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [user-topic])
      --> KSTREAM-MAPVALUES-0000000001
    Processor: KSTREAM-MAPVALUES-0000000001 (stores: [])
      --> KSTREAM-KEY-SELECT-0000000002
      <-- KSTREAM-SOURCE-0000000000
    Processor: KSTREAM-KEY-SELECT-0000000002 (stores: [])
      --> KSTREAM-FILTER-0000000005
      <-- KSTREAM-MAPVALUES-0000000001
    Processor: KSTREAM-FILTER-0000000005 (stores: [])
      --> KSTREAM-SINK-0000000004
      <-- KSTREAM-KEY-SELECT-0000000002
    Sink: KSTREAM-SINK-0000000004 (topic: local-counts-store-repartition)
      <-- KSTREAM-FILTER-0000000005

  Sub-topology: 1
    Source: KSTREAM-SOURCE-0000000006 (topics: [local-counts-store-repartition])
      --> KSTREAM-AGGREGATE-0000000003
    Processor: KSTREAM-AGGREGATE-0000000003 (stores: [local-counts-store])
      --> KTABLE-TOSTREAM-0000000007
      <-- KSTREAM-SOURCE-0000000006
    Processor: KTABLE-TOSTREAM-0000000007 (stores: [])
      --> KSTREAM-MAP-0000000008
      <-- KSTREAM-AGGREGATE-0000000003
    Processor: KSTREAM-MAP-0000000008 (stores: [])
      --> KSTREAM-SINK-0000000009
      <-- KTABLE-TOSTREAM-0000000007
    Sink: KSTREAM-SINK-0000000009 (topic: user-topic-agg-count-by-user)
      <-- KSTREAM-MAP-0000000008

  Sub-topology: 2
    Source: source-user-topic-agg-count-by-user (topics: [user-topic-agg-count-by-user])
      --> PrintUserProcessor
    Processor: PrintUserProcessor (stores: [user-store-counts])
      --> none
      <-- source-user-topic-agg-count-by-user
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
