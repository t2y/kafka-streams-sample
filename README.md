# kafka-streams-sample

Kafka Streams sample code

## How to build/run

Start kafka broker and schema registry servers on localhost.

### EventStreams

#### Use Processor API

```bash
$ ./gradlew runEventStreamsProcessor
```

```
Topologies:
   Sub-topology: 0
    Source: my-event (topics: [my-event])
      --> EventAggregationProcessor
    Processor: EventAggregationProcessor (stores: [chunk-num-aggregation])
      --> my-queue-sink
      <-- my-event
    Sink: my-queue-sink (topic: my-queue)
      <-- EventAggregationProcessor

  Sub-topology: 1
    Source: my-queue (topics: [my-queue])
      --> UserIdRepartitionProcessor
    Processor: UserIdRepartitionProcessor (stores: [])
      --> my-repartition-sink
      <-- my-queue
    Sink: my-repartition-sink (topic: my-repartition)
      <-- UserIdRepartitionProcessor

  Sub-topology: 2
    Source: my-repartition (topics: [my-repartition])
      --> AggregationByUserIdProcessor
    Processor: AggregationByUserIdProcessor (stores: [user-id-aggregation])
      --> my-aggregation-sink
      <-- my-repartition
    Sink: my-aggregation-sink (topic: my-aggregation)
      <-- AggregationByUserIdProcessor
```

#### Use Streams DSL

```bash
$ ./gradlew runEventStreamsDSL
```

Topologies

```
Topologies:
   Sub-topology: 0
    Source: my-event (topics: [my-event])
      --> KSTREAM-KEY-SELECT-0000000003
    Processor: KSTREAM-KEY-SELECT-0000000003 (stores: [])
      --> chunk-num-aggregation-repartition-filter
      <-- my-event
    Processor: chunk-num-aggregation-repartition-filter (stores: [])
      --> chunk-num-aggregation-repartition-sink
      <-- KSTREAM-KEY-SELECT-0000000003
    Sink: chunk-num-aggregation-repartition-sink (topic: chunk-num-aggregation-repartition)
      <-- chunk-num-aggregation-repartition-filter

  Sub-topology: 1
    Source: my-queue (topics: [my-queue])
      --> KSTREAM-KEY-SELECT-0000000011
    Processor: KSTREAM-KEY-SELECT-0000000011 (stores: [])
      --> user-id-aggregation-repartition-filter
      <-- my-queue
    Processor: user-id-aggregation-repartition-filter (stores: [])
      --> user-id-aggregation-repartition-sink
      <-- KSTREAM-KEY-SELECT-0000000011
    Sink: user-id-aggregation-repartition-sink (topic: user-id-aggregation-repartition)
      <-- user-id-aggregation-repartition-filter

  Sub-topology: 2
    Source: my-aggregation (topics: [my-aggregation])
      --> KSTREAM-PROCESSOR-0000000019
    Processor: KSTREAM-PROCESSOR-0000000019 (stores: [])
      --> none
      <-- my-aggregation

  Sub-topology: 3
    Source: chunk-num-aggregation-repartition-source (topics: [chunk-num-aggregation-repartition])
      --> KSTREAM-AGGREGATE-0000000004
    Processor: KSTREAM-AGGREGATE-0000000004 (stores: [chunk-num-aggregation])
      --> KTABLE-TOSTREAM-0000000008
      <-- chunk-num-aggregation-repartition-source
    Processor: KTABLE-TOSTREAM-0000000008 (stores: [])
      --> KSTREAM-KEY-SELECT-0000000009
      <-- KSTREAM-AGGREGATE-0000000004
    Processor: KSTREAM-KEY-SELECT-0000000009 (stores: [])
      --> KSTREAM-SINK-0000000010
      <-- KTABLE-TOSTREAM-0000000008
    Sink: KSTREAM-SINK-0000000010 (topic: my-queue)
      <-- KSTREAM-KEY-SELECT-0000000009

  Sub-topology: 4
    Source: user-id-aggregation-repartition-source (topics: [user-id-aggregation-repartition])
      --> KSTREAM-AGGREGATE-0000000012
    Processor: KSTREAM-AGGREGATE-0000000012 (stores: [user-id-aggregation])
      --> KTABLE-TOSTREAM-0000000016
      <-- user-id-aggregation-repartition-source
    Processor: KTABLE-TOSTREAM-0000000016 (stores: [])
      --> KSTREAM-KEY-SELECT-0000000017
      <-- KSTREAM-AGGREGATE-0000000012
    Processor: KSTREAM-KEY-SELECT-0000000017 (stores: [])
      --> KSTREAM-SINK-0000000018
      <-- KTABLE-TOSTREAM-0000000016
    Sink: KSTREAM-SINK-0000000018 (topic: my-aggregation)
      <-- KSTREAM-KEY-SELECT-0000000017
```

### GlobalTableStreams

```bash
$ ./gradlew --info runStream --args='GlobalTableStreams'
```

```
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [my-order])
      --> KSTREAM-LEFTJOIN-0000000003
    Processor: KSTREAM-LEFTJOIN-0000000003 (stores: [])
      --> KSTREAM-PRINTER-0000000004, KSTREAM-SINK-0000000005
      <-- KSTREAM-SOURCE-0000000000
    Processor: KSTREAM-PRINTER-0000000004 (stores: [])
      --> none
      <-- KSTREAM-LEFTJOIN-0000000003
    Sink: KSTREAM-SINK-0000000005 (topic: my-user-order)
      <-- KSTREAM-LEFTJOIN-0000000003

  Sub-topology: 1 for global store (will not generate tasks)
    Source: KSTREAM-SOURCE-0000000001 (topics: [my-global-users])
      --> KTABLE-SOURCE-0000000002
    Processor: KTABLE-SOURCE-0000000002 (stores: [my-global-users-store])
      --> none
      <-- KSTREAM-SOURCE-0000000001
```

## How to run producer

### for EventStreams

To send records for EventStreams into kafka, run like this.

```bash
$ ./gradlew runEventProducer
...
10:56:08.849 [main] INFO  k.s.sample.producer.EventProducer - sent event: {"user_id": 6, "action": "some", "type": "VIEW", "created_at": 2020-07-31T01:56:08.846Z}
10:56:09.851 [main] INFO  k.s.sample.producer.EventProducer - sent event: {"user_id": 4, "action": "some", "type": "STOCK", "created_at": 2020-07-31T01:56:09.850Z}
...
```

### for GlobalTableStreams

Send user info for Global Table.

```bash
$ ./gradlew runUserProducer
```

Send order messages to join Global Table (user).

```bash
$ ./gradlew runOrderProducer
```

## Reference

* https://kafka.apache.org/documentation/streams/
* https://docs.confluent.io/current/schema-registry/index.html
* https://docs.confluent.io/current/streams/code-examples.html
* https://github.com/confluentinc/kafka-streams-examples
