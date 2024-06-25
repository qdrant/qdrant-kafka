# Usage with Self-Hosted Kafka

## Installation

1) Download the latest connector zip file from [Github Releases](https://github.com/qdrant/qdrant-kafka/releases).

2) Refer to the first 3 steps of the [Kafka Quickstart](https://kafka.apache.org/quickstart#quickstart_download) to set up a local Kafka instance and create a topic named `topic_0`.

3) Navigate to the Kafka installation directory.

4) Unzip and copy the `qdrant-kafka-xxx` directory to your Kafka installation's `libs` directory.

5) Update the `connect-standalone.properties` file in your Kafka installation's `config` directory.

    ```properties
    key.converter.schemas.enable=false
    value.converter.schemas.enable=false
    plugin.path=libs/qdrant-kafka-xxx
    ```

6) Create a `qdrant-kafka.properties` file in your Kafka installation's `config` directory.

    ```properties
    name=qdrant-kafka
    connector.class=io.qdrant.kafka.QdrantSinkConnnector
    qdrant.grpc.url=https://xyz-example.eu-central.aws.cloud.qdrant.io:6334
    qdrant.api.key=<paste-your-api-key-here>
    topics=topic_0
    ```

7) Start Kafka Connect with the configured properties.

  ```sh
  bin/connect-standalone.sh config/connect-standalone.properties config/qdrant-kafka.properties
  ```

8) You can now produce messages for the `topic_0` topic and they'll be written into the configured Qdrant instance.

```sh
bin/kafka-console-producer.sh --topic topic_0 --bootstrap-server localhost:9092
> { "collection_name": "{collection_name}", "id": 1, "vector": [ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8 ], "payload": { "name": "kafka", "description": "Kafka is a distributed streaming platform", "url": "https://kafka.apache.org/" } }
```

Refer to the [message formats](https://github.com/qdrant/qdrant-kafka/blob/main/README.md#message-formats) for the available options when producing messages.
