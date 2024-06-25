# Qdrant Kafka Connector

Use Qdrant as a sink destination in [Kafka connect](https://docs.confluent.io/platform/current/connect/index.html).

## Installation

- Download the latest connector zip file `qdrant-kafka-1.0.0.zip` from [here](https://github.com/qdrant/qdrant-kafka/releases).

- Refer to the first 3 steps of the [Kafka Quickstart](https://kafka.apache.org/quickstart#quickstart_download) to set up a local Kafka instance and create a topic named `topic_0`.

- Navigate to the Kafka installation directory.

- Unzip and copy the `qdrant-kafka-1.0.0` directories to the `libs` directory of your Kafka installation.

- Update the `connect-standalone.properties` file in the `config` directory of your Kafka installation.

    ```properties
    key.converter.schemas.enable=false
    value.converter.schemas.enable=false
    plugin.path=libs/qdrant-kafka-1.0.0
    ```

- Create a `qdrant-kafka.properties` file in the `config` directory of your Kafka installation.

    ```properties
    name=qdrant-kafka
    connector.class=io.qdrant.kafka.QdrantSinkConnnector
    qdrant.grpc.url=https://xyz-example.eu-central.aws.cloud.qdrant.io:6333
    qdrant.api.key=<paste-your-api-key-here>
    topics=topic_0
    ```

- Start the connector with the configured properties

  ```sh
  bin/connect-standalone.sh config/connect-standalone.properties config/qdrant-kafka.properties
  ```
  
## Usage

> [!IMPORTANT]
> Before loading the data using this connector, a collection has to be [created](https://qdrant.tech/documentation/concepts/collections/#create-a-collection) in advance with the appropriate vector dimensions and configurations.

You can now produce messages with the following command to the `topic_0` topic you created and they'll be streamed to the configured Qdrant instance.

```sh
bin/kafka-console-producer.sh --topic topic_0 --bootstrap-server localhost:9092
> { "collection_name": "{collection_name}", "id": 1, "vector": [ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8 ], "payload": { "name": "kafka", "description": "Kafka is a distributed streaming platform", "url": "https://kafka.apache.org/" } }
```

This sink connector supports ingesting multiple named/unnamed, dense/sparse vectors.

_Click each to expand._

<details>
  <summary><b>Unnamed/Default vector</b></summary>

```json
{
    "collection_name": "{collection_name}",
    "id": 1,
    "vector": [
        0.1,
        0.2,
        0.3,
        0.4,
        0.5,
        0.6,
        0.7,
        0.8
    ],
    "payload": {
        "name": "kafka",
        "description": "Kafka is a distributed streaming platform",
        "url": "https://kafka.apache.org/"
    }
}
```

</details>

<details>
  <summary><b>Named vector</b></summary>

```json
{
    "collection_name": "{collection_name}",
    "id": 1,
    "vector": {
        "some-dense": [
            0.1,
            0.2,
            0.3,
            0.4,
            0.5,
            0.6,
            0.7,
            0.8
        ],
        "some-other-dense": [
            0.1,
            0.2,
            0.3,
            0.4,
            0.5,
            0.6,
            0.7,
            0.8
        ]
    },
    "payload": {
        "name": "kafka",
        "description": "Kafka is a distributed streaming platform",
        "url": "https://kafka.apache.org/"
    }
}
```

</details>

<details>
  <summary><b>Sparse vectors</b></summary>

```json
{
    "collection_name": "{collection_name}",
    "id": 1,
    "shard_key_selector": [5235],
    "vector": {
        "some-sparse": {
            "indices": [
                0,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9
            ],
            "values": [
                0.1,
                0.2,
                0.3,
                0.4,
                0.5,
                0.6,
                0.7,
                0.8,
                0.9,
                1.0
            ]
        }
    },
    "payload": {
        "name": "kafka",
        "description": "Kafka is a distributed streaming platform",
        "url": "https://kafka.apache.org/"
    }
}
```

</details>

<details>
  <summary><b>Combination of named dense and sparse vectors</b></summary>

```json
{
    "collection_name": "{collection_name}",
    "id": "a10435b5-2a58-427a-a3a0-a5d845b147b7",
    "shard_key_selector": ["some-key"],
    "vector": {
        "some-other-dense": [
            0.1,
            0.2,
            0.3,
            0.4,
            0.5,
            0.6,
            0.7,
            0.8
        ],
        "some-sparse": {
            "indices": [
                0,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9
            ],
            "values": [
                0.1,
                0.2,
                0.3,
                0.4,
                0.5,
                0.6,
                0.7,
                0.8,
                0.9,
                1.0
            ]
        }
    },
    "payload": {
        "name": "kafka",
        "description": "Kafka is a distributed streaming platform",
        "url": "https://kafka.apache.org/"
    }
}
```

</details>

## LICENSE

Apache 2.0 © [2024](https://github.com/qdrant/qdrant-kafka/blob/main/LICENSE)
