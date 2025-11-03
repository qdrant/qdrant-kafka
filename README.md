# Qdrant Sink Connector

Use Qdrant as a sink destination in [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html). Supports streaming dense/sparse/multi vectors into Qdrant collections.

## Usage

> [!IMPORTANT]
> Qdrant collections have to be [created](https://qdrant.tech/documentation/concepts/collections/#create-a-collection) in advance with the appropriate vector dimensions and configurations.

Learn to use the connector with

- [Kafka on Confluent Cloud](https://github.com/qdrant/qdrant-kafka/blob/main/CONFLUENT.md)

- [Self-hosted Kafka](https://github.com/qdrant/qdrant-kafka/blob/main/KAFKA.md)

## Message Formats

This sink connector supports messages with multiple dense/sparse vectors.

_Click each to expand._

<details>
  <summary><b>Unnamed/Default vector</b></summary>

Reference: [Creating a collection with a default vector](https://qdrant.tech/documentation/concepts/collections/#create-a-collection).

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
  <summary><b>Named multiple vectors</b></summary>

Reference: [Creating a collection with multiple vectors](https://qdrant.tech/documentation/concepts/collections/#collection-with-multiple-vectors).

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

Reference: [Creating a collection with sparse vectors](https://qdrant.tech/documentation/concepts/collections/#collection-with-sparse-vectors).

```json
{
    "collection_name": "{collection_name}",
    "id": 1,
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
  <summary><b>Multi-vector</b></summary>

```json
{
    "collection_name": "{collection_name}",
    "id": 1,
    "vector": {
        "some-multi": [
            [
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
            ],
            [
                1.0,
                0.9,
                0.8,
                0.5,
                0.4,
                0.8,
                0.6,
                0.4,
                0.2,
                0.1
            ]
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
  <summary><b>Combination of named dense and sparse vectors</b></summary>

Reference:

- [Creating a collection with multiple vectors](https://qdrant.tech/documentation/concepts/collections/#collection-with-multiple-vectors).

- [Creating a collection with sparse vectors](https://qdrant.tech/documentation/concepts/collections/#collection-with-sparse-vectors).

```json
{
    "collection_name": "{collection_name}",
    "id": "a10435b5-2a58-427a-a3a0-a5d845b147b7",
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

[Apache 2.0](https://github.com/qdrant/qdrant-kafka/blob/main/LICENSE)
