# Usage with Confluent

## Installation

1) Download the latest connector zip file from [Github Releases](https://github.com/qdrant/qdrant-kafka/releases).

2) Configure an environment and cluster on Confluent and create a topic to produce messages for.

3) Navigate to the `Connectors` section of the Confluent cluster and click `Add Plugin`. Upload the zip file with the following info.

<img width="687" alt="Screenshot 2024-06-26 at 1 51 26 AM" src="https://github.com/qdrant/qdrant-kafka/assets/46051506/876bcef5-d862-40c6-a0e7-838f1586f222">

4) Once installed, navigate to the connector and set the following configuration values.

<img width="899" alt="Screenshot 2024-06-26 at 1 45 57 AM" src="https://github.com/qdrant/qdrant-kafka/assets/46051506/3999976e-a89a-4a49-b53c-a2e5aee68441">

Replace the placeholder values with your credentials.

5) Add the Qdrant instance host to the allowed networking endpoints.

<img width="764" alt="Screenshot 2024-06-26 at 2 46 16 AM" src="https://github.com/qdrant/qdrant-kafka/assets/46051506/8aefd9c3-0584-4aa5-a70c-37c859f6ee1b">

7) Start the connector.

## Usage

You can now produce messages for the configured topic and they'll be written into the configured Qdrant instance.

<img width="1271" alt="Screenshot 2024-06-26 at 2 50 56 AM" src="https://github.com/qdrant/qdrant-kafka/assets/46051506/3d798780-f236-4ac6-aea0-2b266dda4dba">

Refer to the [message formats](https://github.com/qdrant/qdrant-kafka/blob/main/README.md#message-formats) for the available options when producing messages.
