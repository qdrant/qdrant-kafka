/* (C)2024 */
package io.qdrant.kafka;

import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseKafkaConnectTest extends BaseQdrantTest {

  static final Logger LOGGER = LoggerFactory.getLogger(BaseKafkaConnectTest.class);

  EmbeddedConnectCluster connect;

  final String topicName;

  final String connectorName;

  protected BaseKafkaConnectTest(final String topicName, final String connectorName) {
    this.topicName = topicName;
    this.connectorName = connectorName;
  }

  @BeforeEach
  void startConnect() {
    connect = new EmbeddedConnectCluster.Builder().name("qdrant-it-connect-cluster").build();
    connect.start();
    connect.kafka().createTopic(topicName);
  }

  @AfterEach
  void stopConnect() {
    try (final Admin admin = connect.kafka().createAdminClient()) {
      final DeleteTopicsResult result = admin.deleteTopics(Arrays.asList(topicName));
      result.all().get();
    } catch (final ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    connect.stop();
  }

  long waitForConnectorToStart(final String name, final int numTasks) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
        TimeUnit.MINUTES.toMillis(60),
        "Connector tasks did not start in time.");
    return System.currentTimeMillis();
  }

  Optional<Boolean> assertConnectorAndTasksRunning(final String connectorName, final int numTasks) {
    try {
      final ConnectorStateInfo info = connect.connectorStatus(connectorName);
      final boolean result =
          info != null
              && info.tasks().size() >= numTasks
              && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
              && info.tasks().stream()
                  .allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      return Optional.of(result);
    } catch (final Exception e) {
      LOGGER.error("Could not check connector state info.");
      return Optional.empty();
    }
  }

  Map<String, String> connectorProperties() {
    final Map<String, String> props = new HashMap<>(getDefaultProperties());
    props.put(CONNECTOR_CLASS_CONFIG, QdrantSinkConnector.class.getName());
    props.put(TOPICS_CONFIG, topicName);
    props.put(TASKS_MAX_CONFIG, Integer.toString(1));
    props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put("value.converter." + SCHEMAS_ENABLE_CONFIG, "false");
    props.put("key.converter." + SCHEMAS_ENABLE_CONFIG, "false");
    return props;
  }

  void writeUnnamedPoint(String collectionName, int id) {
    connect
        .kafka()
        .produce(
            topicName,
            String.format(
                "{\n"
                    + //
                    "    \"collection_name\": \"%s\",\n"
                    + //
                    "    \"id\": %d,\n"
                    + //
                    "    \"vector\": [\n"
                    + //
                    "        0.1,\n"
                    + //
                    "        0.2,\n"
                    + //
                    "        0.3,\n"
                    + //
                    "        0.4,\n"
                    + //
                    "        0.5,\n"
                    + //
                    "        0.6,\n"
                    + //
                    "        0.7,\n"
                    + //
                    "        0.8\n"
                    + //
                    "    ],\n"
                    + //
                    "    \"payload\": {\n"
                    + //
                    "        \"name\": \"kafka\",\n"
                    + //
                    "        \"description\": \"Kafka is a distributed streaming platform for all\",\n"
                    + //
                    "        \"url\": \"https://kafka.apache.com/\"\n"
                    + //
                    "    }\n"
                    + //
                    "}",
                collectionName, id));
  }
}
