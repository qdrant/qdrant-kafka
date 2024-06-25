/* (C)2024 */
package io.qdrant.kafka;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QdrantSinkConnectorTest extends BaseKafkaConnectTest {

  static final Logger LOGGER = LoggerFactory.getLogger(QdrantSinkConnectorTest.class);

  static final String CONNECTOR_NAME = "qdrant-sink-connector";

  static final String TOPIC_NAME = "qdrant-topic";

  public QdrantSinkConnectorTest() {
    super(TOPIC_NAME, CONNECTOR_NAME);
  }

  @Test
  public void testUnnamedVector() throws Exception {
    connect.configureConnector(CONNECTOR_NAME, connectorProperties());
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    int pointsCount = 2;

    for (int i = 0; i < pointsCount; i++) {
      writeUnnamedPoint(unnamedVecCollection, i);
    }

    waitForPoints(unnamedVecCollection, pointsCount);
  }
}
