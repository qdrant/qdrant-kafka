/* (C)2024 */
package io.qdrant.kafka;

import java.util.UUID;
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

    int pointsCount = randomPositiveInt(100);

    for (int i = 0; i < pointsCount; i++) {
      writeUnnamedVector(unnamedVecCollection, i, unnamedVecSize);
    }

    waitForPoints(unnamedVecCollection, pointsCount);
  }

  @Test
  public void testNamedVector() throws Exception {
    connect.configureConnector(CONNECTOR_NAME, connectorProperties());
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    int pointsCount = randomPositiveInt(100);

    for (int i = 0; i < pointsCount; i++) {
      writeNamedVector(namedVecCollection, i, namedVecSize, namedVecName);
    }

    waitForPoints(namedVecCollection, pointsCount);
  }

  @Test
  public void testSparseVector() throws Exception {
    connect.configureConnector(CONNECTOR_NAME, connectorProperties());
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    int pointsCount = randomPositiveInt(100);
    int sparseVecCount = randomPositiveInt(100);

    for (int i = 0; i < pointsCount; i++) {
      String uuid = UUID.randomUUID().toString();
      writeSparseVector(sparseVecCollection, uuid, sparseVecName, sparseVecCount);
    }

    waitForPoints(sparseVecCollection, pointsCount);
  }

  @Test
  public void testMultiVector() throws Exception {
    connect.configureConnector(CONNECTOR_NAME, connectorProperties());
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    int pointsCount = randomPositiveInt(10);
    int multiSize = randomPositiveInt(10);

    for (int i = 0; i < pointsCount; i++) {
      writeMultiVector(multiVecCollection, i, multiVecSize, multiVecName, multiSize);
    }

    waitForPoints(multiVecCollection, pointsCount);
  }
}
