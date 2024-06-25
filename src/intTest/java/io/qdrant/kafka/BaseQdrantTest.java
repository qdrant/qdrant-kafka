/* (C)2024 */
package io.qdrant.kafka;

import static io.qdrant.kafka.QdrantSinkConfig.GRPC_URL;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections.Distance;
import io.qdrant.client.grpc.Collections.VectorParams;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.qdrant.QdrantContainer;

@Testcontainers
public class BaseQdrantTest {

  @Container static QdrantContainer qdrantContainer = new QdrantContainer(getQdrantImage());

  QdrantClient qdrantClient;

  String unnamedVecCollection = "unnamed-vec-collection";
  String namedVecCollection = "named-vec-collection";
  String sparseVecCollection = "sparse-vec-collection";

  @BeforeEach
  void setup() throws Exception {
    qdrantClient =
        new QdrantClient(
            QdrantGrpcClient.newBuilder(
                    qdrantContainer.getHost(), qdrantContainer.getMappedPort(6334), false)
                .build());

    qdrantClient
        .createCollectionAsync(
            unnamedVecCollection,
            VectorParams.newBuilder().setSize(8).setDistance(Distance.Dot).build())
        .get();
  }

  @AfterEach
  void tearDown() {
    if (Objects.nonNull(qdrantClient)) {
      qdrantClient.deleteCollectionAsync(unnamedVecCollection);
      qdrantClient.close();
    }
  }

  protected void waitForPoints(final String collectionName, final int expectedPoints)
      throws InterruptedException {
    TestUtils.waitForCondition(
        () -> {
          return expectedPoints == qdrantClient.countAsync(collectionName).get();
        },
        TimeUnit.MINUTES.toMillis(1),
        String.format("Could not find %d points in time.", expectedPoints));
  }

  private static String getQdrantImage() {
    return "qdrant/qdrant:latest";
  }

  protected Map<String, String> getDefaultProperties() {
    Map<String, String> properties = new HashMap<>();

    properties.put(GRPC_URL, "http://" + qdrantContainer.getGrpcHostAddress());

    return properties;
  }
}
