/* (C)2024 */
package io.qdrant.kafka;

import static io.qdrant.kafka.QdrantSinkConfig.GRPC_URL;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections.CreateCollection;
import io.qdrant.client.grpc.Collections.Distance;
import io.qdrant.client.grpc.Collections.MultiVectorComparator;
import io.qdrant.client.grpc.Collections.MultiVectorConfig;
import io.qdrant.client.grpc.Collections.SparseVectorConfig;
import io.qdrant.client.grpc.Collections.SparseVectorParams;
import io.qdrant.client.grpc.Collections.VectorParams;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
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
  int unnamedVecSize = 1536;

  String namedVecCollection = "named-vec-collection";
  String namedVecName = "named-vec";
  int namedVecSize = 384;

  String sparseVecCollection = "sparse-vec-collection";
  String sparseVecName = "sparse-vec";

  String multiVecCollection = "multi-vec-collection";
  String multiVecName = "multi-vec";
  int multiVecSize = 58;

  @BeforeEach
  void setup() throws Exception {
    qdrantClient =
        new QdrantClient(
            QdrantGrpcClient.newBuilder(
                    qdrantContainer.getHost(), qdrantContainer.getMappedPort(6334), false)
                .build());

    // Create unnamed vector collection
    qdrantClient
        .createCollectionAsync(
            unnamedVecCollection,
            VectorParams.newBuilder().setSize(unnamedVecSize).setDistance(Distance.Dot).build())
        .get();

    // Create named vector collection
    Map<String, VectorParams> namedVecParams = new HashMap<>();
    namedVecParams.put(
        namedVecName,
        VectorParams.newBuilder().setSize(namedVecSize).setDistance(Distance.Dot).build());
    qdrantClient.createCollectionAsync(namedVecCollection, namedVecParams).get();

    // Create sparse vector collection
    qdrantClient
        .createCollectionAsync(
            CreateCollection.newBuilder()
                .setCollectionName(sparseVecCollection)
                .setSparseVectorsConfig(
                    SparseVectorConfig.newBuilder()
                        .putMap(sparseVecName, SparseVectorParams.getDefaultInstance()))
                .build())
        .get();

    // Create multi vector collection
    Map<String, VectorParams> multiVecParams = new HashMap<>();
    multiVecParams.put(
        multiVecName,
        VectorParams.newBuilder()
            .setSize(multiVecSize)
            .setDistance(Distance.Dot)
            .setMultivectorConfig(
                MultiVectorConfig.newBuilder().setComparator(MultiVectorComparator.MaxSim).build())
            .build());

    qdrantClient.createCollectionAsync(multiVecCollection, multiVecParams).get();
  }

  @AfterEach
  void tearDown() {
    if (Objects.nonNull(qdrantClient)) {
      qdrantClient.deleteCollectionAsync(unnamedVecCollection);
      qdrantClient.deleteCollectionAsync(namedVecCollection);
      qdrantClient.deleteCollectionAsync(sparseVecCollection);
      qdrantClient.deleteCollectionAsync(multiVecCollection);
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

  List<Float> randomVector(int size) {
    List<Float> vector = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      vector.add((float) Math.random());
    }
    return vector;
  }

  List<Integer> randomIndices(int numberOfRandomNumbers) {
    Random random = new Random();
    Set<Integer> randomNumbersSet = new HashSet<>();

    while (randomNumbersSet.size() < numberOfRandomNumbers) {
      int number = random.nextInt(10000) + 1;
      randomNumbersSet.add(number);
    }

    List<Integer> randomNumbersList = new ArrayList<>(randomNumbersSet);
    Collections.sort(randomNumbersList);

    return randomNumbersList;
  }

  int randomPositiveInt(int bound) {
    return new Random().nextInt(bound) + 1;
  }
}
