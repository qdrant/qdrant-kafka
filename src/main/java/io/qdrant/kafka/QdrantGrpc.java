package io.qdrant.kafka;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Points.PointStruct;
import io.qdrant.client.grpc.Points.ShardKeySelector;
import io.qdrant.client.grpc.Points.UpsertPoints;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;

/** Client for interacting with the Qdrant gRPC API. */
public class QdrantGrpc implements Serializable {

  private final QdrantClient client;

  public QdrantGrpc(QdrantSinkConfig config) {
    try {
      URL url = new URL(config.getGrpcUrl());
      String apiKey = config.getApiKey().value();

      String host = url.getHost();
      int port = url.getPort() == -1 ? 6334 : url.getPort();
      boolean useTls = url.getProtocol().equalsIgnoreCase("https");

      client =
          new QdrantClient(
              QdrantGrpcClient.newBuilder(host, port, useTls).withApiKey(apiKey).build());
    } catch (MalformedURLException e) {
      throw new RuntimeException("Invalid GRPC URL: " + config.getGrpcUrl(), e);
    }
  }

  public void upsert(
      String collectionName, List<PointStruct> points, ShardKeySelector shardKeySelector)
      throws InterruptedException, ExecutionException {
    UpsertPoints.Builder upsertPoints =
        UpsertPoints.newBuilder().setCollectionName(collectionName).addAllPoints(points);
    if (shardKeySelector != null) {
      upsertPoints.setShardKeySelector(shardKeySelector);
    }
    client.upsertAsync(upsertPoints.build()).get();
  }

  public void close() {
    client.close();
  }
}
