package io.qdrant.kafka;

import io.qdrant.client.grpc.Points.PointStruct;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QdrantSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(QdrantSinkTask.class);
  private QdrantSinkConfig config;
  private QdrantGrpc qdrantGrpc;

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> props) {
    start(props, null);
  }

  protected void start(Map<String, String> props, QdrantGrpc qdrantGrpc) {
    this.config = new QdrantSinkConfig(props);
    this.qdrantGrpc = qdrantGrpc == null ? new QdrantGrpc(config) : qdrantGrpc;
    log.info("Starting QdrantSinkTask at " + config.getGrpcUrl());
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }

    try {

      Map<String, List<PointStruct>> points = new HashMap<>();
      for (SinkRecord record : records) {
        if (record.value() == null) {
          log.warn("Record value is null. Skipping.");
          continue;
        }
        ValueExtractor e = new ValueExtractor(record.value());
        e.validateOptions();

        points
            .computeIfAbsent(e.getCollectionName(), k -> new ArrayList<>())
            .add(e.getPointStruct());
      }

      points.forEach(
          (collectionName, pointsList) -> {
            try {
              qdrantGrpc.upsert(collectionName, pointsList, null);
            } catch (InterruptedException | ExecutionException e) {
              throw new DataException("Qdrant server exception.", e);
            }
          });
    } catch (Exception e) {
      throw new DataException("Failed to put record.", e);
    }
  }

  @Override
  public void stop() {
    qdrantGrpc.close();
  }
}
