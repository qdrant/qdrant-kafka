package io.qdrant.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.InvalidProtocolBufferException;
import io.qdrant.client.grpc.Common.PointId;
import io.qdrant.client.grpc.Points.PointStruct;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QdrantSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(QdrantSinkTask.class);
  private QdrantSinkConfig config;
  private QdrantGrpc qdrantGrpc;
  private ErrantRecordReporter reporter;

  @Override
  public String version() {
    return "1.4.0";
  }

  @Override
  public void start(Map<String, String> props) {
    start(props, null);
  }

  protected void start(Map<String, String> props, QdrantGrpc qdrantGrpc) {
    this.config = new QdrantSinkConfig(props);
    this.qdrantGrpc = qdrantGrpc == null ? new QdrantGrpc(config) : qdrantGrpc;
    this.reporter = context.errantRecordReporter();
    if (reporter == null) {
      log.warn("Errant record reporter is not configured.");
    }
    log.info("Starting QdrantSinkTask at " + config.getGrpcUrl());
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    Map<String, Map<PointStruct, SinkRecord>> pointsWithRecords = new HashMap<>();

    String collectionNameOverride = config.getCollectionName();

    for (SinkRecord record : records) {
      if (record.value() == null) {
        // Upserts are flushed first to preserve Kafka record order
        // when a batch contains both an
        // upsert and a later tombstone for the same point.
        upsert(pointsWithRecords);
        pointsWithRecords.clear();
      }
      try {
        if (record.value() == null) {
          ValueExtractor key = new ValueExtractor(record.key(), collectionNameOverride);
          delete(key.getCollectionName(), key.getPointId(), record);
          continue;
        }
        ValueExtractor e = new ValueExtractor(record.value(), collectionNameOverride);
        e.validateOptions();
        pointsWithRecords
            .computeIfAbsent(e.getCollectionName(), k -> new HashMap<>())
            .put(e.getPointStruct(), record);
      } catch (InvalidProtocolBufferException | JsonProcessingException | DataException e) {
        if (reporter == null) throw new DataException("Invalid sink record", e);
        reporter.report(record, e);
      }
    }

    upsert(pointsWithRecords);
  }

  private void upsert(Map<String, Map<PointStruct, SinkRecord>> pointsWithRecords) {
    pointsWithRecords.forEach(
        (collectionName, pointsMap) -> {
          List<PointStruct> pointsList = new ArrayList<>(pointsMap.keySet());
          try {
            qdrantGrpc.upsert(collectionName, pointsList, null);
          } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            pointsMap
                .values()
                .forEach(record -> report(record, "Qdrant server exception during upsert.", e));
          }
        });
  }

  private void delete(String collectionName, PointId pointId, SinkRecord record) {
    try {
      qdrantGrpc.delete(collectionName, pointId);
    } catch (InterruptedException | ExecutionException e) {
      if (e instanceof InterruptedException) Thread.currentThread().interrupt();
      report(record, "Qdrant server exception during delete.", e);
    }
  }

  private void report(SinkRecord record, String message, Exception exception) {
    if (reporter == null) throw new DataException(message, exception);
    reporter.report(record, exception);
  }

  @Override
  public void stop() {
    qdrantGrpc.close();
  }
}
