package io.qdrant.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.InvalidProtocolBufferException;
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
    return "1.3.0";
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
      try {
        if (record.value() == null) {
          log.warn("Record value is null. Skipping.");
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

    pointsWithRecords.forEach(
        (collectionName, pointsMap) -> {
          List<PointStruct> pointsList = new ArrayList<>(pointsMap.keySet());
          try {
            qdrantGrpc.upsert(collectionName, pointsList, null);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pointsMap
                .values()
                .forEach(
                    record -> {
                      if (reporter == null)
                        throw new DataException("Qdrant server exception during upsert.", e);
                      reporter.report(record, e);
                    });
          } catch (ExecutionException e) {
            pointsMap
                .values()
                .forEach(
                    record -> {
                      if (reporter == null)
                        throw new DataException("Qdrant server exception during upsert.", e);
                      reporter.report(record, e);
                    });
          }
        });
  }

  @Override
  public void stop() {
    qdrantGrpc.close();
  }
}
