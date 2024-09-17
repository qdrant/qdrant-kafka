package io.qdrant.kafka;

import static io.qdrant.client.PointIdFactory.id;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.InvalidProtocolBufferException;
import io.qdrant.client.grpc.JsonWithInt.Value;
import io.qdrant.client.grpc.Points.NamedVectors;
import io.qdrant.client.grpc.Points.PointId;
import io.qdrant.client.grpc.Points.PointStruct;
import io.qdrant.client.grpc.Points.Vectors;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.connect.errors.DataException;

class ValueExtractor {
  private static final String ID_KEY = "id";
  private static final String COLLECTION_NAME_KEY = "collection_name";
  private static final String VECTOR_KEY = "vector";
  private static final String PAYLOAD_KEY = "payload";
  private static final String[] REQUIRED_FIELDS = {"collection_name", "id"};

  private final Map<String, Value> valueMap;

  ValueExtractor(Object object) throws InvalidProtocolBufferException, JsonProcessingException {
    // We turn the entire message into Map<String, Value>
    // This is a bit of a hack, but it's the easiest way to work with the data
    // We get the nice type checking of Value, and we can easily access the fields.
    this.valueMap = ValueMapFactory.valueMap(object);
  }

  public String getCollectionName() {
    return this.valueMap.get(COLLECTION_NAME_KEY).getStringValue();
  }

  public PointId getPointId() {
    Value idCandidate = this.valueMap.get(ID_KEY);

    switch (idCandidate.getKindCase()) {
      case STRING_VALUE:
        UUID uuid = UUID.fromString(idCandidate.getStringValue());
        return id(uuid);

      case INTEGER_VALUE:
        long numId = idCandidate.getIntegerValue();

        if (numId < 0) {
          throw new DataException("Point ID must be a positive integer");
        }
        return id(numId);

      default:
        throw new DataException("Point ID must be a UUID string or an integer");
    }
  }

  public Map<String, Value> getPayload() {
    Value payload = this.valueMap.get(PAYLOAD_KEY);

    if (payload == null) {
      return new HashMap<>();
    }

    switch (payload.getKindCase()) {
      case STRUCT_VALUE:
        return payload.getStructValue().getFieldsMap();

      case NULL_VALUE:
        return new HashMap<>();

      default:
        throw new DataException("Payload must be an object.");
    }
  }

  public Vectors getVector() {
    Value vectorValue = this.valueMap.get(VECTOR_KEY);

    if (vectorValue == null || vectorValue.hasNullValue()) {
      return Vectors.newBuilder().setVectors(NamedVectors.getDefaultInstance()).build();
    }

    return VectorsFactory.vectors(vectorValue);
  }

  public PointStruct getPointStruct() {
    return PointStruct.newBuilder()
        .setId(getPointId())
        .putAllPayload(getPayload())
        .setVectors(getVector())
        .build();
  }

  public void validateOptions() {
    for (String field : REQUIRED_FIELDS) {
      if (!this.valueMap.containsKey(field) || this.valueMap.get(field) == null) {
        throw new DataException(String.format("'%s' value is required", field));
      }
    }
  }
}
