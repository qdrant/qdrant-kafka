package io.qdrant.kafka;

import static io.qdrant.client.PointIdFactory.id;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.InvalidProtocolBufferException;
import io.qdrant.client.grpc.JsonWithInt.Value;
import io.qdrant.client.grpc.Points.PointId;
import io.qdrant.client.grpc.Points.Vector;
import io.qdrant.client.grpc.Points.Vectors;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

class ExtractorTest {

  @Test
  void testGetCollectionName() throws InvalidProtocolBufferException, JsonProcessingException {
    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put("collection_name", "test_collection");

    ValueExtractor extractor = new ValueExtractor(valueMap);
    assertEquals("test_collection", extractor.getCollectionName());
  }

  @Test
  void testGetPointIdWithUUID() throws InvalidProtocolBufferException, JsonProcessingException {
    UUID uuid = UUID.randomUUID();
    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put("id", uuid.toString());

    ValueExtractor extractor = new ValueExtractor(valueMap);
    PointId pointId = extractor.getPointId();
    assertEquals(id(uuid), pointId);
  }

  @Test
  void testGetPointIdWithInteger() throws InvalidProtocolBufferException, JsonProcessingException {
    long idValue = 12345;
    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put("id", idValue);

    ValueExtractor extractor = new ValueExtractor(valueMap);
    PointId pointId = extractor.getPointId();
    assertEquals(id(idValue), pointId);
  }

  @Test
  void testGetPointIdWithNegativeInteger()
      throws InvalidProtocolBufferException, JsonProcessingException {
    long idValue = -23452;
    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put("id", idValue);

    ValueExtractor extractor = new ValueExtractor(valueMap);
    assertThrows(DataException.class, extractor::getPointId);
  }

  @Test
  void testGetPayload() throws InvalidProtocolBufferException, JsonProcessingException {
    Map<String, Object> payloadMap = new HashMap<>();
    payloadMap.put("field1", "value1");
    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put("payload", payloadMap);

    ValueExtractor extractor = new ValueExtractor(valueMap);
    Map<String, Value> payload = extractor.getPayload();

    Map<String, Value> expectedPayload = new HashMap<>();
    expectedPayload.put("field1", Value.newBuilder().setStringValue("value1").build());
    assertEquals(expectedPayload, payload);
  }

  @Test
  void testGetPayloadNull() throws InvalidProtocolBufferException, JsonProcessingException {
    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put("payload", null);

    ValueExtractor extractor = new ValueExtractor(valueMap);
    assertEquals(extractor.getPayload(), new HashMap<String, Value>());
  }

  @Test
  void testGetVector() throws InvalidProtocolBufferException, JsonProcessingException {
    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put("vector", new float[] {0.32f, 0.432f, 0.423f, 0.52354f});

    ValueExtractor extractor = new ValueExtractor(valueMap);
    Vectors vectors = extractor.getVector();

    Vector expectedVector =
        Vector.newBuilder()
            .addData(0.32f)
            .addData(0.432f)
            .addData(0.423f)
            .addData(0.52354f)
            .build();

    assertTrue(vectors.hasVector());
    assertEquals(expectedVector, vectors.getVector());
  }

  @Test
  void testValidateOptions() throws InvalidProtocolBufferException, JsonProcessingException {
    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put("collection_name", "test_collection");
    valueMap.put("id", 12345L);
    valueMap.put("vector", new float[] {0.32f, 0.432f, 0.423f, 0.52354f});

    ValueExtractor extractor = new ValueExtractor(valueMap);
    assertDoesNotThrow(extractor::validateOptions);
  }

  @Test
  void testValidateOptionsMissingField()
      throws InvalidProtocolBufferException, JsonProcessingException {
    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put("collection_name", "test_collection");
    valueMap.put("id", 12345L);

    ValueExtractor extractor = new ValueExtractor(valueMap);
    assertThrows(DataException.class, extractor::validateOptions);
  }

  @Test
  void testInvalidPointIdFormat() throws InvalidProtocolBufferException, JsonProcessingException {
    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put("id", true);

    ValueExtractor extractor = new ValueExtractor(valueMap);
    assertThrows(DataException.class, extractor::getPointId);
  }

  @Test
  void testInvalidPayloadFormat() throws InvalidProtocolBufferException, JsonProcessingException {
    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put("payload", true);

    ValueExtractor extractor = new ValueExtractor(valueMap);
    assertThrows(DataException.class, extractor::getPayload);
  }
}
