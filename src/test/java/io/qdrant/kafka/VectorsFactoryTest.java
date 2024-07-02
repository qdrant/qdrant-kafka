package io.qdrant.kafka;

import static org.junit.jupiter.api.Assertions.*;

import io.qdrant.client.grpc.JsonWithInt.ListValue;
import io.qdrant.client.grpc.JsonWithInt.Struct;
import io.qdrant.client.grpc.JsonWithInt.Value;
import io.qdrant.client.grpc.Points.NamedVectors;
import io.qdrant.client.grpc.Points.SparseIndices;
import io.qdrant.client.grpc.Points.Vector;
import io.qdrant.client.grpc.Points.Vectors;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

class VectorsFactoryTest {

  @Test
  void testDenseVector() {
    ListValue listValue =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setDoubleValue(0.32).build())
            .addValues(Value.newBuilder().setDoubleValue(0.432).build())
            .addValues(Value.newBuilder().setDoubleValue(0.423).build())
            .addValues(Value.newBuilder().setDoubleValue(0.52354).build())
            .build();

    Value vectorValue = Value.newBuilder().setListValue(listValue).build();

    Vectors vectors = VectorsFactory.vectors(vectorValue);
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
  void testNamedVectors() {
    Map<String, Value> fieldsMap = new HashMap<>();
    fieldsMap.put(
        "boi",
        Value.newBuilder()
            .setListValue(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setDoubleValue(0.32).build())
                    .addValues(Value.newBuilder().setDoubleValue(0.432).build())
                    .addValues(Value.newBuilder().setDoubleValue(0.423).build())
                    .addValues(Value.newBuilder().setDoubleValue(0.52354).build())
                    .build())
            .build());
    fieldsMap.put(
        "gal",
        Value.newBuilder()
            .setListValue(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setDoubleValue(0.3422).build())
                    .addValues(Value.newBuilder().setDoubleValue(0.341432).build())
                    .addValues(Value.newBuilder().setDoubleValue(0.41423).build())
                    .addValues(Value.newBuilder().setDoubleValue(0.52354).build())
                    .build())
            .build());
    fieldsMap.put(
        "multi",
        Value.newBuilder()
            .setListValue(
                ListValue.newBuilder()
                    .addValues(
                        Value.newBuilder()
                            .setListValue(
                                ListValue.newBuilder()
                                    .addValues(Value.newBuilder().setDoubleValue(0.32).build())
                                    .addValues(Value.newBuilder().setDoubleValue(0.432).build())
                                    .addValues(Value.newBuilder().setDoubleValue(0.423).build())
                                    .addValues(Value.newBuilder().setDoubleValue(0.52354).build())
                                    .build())
                            .build())
                    .addValues(
                        Value.newBuilder()
                            .setListValue(
                                ListValue.newBuilder()
                                    .addValues(Value.newBuilder().setDoubleValue(0.32).build())
                                    .addValues(Value.newBuilder().setDoubleValue(0.432).build())
                                    .addValues(Value.newBuilder().setDoubleValue(0.423).build())
                                    .addValues(Value.newBuilder().setDoubleValue(0.52354).build())
                                    .build())
                            .build())
                    .addValues(
                        Value.newBuilder()
                            .setListValue(
                                ListValue.newBuilder()
                                    .addValues(Value.newBuilder().setDoubleValue(0.32).build())
                                    .addValues(Value.newBuilder().setDoubleValue(0.432).build())
                                    .addValues(Value.newBuilder().setDoubleValue(0.423).build())
                                    .addValues(Value.newBuilder().setDoubleValue(0.52354).build())
                                    .build())
                            .build())
                    .addValues(
                        Value.newBuilder()
                            .setListValue(
                                ListValue.newBuilder()
                                    .addValues(Value.newBuilder().setDoubleValue(0.32).build())
                                    .addValues(Value.newBuilder().setDoubleValue(0.432).build())
                                    .addValues(Value.newBuilder().setDoubleValue(0.423).build())
                                    .addValues(Value.newBuilder().setDoubleValue(0.52354).build())
                                    .build())
                            .build())
                    .build())
            .build());

    Struct struct = Struct.newBuilder().putAllFields(fieldsMap).build();
    Value vectorValue = Value.newBuilder().setStructValue(struct).build();

    Vectors vectors = VectorsFactory.vectors(vectorValue);

    assertTrue(vectors.hasVectors());
    NamedVectors namedVectors = vectors.getVectors();
    assertEquals(3, namedVectors.getVectorsCount());
    assertTrue(namedVectors.containsVectors("boi"));
    assertTrue(namedVectors.containsVectors("gal"));
    assertTrue(namedVectors.containsVectors("multi"));

    Vector multiVector = namedVectors.getVectorsOrThrow("multi");
    assertEquals(4, multiVector.getVectorsCount());
  }

  @Test
  void testSparseVector() {
    ListValue indicesListValue =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setIntegerValue(423).build())
            .addValues(Value.newBuilder().setIntegerValue(412).build())
            .addValues(Value.newBuilder().setIntegerValue(4123).build())
            .build();

    ListValue valuesListValue =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setDoubleValue(0.423).build())
            .addValues(Value.newBuilder().setDoubleValue(0.6423).build())
            .addValues(Value.newBuilder().setDoubleValue(0.6342).build())
            .build();

    Map<String, Value> fieldsMap = new HashMap<>();
    fieldsMap.put("indices", Value.newBuilder().setListValue(indicesListValue).build());
    fieldsMap.put("values", Value.newBuilder().setListValue(valuesListValue).build());

    Struct struct = Struct.newBuilder().putAllFields(fieldsMap).build();
    Map<String, Value> outerFieldsMap = new HashMap<>();
    outerFieldsMap.put("some-vec", Value.newBuilder().setStructValue(struct).build());
    Struct outerStruct = Struct.newBuilder().putAllFields(outerFieldsMap).build();
    Value vectorValue = Value.newBuilder().setStructValue(outerStruct).build();

    Vectors vectors = VectorsFactory.vectors(vectorValue);

    assertTrue(vectors.hasVectors());
    NamedVectors namedVectors = vectors.getVectors();
    assertTrue(namedVectors.containsVectors("some-vec"));
    Vector sparseVector = namedVectors.getVectorsOrThrow("some-vec");
    assertTrue(sparseVector.hasIndices());
    SparseIndices indices = sparseVector.getIndices();
    assertEquals(3, indices.getDataCount());
    assertEquals(423, indices.getData(0));
    assertEquals(412, indices.getData(1));
    assertEquals(4123, indices.getData(2));
    assertEquals(0.423f, sparseVector.getData(0));
    assertEquals(0.6423f, sparseVector.getData(1));
    assertEquals(0.6342f, sparseVector.getData(2));
  }

  @Test
  void testInvalidVectorFormat() {
    Value invalidValue = Value.newBuilder().setStringValue("invalid").build();
    assertThrows(DataException.class, () -> VectorsFactory.vectors(invalidValue));
  }

  @Test
  void testInvalidDenseVectorDataType() {
    ListValue invalidListValue =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("invalid").build())
            .build();
    Value vectorValue = Value.newBuilder().setListValue(invalidListValue).build();
    assertThrows(DataException.class, () -> VectorsFactory.vectors(vectorValue));
  }

  @Test
  void testInvalidSparseVectorIndicesType() {
    ListValue indicesListValue =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("invalid").build())
            .build();
    ListValue valuesListValue =
        ListValue.newBuilder().addValues(Value.newBuilder().setDoubleValue(0.423).build()).build();
    Map<String, Value> fieldsMap = new HashMap<>();
    fieldsMap.put("indices", Value.newBuilder().setListValue(indicesListValue).build());
    fieldsMap.put("values", Value.newBuilder().setListValue(valuesListValue).build());
    Struct struct = Struct.newBuilder().putAllFields(fieldsMap).build();
    Value vectorValue = Value.newBuilder().setStructValue(struct).build();
    assertThrows(DataException.class, () -> VectorsFactory.vectors(vectorValue));
  }

  @Test
  void testSparseVectorMissingFields() {
    ListValue valuesListValue =
        ListValue.newBuilder().addValues(Value.newBuilder().setDoubleValue(0.423).build()).build();

    Map<String, Value> fieldsMap = new HashMap<>();
    fieldsMap.put("values", Value.newBuilder().setListValue(valuesListValue).build());

    Struct struct = Struct.newBuilder().putAllFields(fieldsMap).build();
    Map<String, Value> outerFieldsMap = new HashMap<>();
    outerFieldsMap.put("some-vec", Value.newBuilder().setStructValue(struct).build());
    Struct outerStruct = Struct.newBuilder().putAllFields(outerFieldsMap).build();
    Value vectorValue = Value.newBuilder().setStructValue(outerStruct).build();
    assertThrows(DataException.class, () -> VectorsFactory.vectors(vectorValue));
  }
}
