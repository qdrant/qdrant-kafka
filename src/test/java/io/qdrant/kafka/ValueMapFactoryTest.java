package io.qdrant.kafka;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.*;
import io.qdrant.client.ValueFactory;
import io.qdrant.client.grpc.JsonWithInt.Struct;
import io.qdrant.client.grpc.JsonWithInt.Value;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ValueMapFactoryTest {

  @Test
  void testParseNullValue() {
    com.google.protobuf.Value input =
        com.google.protobuf.Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();

    Value result = ValueMapFactory.parse(input);

    assertEquals(ValueFactory.nullValue(), result);
  }

  @Test
  void testParseBoolValue() {
    com.google.protobuf.Value input =
        com.google.protobuf.Value.newBuilder().setBoolValue(true).build();

    Value result = ValueMapFactory.parse(input);

    assertEquals(ValueFactory.value(true), result);
  }

  @Test
  void testParseStringValue() {
    com.google.protobuf.Value input =
        com.google.protobuf.Value.newBuilder().setStringValue("test").build();

    Value result = ValueMapFactory.parse(input);

    assertEquals(ValueFactory.value("test"), result);
  }

  @Test
  void testParseNumberValue() {
    com.google.protobuf.Value input =
        com.google.protobuf.Value.newBuilder().setNumberValue(42).build();

    Value result = ValueMapFactory.parse(input);

    assertEquals(ValueFactory.value(42L), result);
  }

  @Test
  void testParseDoubleValue() {
    com.google.protobuf.Value input =
        com.google.protobuf.Value.newBuilder().setNumberValue(42.42).build();

    Value result = ValueMapFactory.parse(input);

    assertEquals(ValueFactory.value(42.42), result);
  }

  @Test
  void testParseStructValue() {
    Map<String, com.google.protobuf.Value> fields = new HashMap<>();
    fields.put(
        "innerKey", com.google.protobuf.Value.newBuilder().setStringValue("innerValue").build());

    com.google.protobuf.Struct struct =
        com.google.protobuf.Struct.newBuilder().putAllFields(fields).build();
    com.google.protobuf.Value input =
        com.google.protobuf.Value.newBuilder().setStructValue(struct).build();

    Value result = ValueMapFactory.parse(input);

    Struct.Builder structBuilder = Struct.newBuilder();
    structBuilder.putFields("innerKey", ValueFactory.value("innerValue"));

    Value expectedStructValue = Value.newBuilder().setStructValue(structBuilder).build();

    assertEquals(expectedStructValue, result);
  }

  @Test
  void testParseListValue() {
    List<com.google.protobuf.Value> values =
        Arrays.asList(
            com.google.protobuf.Value.newBuilder().setStringValue("item1").build(),
            com.google.protobuf.Value.newBuilder().setNumberValue(123).build());

    com.google.protobuf.ListValue listValue =
        com.google.protobuf.ListValue.newBuilder().addAllValues(values).build();
    com.google.protobuf.Value input =
        com.google.protobuf.Value.newBuilder().setListValue(listValue).build();

    Value result = ValueMapFactory.parse(input);

    Value expectedListValue =
        ValueFactory.list(Arrays.asList(ValueFactory.value("item1"), ValueFactory.value(123L)));

    assertEquals(expectedListValue, result);
  }

  @Test
  void testParseEmptyValueType() {
    com.google.protobuf.Value input = com.google.protobuf.Value.newBuilder().build();

    Value result = ValueMapFactory.parse(input);

    Value expectedValue = Value.newBuilder().build();

    assertEquals(expectedValue, result);
  }

  // Ref: https://github.com/qdrant/qdrant-kafka/issues/9
  @Test
  void testParseFloatListValue() {
    List<com.google.protobuf.Value> floatArray =
        Arrays.asList(
            com.google.protobuf.Value.newBuilder().setNumberValue(0.0).build(),
            com.google.protobuf.Value.newBuilder().setNumberValue(1.1).build());
    com.google.protobuf.ListValue floatListValue =
        com.google.protobuf.ListValue.newBuilder().addAllValues(floatArray).build();
    List<com.google.protobuf.Value> values =
        Arrays.asList(
            com.google.protobuf.Value.newBuilder().setStringValue("item1").build(),
            com.google.protobuf.Value.newBuilder().setListValue(floatListValue).build());

    com.google.protobuf.ListValue listValue =
        com.google.protobuf.ListValue.newBuilder().addAllValues(values).build();
    com.google.protobuf.Value input =
        com.google.protobuf.Value.newBuilder().setListValue(listValue).build();

    Value result = ValueMapFactory.parse(input);

    Value expectedListValue =
        ValueFactory.list(
            Arrays.asList(
                ValueFactory.value("item1"),
                ValueFactory.list(Arrays.asList(ValueFactory.value(0L), ValueFactory.value(1.1)))));

    assertEquals(expectedListValue, result);
  }
}
