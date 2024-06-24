package io.qdrant.kafka;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.NullValue;
import io.qdrant.client.ValueFactory;
import io.qdrant.client.grpc.JsonWithInt.Struct;
import io.qdrant.client.grpc.JsonWithInt.Value;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ValueMapFactoryParseTest {

  private Map<String, com.google.protobuf.Value> protobufMap;
  private Map<String, Value> expectedMap;

  @BeforeEach
  void setUp() {
    protobufMap = new HashMap<>();
    expectedMap = new HashMap<>();
  }

  @Test
  void testParseNullValue() {
    com.google.protobuf.Value boolValue =
        com.google.protobuf.Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    protobufMap.put("nullKey", boolValue);
    expectedMap.put("nullKey", ValueFactory.nullValue());

    Map<String, Value> resultMap = ValueMapFactory.parse(protobufMap);

    assertEquals(expectedMap, resultMap);
  }

  @Test
  void testParseBoolValue() {
    com.google.protobuf.Value boolValue =
        com.google.protobuf.Value.newBuilder().setBoolValue(true).build();
    protobufMap.put("boolKey", boolValue);
    expectedMap.put("boolKey", ValueFactory.value(true));

    Map<String, Value> resultMap = ValueMapFactory.parse(protobufMap);

    assertEquals(expectedMap, resultMap);
  }

  @Test
  void testParseStringValue() {
    com.google.protobuf.Value stringValue =
        com.google.protobuf.Value.newBuilder().setStringValue("test").build();
    protobufMap.put("stringKey", stringValue);
    expectedMap.put("stringKey", ValueFactory.value("test"));

    Map<String, Value> resultMap = ValueMapFactory.parse(protobufMap);

    assertEquals(expectedMap, resultMap);
  }

  @Test
  void testParseNumberValue() {
    com.google.protobuf.Value numberValue =
        com.google.protobuf.Value.newBuilder().setNumberValue(42).build();
    protobufMap.put("numberKey", numberValue);
    expectedMap.put("numberKey", ValueFactory.value(42L));

    Map<String, Value> resultMap = ValueMapFactory.parse(protobufMap);

    assertEquals(expectedMap, resultMap);
  }

  @Test
  void testParseDoubleValue() {
    com.google.protobuf.Value doubleValue =
        com.google.protobuf.Value.newBuilder().setNumberValue(42.42).build();
    protobufMap.put("doubleKey", doubleValue);
    expectedMap.put("doubleKey", ValueFactory.value(42.42));

    Map<String, Value> resultMap = ValueMapFactory.parse(protobufMap);

    assertEquals(expectedMap, resultMap);
  }

  @Test
  void testParseLongValue() {
    com.google.protobuf.Value longValue =
        com.google.protobuf.Value.newBuilder().setNumberValue(9223372036854775807L).build();
    protobufMap.put("longKey", longValue);
    expectedMap.put("longKey", ValueFactory.value(9223372036854775807L));

    Map<String, Value> resultMap = ValueMapFactory.parse(protobufMap);

    assertEquals(expectedMap, resultMap);
  }

  @Test
  void testParseStructValue() {
    com.google.protobuf.Value innerStringValue =
        com.google.protobuf.Value.newBuilder().setStringValue("inner").build();
    com.google.protobuf.Struct struct =
        com.google.protobuf.Struct.newBuilder().putFields("innerKey", innerStringValue).build();
    com.google.protobuf.Value structValue =
        com.google.protobuf.Value.newBuilder().setStructValue(struct).build();

    protobufMap.put("structKey", structValue);

    Struct.Builder structBuilder = Struct.newBuilder();
    structBuilder.putFields("innerKey", ValueFactory.value("inner"));
    Value expectedStructValue = Value.newBuilder().setStructValue(structBuilder).build();
    expectedMap.put("structKey", expectedStructValue);

    Map<String, Value> resultMap = ValueMapFactory.parse(protobufMap);

    assertEquals(expectedMap, resultMap);
  }

  @Test
  void testParseListValue() {
    com.google.protobuf.Value listValue1 =
        com.google.protobuf.Value.newBuilder().setStringValue("item1").build();
    com.google.protobuf.Value listValue2 =
        com.google.protobuf.Value.newBuilder().setNumberValue(123).build();
    com.google.protobuf.ListValue list =
        com.google.protobuf.ListValue.newBuilder()
            .addValues(listValue1)
            .addValues(listValue2)
            .build();
    com.google.protobuf.Value listValue =
        com.google.protobuf.Value.newBuilder().setListValue(list).build();

    protobufMap.put("listKey", listValue);

    List<Value> expectedList = Arrays.asList(ValueFactory.value("item1"), ValueFactory.value(123L));
    expectedMap.put("listKey", ValueFactory.list(expectedList));

    Map<String, Value> resultMap = ValueMapFactory.parse(protobufMap);

    assertEquals(expectedMap, resultMap);
  }

  @Test
  void testEmptyValueType() {
    com.google.protobuf.Value emptyValue = com.google.protobuf.Value.newBuilder().build();

    protobufMap.put("emptyValueKey", emptyValue);
    expectedMap.put("emptyValueKey", Value.newBuilder().build());

    Map<String, Value> resultMap = ValueMapFactory.parse(protobufMap);

    assertEquals(expectedMap, resultMap);
  }
}
