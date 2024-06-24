package io.qdrant.kafka;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.qdrant.client.ValueFactory;
import io.qdrant.client.grpc.JsonWithInt.Struct;
import io.qdrant.client.grpc.JsonWithInt.Value;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class ValueMapFactoryMapTest {

  @Test
  void testValueMapWithNullValue() throws Exception {
    Map<String, Object> inputMap = new HashMap<>();
    inputMap.put("nullKey", null);

    Map<String, Value> resultMap = ValueMapFactory.valueMap(inputMap);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("nullKey"));
    assertEquals(ValueFactory.nullValue(), resultMap.get("nullKey"));
  }

  @Test
  void testValueMapWithBooleanValue() throws Exception {
    Map<String, Object> inputMap = new HashMap<>();
    inputMap.put("boolKey", true);

    Map<String, Value> resultMap = ValueMapFactory.valueMap(inputMap);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("boolKey"));
    assertEquals(ValueFactory.value(true), resultMap.get("boolKey"));
  }

  @Test
  void testValueMapWithStringValue() throws Exception {
    Map<String, Object> inputMap = new HashMap<>();
    inputMap.put("stringKey", "test");

    Map<String, Value> resultMap = ValueMapFactory.valueMap(inputMap);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("stringKey"));
    assertEquals(ValueFactory.value("test"), resultMap.get("stringKey"));
  }

  @Test
  void testValueMapWithNumberValue() throws Exception {
    Map<String, Object> inputMap = new HashMap<>();
    inputMap.put("numberKey", 42);

    Map<String, Value> resultMap = ValueMapFactory.valueMap(inputMap);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("numberKey"));
    assertEquals(ValueFactory.value(42L), resultMap.get("numberKey"));
  }

  @Test
  void testValueMapWithDoubleValue() throws Exception {
    Map<String, Object> inputMap = new HashMap<>();
    inputMap.put("doubleKey", 42.42);

    Map<String, Value> resultMap = ValueMapFactory.valueMap(inputMap);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("doubleKey"));
    assertEquals(ValueFactory.value(42.42), resultMap.get("doubleKey"));
  }

  @Test
  void testValueMapWithLongValue() throws Exception {
    Map<String, Object> inputMap = new HashMap<>();
    inputMap.put("longKey", 9223372036854775807L);

    Map<String, Value> resultMap = ValueMapFactory.valueMap(inputMap);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("longKey"));
    assertEquals(ValueFactory.value(9223372036854775807L), resultMap.get("longKey"));
  }

  @Test
  void testValueMapWithStructValue() throws Exception {
    Map<String, Object> inputMap = new HashMap<>();
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put("innerKey", "innerValue");
    inputMap.put("structKey", innerMap);

    Map<String, Value> resultMap = ValueMapFactory.valueMap(inputMap);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("structKey"));

    Struct.Builder innerStructBuilder = Struct.newBuilder();
    innerStructBuilder.putFields("innerKey", ValueFactory.value("innerValue"));

    Value expectedStructValue = Value.newBuilder().setStructValue(innerStructBuilder).build();

    assertEquals(expectedStructValue, resultMap.get("structKey"));
  }

  @Test
  void testValueMapWithListValue() throws Exception {
    Map<String, Object> inputMap = new HashMap<>();
    inputMap.put("listKey", Arrays.asList("item1", 123));

    Map<String, Value> resultMap = ValueMapFactory.valueMap(inputMap);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("listKey"));

    Value expectedListValue =
        ValueFactory.list(Arrays.asList(ValueFactory.value("item1"), ValueFactory.value(123L)));

    assertEquals(expectedListValue, resultMap.get("listKey"));
  }

  @Test
  void testValueMapWithNestedStruct() throws Exception {
    Map<String, Object> inputMap = new HashMap<>();
    Map<String, Object> innerMap = new HashMap<>();
    Map<String, Object> nestedMap = new HashMap<>();
    nestedMap.put("innerKey", "innerValue");
    innerMap.put("innerStructKey", nestedMap);
    inputMap.put("outerStructKey", innerMap);

    Map<String, Value> resultMap = ValueMapFactory.valueMap(inputMap);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("outerStructKey"));

    Struct.Builder nestedStructBuilder = Struct.newBuilder();
    nestedStructBuilder.putFields("innerKey", ValueFactory.value("innerValue"));

    Struct.Builder innerStructBuilder = Struct.newBuilder();
    innerStructBuilder.putFields(
        "innerStructKey", Value.newBuilder().setStructValue(nestedStructBuilder).build());

    Value expectedOuterStructValue = Value.newBuilder().setStructValue(innerStructBuilder).build();

    assertEquals(expectedOuterStructValue, resultMap.get("outerStructKey"));
  }

  @Test
  void testValueMapWithNestedList() throws Exception {
    Map<String, Object> inputMap = new HashMap<>();
    inputMap.put(
        "outerListKey", Arrays.asList(Arrays.asList("item1", 123), Arrays.asList(true, "item2")));

    Map<String, Value> resultMap = ValueMapFactory.valueMap(inputMap);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("outerListKey"));

    Value innerListValue1 =
        ValueFactory.list(Arrays.asList(ValueFactory.value("item1"), ValueFactory.value(123L)));
    Value innerListValue2 =
        ValueFactory.list(Arrays.asList(ValueFactory.value(true), ValueFactory.value("item2")));

    Value expectedOuterListValue =
        ValueFactory.list(Arrays.asList(innerListValue1, innerListValue2));

    assertEquals(expectedOuterListValue, resultMap.get("outerListKey"));
  }

  @Test
  void testValueMapWithInvalidObject() {
    Map<String, Object> inputMap = new HashMap<>();
    inputMap.put("invalidKey", new Object()); // Object that cannot be serialized to JSON

    Executable executable = () -> ValueMapFactory.valueMap(inputMap);

    assertThrows(JsonProcessingException.class, executable);
  }
}
