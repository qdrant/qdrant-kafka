package io.qdrant.kafka;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.InvalidProtocolBufferException;
import io.qdrant.client.ValueFactory;
import io.qdrant.client.grpc.JsonWithInt.Struct;
import io.qdrant.client.grpc.JsonWithInt.Value;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class ValueMapFactoryJsonTest {

  @Test
  void testParseJsonStringWithNullValue() throws Exception {
    String json = "{\"nullKey\": null}";

    Map<String, Value> resultMap = ValueMapFactory.parse(json);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("nullKey"));
    assertEquals(ValueFactory.nullValue(), resultMap.get("nullKey"));
  }

  @Test
  void testParseJsonStringWithBooleanValue() throws Exception {
    String json = "{\"boolKey\": true}";

    Map<String, Value> resultMap = ValueMapFactory.parse(json);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("boolKey"));
    assertEquals(ValueFactory.value(true), resultMap.get("boolKey"));
  }

  @Test
  void testParseJsonStringWithStringValue() throws Exception {
    String json = "{\"stringKey\": \"test\"}";

    Map<String, Value> resultMap = ValueMapFactory.parse(json);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("stringKey"));
    assertEquals(ValueFactory.value("test"), resultMap.get("stringKey"));
  }

  @Test
  void testParseJsonStringWithNumberValue() throws Exception {
    String json = "{\"numberKey\": 42}";

    Map<String, Value> resultMap = ValueMapFactory.parse(json);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("numberKey"));
    assertEquals(ValueFactory.value(42L), resultMap.get("numberKey"));
  }

  @Test
  void testParseJsonStringWithDoubleValue() throws Exception {
    String json = "{\"doubleKey\": 42.42}";

    Map<String, Value> resultMap = ValueMapFactory.parse(json);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("doubleKey"));
    assertEquals(ValueFactory.value(42.42), resultMap.get("doubleKey"));
  }

  @Test
  void testParseJsonStringWithLongValue() throws Exception {
    String json = "{\"longKey\": 9223372036854775807}";

    Map<String, Value> resultMap = ValueMapFactory.parse(json);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("longKey"));
    assertEquals(ValueFactory.value(9223372036854775807L), resultMap.get("longKey"));
  }

  @Test
  void testParseJsonStringWithStructValue() throws Exception {
    String json = "{\"structKey\": {\"innerKey\": \"innerValue\"}}";

    Map<String, Value> resultMap = ValueMapFactory.parse(json);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("structKey"));

    Value expectedStructValue =
        Value.newBuilder()
            .setStructValue(
                io.qdrant.client.grpc.JsonWithInt.Struct.newBuilder()
                    .putFields("innerKey", ValueFactory.value("innerValue"))
                    .build())
            .build();

    assertEquals(expectedStructValue, resultMap.get("structKey"));
  }

  @Test
  void testParseJsonStringWithListValue() throws Exception {
    String json = "{\"listKey\": [\"item1\", 123]}";

    Map<String, Value> resultMap = ValueMapFactory.parse(json);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("listKey"));

    Value expectedListValue =
        ValueFactory.list(Arrays.asList(ValueFactory.value("item1"), ValueFactory.value(123L)));

    assertEquals(expectedListValue, resultMap.get("listKey"));
  }

  @Test
  void testParseJsonStringWithNestedStruct() throws Exception {
    String json = "{\"outerStructKey\": {\"innerStructKey\": {\"innerKey\": \"innerValue\"}}}";

    Map<String, Value> resultMap = ValueMapFactory.parse(json);

    assertEquals(1, resultMap.size());
    assertTrue(resultMap.containsKey("outerStructKey"));

    Struct.Builder innerStructBuilder = Struct.newBuilder();
    innerStructBuilder.putFields("innerKey", ValueFactory.value("innerValue"));

    Struct.Builder outerStructBuilder = Struct.newBuilder();
    outerStructBuilder.putFields(
        "innerStructKey", Value.newBuilder().setStructValue(innerStructBuilder).build());

    Value expectedOuterStructValue = Value.newBuilder().setStructValue(outerStructBuilder).build();

    assertEquals(expectedOuterStructValue, resultMap.get("outerStructKey"));
  }

  @Test
  void testParseJsonStringWithNestedList() throws Exception {
    String json = "{\"outerListKey\": [[\"item1\", 123], [true, \"item2\"]]}";

    Map<String, Value> resultMap = ValueMapFactory.parse(json);

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
  void testParseInvalidJsonString() {
    String json = "{invalidJson}";

    Executable executable = () -> ValueMapFactory.parse(json);

    assertThrows(InvalidProtocolBufferException.class, executable);
  }
}
