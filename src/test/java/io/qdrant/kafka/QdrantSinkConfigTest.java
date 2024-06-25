/* (C)2024 */
package io.qdrant.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.Test;

class QdrantSinkConfigTest {

  @Test
  void testDefaultConfigValues() {
    QdrantSinkConfig config = new QdrantSinkConfig(new HashMap<>());
    assertEquals("http://localhost:6334", config.getGrpcUrl());
    assertEquals(new Password(""), config.getApiKey());
  }

  @Test
  void testCustomConfigValues() {
    Map<String, String> customConfig = new HashMap<>();
    customConfig.put(QdrantSinkConfig.GRPC_URL, "http://custom-url:6334/");
    customConfig.put(QdrantSinkConfig.API_KEY, "custom-api-key");

    QdrantSinkConfig config = new QdrantSinkConfig(customConfig);
    assertEquals("http://custom-url:6334/", config.getGrpcUrl());
    assertEquals(new Password("custom-api-key"), config.getApiKey());
  }

  @Test
  void testMissingConfigValues() {
    Map<String, String> customConfig = new HashMap<>();
    customConfig.put(QdrantSinkConfig.API_KEY, "custom-api-key");

    QdrantSinkConfig config = new QdrantSinkConfig(customConfig);
    assertEquals("http://localhost:6334", config.getGrpcUrl());
    assertEquals(new Password("custom-api-key"), config.getApiKey());
  }

  @Test
  void testConfigDef() {
    ConfigDef configDef = QdrantSinkConfig.conf();
    assertNotNull(configDef);

    assertEquals("http://localhost:6334", configDef.defaultValues().get(QdrantSinkConfig.GRPC_URL));
    assertEquals("", ((Password) configDef.defaultValues().get(QdrantSinkConfig.API_KEY)).value());
  }

  @Test
  void testInvalidConfigValue() {
    Map<String, String> customConfig = new HashMap<>();
    customConfig.put(QdrantSinkConfig.GRPC_URL, "invalid-url");

    QdrantSinkConfig config = new QdrantSinkConfig(customConfig);
    assertNotEquals("http://localhost:6334/", config.getGrpcUrl());
    assertEquals("invalid-url", config.getGrpcUrl());
  }
}
