package io.qdrant.kafka;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

public class QdrantSinkConfig extends AbstractConfig {
  protected static final String GRPC_URL = "qdrant.grpc.url";
  protected static final String API_KEY = "qdrant.api.key";

  public QdrantSinkConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public QdrantSinkConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
        .define(
            GRPC_URL,
            ConfigDef.Type.STRING,
            "http://localhost:6334/",
            ConfigDef.Importance.HIGH,
            "Qdrant gRPC URL")
        .define(
            API_KEY,
            ConfigDef.Type.PASSWORD,
            "",
            ConfigDef.Importance.HIGH,
            "API key to authenticate with Qdrant server");
  }

  public String getGrpcUrl() {
    return getString(GRPC_URL);
  }

  public Password getApiKey() {
    return getPassword(API_KEY);
  }
}
