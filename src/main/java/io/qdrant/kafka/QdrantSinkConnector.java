package io.qdrant.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class QdrantSinkConnector extends SinkConnector {

  private Map<String, String> configProperties;

  @Override
  public void start(Map<String, String> props) {
    configProperties = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return QdrantSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    final List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(configProperties);
    }
    return taskConfigs;
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return QdrantSinkConfig.conf();
  }

  @Override
  public String version() {
    return "1.0.0";
  }
}
