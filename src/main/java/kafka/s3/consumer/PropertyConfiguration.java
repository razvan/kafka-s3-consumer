package kafka.s3.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertyConfiguration implements Configuration {

  private final Properties props;

  private static final String PROP_S3_ACCESS_KEY = "s3.accesskey";
  private static final String PROP_S3_SECRET_KEY = "s3.secretkey";
  private static final String PROP_S3_BUCKET = "s3.bucket";
  private static final String PROP_S3_PREFIX = "s3.prefix";
  private static final String PROP_KAFKA_HOST = "kafka.host";
  private static final String PROP_KAFKA_PORT = "kafka.port";
  private static final String PROP_S3_MAX_OBJECT_SIZE = "s3.maxobjectsize";
  private static final String PROP_KAFKA_MAX_MESSAGE_SIZE = "kafka.maxmessagesize";
  private static final String PROP_KAFKA_TOPICS = "kafka.topics";

  public PropertyConfiguration(Properties props) {
    this.props = props;
  }

  @Override
  public String getS3AccessKey() {
    String s3AccessKey = props.getProperty(PROP_S3_ACCESS_KEY);
    if (s3AccessKey == null || s3AccessKey.isEmpty()) {
      throw new RuntimeException("Invalid property " + PROP_S3_ACCESS_KEY);
    }
    return s3AccessKey;
  }

  @Override
  public String getS3SecretKey() {
    String s3SecretKey = props.getProperty(PROP_S3_SECRET_KEY);
    if (s3SecretKey == null || s3SecretKey.isEmpty()) {
      throw new RuntimeException("Invalid property " + PROP_S3_SECRET_KEY);
    }
    return s3SecretKey;
  }

  @Override
  public String getS3Bucket() {
    String s3Bucket = props.getProperty(PROP_S3_BUCKET);
    if (s3Bucket == null || s3Bucket.isEmpty()) {
      throw new RuntimeException("Invalid property " + PROP_S3_BUCKET);
    }
    return s3Bucket;
  }

  @Override
  public String getS3Prefix() {
    String s3Prefix = props.getProperty(PROP_S3_PREFIX);
    if (s3Prefix == null || s3Prefix.isEmpty()) {
      throw new RuntimeException("Invalid property " + PROP_S3_PREFIX);
    }
    return s3Prefix.replaceAll("/$", "");
  }

  @Override
  public String getKafkaHost() {
    String kafkaHost = props.getProperty(PROP_KAFKA_HOST);
    if (kafkaHost == null || kafkaHost.isEmpty()) {
      throw new RuntimeException("Invalid property " + PROP_KAFKA_HOST);
    }
    return kafkaHost;
  }

  @Override
  public int getKafkaPort() {
    String kafkaPort = props.getProperty(PROP_KAFKA_PORT);
    if (kafkaPort == null || kafkaPort.isEmpty()) {
      throw new RuntimeException("Invalid property " + PROP_KAFKA_PORT);
    }
    return Integer.valueOf(kafkaPort);
  }

  @Override
  public Map<String, Integer> getTopicsAndPartitions() {
    HashMap<String, Integer> result = new HashMap<String,Integer>();
    String kafkaTopics = props.getProperty(PROP_KAFKA_TOPICS);
    if (kafkaTopics == null || kafkaTopics.isEmpty()) {
      throw new RuntimeException("Invalid property " + PROP_KAFKA_TOPICS);
    }
    for (String topics: kafkaTopics.split(",")) {
      String[] topicPart = topics.split(":");
      if (result.containsKey(topicPart[0]))
        throw new RuntimeException("Duplicate topic " + topicPart[0]);
      result.put(topicPart[0], Integer.valueOf(topicPart[1]));
    }
    return result;
  }

  @Override
  public int getS3MaxObjectSize() {
    String maxBatchObjectSize = props.getProperty(PROP_S3_MAX_OBJECT_SIZE);
    if (maxBatchObjectSize == null || maxBatchObjectSize.isEmpty()) {
      return 256;
    }
    return Integer.valueOf(maxBatchObjectSize);

  }

  @Override
  public int getKafkaMaxMessageSize() {
    String maxMessageSize = props.getProperty(PROP_KAFKA_MAX_MESSAGE_SIZE);
    if (maxMessageSize == null || maxMessageSize.isEmpty()) {
      return 256;
    }
    return Integer.valueOf(maxMessageSize);

  }
}
