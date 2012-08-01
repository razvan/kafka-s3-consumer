package kafka.s3.consumer;

import java.util.Map;

public interface Configuration {

  public String getS3AccessKey();
  public String getS3SecretKey();
  public String getS3Bucket();
  public String getS3Prefix();

  public String getKafkaHost();
  public int getKafkaPort();
  public int getKafkaBrokerId();
  public Map<String, Integer> getTopicsAndPartitions();

  public int getS3MaxObjectSize();
  public int getKafkaMaxMessageSize();
}
