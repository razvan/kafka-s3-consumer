package kafka.s3.consumer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.MessageSet;
import kafka.message.MessageAndOffset;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class App
{

  static Configuration conf;
  static String bucket;
  static AmazonS3Client awsClient;

  /*
  mvn exec:java -Dexec.mainClass="kafka.s3.consumer.App" -Dexec.args="app.properties"
   */
    public static void main( String[] args ) throws IOException, java.lang.InterruptedException {
      conf = loadConfiguration(args);

      awsClient = new AmazonS3Client(new BasicAWSCredentials(conf.getS3AccessKey(), conf.getS3SecretKey()));
      bucket = conf.getS3Bucket();

      String topic = conf.getKafkaTopic();
      String path  = conf.getS3Prefix() + "/" + topic + "/";

      long offset = getMaxOffsetFromPath(path);
      long curOffset = offset;
      byte[] buffer = new byte[conf.getS3MaxObjectSize()];
      int bytesWritten = 0;

      Iterator<MessageAndOffset> messages = new MessageStream(topic,0,offset);
      while (messages.hasNext()) {
        MessageAndOffset messageAndOffset = messages.next();
        int messageSize = messageAndOffset.message().payload().remaining();
        System.err.println("Writing message with size: " + messageSize);

        if (bytesWritten + messageSize + 1 > conf.getS3MaxObjectSize()) {
          System.err.println("Flushing buffer to disk. size: " + bytesWritten);
          awsClient.putObject(bucket,path + offset + "_" + curOffset,new ByteArrayInputStream(buffer,0,bytesWritten), new ObjectMetadata());
          offset = curOffset;
          bytesWritten = 0;
        }

        messageAndOffset.message().payload().get(buffer,bytesWritten,messageSize);
        bytesWritten += messageSize;
        buffer[bytesWritten] = '\n';
        bytesWritten += 1;

        curOffset = messageAndOffset.offset();
      }
    }

  private static Configuration loadConfiguration(String[] args) {
    Properties props = new Properties();

    try {
      if (args == null || args.length != 1) {
        props.load(App.class.getResourceAsStream("/app.properties"));
      } else {
        props.load(new FileInputStream(new File(args[0])));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new PropertyConfiguration(props);
  }

  public static long getMaxOffsetFromPath(String path) {
    List<S3ObjectSummary> objectSummaries = awsClient.listObjects(new ListObjectsRequest().withBucketName(bucket).withDelimiter("/").withPrefix(path)).getObjectSummaries();

    long maxOffset = 0;

    for (S3ObjectSummary objectSummary : objectSummaries) {
      long[] offsets = getOffsetsFromFileName(objectSummary.getKey().substring(path.length()));
      if (offsets[1] > maxOffset)
        maxOffset = offsets[1];
    }
    return maxOffset;
  }

  public static long[] getOffsetsFromFileName(String fileName) {
    long[] result = new long[2];
    int i = 0;
    for (String offset : fileName.split("_")) {
      result[i++] = Long.valueOf(offset);
    }
    return result;
  }

  private static class MessageStream implements Iterator<MessageAndOffset> {

    private SimpleConsumer consumer;
    private Iterator<MessageAndOffset> messageSetIterator;

    private String topic;
    private int partition;
    private long offset;

    public MessageStream(String topic, int partition, long offset) {
      this.topic = topic;
      this.partition = partition;
      this.offset = offset;
      consumer = new SimpleConsumer(conf.getKafkaHost(), conf.getKafkaPort(), 5000, 4*1024);
    }

    public boolean hasNext() {
      return true;
    }

    public MessageAndOffset next() {
      if (messageSetIterator == null || !messageSetIterator.hasNext()) {
        System.err.println("Fetching message from offset: " + offset);
        FetchRequest fetchRequest = new FetchRequest(topic, partition, offset, conf.getKafkaMaxMessageSize());
        MessageSet messageSet = consumer.fetch(fetchRequest);
        while (!messageSet.iterator().hasNext()) {
          System.err.println("No messages returned. Sleeping for 10s.");
          try {
            Thread.sleep(10000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          messageSet = consumer.fetch(fetchRequest);
         }
        messageSetIterator = messageSet.iterator();
      }
      MessageAndOffset message = messageSetIterator.next();
      offset = message.offset();
      return message;
    }

    public void remove() {
      throw new UnsupportedOperationException("Method remove is not supported by this iterator.");
    }
  }
}
