package kafka.s3.consumer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.*;
import java.io.File;
import java.lang.Long;
import java.lang.Package;
import java.lang.String;
import java.lang.System;
import java.nio.ByteBuffer;
import java.util.Iterator;

import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.MessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

public class App
{

  static SimpleConsumer consumer;

  /*
  mvn exec:java -Dexec.mainClass="kafka.s3.consumer.App" -Dexec.args="feierabend /tmp/xx"
   */
    public static void main( String[] args ) throws IOException {
      String topic = args[0];
      String path  = args[1];

      consumer = new SimpleConsumer("127.0.0.1", 9092, 5000, 4*1024);

      while (true) {
        long offset = getMaxOffsetFromPath(path);
        System.err.println("Start consuming from offset " + offset);
        Iterator<MessageAndOffset> iterator = getMessages(topic, offset);

        File tmpFile = File.createTempFile("xxx", "");
        FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);

        long curOffset = offset;
        long bytesWritten = 0;

        while (iterator.hasNext()) {
          MessageAndOffset messageAndOffset = iterator.next();

          System.err.println(getMessage(messageAndOffset.message()));

          ByteBuffer buf = messageAndOffset.message().payload();

          curOffset = messageAndOffset.offset();
          bytesWritten += buf.remaining() + 1;
          System.err.println("Bytes written " + bytesWritten);
          fileOutputStream.write(buf.array());
          fileOutputStream.write('\n');
        }

        fileOutputStream.close();

        if (bytesWritten > 64) {
          System.err.println("Renaming file.");
          tmpFile.renameTo(new File(new File(path), offset + "_" + curOffset));
        }
        else {
          tmpFile.delete();
          try {
            System.err.println("Sleeping...");
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }

      }

    }

  public static Iterator<MessageAndOffset> getMessages(String topic, long offset) {
    FetchRequest fetchRequest = new FetchRequest(topic, 0, offset, 1024);
    MessageSet messageSet = consumer.fetch(fetchRequest);
    return messageSet.iterator();
  }

  public static long getMaxOffsetFromPath(String path) {
    long maxOffset = 0;

    File filePath = new File(path);

    for(File file : filePath.listFiles()) {
      long[] offsets = getOffsetsFromFileName(file.getName());
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
  public static String getMessage(Message message)
  {
    ByteBuffer buffer = message.payload();
    byte [] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }
}
