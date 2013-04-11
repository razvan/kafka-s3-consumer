package kafka.s3.consumer;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import kafka.api.FetchRequest;
import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.MessageSet;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.charset.Charset;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

public class App {
    static Logger logger = Logger.getLogger(App.class);

    static Configuration conf;
    private static ExecutorService pool;
    private static boolean cleanStart = false;

    /*
   mvn exec:java -Dexec.mainClass="kafka.s3.consumer.App" -Dexec.args="app.properties"
    */
    public static void main(String[] args) throws IOException, java.lang.InterruptedException {

        conf = loadConfiguration(args);

        Map<String, Integer> topics = conf.getTopicsAndPartitions();

        List<Worker> workers = new LinkedList<Worker>();

        for (String topic : topics.keySet()) {
            for (int partition = 0; partition < topics.get(topic); partition++) {
                workers.add(new Worker(topic, partition));
            }
        }

        pool = Executors.newFixedThreadPool(workers.size());

        Runtime.getRuntime().addShutdownHook(new GracefulWorkerShutdown(pool));

        for (Worker worker : workers) {
            pool.submit(worker);
        }
    }

    private static class GracefulWorkerShutdown extends Thread {
        private final ExecutorService pool;
        private GracefulWorkerShutdown(ExecutorService pool) {
            this.pool = pool;
        }

        @Override
        public void run() {
            System.out.println("Caught shutdown signal, closing pool.");
            this.pool.shutdown();
        }
    }

    private static class Worker implements Runnable {

        private final String topic;
        private final int partition;
        private S3Sink sink;

        private Worker(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        @Override
        public void run() {
            try {
                this.sink = new S3Sink(topic, partition, conf.isCompressed());
                long offset = this.sink.getMaxCommittedOffset();
                Iterator<MessageAndOffset> messages = new MessageStream(topic, partition, offset);
                while (messages.hasNext()) {
                    MessageAndOffset messageAndOffset = messages.next();
                    this.sink.append(messageAndOffset);
                }
            } catch (IOException e) {
                closeAndNullifySink("exception encountered");
                throw new RuntimeException(e);
            }
            finally {
                closeAndNullifySink("thread run() completed");
            }
        }
        
        private void closeAndNullifySink(String logline) {
            if (this.sink != null) { 
                try { 
                    logger.debug("Writing out current buffer to s3 before termination: " + logline);
                    this.sink.exportCurrentChunkToS3();
                    this.sink = null;
                } 
                catch(IOException ignored) {}  
            }
        }
        
        @Override
        protected void finalize() throws Throwable {
            try{
                if (this.sink != null) { this.sink.exportCurrentChunkToS3(); }    
            } catch(Throwable t) {
                throw t;
            } finally {
                super.finalize();
            }
        }
    }

    private static Configuration loadConfiguration(String[] args) {
        Properties props = new Properties();

        try {
            if(args.length > 0) {
                props.load(new ByteArrayInputStream(resolveEnvVars(args[0]).getBytes()));
                if (args.length >= 2) {
                    if (args[1].equals("clean")) {
                        cleanStart = true;
                    }
                }
            } else {
               System.err.println("Usage: jar -jar <jarfile> property file <optional: clean>");
               System.exit(1);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new PropertyConfiguration(props);
    }

    /* Environment variable substitution for properties files from http://stackoverflow.com/a/9725352/370800
     * Returns input string with environment variable references expanded, e.g. $SOME_VAR or ${SOME_VAR}
     */
    private static String resolveEnvVars(String filename) throws IOException {
        String input;
        FileInputStream stream = new FileInputStream(new File(filename));
        try {
            FileChannel fc = stream.getChannel();
            MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            /* Instead of using default, pass in a decoder. */
            input = Charset.defaultCharset().decode(bb).toString();
        }
        finally {
            stream.close();
        }
          
        if (null == input) {
            return null;
        }
        // match ${ENV_VAR_NAME} or $ENV_VAR_NAME
        Pattern p = Pattern.compile("\\$\\{(\\w+)\\}|\\$(\\w+)");
        Matcher m = p.matcher(input); // get a matcher object
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String envVarName = null == m.group(1) ? m.group(2) : m.group(1);
            String envVarValue = System.getenv(envVarName);
            m.appendReplacement(sb, null == envVarValue ? "" : envVarValue);
        }
        m.appendTail(sb);
        return sb.toString();
    }


    private static class S3Sink {

        private String topic;
        private int partition;
        private boolean compression;

        private String bucket;
        private AmazonS3Client awsClient;

        long startOffset;
        long endOffset;
        int bytesWritten;

        File tmpFile;
        OutputStream tmpOutputStream;
        WritableByteChannel tmpChannel;

        public S3Sink(String topic, int partition, boolean compression) throws FileNotFoundException, IOException {
            this.topic = topic;
            this.partition = partition;
            this.compression = compression;

            bucket = conf.getS3Bucket();
            awsClient = new AmazonS3Client(new BasicAWSCredentials(conf.getS3AccessKey(), conf.getS3SecretKey()));

            startOffset = endOffset = fetchLastCommittedOffset();
            bytesWritten = 0;

            tmpFile = File.createTempFile("s3sink", null);
            logger.debug("Created tmpFile: " + tmpFile);

            logger.debug("Compression: " + compression);
            if (compression) {
                tmpOutputStream = new GZIPOutputStream(new FileOutputStream(tmpFile));
            } else {
                tmpOutputStream = new FileOutputStream(tmpFile);
            }
            tmpChannel = Channels.newChannel(tmpOutputStream);
        }

        public void exportCurrentChunkToS3() throws IOException {
            logger.debug("Uploading chunk to S3. Size is: " + bytesWritten);
            String key = getKeyPrefix() + startOffset + "_" + endOffset;
            awsClient.putObject(bucket, key, tmpFile);
            tmpChannel.close();
            tmpOutputStream.close();
            tmpFile.delete();
            tmpFile = File.createTempFile("s3sink", null);
            logger.debug("Created tmpFile: " + tmpFile);
            if (compression) {
                tmpOutputStream = new GZIPOutputStream(new FileOutputStream(tmpFile));
            } else {
                tmpOutputStream = new FileOutputStream(tmpFile);
            }
            tmpChannel = Channels.newChannel(tmpOutputStream);
            startOffset = endOffset;
            bytesWritten = 0;
        }

        public void append(MessageAndOffset messageAndOffset) throws IOException {

            int messageSize = messageAndOffset.message().payload().remaining();
            logger.debug("Appending message with size: " + messageSize);

            if (bytesWritten + messageSize + 1 > conf.getS3MaxObjectSize()) {
                exportCurrentChunkToS3();
            }

            tmpChannel.write(messageAndOffset.message().payload());
            tmpOutputStream.write('\n');
            bytesWritten += messageSize + 1;

            endOffset = messageAndOffset.offset();
        }

        public long getMaxCommittedOffset() {
            return startOffset;
        }

        public long fetchLastCommittedOffset() {
            logger.debug("Getting max offset for " + topic + ":" + partition);
            String prefix = getKeyPrefix();
            logger.debug("Listing keys for bucket/prefix " + bucket + "/" + prefix);
            List<S3ObjectSummary> objectSummaries = awsClient.listObjects(new ListObjectsRequest().withBucketName(bucket).withDelimiter("/").withPrefix(prefix)).getObjectSummaries();
            logger.debug("Received result " + objectSummaries);

            long maxOffset = 0;

            for (S3ObjectSummary objectSummary : objectSummaries) {
                logger.debug(objectSummary.getKey());
                String[] offsets = objectSummary.getKey().substring(prefix.length()).split("_");
                long endOffset = Long.valueOf(offsets[1]);
                if (endOffset > maxOffset)
                    maxOffset = endOffset;
            }
            /**
             * Fix to start in the beginning after rotate, to avoid errors
             */


            return maxOffset;
        }

        private String getKeyPrefix() {
            return conf.getS3Prefix() + "/" + topic + "/" + conf.getKafkaBrokerId() + "_" + partition + "_";
        }
    }

    private static class MessageStream implements Iterator<MessageAndOffset> {

        private SimpleConsumer consumer;
        private Iterator<MessageAndOffset> messageSetIterator;

        private String topic;
        private int partition;
        private long offset;

        public MessageStream(String topic, int partition, long offset) {
            logger.debug("Message stream created: " + topic + ":" + partition + "/" + offset);
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            consumer = new SimpleConsumer(conf.getKafkaHost(), conf.getKafkaPort(), 5000, 4 * 1024);
            logger.debug("Created kafka consumer: " + consumer);
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public MessageAndOffset next() {
            try {
                if (offset == 0 || cleanStart) {
                    offset = consumer.getOffsetsBefore(topic, partition, OffsetRequest.EarliestTime(), 1)[0];
                    logger.info("Offset re-configured to :" + offset);
                    cleanStart = false;
                }

                if (messageSetIterator == null || !messageSetIterator.hasNext()) {
                    logger.debug("Fetching message from offset: " + offset);
                    FetchRequest fetchRequest = new FetchRequest(topic, partition, offset, conf.getKafkaMaxMessageSize());
                    MessageSet messageSet = consumer.fetch(fetchRequest);
                    while (!messageSet.iterator().hasNext()) {
                        logger.debug("No messages returned. Sleeping for 10s.");
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
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Method remove is not supported by this iterator.");
        }
    }
}

