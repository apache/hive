package org.apache.hive.kafka;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

import org.apache.hadoop.service.AbstractService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.common.base.Throwables;
import com.google.common.io.Files;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

/**
 * This class has the hooks to start and stop single node kafka cluster.
 * The kafka broker is started on port 9092
 */
public class SingleNodeKafkaCluster extends AbstractService {
  private static final Logger log = LoggerFactory.getLogger(SingleNodeKafkaCluster.class);

  private final KafkaServerStartable serverStartable;
  private final String zkString;

  public SingleNodeKafkaCluster(String name, String logDir, Integer zkPort){
    super(name);
    Properties properties = new Properties();
    this.zkString = String.format("localhost:%d", zkPort);
    properties.setProperty("zookeeper.connect", zkString);
    properties.setProperty("broker.id", String.valueOf(1));
    properties.setProperty("host.name", "localhost");
    properties.setProperty("port", Integer.toString(9092));
    properties.setProperty("log.dir", logDir);
    properties.setProperty("log.flush.interval.messages", String.valueOf(1));
    properties.setProperty("offsets.topic.replication.factor", String.valueOf(1));
    properties.setProperty("offsets.topic.num.partitions", String.valueOf(1));
    properties.setProperty("transaction.state.log.replication.factor", String.valueOf(1));
    properties.setProperty("transaction.state.log.min.isr", String.valueOf(1));
    properties.setProperty("log.cleaner.dedupe.buffer.size", "1048577");

    this.serverStartable = new KafkaServerStartable(KafkaConfig.fromProps(properties));
  }


  @Override
  protected void serviceStart() throws Exception {
    serverStartable.startup();
    log.info("Kafka Server Started");

  }

  @Override
  protected void serviceStop() throws Exception {
    log.info("Stopping Kafka Server");
    serverStartable.shutdown();
    log.info("Kafka Server Stopped");
  }

  /**
   * Creates a topic and inserts data from the specified datafile.
   * Each line in the datafile is sent to kafka as a single message.
   * @param topicName
   * @param datafile
   */
  public void createTopicWithData(String topicName, File datafile){
    createTopic(topicName);
    // set up kafka producer
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("acks", "1");
    properties.put("retries", "3");

    try(KafkaProducer<String, String> producer = new KafkaProducer<>(
        properties,
        new StringSerializer(),
        new StringSerializer()
    )){
      List<String> events = Files.readLines(datafile, Charset.forName("UTF-8"));
      for(String event : events){
        producer.send(new ProducerRecord<String, String>(topicName, event));
      }
    } catch (IOException e) {
      Throwables.propagate(e);
    }

  }

  public void createTopic(String topic) {
    int sessionTimeoutMs = 1000;
    ZkClient zkClient = new ZkClient(
        this.zkString, sessionTimeoutMs, sessionTimeoutMs,
        ZKStringSerializer$.MODULE$
    );
    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkString, sessionTimeoutMs), false);
    int numPartitions = 1;
    int replicationFactor = 1;
    Properties topicConfig = new Properties();
    AdminUtils.createTopic(
        zkUtils,
        topic,
        numPartitions,
        replicationFactor,
        topicConfig,
        RackAwareMode.Disabled$.MODULE$
    );
  }

}
