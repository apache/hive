/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.kafka;

import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.zk.AdminZkClient;
import kafka.zk.EmbeddedZookeeper;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestSslUtils;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Test Helper Class to start and stop a kafka broker.
 */
class KafkaBrokerResource extends ExternalResource {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaBrokerResource.class);
  private static final String TOPIC = "TEST-CREATE_TOPIC";
  static final String BROKER_IP_PORT = "127.0.0.1:9092";
  static final String BROKER_SASL_PORT = "127.0.0.1:9093";
  static final String BROKER_SASL_SSL_PORT = "127.0.0.1:9094";
  private EmbeddedZookeeper zkServer;
  private KafkaServer kafkaServer;
  private AdminZkClient adminZkClient;
  private Path tmpLogDir;
  private String principal;
  private String keytab;
  private File truststoreFile;

  /**
   * Enables SASL for broker using the principal and keytab provided.
   * <p>The method must be called before starting the KafkaServer (see {@link #before()}).</p>
   * @return the same broker resource with SASL setup enabled.
   */
  KafkaBrokerResource enableSASL(String principal, String keytab) {
    this.principal = principal;
    this.keytab = keytab;
    return this;
  }

  /**
   * Override to set up your specific external resource.
   *
   * @throws Throwable if setup fails (which will disable {@code after}
   */
  @Override protected void before() throws Throwable {
    // Start the ZK and the Broker
    LOG.info("init embedded Zookeeper");
    tmpLogDir = Files.createTempDirectory("kafka-log-dir-").toAbsolutePath();
    zkServer = new EmbeddedZookeeper();
    String zkConnect = "127.0.0.1:" + zkServer.port();
    LOG.info("init kafka broker");
    Properties brokerProps = new Properties();
    brokerProps.setProperty("zookeeper.connect", zkConnect);
    brokerProps.setProperty("broker.id", "0");
    brokerProps.setProperty("log.dir", tmpLogDir.toString());
    Map<String, BrokerListener> listeners = new HashMap<>();
    listeners.put("L1", new BrokerListener(BROKER_IP_PORT, "PLAINTEXT"));
    if (principal != null) {
      listeners.put("L2", new BrokerListener(BROKER_SASL_PORT, "SASL_PLAINTEXT"));
      listeners.put("L3", new BrokerListener(BROKER_SASL_SSL_PORT, "SASL_SSL"));
    }
    String listenersURLs = listeners.entrySet().stream().map((e) -> e.getKey() + "://" + e.getValue().url)
        .collect(Collectors.joining(","));
    brokerProps.setProperty("listeners", listenersURLs);
    String listenersProtocols = listeners.entrySet().stream().map((e) -> e.getKey() + ":" + e.getValue().protocol)
        .collect(Collectors.joining(","));
    brokerProps.setProperty("listener.security.protocol.map", listenersProtocols);
    brokerProps.setProperty("inter.broker.listener.name", "L1");
    if (principal != null) {
      String jaasConfig = String.format("%s %s %s %s serviceName=\"%s\" keyTab=\"%s\" principal=\"%s\";",
          "com.sun.security.auth.module.Krb5LoginModule required", "debug=true", "useKeyTab=true", "storeKey=true",
          principal, keytab, principal + "/localhost");
      brokerProps.setProperty("listener.name.l2.gssapi.sasl.jaas.config", jaasConfig);
      brokerProps.setProperty("listener.name.l3.gssapi.sasl.jaas.config", jaasConfig);
      truststoreFile = File.createTempFile("kafka_truststore", "jks");
      brokerProps.putAll(new TestSslUtils.SslConfigsBuilder(Mode.SERVER).createNewTrustStore(truststoreFile).build());
      brokerProps.setProperty("delegation.token.master.key", "AnyValueShouldDoHereItDoesntMatter");
    }
    brokerProps.setProperty("offsets.topic.replication.factor", "1");
    brokerProps.setProperty("transaction.state.log.replication.factor", "1");
    brokerProps.setProperty("transaction.state.log.min.isr", "1");
    KafkaConfig config = new KafkaConfig(brokerProps);
    kafkaServer = TestUtils.createServer(config, Time.SYSTEM);
    kafkaServer.startup();
    kafkaServer.zkClient();
    adminZkClient = new AdminZkClient(kafkaServer.zkClient());
    LOG.info("Creating kafka TOPIC [{}]", TOPIC);
    adminZkClient.createTopic(TOPIC, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
  }

  /**
   * Override to tear down your specific external resource.
   */
  @Override protected void after() {
    super.after();
    if (kafkaServer != null) {
      kafkaServer.shutdown();
      kafkaServer.awaitShutdown();
    }
    if (zkServer != null) {
      zkServer.shutdown();
    }
    try {
     FileUtils.deleteDirectory(new File(tmpLogDir.toString()));
    } catch (IOException e) {
      LOG.warn("did not clean " + tmpLogDir.toString(), e);
    }
  }

  Path getTruststorePath() {
    if (truststoreFile == null) {
      throw new IllegalStateException("Truststore is available only when SASL is in use");
    }
    return truststoreFile.toPath();
  }

  String getTruststorePwd() {
    return TestSslUtils.TRUST_STORE_PASSWORD;
  }

  void deleteTopic(@SuppressWarnings("SameParameterValue") String topic) {
    adminZkClient.deleteTopic(topic);
  }

  private static class BrokerListener {
    String url;
    String protocol;

    public BrokerListener(final String url, final String protocol) {
      this.url = url;
      this.protocol = protocol;
    }
  }
}
