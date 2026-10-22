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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.kafka;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Test class for properties setting.
 */
public class KafkaStorageHandlerTest {

  private static final String TEST_TOPIC = "test-topic";
  private static final String LOCALHOST_9291 = "localhost:9291";

  @Test public void configureJobPropertiesWithDefaultValues() throws MetaException {
    KafkaStorageHandler kafkaStorageHandler = new KafkaStorageHandler();
    TableDesc tableDesc = Mockito.mock(TableDesc.class);
    Properties properties = new Properties();
    Table preCreateTable = new Table();
    preCreateTable.putToParameters(KafkaTableProperties.HIVE_KAFKA_TOPIC.getName(), TEST_TOPIC);
    preCreateTable.putToParameters(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName(), LOCALHOST_9291);
    preCreateTable.setTableType(TableType.EXTERNAL_TABLE.toString());
    kafkaStorageHandler.preCreateTable(preCreateTable);
    preCreateTable.getParameters().forEach(properties::setProperty);
    Mockito.when(tableDesc.getProperties()).thenReturn(properties);
    Map<String, String> jobProperties = new HashMap<>();
    kafkaStorageHandler.configureInputJobProperties(tableDesc, jobProperties);
    kafkaStorageHandler.configureOutputJobProperties(tableDesc, jobProperties);
    Assert.assertEquals(jobProperties.get(KafkaTableProperties.HIVE_KAFKA_TOPIC.getName()), TEST_TOPIC);
    Assert.assertEquals(jobProperties.get(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName()), LOCALHOST_9291);
    Arrays.stream(KafkaTableProperties.values())
        .filter(key -> !key.isMandatory())
        .forEach((key) -> Assert.assertEquals("Wrong match for key " + key.getName(),
            key.getDefaultValue(),
            jobProperties.get(key.getName())));
  }

  @Test public void configureInputJobProperties() throws MetaException {
    KafkaStorageHandler kafkaStorageHandler = new KafkaStorageHandler();
    TableDesc tableDesc = Mockito.mock(TableDesc.class);
    Properties properties = new Properties();
    // set the mandatory properties
    Table preCreateTable = new Table();
    preCreateTable.setTableType(TableType.EXTERNAL_TABLE.toString());
    preCreateTable.putToParameters(KafkaTableProperties.HIVE_KAFKA_TOPIC.getName(), TEST_TOPIC);
    preCreateTable.putToParameters(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName(), LOCALHOST_9291);
    kafkaStorageHandler.preCreateTable(preCreateTable);
    preCreateTable.getParameters().forEach(properties::setProperty);
    properties.setProperty(KafkaTableProperties.WRITE_SEMANTIC_PROPERTY.getName(),
        KafkaOutputFormat.WriteSemantic.EXACTLY_ONCE.name());
    properties.setProperty(KafkaTableProperties.SERDE_CLASS_NAME.getName(), AvroSerDe.class.getName());
    properties.setProperty(KafkaTableProperties.KAFKA_POLL_TIMEOUT.getName(), "7000");
    Mockito.when(tableDesc.getProperties()).thenReturn(properties);
    Map<String, String> jobProperties = new HashMap<>();
    kafkaStorageHandler.configureInputJobProperties(tableDesc, jobProperties);

    Assert.assertEquals(jobProperties.get(KafkaTableProperties.HIVE_KAFKA_TOPIC.getName()), TEST_TOPIC);
    Assert.assertEquals(jobProperties.get(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName()), LOCALHOST_9291);
    Assert.assertEquals(AvroSerDe.class.getName(), jobProperties.get(KafkaTableProperties.SERDE_CLASS_NAME.getName()));
    Assert.assertEquals("7000", jobProperties.get(KafkaTableProperties.KAFKA_POLL_TIMEOUT.getName()));
    JobConf jobConf = new JobConf();
    jobProperties.forEach(jobConf::set);
    jobConf.set("mapred.task.id", "task_id_test_0001");
    Properties kafkaProperties = KafkaUtils.consumerProperties(jobConf);
    Assert.assertEquals(LOCALHOST_9291, kafkaProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    Assert.assertEquals("read_committed", kafkaProperties.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
    Assert.assertEquals("false", kafkaProperties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    Assert.assertEquals("none", kafkaProperties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    Assert.assertEquals(Utilities.getTaskId(jobConf), kafkaProperties.get(CommonClientConfigs.CLIENT_ID_CONFIG));

  }

  @Test public void configureOutJobProperties() throws MetaException {
    KafkaStorageHandler kafkaStorageHandler = new KafkaStorageHandler();
    TableDesc tableDesc = Mockito.mock(TableDesc.class);
    Properties properties = new Properties();
    // set the mandatory properties
    Table preCreateTable = new Table();
    preCreateTable.putToParameters(KafkaTableProperties.HIVE_KAFKA_TOPIC.getName(), TEST_TOPIC);
    preCreateTable.putToParameters(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName(), LOCALHOST_9291);
    preCreateTable.setTableType(TableType.EXTERNAL_TABLE.toString());
    kafkaStorageHandler.preCreateTable(preCreateTable);
    preCreateTable.getParameters().forEach(properties::setProperty);

    properties.setProperty(KafkaTableProperties.WRITE_SEMANTIC_PROPERTY.getName(),
        KafkaOutputFormat.WriteSemantic.EXACTLY_ONCE.name());
    properties.setProperty(KafkaTableProperties.HIVE_KAFKA_OPTIMISTIC_COMMIT.getName(), "false");
    Mockito.when(tableDesc.getProperties()).thenReturn(properties);
    Map<String, String> jobProperties = new HashMap<>();
    kafkaStorageHandler.configureOutputJobProperties(tableDesc, jobProperties);
    Assert.assertEquals("false", jobProperties.get(KafkaTableProperties.HIVE_KAFKA_OPTIMISTIC_COMMIT.getName()));
    Assert.assertEquals(jobProperties.get(KafkaTableProperties.HIVE_KAFKA_TOPIC.getName()), TEST_TOPIC);
    Assert.assertEquals(jobProperties.get(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName()), LOCALHOST_9291);
    Assert.assertEquals(KafkaOutputFormat.WriteSemantic.EXACTLY_ONCE.name(),
        jobProperties.get(KafkaTableProperties.WRITE_SEMANTIC_PROPERTY.getName()));
    Assert.assertEquals(jobProperties.get(KafkaUtils.CONSUMER_CONFIGURATION_PREFIX
        + "."
        + ConsumerConfig.ISOLATION_LEVEL_CONFIG), "read_committed");
    Assert.assertEquals(KafkaOutputFormat.WriteSemantic.EXACTLY_ONCE.name(),
        jobProperties.get(KafkaTableProperties.WRITE_SEMANTIC_PROPERTY.getName()));

    JobConf jobConf = new JobConf();
    jobProperties.forEach(jobConf::set);
    jobConf.set("mapred.task.id", "task_id_test");
    Properties producerProperties = KafkaUtils.producerProperties(jobConf);
    Assert.assertEquals(LOCALHOST_9291, producerProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    Assert.assertEquals("task_id_test", producerProperties.get(CommonClientConfigs.CLIENT_ID_CONFIG));
    Assert.assertEquals("all", producerProperties.get(ProducerConfig.ACKS_CONFIG));
    Assert.assertEquals(String.valueOf(Integer.MAX_VALUE), producerProperties.get(ProducerConfig.RETRIES_CONFIG));
    Assert.assertEquals("true", producerProperties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
  }

  private static Table kafkaTable(String key, String value) {
    Table table = new Table();
    table.setTableType(TableType.EXTERNAL_TABLE.toString());
    table.putToParameters(KafkaTableProperties.HIVE_KAFKA_TOPIC.getName(), TEST_TOPIC);
    table.putToParameters(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName(), LOCALHOST_9291);
    table.putToParameters(key, value);
    return table;
  }

  @Test(expected = IllegalArgumentException.class) public void preCreateTableRefusesDisallowedLoginModule()
      throws MetaException {
    new KafkaStorageHandler().preCreateTable(kafkaTable(
        KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + "." + SaslConfigs.SASL_JAAS_CONFIG,
        "com.sun.security.auth.module.JndiLoginModule required user.provider.url=\"ldap://ldap.example.com:389/ou=users\";"));
  }

  @Test(expected = IllegalArgumentException.class) public void preCreateTableRefusesCallbackHandlerClass()
      throws MetaException {
    new KafkaStorageHandler().preCreateTable(kafkaTable(
        KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + "." + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        "com.example.CustomCallbackHandler"));
  }

  @Test(expected = IllegalArgumentException.class) public void preCreateTableRefusesUnprefixedEngineFactory()
      throws MetaException {
    new KafkaStorageHandler().preCreateTable(
        kafkaTable(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, "com.example.CustomEngineFactory"));
  }

  @Test(expected = IllegalArgumentException.class) public void preAlterTableRefusesCallbackHandlerClass()
      throws MetaException {
    new KafkaStorageHandler().preAlterTable(kafkaTable(
        KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + "." + SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
        "com.example.CustomCallbackHandler"), null);
  }

  @Test(expected = IllegalArgumentException.class) public void preCreateTableRefusesProducerSideEngineFactory()
      throws MetaException {
    new KafkaStorageHandler().preCreateTable(kafkaTable(
        KafkaUtils.PRODUCER_CONFIGURATION_PREFIX + "." + SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG,
        "com.example.CustomEngineFactory"));
  }

  @Test(expected = IllegalArgumentException.class) public void preCreateTableRefusesConfigProviderClass()
      throws MetaException {
    new KafkaStorageHandler().preCreateTable(kafkaTable(
        KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + "." + KafkaUtils.CONFIG_PROVIDERS_PREFIX + "custom.class",
        "com.example.CustomConfigProvider"));
  }

  @Test public void preCreateTableAcceptsOrdinaryConsumerProperty() throws MetaException {
    Table table = kafkaTable(KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + ".max.poll.records", "5000");
    new KafkaStorageHandler().preCreateTable(table);
    Assert.assertEquals("5000",
        table.getParameters().get(KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + ".max.poll.records"));
  }

  @Test(expected = IllegalArgumentException.class) public void storageHandlerInfoRefusesCallbackHandlerClass()
      throws MetaException {
    KafkaStorageHandler kafkaStorageHandler = new KafkaStorageHandler();
    kafkaStorageHandler.setConf(new JobConf());
    kafkaStorageHandler.getStorageHandlerInfo(kafkaTable(
        KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + "." + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        "com.example.CustomCallbackHandler"));
  }

  @Test(expected = IllegalArgumentException.class) public void storageHandlerInfoRefusesConfigProviderClass()
      throws MetaException {
    KafkaStorageHandler kafkaStorageHandler = new KafkaStorageHandler();
    kafkaStorageHandler.setConf(new JobConf());
    kafkaStorageHandler.getStorageHandlerInfo(kafkaTable(
        KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + "." + KafkaUtils.CONFIG_PROVIDERS_PREFIX + "custom.class",
        "com.example.CustomConfigProvider"));
  }

  @Test(expected = IllegalArgumentException.class) public void storageHandlerInfoRefusesDisallowedLoginModule()
      throws MetaException {
    KafkaStorageHandler kafkaStorageHandler = new KafkaStorageHandler();
    kafkaStorageHandler.setConf(new JobConf());
    kafkaStorageHandler.getStorageHandlerInfo(kafkaTable(
        KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + "." + SaslConfigs.SASL_JAAS_CONFIG,
        "com.sun.security.auth.module.JndiLoginModule required user.provider.url=\"ldap://ldap.example.com:389/ou=users\";"));
  }
}
