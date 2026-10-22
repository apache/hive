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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test for Utility class.
 */
public class KafkaUtilsTest {
  public KafkaUtilsTest() {
  }

  @Test public void testConsumerProperties() {
    Configuration configuration = new Configuration();
    configuration.set("kafka.bootstrap.servers", "localhost:9090");
    configuration.set("kafka.consumer.fetch.max.wait.ms", "40");
    configuration.set("kafka.consumer.my.new.wait.ms", "400");
    Properties properties = KafkaUtils.consumerProperties(configuration);
    Assert.assertEquals("localhost:9090", properties.getProperty("bootstrap.servers"));
    Assert.assertEquals("40", properties.getProperty("fetch.max.wait.ms"));
    Assert.assertEquals("400", properties.getProperty("my.new.wait.ms"));
  }

  @Test(expected = IllegalArgumentException.class) public void canNotSetForbiddenProp() {
    Configuration configuration = new Configuration();
    configuration.set("kafka.bootstrap.servers", "localhost:9090");
    configuration.set("kafka.consumer." + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    KafkaUtils.consumerProperties(configuration);
  }

  @Test(expected = IllegalArgumentException.class) public void canNotSetForbiddenProp2() {
    Configuration configuration = new Configuration();
    configuration.set("kafka.bootstrap.servers", "localhost:9090");
    configuration.set("kafka.consumer." + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "value");
    KafkaUtils.consumerProperties(configuration);
  }

  @After public void clearClassNamePropertyOptIn() {
    System.clearProperty(KafkaUtils.ALLOW_CLASS_NAME_PROPERTIES);
  }

  private static Configuration consumerConfig(String key, String value) {
    Configuration configuration = new Configuration();
    configuration.set("kafka.bootstrap.servers", "localhost:9090");
    configuration.set(KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + "." + key, value);
    return configuration;
  }

  private static final String CUSTOM_CLASS = "com.example.CustomClass";
  private static final String CUSTOM_CONFIG_PROVIDER = "com.example.CustomConfigProvider";

  @Test public void canNotSetClassNamingPropertiesOnConsumer() {
    for (String key : KafkaUtils.CLASS_NAME_PROPERTIES) {
      try {
        KafkaUtils.consumerProperties(consumerConfig(key, CUSTOM_CLASS));
        Assert.fail("Expected " + key + " to be refused");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains(key));
      }
    }
  }

  @Test public void canNotSetClassNamingPropertiesOnProducer() {
    for (String key : KafkaUtils.CLASS_NAME_PROPERTIES) {
      Configuration configuration = new Configuration();
      configuration.set("kafka.bootstrap.servers", "localhost:9090");
      configuration.set(KafkaTableProperties.WRITE_SEMANTIC_PROPERTY.getName(),
          KafkaOutputFormat.WriteSemantic.AT_LEAST_ONCE.name());
      configuration.set(KafkaUtils.PRODUCER_CONFIGURATION_PREFIX + "." + key, CUSTOM_CLASS);
      try {
        KafkaUtils.producerProperties(configuration);
        Assert.fail("Expected " + key + " to be refused");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains(key));
      }
    }
  }

  @Test public void operatorCanOptInToClassNamingPropertiesGlobally() {
    System.setProperty(KafkaUtils.ALLOW_CLASS_NAME_PROPERTIES, "true");
    Properties properties =
        KafkaUtils.consumerProperties(consumerConfig(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "com.acme.OAuth"));
    Assert.assertEquals("com.acme.OAuth", properties.getProperty(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS));
  }

  @Test public void optInDoesNotReachTableProperties() {
    System.setProperty(KafkaUtils.ALLOW_CLASS_NAME_PROPERTIES, "true");
    assertTablePropertyRefused(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, "com.example.CustomEngineFactory");
  }

  private static void assertTablePropertyRefused(String key, String value) {
    assertTablePropertyRefused(key, value, key);
  }

  private static void assertTablePropertyRefused(String key, String value, String expectedInMessage) {
    try {
      KafkaUtils.checkTableProperties(Collections.singletonMap(key, value));
      Assert.fail("Expected table property " + key + " to be refused");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains(expectedInMessage));
    }
  }

  /**
   * The un-prefixed spelling reaches the Kafka client through copySSLProperties, so it has to be
   * refused at DDL time, where we still know the value came from a table property.
   */
  @Test public void unprefixedClassNamingTablePropertyIsRefused() {
    for (String key : KafkaUtils.CLASS_NAME_PROPERTIES) {
      assertTablePropertyRefused(key, CUSTOM_CLASS);
    }
  }

  @Test public void prefixedClassNamingTablePropertiesAreRefused() {
    for (String prefix : new String[] {KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + ".",
        KafkaUtils.PRODUCER_CONFIGURATION_PREFIX + "."}) {
      for (String key : KafkaUtils.CLASS_NAME_PROPERTIES) {
        assertTablePropertyRefused(prefix + key, CUSTOM_CLASS);
      }
    }
  }

  /**
   * The config provider keys carry a user chosen name in the middle, so they are matched by prefix.
   */
  @Test public void configProviderClassTablePropertyIsRefused() {
    assertTablePropertyRefused(KafkaUtils.CONFIG_PROVIDERS_PREFIX + "custom.class", CUSTOM_CONFIG_PROVIDER);
  }

  @Test public void prefixedConfigProviderClassTablePropertyIsRefused() {
    assertTablePropertyRefused(KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + "."
            + KafkaUtils.CONFIG_PROVIDERS_PREFIX + "custom.class", CUSTOM_CONFIG_PROVIDER,
        KafkaUtils.CONFIG_PROVIDERS_PREFIX + "custom.class");
  }

  /**
   * The parameters handed to such a class share the prefix, so they are refused with it. On their
   * own they are inert, Kafka only reads them once a provider class is configured.
   */
  @Test public void configProviderParameterTablePropertyIsRefused() {
    assertTablePropertyRefused(KafkaUtils.CONFIG_PROVIDERS_PREFIX + "custom.param.allowed.paths", "/etc");
  }

  @Test public void configProviderClassIsRefusedOnConfigurationPath() {
    try {
      KafkaUtils.consumerProperties(consumerConfig(KafkaUtils.CONFIG_PROVIDERS_PREFIX + "custom.class",
          CUSTOM_CONFIG_PROVIDER));
      Assert.fail("Expected the config provider class to be refused");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected message: " + e.getMessage(),
          e.getMessage().contains(KafkaUtils.CONFIG_PROVIDERS_PREFIX + "custom.class"));
    }
  }

  @Test public void operatorCanOptInToConfigProvidersGlobally() {
    System.setProperty(KafkaUtils.ALLOW_CLASS_NAME_PROPERTIES, "true");
    Properties properties = KafkaUtils.consumerProperties(
        consumerConfig(KafkaUtils.CONFIG_PROVIDERS_PREFIX + "custom.class", CUSTOM_CONFIG_PROVIDER));
    Assert.assertEquals(CUSTOM_CONFIG_PROVIDER,
        properties.getProperty(KafkaUtils.CONFIG_PROVIDERS_PREFIX + "custom.class"));
  }

  @Test public void optInDoesNotReachConfigProvidersOnTableClientPath() {
    System.setProperty(KafkaUtils.ALLOW_CLASS_NAME_PROPERTIES, "true");
    try {
      KafkaUtils.checkTableClientProperty(KafkaUtils.CONFIG_PROVIDERS_PREFIX + "custom.class",
          CUSTOM_CONFIG_PROVIDER);
      Assert.fail("Expected the config provider class to be refused");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected message: " + e.getMessage(),
          e.getMessage().contains(KafkaUtils.CONFIG_PROVIDERS_PREFIX + "custom.class"));
    }
  }

  /**
   * Kafka marks the properties it resolves into a class with {@link ConfigDef.Type#CLASS}. Pinning
   * that set against the two lists in KafkaUtils means a Kafka upgrade that adds one of them fails
   * here, rather than quietly arriving as a table property nothing looks at. It cannot cover the
   * handful Kafka types as a list or a plain string, those stay enumerated by hand.
   */
  @Test public void everyClassTypedKafkaPropertyIsListed() {
    Set<String> classTyped = Stream.of(ConsumerConfig.configDef(), ProducerConfig.configDef())
        .flatMap(configDef -> configDef.configKeys().entrySet().stream())
        .filter(entry -> entry.getValue().type == ConfigDef.Type.CLASS)
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
    // Guard against the lookup itself silently returning nothing, which would leave the assertion
    // below passing for the wrong reason.
    Assert.assertTrue("Expected to find the class typed Kafka properties, found " + classTyped,
        classTyped.contains(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG)
            && classTyped.contains(ProducerConfig.PARTITIONER_CLASS_CONFIG));

    Set<String> listed = new HashSet<>(KafkaUtils.CLASS_NAME_PROPERTIES);
    listed.addAll(KafkaUtils.FORBIDDEN_PROPERTIES);
    List<String> unlisted = classTyped.stream().filter(name -> !listed.contains(name)).sorted()
        .collect(Collectors.toList());
    Assert.assertEquals("Kafka properties resolved into a class that neither list in KafkaUtils covers",
        Collections.emptyList(), unlisted);
  }

  @Test public void ordinaryTablePropertiesAreAccepted() {
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put(KafkaTableProperties.HIVE_KAFKA_TOPIC.getName(), "a-topic");
    tableProperties.put(KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + ".max.poll.records", "5000");
    tableProperties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/ssl/truststore.jks");
    KafkaUtils.checkTableProperties(tableProperties);
  }

  @Test public void nullTablePropertiesAreAccepted() {
    KafkaUtils.checkTableProperties(null);
  }

  @Test public void forbiddenPropertyIsRefusedOnTableClientPath() {
    for (String key : KafkaUtils.FORBIDDEN_PROPERTIES) {
      try {
        KafkaUtils.checkTableClientProperty(key, "whatever");
        Assert.fail("Expected " + key + " to be refused");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains(key));
      }
    }
  }

  /**
   * The operator opt-in covers values an operator put in hive-site.xml. A value read back out of the
   * table parameters is user supplied DDL whatever the opt-in says, so it must stay refused.
   */
  @Test public void optInDoesNotReachTableClientPath() {
    System.setProperty(KafkaUtils.ALLOW_CLASS_NAME_PROPERTIES, "true");
    for (String key : KafkaUtils.CLASS_NAME_PROPERTIES) {
      try {
        KafkaUtils.checkTableClientProperty(key, CUSTOM_CLASS);
        Assert.fail("Expected " + key + " to be refused");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains(key));
      }
    }
  }

  @Test public void disallowedLoginModuleIsRefusedOnTableClientPath() {
    try {
      KafkaUtils.checkTableClientProperty(SaslConfigs.SASL_JAAS_CONFIG,
          "com.sun.security.auth.module.JndiLoginModule required user.provider.url=\"ldap://ldap.example.com:389/ou=users\";");
      Assert.fail("Expected the disallowed login module to be refused");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains("JndiLoginModule"));
    }
  }

  @Test public void ordinaryPropertyIsAcceptedOnTableClientPath() {
    KafkaUtils.checkTableClientProperty("max.poll.records", "5000");
  }

  /**
   * The functional guard rails now fail the DDL rather than only the query, since checkTableProperties
   * and the paths reading the parameters back share one gate.
   */
  @Test public void forbiddenTablePropertyIsRefusedAtDdl() {
    assertTablePropertyRefused(KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + "."
        + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true", ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
  }

  /**
   * A login module Kafka itself refuses through {@code org.apache.kafka.disallowed.login.modules} is
   * reported at DDL time instead of at query time. The prefixed key also covers the prefix
   * stripping, which is why the message names the un-prefixed Kafka property.
   */
  @Test public void disallowedLoginModuleIsRefusedInJaasConfig() {
    assertTablePropertyRefused(KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + "." + SaslConfigs.SASL_JAAS_CONFIG,
        "com.sun.security.auth.module.JndiLoginModule required user.provider.url=\"ldap://ldap.example.com:389/ou=users\";",
        "JndiLoginModule");
  }

  @Test public void secondDisallowedLoginModuleIsRefusedInJaasConfig() {
    assertTablePropertyRefused(SaslConfigs.SASL_JAAS_CONFIG,
        "com.sun.security.auth.module.LdapLoginModule required userProvider=\"ldap://ldap.example.com:389/o=example\";",
        "LdapLoginModule");
  }

  @Test public void unparseableJaasConfigIsRefused() {
    assertTablePropertyRefused(SaslConfigs.SASL_JAAS_CONFIG, "not a jaas config at all");
  }

  @Test public void standardLoginModuleIsAcceptedInJaasConfig() {
    KafkaUtils.checkJaasConfig("org.apache.kafka.common.security.plain.PlainLoginModule required "
        + "username=\"user\" password=\"pass\";");
  }

  /**
   * A semicolon inside a quoted option value must not be mistaken for an entry separator, which is
   * why Kafka's own parser is used rather than splitting the value by hand.
   */
  @Test public void semicolonInsideQuotedValueIsAcceptedInJaasConfig() {
    KafkaUtils.checkJaasConfig("org.apache.kafka.common.security.scram.ScramLoginModule required "
        + "username=\"user\" password=\"pa;ss\";");
  }

  /**
   * Third party login modules such as the AWS MSK IAM one stay usable: the policy is Kafka's deny
   * list, not an allow list of the four modules Hive happens to know about.
   */
  @Test public void thirdPartyLoginModuleIsAcceptedInJaasConfig() {
    KafkaUtils.checkJaasConfig("software.amazon.msk.auth.iam.IAMLoginModule required;");
  }

  /**
   * A value an operator put in hive-site.xml arrives literal and has to keep working, which is the
   * whole reason the Configuration path is more permissive than the table property path.
   */
  @Test public void configuredJaasValueFromSiteConfigIsAccepted() {
    Properties properties = KafkaUtils.consumerProperties(consumerConfig(SaslConfigs.SASL_JAAS_CONFIG,
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";"));
    Assert.assertNotNull(properties.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
  }

  /**
   * The value is checked on the Configuration path too. A table property could only arrive here
   * escapeJava'd by Utilities.copyTableJobPropertiesToConf, which Kafka's own JAAS parser rejects
   * anyway, so there is no working table this leniency would have protected.
   */
  @Test public void configuredJaasValueWithDisallowedLoginModuleIsRefused() {
    try {
      KafkaUtils.consumerProperties(consumerConfig(SaslConfigs.SASL_JAAS_CONFIG,
          "com.sun.security.auth.module.JndiLoginModule required user.provider.url=\"ldap://ldap.example.com:389/ou=users\";"));
      Assert.fail("Expected the disallowed login module to be refused");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains("JndiLoginModule"));
    }
  }

  /**
   * The compile time check runs on the table sourced TableDesc properties, before Hive merges them
   * into the job configuration, so a table property never becomes indistinguishable from operator
   * configuration. Without it the operator opt-in would also re-enable table properties.
   */
  @Test public void tablePropertiesAreRefusedEvenWhenOperatorOptedIn() {
    System.setProperty(KafkaUtils.ALLOW_CLASS_NAME_PROPERTIES, "true");
    Properties tableProperties = new Properties();
    tableProperties.setProperty(KafkaUtils.CONSUMER_CONFIGURATION_PREFIX + "."
        + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "com.example.CustomCallbackHandler");
    try {
      KafkaUtils.checkTableDescProperties(tableProperties);
      Assert.fail("Expected the table property to be refused");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected message: " + e.getMessage(),
          e.getMessage().contains(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS));
    }
  }

  @Test public void nullPropertiesViewIsAccepted() {
    KafkaUtils.checkTableDescProperties(null);
  }

  @Test public void testSetupKafkaSslPropertiesNoSslIsUnchanged() {
    Configuration config = new Configuration();
    Properties props = new Properties();
    KafkaUtils.setupKafkaSslProperties(config, props);
    Assert.assertEquals(new Properties(), props);
  }

  @Test(expected = IllegalStateException.class) public void testSetupKafkaSslPropertiesSslNotInHdfs() {
    Configuration config = new Configuration();
    config.set(KafkaTableProperties.HIVE_KAFKA_SSL_CREDENTIAL_KEYSTORE.getName(), "nonexistentfile");
    config.set(KafkaTableProperties.HIVE_SSL_TRUSTSTORE_LOCATION_CONFIG.getName(), "madeup");
    Properties props = new Properties();
    KafkaUtils.setupKafkaSslProperties(config, props);
  }

  @Test(expected = IllegalStateException.class) public void testSetupKafkaSslPropertiesCantRetrieveStore() {
    Configuration config = new Configuration();
    config.set(KafkaTableProperties.HIVE_KAFKA_SSL_CREDENTIAL_KEYSTORE.getName(), "nonexistentfile");
    config.set(KafkaTableProperties.HIVE_SSL_TRUSTSTORE_LOCATION_CONFIG.getName(), "hdfs://localhost/tmp/madeup");
    Properties props = new Properties();
    KafkaUtils.setupKafkaSslProperties(config, props);
  }

  @Test public void testMetadataEnumLookupMapper() {
    int partition = 1;
    long offset = 5L;
    long ts = 1000000L;
    byte[] value = "value".getBytes();
    byte[] key = "key".getBytes();
    // ORDER MATTERS here.
    List<Writable>
        expectedWritables =
        Arrays.asList(new BytesWritable(key),
            new IntWritable(partition),
            new LongWritable(offset),
            new LongWritable(ts));
    KafkaWritable kafkaWritable = new KafkaWritable(partition, offset, ts, value, key);

    List<Writable>
        actual =
        MetadataColumn.KAFKA_METADATA_COLUMN_NAMES.stream()
            .map(MetadataColumn::forName)
            .map(kafkaWritable::getHiveWritable)
            .collect(Collectors.toList());

    Assert.assertEquals(expectedWritables, actual);
  }

  @Test public void testEnsureThatAllTheColumnAreListed() {
    Assert.assertEquals(MetadataColumn.values().length, MetadataColumn.KAFKA_METADATA_COLUMN_NAMES.size());
    Assert.assertEquals(MetadataColumn.values().length, MetadataColumn.KAFKA_METADATA_INSPECTORS.size());
    Assert.assertFalse(Arrays.stream(MetadataColumn.values())
        .map(MetadataColumn::getName)
        .anyMatch(name -> !MetadataColumn.KAFKA_METADATA_COLUMN_NAMES.contains(name)));
    Arrays.stream(MetadataColumn.values())
        .forEach(element -> Assert.assertNotNull(MetadataColumn.forName(element.getName())));
  }

  @Test public void testGetTaskId() {
    String[]
        ids =
        {"attempt_200707121733_0003_m_000005_0", "attempt_local_0001_m_000005_0", "task_200709221812_0001_m_000005_0",
            "task_local_0001_r_000005_0", "task_local_0001_r_000005_2"};

    String[]
        expectedIds =
        {"attempt_200707121733_0003_m_000005", "attempt_local_0001_m_000005", "task_200709221812_0001_m_000005",
            "task_local_0001_r_000005", "task_local_0001_r_000005"};

    Object[] actualIds = Arrays.stream(ids).map(id -> {
      Configuration configuration = new Configuration();
      configuration.set("mapred.task.id", id);
      return configuration;
    }).map(KafkaUtils::getTaskId).toArray();
    Assert.assertArrayEquals(expectedIds, actualIds);
  }
}
