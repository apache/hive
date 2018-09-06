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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Hive Kafka storage handler to allow user querying Stream of tuples from a Kafka queue.
 */
public class KafkaStorageHandler implements HiveStorageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaStorageHandler.class);

  Configuration configuration;

  @Override public Class<? extends InputFormat> getInputFormatClass() {
    return KafkaPullerInputFormat.class;
  }

  @Override public Class<? extends OutputFormat> getOutputFormatClass() {
    return NullOutputFormat.class;
  }

  @Override public Class<? extends AbstractSerDe> getSerDeClass() {
    return GenericKafkaSerDe.class;
  }

  @Override public HiveMetaHook getMetaHook() {
    return null;
  }

  @Override public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return new DefaultHiveAuthorizationProvider();
  }

  @Override public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    jobProperties.put(KafkaStreamingUtils.HIVE_KAFKA_TOPIC,
        Preconditions.checkNotNull(tableDesc.getProperties().getProperty(KafkaStreamingUtils.HIVE_KAFKA_TOPIC),
            "kafka topic missing set table property->" + KafkaStreamingUtils.HIVE_KAFKA_TOPIC));
    LOG.debug("Table properties: Kafka Topic {}", tableDesc.getProperties().getProperty(KafkaStreamingUtils.HIVE_KAFKA_TOPIC));
    jobProperties.put(KafkaStreamingUtils.HIVE_KAFKA_BOOTSTRAP_SERVERS,
        Preconditions.checkNotNull(tableDesc.getProperties().getProperty(KafkaStreamingUtils.HIVE_KAFKA_BOOTSTRAP_SERVERS),
            "Broker address missing set table property->" + KafkaStreamingUtils.HIVE_KAFKA_BOOTSTRAP_SERVERS));
    LOG.debug("Table properties: Kafka broker {}", tableDesc.getProperties().getProperty(KafkaStreamingUtils.HIVE_KAFKA_BOOTSTRAP_SERVERS));
    jobProperties.put(KafkaStreamingUtils.SERDE_CLASS_NAME,
        tableDesc.getProperties().getProperty(KafkaStreamingUtils.SERDE_CLASS_NAME, KafkaJsonSerDe.class.getName()));

    LOG.debug("Table properties: SerDe class name {}", jobProperties.get(KafkaStreamingUtils.SERDE_CLASS_NAME));

    //set extra properties
    tableDesc.getProperties()
        .entrySet()
        .stream()
        .filter(objectObjectEntry -> objectObjectEntry.getKey()
            .toString()
            .toLowerCase()
            .startsWith(KafkaStreamingUtils.CONSUMER_CONFIGURATION_PREFIX))
        .forEach(entry -> {
          String key = entry.getKey().toString().substring(KafkaStreamingUtils.CONSUMER_CONFIGURATION_PREFIX.length() + 1);
          if (KafkaStreamingUtils.FORBIDDEN_PROPERTIES.contains(key)) {
            throw new IllegalArgumentException("Not suppose to set Kafka Property " + key);
          }
          String value = entry.getValue().toString();
          jobProperties.put(key, value);
          LOG.info("Setting extra job properties: key [{}] -> value [{}]", key, value);

        });
  }

  @Override public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> secrets) {

  }

  @Override public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    configureInputJobProperties(tableDesc, jobProperties);
  }

  @Override public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    Map<String, String> properties = new HashMap<>();
    configureInputJobProperties(tableDesc, properties);
    properties.forEach((key, value) -> jobConf.set(key, value));
    try {
      KafkaStreamingUtils.copyDependencyJars(jobConf, KafkaStorageHandler.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public void setConf(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override public Configuration getConf() {
    return configuration;
  }

  @Override public String toString() {
    return "org.apache.hadoop.hive.kafka.KafkaStorageHandler";
  }
}
