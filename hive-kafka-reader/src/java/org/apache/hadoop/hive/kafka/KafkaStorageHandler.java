/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.kafka.KafkaPullerInputFormat.CONSUMER_CONFIGURATION_PREFIX;
import static org.apache.hadoop.hive.kafka.KafkaPullerInputFormat.HIVE_KAFKA_BOOTSTRAP_SERVERS;
import static org.apache.hadoop.hive.kafka.KafkaPullerInputFormat.HIVE_KAFKA_TOPIC;

public class KafkaStorageHandler implements HiveStorageHandler
{

  public static final String __TIMESTAMP = "__timestamp";
  public static final String __PARTITION = "__partition";
  public static final String __OFFSET = "__offset";
  public static final String SERDE_CLASS_NAME = "kafka.serde.class";

  private static final Logger log = LoggerFactory.getLogger(KafkaStorageHandler.class);

  Configuration configuration;

  @Override
  public Class<? extends InputFormat> getInputFormatClass()
  {
    return KafkaPullerInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass()
  {
    return NullOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass()
  {
    return GenericKafkaSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook()
  {
    return null;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException
  {
    return new DefaultHiveAuthorizationProvider();
  }

  @Override
  public void configureInputJobProperties(
      TableDesc tableDesc,
      Map<String, String> jobProperties
  )
  {
    jobProperties.put(HIVE_KAFKA_TOPIC, Preconditions
        .checkNotNull(
            tableDesc.getProperties().getProperty(HIVE_KAFKA_TOPIC),
            "kafka topic missing set table property->" + HIVE_KAFKA_TOPIC
        ));
    log.debug("Table properties: Kafka Topic {}", tableDesc.getProperties().getProperty(HIVE_KAFKA_TOPIC));
    jobProperties.put(HIVE_KAFKA_BOOTSTRAP_SERVERS, Preconditions
        .checkNotNull(
            tableDesc.getProperties().getProperty(HIVE_KAFKA_BOOTSTRAP_SERVERS),
            "Broker address missing set table property->" + HIVE_KAFKA_BOOTSTRAP_SERVERS
        ));
    log.debug("Table properties: Kafka broker {}", tableDesc.getProperties().getProperty(HIVE_KAFKA_BOOTSTRAP_SERVERS));
    jobProperties.put(
        SERDE_CLASS_NAME,
        tableDesc.getProperties().getProperty(SERDE_CLASS_NAME, KafkaJsonSerDe.class.getName())
    );

    log.info("Table properties: SerDe class name {}", jobProperties.get(SERDE_CLASS_NAME));

    //set extra properties
    tableDesc.getProperties()
             .entrySet()
             .stream()
             .filter(
                 objectObjectEntry -> objectObjectEntry.getKey()
                                                       .toString()
                                                       .toLowerCase()
                                                       .startsWith(CONSUMER_CONFIGURATION_PREFIX))
             .forEach(entry -> {
               String key = entry.getKey().toString().substring(CONSUMER_CONFIGURATION_PREFIX.length() + 1);
               String value = entry.getValue().toString();
               jobProperties.put(key, value);
               log.info("Setting extra job properties: key [{}] -> value [{}]", key, value );

             });
  }

  @Override
  public void configureInputJobCredentials(
      TableDesc tableDesc,
      Map<String, String> secrets
  )
  {

  }

  @Override
  public void configureOutputJobProperties(
      TableDesc tableDesc,
      Map<String, String> jobProperties
  )
  {

  }

  @Override
  public void configureTableJobProperties(
      TableDesc tableDesc,
      Map<String, String> jobProperties
  )
  {
    configureInputJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf)
  {

    Map<String, String> properties = new HashMap<>();
    configureInputJobProperties(tableDesc, properties);
    properties.forEach((key, value) -> jobConf.set(key, value));
  }

  @Override
  public void setConf(Configuration configuration)
  {
    this.configuration = configuration;
  }

  @Override
  public Configuration getConf()
  {
    return configuration;
  }

  @Override
  public String toString()
  {
    return "org.apache.hadoop.hive.kafka.KafkaStorageHandler";
  }
}
