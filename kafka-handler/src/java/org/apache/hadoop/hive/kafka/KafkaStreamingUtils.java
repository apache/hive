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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.util.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * Constant, Table properties, Utilities class.
 */
public final class KafkaStreamingUtils {

  /**
   * MANDATORY Table property indicating kafka topic backing the table
   */
  public static final String HIVE_KAFKA_TOPIC = "kafka.topic";
  /**
   * MANDATORY Table property indicating kafka broker(s) connection string.
   */
  public static final String HIVE_KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
  /**
   * Table property indicating which delegate serde to be used, NOT MANDATORY defaults to {@link KafkaJsonSerDe}
   */
  public static final String SERDE_CLASS_NAME = "kafka.serde.class";
  /**
   * Table property indicating poll/fetch timeout period in millis.
   * FYI this is independent from internal Kafka consumer timeouts, defaults to {@DEFAULT_CONSUMER_POLL_TIMEOUT_MS}
   */
  public static final String HIVE_KAFKA_POLL_TIMEOUT = "hive.kafka.poll.timeout.ms";
  /**
   * default poll timeout for fetching metadata and record batch
   */
  public static final long DEFAULT_CONSUMER_POLL_TIMEOUT_MS = 5000L; // 5 seconds
  /**
   * Record Timestamp column name, added as extra meta column of type long
   */
  public static final String TIMESTAMP_COLUMN = "__timestamp";
  /**
   * Record Kafka Partition column name added as extra meta column of type int
   */
  public static final String PARTITION_COLUMN = "__partition";
  /**
   * Record offset column name added as extra metadata column to row as long
   */
  public static final String OFFSET_COLUMN = "__offset";
  /**
   * Table property prefix used to inject kafka consumer properties, e.g "kafka.consumer.max.poll.records" = "5000"
   * this will lead to inject max.poll.records=5000 to the Kafka Consumer. NOT MANDATORY defaults to nothing
   */
  protected static final String CONSUMER_CONFIGURATION_PREFIX = "kafka.consumer";

  private KafkaStreamingUtils() {
  }

  /**
   * @param configuration Job configs
   *
   * @return default consumer properties
   */
  public static Properties consumerProperties(Configuration configuration) {
    final Properties props = new Properties();
    // those are very important to set to avoid long blocking
    props.setProperty("request.timeout.ms", "10001");
    props.setProperty("fetch.max.wait.ms", "10000");
    props.setProperty("session.timeout.ms", "10000");
    // we are managing the commit offset
    props.setProperty("enable.auto.commit", "false");
    // we are seeking in the stream so no reset
    props.setProperty("auto.offset.reset", "none");
    String brokerEndPoint = configuration.get(HIVE_KAFKA_BOOTSTRAP_SERVERS);
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerEndPoint);
    props.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    // user can always override stuff
    final Map<String, String>
        kafkaProperties =
        configuration.getValByRegex("^" + CONSUMER_CONFIGURATION_PREFIX + "\\..*");
    for (Map.Entry<String, String> entry : kafkaProperties.entrySet()) {
      props.setProperty(entry.getKey().substring(CONSUMER_CONFIGURATION_PREFIX.length() + 1),
          entry.getValue());
    }
    return props;
  }

  public static void copyDependencyJars(Configuration conf, Class<?>... classes) throws IOException {
    Set<String> jars = new HashSet<>();
    FileSystem localFs = FileSystem.getLocal(conf);
    jars.addAll(conf.getStringCollection("tmpjars"));
    jars.addAll(Arrays.asList(classes).stream().filter(aClass -> aClass != null)
        .map(clazz -> {
          String path = Utilities.jarFinderGetJar(clazz);
          if (path == null) {
            throw new RuntimeException("Could not find jar for class "
                + clazz
                + " in order to ship it to the cluster.");
          }
          try {
            if (!localFs.exists(new Path(path))) {
              throw new RuntimeException("Could not validate jar file " + path + " for class " + clazz);
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return path;
        }).collect(Collectors.toList()));

    if (jars.isEmpty()) {
      return;
    }
    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[jars.size()])));
  }
}
