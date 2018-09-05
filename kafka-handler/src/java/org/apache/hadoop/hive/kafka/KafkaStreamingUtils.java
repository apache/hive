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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Constant, Table properties, Utilities class.
 */
final class KafkaStreamingUtils {

  /**
   * MANDATORY Table property indicating kafka topic backing the table
   */
  static final String HIVE_KAFKA_TOPIC = "kafka.topic";
  /**
   * MANDATORY Table property indicating kafka broker(s) connection string.
   */
  static final String HIVE_KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
  /**
   * Table property indicating which delegate serde to be used, NOT MANDATORY defaults to {@link KafkaJsonSerDe}
   */
  static final String SERDE_CLASS_NAME = "kafka.serde.class";
  /**
   * Table property indicating poll/fetch timeout period in millis.
   * FYI this is independent from internal Kafka consumer timeouts, defaults to {@DEFAULT_CONSUMER_POLL_TIMEOUT_MS}
   */
  static final String HIVE_KAFKA_POLL_TIMEOUT = "hive.kafka.poll.timeout.ms";
  /**
   * default poll timeout for fetching metadata and record batch
   */
  static final long DEFAULT_CONSUMER_POLL_TIMEOUT_MS = 5000L; // 5 seconds
  /**
   * Record Timestamp column name, added as extra meta column of type long
   */
  static final String TIMESTAMP_COLUMN = "__timestamp";
  /**
   * Record Kafka Partition column name added as extra meta column of type int
   */
  static final String PARTITION_COLUMN = "__partition";
  /**
   * Record offset column name added as extra metadata column to row as long
   */
  static final String OFFSET_COLUMN = "__offset";

  /**
   * Start offset given by the input split, this will reflect the actual start of TP or start given by split pruner
   */
  static final String START_OFFSET_COLUMN = "__start_offset";

  /**
   * End offset given by input split at run time
   */
  static final String END_OFFSET_COLUMN = "__end_offset";
  /**
   * Table property prefix used to inject kafka consumer properties, e.g "kafka.consumer.max.poll.records" = "5000"
   * this will lead to inject max.poll.records=5000 to the Kafka Consumer. NOT MANDATORY defaults to nothing
   */
  static final String CONSUMER_CONFIGURATION_PREFIX = "kafka.consumer";

  /**
   * Set of Kafka properties that the user can not set via DDLs
   */
  static final HashSet<String> FORBIDDEN_PROPERTIES =
      new HashSet<>(ImmutableList.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));

  private KafkaStreamingUtils() {
  }

  /**
   * @param configuration Job configs
   *
   * @return default consumer properties
   */
  static Properties consumerProperties(Configuration configuration) {
    final Properties props = new Properties();
    // we are managing the commit offset
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // we are seeking in the stream so no reset
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    String brokerEndPoint = configuration.get(HIVE_KAFKA_BOOTSTRAP_SERVERS);
    if (brokerEndPoint == null || brokerEndPoint.isEmpty()) {
      throw new IllegalArgumentException("Kafka Broker End Point is missing Please set Config "
          + HIVE_KAFKA_BOOTSTRAP_SERVERS);
    }
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerEndPoint);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    // user can always override stuff
    final Map<String, String>
        kafkaProperties =
        configuration.getValByRegex("^" + CONSUMER_CONFIGURATION_PREFIX + "\\..*");
    for (Map.Entry<String, String> entry : kafkaProperties.entrySet()) {
      String key = entry.getKey().substring(CONSUMER_CONFIGURATION_PREFIX.length() + 1);
      if (FORBIDDEN_PROPERTIES.contains(key)) {
        throw new IllegalArgumentException("Not suppose to set Kafka Property " + key);
      }
      props.setProperty(key, entry.getValue());
    }
    return props;
  }

  static void copyDependencyJars(Configuration conf, Class<?>... classes) throws IOException {
    Set<String> jars = new HashSet<>();
    FileSystem localFs = FileSystem.getLocal(conf);
    jars.addAll(conf.getStringCollection("tmpjars"));
    jars.addAll(Arrays.stream(classes).filter(Objects::nonNull).map(clazz -> {
      String path = Utilities.jarFinderGetJar(clazz);
      if (path == null) {
        throw new RuntimeException("Could not find jar for class " + clazz + " in order to ship it to the cluster.");
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
    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[0])));
  }

  static AbstractSerDe createDelegate(String className) {
    final Class<? extends AbstractSerDe> clazz;
    try {
      //noinspection unchecked
      clazz = (Class<? extends AbstractSerDe>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    // we are not setting conf thus null is okay
    return ReflectionUtil.newInstance(clazz, null);
  }
}
