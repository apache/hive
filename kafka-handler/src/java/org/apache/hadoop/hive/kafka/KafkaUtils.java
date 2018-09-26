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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Utils class for Kafka Storage handler plus some Constants.
 */
final class KafkaUtils {

  private KafkaUtils() {
  }

  /**
   * Table property prefix used to inject kafka consumer properties, e.g "kafka.consumer.max.poll.records" = "5000"
   * this will lead to inject max.poll.records=5000 to the Kafka Consumer. NOT MANDATORY defaults to nothing
   */
  static final String CONSUMER_CONFIGURATION_PREFIX = "kafka.consumer";

  /**
   * Table property prefix used to inject kafka producer properties, e.g "kafka.producer.lingers.ms" = "100".
   */
  static final String PRODUCER_CONFIGURATION_PREFIX = "kafka.producer";

  /**
   * Set of Kafka properties that the user can not set via DDLs.
   */
  static final Set<String>
      FORBIDDEN_PROPERTIES =
      new HashSet<>(ImmutableList.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          ProducerConfig.TRANSACTIONAL_ID_CONFIG,
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));

  /**
   * @param configuration Job configs
   *
   * @return default consumer properties
   */
  static Properties consumerProperties(Configuration configuration) {
    final Properties props = new Properties();
    // we are managing the commit offset
    props.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, Utilities.getTaskId(configuration));
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // we are seeking in the stream so no reset
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    String brokerEndPoint = configuration.get(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName());
    if (brokerEndPoint == null || brokerEndPoint.isEmpty()) {
      throw new IllegalArgumentException("Kafka Broker End Point is missing Please set Config "
          + KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName());
    }
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerEndPoint);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    // user can always override stuff
    props.putAll(extractExtraProperties(configuration, CONSUMER_CONFIGURATION_PREFIX));
    return props;
  }

  private static Map<String, String> extractExtraProperties(final Configuration configuration, String prefix) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    final Map<String, String> kafkaProperties = configuration.getValByRegex("^" + prefix + "\\..*");
    for (Map.Entry<String, String> entry : kafkaProperties.entrySet()) {
      String key = entry.getKey().substring(prefix.length() + 1);
      if (FORBIDDEN_PROPERTIES.contains(key)) {
        throw new IllegalArgumentException("Not suppose to set Kafka Property " + key);
      }
      builder.put(key, entry.getValue());
    }
    return builder.build();
  }

  static Properties producerProperties(Configuration configuration) {
    final String writeSemanticValue = configuration.get(KafkaTableProperties.WRITE_SEMANTIC_PROPERTY.getName());
    final KafkaOutputFormat.WriteSemantic writeSemantic = KafkaOutputFormat.WriteSemantic.valueOf(writeSemanticValue);
    final Properties properties = new Properties();
    String brokerEndPoint = configuration.get(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName());
    if (brokerEndPoint == null || brokerEndPoint.isEmpty()) {
      throw new IllegalArgumentException("Kafka Broker End Point is missing Please set Config "
          + KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName());
    }
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerEndPoint);
    // user can always override stuff
    properties.putAll(extractExtraProperties(configuration, PRODUCER_CONFIGURATION_PREFIX));
    String taskId = configuration.get("mapred.task.id", null);
    properties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG,
        taskId == null ? "random_" + UUID.randomUUID().toString() : taskId);
    switch (writeSemantic) {
    case BEST_EFFORT:
      break;
    case AT_LEAST_ONCE:
      properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
      //The number of acknowledgments the producer requires the leader to have received before considering a request as
      // complete, all means from all replicas.
      properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
      break;
    case EXACTLY_ONCE:
      // Assuming that TaskId is ReducerId_attemptId. need the Reducer ID to fence out zombie kafka producers.
      String reducerId = getTaskId(configuration);
      //The number of acknowledgments the producer requires the leader to have received before considering a request as
      // complete, all means from all replicas.
      properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
      properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
      properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, reducerId);
      //Producer set to be IDEMPOTENT eg ensure that send() retries are idempotent.
      properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
      break;
    default:
      throw new IllegalArgumentException("Unknown Semantic " + writeSemantic);
    }
    return properties;
  }

  @SuppressWarnings("SameParameterValue") static void copyDependencyJars(Configuration conf, Class<?>... classes)
      throws IOException {
    Set<String> jars = new HashSet<>();
    FileSystem localFs = FileSystem.getLocal(conf);
    jars.addAll(conf.getStringCollection("tmpjars"));
    jars.addAll(Arrays.stream(classes)
        .filter(Objects::nonNull)
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

  static ProducerRecord<byte[], byte[]> toProducerRecord(String topic, KafkaWritable value) {
    return new ProducerRecord<>(topic,
        value.getPartition() != -1 ? value.getPartition() : null,
        value.getTimestamp() != -1L ? value.getTimestamp() : null,
        value.getRecordKey(),
        value.getValue());
  }

  /**
   * Check if the exception is Non-Retriable there a show stopper all we can do is clean and exit.
   * @param exception input exception object.
   * @return true if the exception is fatal thus we only can abort and rethrow the cause.
   */
  static boolean exceptionIsFatal(final Throwable exception) {
    final boolean
        securityException =
        exception instanceof AuthenticationException
            || exception instanceof AuthorizationException
            || exception instanceof SecurityDisabledException;

    final boolean
        communicationException =
        exception instanceof InvalidTopicException
            || exception instanceof UnknownServerException
            || exception instanceof SerializationException
            || exception instanceof OffsetMetadataTooLarge
            || exception instanceof IllegalStateException;

    return securityException || communicationException;
  }

  /**
   * Computes the kafka producer transaction id. The Tx id HAS to be the same across task restarts,
   * that is why we are excluding the attempt id by removing the last string after last `_`.
   * Assuming the taskId format is taskId_[m-r]_attemptId.
   *
   * @param hiveConf Hive Configuration.
   * @return the taskId without the attempt id.
   */
  static String getTaskId(Configuration hiveConf) {
    String id = Preconditions.checkNotNull(hiveConf.get("mapred.task.id", null));
    int index = id.lastIndexOf("_");
    if (index != -1) {
      return id.substring(0, index);
    }
    return id;
  }

}
