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

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;


/**
 * Utilities class
 */
public class KafkaStreamingUtils {

  private KafkaStreamingUtils() {}

  /**
   * @param configuration Job configs
   *
   * @return default consumer properties
   */
  public static Properties consumerProperties(Configuration configuration) {
    final Properties props = new Properties();
    // those are very important to set to avoid long blocking
    props.setProperty("request.timeout.ms", "10001" );
    props.setProperty("fetch.max.wait.ms", "10000" );
    props.setProperty("session.timeout.ms", "10000" );
    // we are managing the commit offset
    props.setProperty("enable.auto.commit", "false");
    // we are seeking in the stream so no reset
    props.setProperty("auto.offset.reset", "none");
    String brokerEndPoint = configuration.get(KafkaPullerInputFormat.HIVE_KAFKA_BOOTSTRAP_SERVERS);
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerEndPoint);
    props.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    // user can always override stuff
    final Map<String, String> kafkaProperties =
        configuration.getValByRegex("^" + KafkaPullerInputFormat.CONSUMER_CONFIGURATION_PREFIX + "\\..*");
    for (Map.Entry<String, String> entry : kafkaProperties.entrySet()) {
      props.setProperty(entry.getKey().substring(
          KafkaPullerInputFormat.CONSUMER_CONFIGURATION_PREFIX.length() + 1),
          entry.getValue()
      );
    }
    return props;
  }
}
