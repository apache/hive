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

import org.apache.hadoop.hive.ql.metadata.StorageHandlerInfo;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Kafka Storage Handler info.
 */
class KafkaStorageHandlerInfo implements StorageHandlerInfo {
  private final String topic;
  private final Properties consumerProperties;

  KafkaStorageHandlerInfo(String topic, Properties consumerProperties) {
    this.topic = topic;
    this.consumerProperties = consumerProperties;
  }

  @Override public String formatAsText() {

    try (KafkaConsumer consumer = new KafkaConsumer(consumerProperties) {
    }) {
      //noinspection unchecked
      List<PartitionInfo> partitionsInfo = consumer.partitionsFor(topic);
      List<TopicPartition>
          topicPartitions =
          partitionsInfo.stream()
              .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
              .collect(Collectors.toList());
      Map endOffsets = consumer.endOffsets(topicPartitions);
      Map startOffsets = consumer.beginningOffsets(topicPartitions);

      return partitionsInfo.stream()
          .map(partitionInfo -> String.format("%s [start offset = [%s], end offset = [%s]]",
              partitionInfo.toString(),
              startOffsets.get(new TopicPartition(partitionInfo.topic(), partitionInfo.partition())),
              endOffsets.get(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))))
          .collect(Collectors.joining("\n"));
    } catch (Exception e) {
      return String.format("ERROR fetching metadata for Topic [%s], Connection String [%s], Error [%s]",
          topic,
          consumerProperties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
          e.getMessage());
    }
  }
}
