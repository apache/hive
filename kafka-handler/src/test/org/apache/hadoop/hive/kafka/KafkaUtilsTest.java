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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

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
