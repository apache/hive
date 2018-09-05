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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

/**
 * Test for Utility class.
 */
public class KafkaStreamingUtilsTest {
  public KafkaStreamingUtilsTest() {
  }

  @Test public void testConsumerProperties() {
    Configuration configuration = new Configuration();
    configuration.set("kafka.bootstrap.servers", "localhost:9090");
    configuration.set("kafka.consumer.fetch.max.wait.ms", "40");
    configuration.set("kafka.consumer.my.new.wait.ms", "400");
    Properties properties = KafkaStreamingUtils.consumerProperties(configuration);
    Assert.assertEquals("localhost:9090", properties.getProperty("bootstrap.servers"));
    Assert.assertEquals("40", properties.getProperty("fetch.max.wait.ms"));
    Assert.assertEquals("400", properties.getProperty("my.new.wait.ms"));
  }

  @Test(expected = IllegalArgumentException.class) public void canNotSetForbiddenProp() {
    Configuration configuration = new Configuration();
    configuration.set("kafka.bootstrap.servers", "localhost:9090");
    configuration.set("kafka.consumer." + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    KafkaStreamingUtils.consumerProperties(configuration);
  }

  @Test(expected = IllegalArgumentException.class) public void canNotSetForbiddenProp2() {
    Configuration configuration = new Configuration();
    configuration.set("kafka.bootstrap.servers", "localhost:9090");
    configuration.set("kafka.consumer." + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "value");
    KafkaStreamingUtils.consumerProperties(configuration);
  }
}
