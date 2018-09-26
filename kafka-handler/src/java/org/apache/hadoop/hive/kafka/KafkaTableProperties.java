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

import org.apache.hadoop.hive.serde2.JsonSerDe;

/**
 * Table properties used by Kafka Storage handler.
 */
enum KafkaTableProperties {
  /**
   * MANDATORY Table property indicating kafka topic backing the table.
   */
  HIVE_KAFKA_TOPIC("kafka.topic", null),
  /**
   * MANDATORY Table property indicating kafka broker(s) connection string.
   */
  HIVE_KAFKA_BOOTSTRAP_SERVERS("kafka.bootstrap.servers", null),
  /**
   * Table property indicating which delegate serde to be used.
   */
  SERDE_CLASS_NAME("kafka.serde.class", JsonSerDe.class.getName()),
  /**
   * Table property indicating poll/fetch timeout period in millis.
   * FYI this is independent from internal Kafka consumer timeouts.
   */
  KAFKA_POLL_TIMEOUT("hive.kafka.poll.timeout.ms", "5000"),

  MAX_RETRIES("hive.kafka.max.retries", "6"), KAFKA_FETCH_METADATA_TIMEOUT("hive.kafka.metadata.poll.timeout.ms",
      "30000"),
  /**
   * Table property indicating the write semantic possible enum values are:
   * {@link KafkaOutputFormat.WriteSemantic}.
   */
  WRITE_SEMANTIC_PROPERTY("kafka.write.semantic", KafkaOutputFormat.WriteSemantic.AT_LEAST_ONCE.name()),
  /**
   * Table property that indicates if we should commit within the task or delay it to the Metadata Hook Commit call.
   */
  HIVE_KAFKA_OPTIMISTIC_COMMIT("hive.kafka.optimistic.commit", "false");

  /**
   * Kafka storage handler table properties constructor.
   * @param name property name.
   * @param defaultValue default value, set to NULL if the property is mandatory and need to be set by the user.
   */
  KafkaTableProperties(String name, String defaultValue) {
    this.name = name;
    this.defaultValue = defaultValue;
    this.mandatory = defaultValue == null;
  }

  public String getName() {
    return name;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public boolean isMandatory() {
    return mandatory;
  }

  private final String name;
  private final String defaultValue;
  private final boolean mandatory;
}
