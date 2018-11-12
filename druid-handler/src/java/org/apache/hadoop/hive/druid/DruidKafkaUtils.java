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

package org.apache.hadoop.hive.druid;

import com.google.common.collect.ImmutableMap;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.FullResponseHandler;
import io.druid.java.util.http.client.response.FullResponseHolder;
import io.druid.segment.IndexSpec;
import io.druid.segment.indexing.DataSchema;
import org.apache.hadoop.hive.druid.json.KafkaSupervisorIOConfig;
import org.apache.hadoop.hive.druid.json.KafkaSupervisorSpec;
import org.apache.hadoop.hive.druid.json.KafkaSupervisorTuningConfig;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.druid.DruidStorageHandlerUtils.JSON_MAPPER;

/**
 * Class containing some Utility methods for Kafka Ingestion.
 */
final class DruidKafkaUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DruidKafkaUtils.class);
  private static final SessionState.LogHelper CONSOLE = new SessionState.LogHelper(LOG);

  private DruidKafkaUtils() {
  }

  static KafkaSupervisorSpec createKafkaSupervisorSpec(Table table,
      String kafkaTopic,
      String kafkaServers,
      DataSchema dataSchema,
      IndexSpec indexSpec) {
    return new KafkaSupervisorSpec(dataSchema,
        new KafkaSupervisorTuningConfig(DruidStorageHandlerUtils.getIntegerProperty(table,
            DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "maxRowsInMemory"),
            DruidStorageHandlerUtils.getIntegerProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "maxRowsPerSegment"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "intermediatePersistPeriod"),
            null,
            // basePersistDirectory - use druid default, no need to be configured by user
            DruidStorageHandlerUtils.getIntegerProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "maxPendingPersists"),
            indexSpec,
            null,
            // buildV9Directly - use druid default, no need to be configured by user
            DruidStorageHandlerUtils.getBooleanProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "reportParseExceptions"),
            DruidStorageHandlerUtils.getLongProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "handoffConditionTimeout"),
            DruidStorageHandlerUtils.getBooleanProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "resetOffsetAutomatically"),
            DruidStorageHandlerUtils.getIntegerProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "workerThreads"),
            DruidStorageHandlerUtils.getIntegerProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "chatThreads"),
            DruidStorageHandlerUtils.getLongProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "chatRetries"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "httpTimeout"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "shutdownTimeout"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "offsetFetchPeriod")),
        new KafkaSupervisorIOConfig(kafkaTopic,
            // Mandatory Property
            DruidStorageHandlerUtils.getIntegerProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "replicas"),
            DruidStorageHandlerUtils.getIntegerProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "taskCount"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "taskDuration"),
            getKafkaConsumerProperties(table, kafkaServers),
            // Mandatory Property
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "startDelay"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "period"),
            DruidStorageHandlerUtils.getBooleanProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "useEarliestOffset"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "completionTimeout"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "lateMessageRejectionPeriod"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "earlyMessageRejectionPeriod"),
            DruidStorageHandlerUtils.getBooleanProperty(table,
                DruidStorageHandlerUtils.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "skipOffsetGaps")),
        new HashMap<>());
  }

  private static Map<String, String> getKafkaConsumerProperties(Table table, String kafkaServers) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put(KafkaSupervisorIOConfig.BOOTSTRAP_SERVERS_KEY, kafkaServers);
    for (Map.Entry<String, String> entry : table.getParameters().entrySet()) {
      if (entry.getKey().startsWith(DruidStorageHandlerUtils.DRUID_KAFKA_CONSUMER_PROPERTY_PREFIX)) {
        String
            propertyName =
            entry.getKey().substring(DruidStorageHandlerUtils.DRUID_KAFKA_CONSUMER_PROPERTY_PREFIX.length());
        builder.put(propertyName, entry.getValue());
      }
    }
    return builder.build();
  }

  static void updateKafkaIngestionSpec(String overlordAddress, KafkaSupervisorSpec spec) {
    try {
      String task = JSON_MAPPER.writeValueAsString(spec);
      CONSOLE.printInfo("submitting kafka Spec {}", task);
      LOG.info("submitting kafka Supervisor Spec {}", task);
      FullResponseHolder
          response =
          DruidStorageHandlerUtils.getResponseFromCurrentLeader(DruidStorageHandler.getHttpClient(),
              new Request(HttpMethod.POST,
                  new URL(String.format("http://%s/druid/indexer/v1/supervisor", overlordAddress))).setContent(
                  "application/json",
                  JSON_MAPPER.writeValueAsBytes(spec)),
              new FullResponseHandler(Charset.forName("UTF-8")));
      if (response.getStatus().equals(HttpResponseStatus.OK)) {
        String
            msg =
            String.format("Kafka Supervisor for [%s] Submitted Successfully to druid.",
                spec.getDataSchema().getDataSource());
        LOG.info(msg);
        CONSOLE.printInfo(msg);
      } else {
        throw new IOException(String.format("Unable to update Kafka Ingestion for Druid status [%d] full response [%s]",
            response.getStatus().getCode(),
            response.getContent()));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static boolean isKafkaStreamingTable(Table table) {
    // For kafka Streaming tables it is mandatory to set a kafka topic.
    return DruidStorageHandlerUtils.getTableProperty(table, DruidStorageHandlerUtils.KAFKA_TOPIC) != null;
  }
}
