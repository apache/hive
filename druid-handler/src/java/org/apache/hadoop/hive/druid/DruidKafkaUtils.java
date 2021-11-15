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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DelimitedParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.hadoop.hive.druid.conf.DruidConstants;
import org.apache.hadoop.hive.druid.json.AvroParseSpec;
import org.apache.hadoop.hive.druid.json.AvroStreamInputRowParser;
import org.apache.hadoop.hive.druid.json.InlineSchemaAvroBytesDecoder;
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

  static KafkaSupervisorSpec createKafkaSupervisorSpec(
      Table table,
      String kafkaTopic,
      String kafkaServers,
      DataSchema dataSchema,
      IndexSpec indexSpec
  ) {
    return new KafkaSupervisorSpec(dataSchema,
        new KafkaSupervisorTuningConfig(DruidStorageHandlerUtils.getIntegerProperty(table,
            DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "maxRowsInMemory"),
                DruidStorageHandlerUtils.getLongProperty(table,
                        DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "maxBytesInMemory"),

            DruidStorageHandlerUtils.getIntegerProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "maxRowsPerSegment"), DruidStorageHandlerUtils
            .getLongProperty(table, DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "maxTotalRows"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "intermediatePersistPeriod"), null,
            // basePersistDirectory - use druid default, no need to be configured by user
            DruidStorageHandlerUtils
                .getIntegerProperty(table, DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "maxPendingPersists"),
            indexSpec, null, null,
            // buildV9Directly - use druid default, no need to be configured by user
            DruidStorageHandlerUtils.getBooleanProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "reportParseExceptions"),
            DruidStorageHandlerUtils.getLongProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "handoffConditionTimeout"),
            DruidStorageHandlerUtils.getBooleanProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "resetOffsetAutomatically"),
            TmpFileSegmentWriteOutMediumFactory.instance(), DruidStorageHandlerUtils
            .getIntegerProperty(table, DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "workerThreads"),
            DruidStorageHandlerUtils.getIntegerProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "chatThreads"),
            DruidStorageHandlerUtils.getLongProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "chatRetries"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "httpTimeout"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "shutdownTimeout"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "offsetFetchPeriod"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                    DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "intermediateHandoffPeriod"),
    DruidStorageHandlerUtils.getBooleanProperty(table,
            DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "logParseExceptions"),
    DruidStorageHandlerUtils.getIntegerProperty(table,
            DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "maxParseExceptions"),
    DruidStorageHandlerUtils.getIntegerProperty(table,
            DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "maxSavedParseExceptions")),
        new KafkaSupervisorIOConfig(kafkaTopic,
            // Mandatory Property
            DruidStorageHandlerUtils.getIntegerProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "replicas"),
            DruidStorageHandlerUtils.getIntegerProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "taskCount"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "taskDuration"),
            getKafkaConsumerProperties(table, kafkaServers),
            // Mandatory Property
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "startDelay"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "period"),
            DruidStorageHandlerUtils.getBooleanProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "useEarliestOffset"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "completionTimeout"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "lateMessageRejectionPeriod"),
            DruidStorageHandlerUtils.getPeriodProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "earlyMessageRejectionPeriod"),
            DruidStorageHandlerUtils.getBooleanProperty(table,
                DruidConstants.DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "skipOffsetGaps")),
        new HashMap<>());
  }

  private static Map<String, String> getKafkaConsumerProperties(Table table, String kafkaServers) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put(KafkaSupervisorIOConfig.BOOTSTRAP_SERVERS_KEY, kafkaServers);
    for (Map.Entry<String, String> entry : table.getParameters().entrySet()) {
      if (entry.getKey().startsWith(DruidConstants.DRUID_KAFKA_CONSUMER_PROPERTY_PREFIX)) {
        String propertyName = entry.getKey().substring(DruidConstants.DRUID_KAFKA_CONSUMER_PROPERTY_PREFIX.length());
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
      StringFullResponseHolder response = DruidStorageHandlerUtils
          .getResponseFromCurrentLeader(DruidStorageHandler.getHttpClient(), new Request(HttpMethod.POST,
                  new URL(String.format("http://%s/druid/indexer/v1/supervisor", overlordAddress)))
                  .setContent("application/json", JSON_MAPPER.writeValueAsBytes(spec)),
              new StringFullResponseHandler(Charset.forName("UTF-8")));
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
    return DruidStorageHandlerUtils.getTableProperty(table, DruidConstants.KAFKA_TOPIC) != null;
  }

  static InputRowParser getInputRowParser(Table table, TimestampSpec timestampSpec, DimensionsSpec dimensionsSpec) {
    String parseSpecFormat = DruidStorageHandlerUtils.getTableProperty(table, DruidConstants.DRUID_PARSE_SPEC_FORMAT);

    // Default case JSON
    if ((parseSpecFormat == null) || "json".equalsIgnoreCase(parseSpecFormat)) {
      return new StringInputRowParser(new JSONParseSpec(timestampSpec, dimensionsSpec, null, null), "UTF-8");
    } else if ("csv".equalsIgnoreCase(parseSpecFormat)) {
      return new StringInputRowParser(new CSVParseSpec(timestampSpec,
          dimensionsSpec,
          DruidStorageHandlerUtils.getTableProperty(table, DruidConstants.DRUID_PARSE_SPEC_LIST_DELIMITER),
          DruidStorageHandlerUtils.getListProperty(table, DruidConstants.DRUID_PARSE_SPEC_COLUMNS),
          DruidStorageHandlerUtils.getBooleanProperty(table, DruidConstants.DRUID_PARSE_SPEC_HAS_HEADER_ROWS, false),
          DruidStorageHandlerUtils.getIntegerProperty(table, DruidConstants.DRUID_PARSE_SPEC_SKIP_HEADER_ROWS, 0)),
          "UTF-8");
    } else if ("delimited".equalsIgnoreCase(parseSpecFormat)) {
      return new StringInputRowParser(new DelimitedParseSpec(timestampSpec,
          dimensionsSpec,
          DruidStorageHandlerUtils.getTableProperty(table, DruidConstants.DRUID_PARSE_SPEC_DELIMITER),
          DruidStorageHandlerUtils.getTableProperty(table, DruidConstants.DRUID_PARSE_SPEC_LIST_DELIMITER),
          DruidStorageHandlerUtils.getListProperty(table, DruidConstants.DRUID_PARSE_SPEC_COLUMNS),
          DruidStorageHandlerUtils.getBooleanProperty(table, DruidConstants.DRUID_PARSE_SPEC_HAS_HEADER_ROWS, false),
          DruidStorageHandlerUtils.getIntegerProperty(table, DruidConstants.DRUID_PARSE_SPEC_SKIP_HEADER_ROWS, 0)),
          "UTF-8");
    } else if ("avro".equalsIgnoreCase(parseSpecFormat)) {
      try {
        String avroSchemaLiteral = DruidStorageHandlerUtils.getTableProperty(table, DruidConstants.AVRO_SCHEMA_LITERAL);
        Preconditions.checkNotNull(avroSchemaLiteral, "Please specify avro schema literal when using avro parser");
        Map<String, Object>
            avroSchema =
            JSON_MAPPER.readValue(avroSchemaLiteral, new TypeReference<Map<String, Object>>() {
            });
        return new AvroStreamInputRowParser(new AvroParseSpec(timestampSpec, dimensionsSpec, null),
            new InlineSchemaAvroBytesDecoder(avroSchema));
      } catch (Exception e) {
        throw new IllegalStateException("Exception while creating avro schema", e);
      }
    }

    throw new IllegalArgumentException("Invalid parse spec format ["
        + parseSpecFormat
        + "]. "
        + "Supported types are : json, csv, tsv, avro");
  }
}
