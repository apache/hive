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
package org.apache.hadoop.hive.druid.conf;

/**
 * Utility class for Druid Constants.
 */
public final class DruidConstants {
  private DruidConstants() {
  }

  public static final String DRUID_TARGET_SHARDS_PER_GRANULARITY =
          "druid.segment.targetShardsPerGranularity";
  public static final String DRUID_QUERY_FIELD_NAMES = "druid.fieldNames";
  public static final String DRUID_QUERY_FIELD_TYPES = "druid.fieldTypes";
  public static final String DRUID_SHARD_KEY_COL_NAME = "__druid_extra_partition_key";

  public static final String DRUID_QUERY_FETCH = "druid.query.fetch";

  public static final String DRUID_ROLLUP = "druid.rollup";

  public static final String DRUID_HLL_LG_K = "druid.hll.lg.k";
  /*
   * HLL_4(default),The type of the target HLL sketch. Must be "HLL_4", "HLL_6" or "HLL_8"
   * */
  public static final String DRUID_HLL_TGT_TYPE = "druid.hll.tgt.type";
  /*fields used to calculate count distinct with hll algorithm,for example uid,vid*/
  public static final String DRUID_HLL_SKETCH_FIELDS = "druid.hll.fields";
  /*fields used to calculate count distinct with theta algorithm,for example uid,vid*/
  public static final String DRUID_THETA_SKETCH_FIELDS = "druid.theta.fields";
  /*fields used to calculate count distinct with theta algorithm,for example uid,vid*/
  public static final String DRUID_EXCLUDE_FIELDS = "druid.exclude.fields";
  // see https://datasketches.github.io/docs/Theta/ThetaSize.html
  // Must be a power of 2. Internally, size refers to the maximum number of entries sketch object
  // will retain. Higher size means higher accuracy but more space to store sketches.
  // Note that after you index with a particular size, druid will persist sketch in segments
  // and you will use size greater or equal to that at query time. See the DataSketches
  // site for details. In general, We recommend just sticking to default size.
  public static final String DRUID_SKETCH_THETA_SIZE = "druid.sketch.theta.size";

  public static final String DRUID_QUERY_GRANULARITY = "druid.query.granularity";

  public static final String DRUID_SEGMENT_DIRECTORY = "druid.storage.storageDirectory";

  public static final String DRUID_SEGMENT_INTERMEDIATE_DIRECTORY = "druid.storage.storageDirectory.intermediate";

  public static final String DRUID_SEGMENT_VERSION = "druid.segment.version";

  public static final String DRUID_JOB_WORKING_DIRECTORY = "druid.job.workingDirectory";

  public static final String KAFKA_TOPIC = "kafka.topic";

  public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

  public static final String DRUID_KAFKA_INGESTION_PROPERTY_PREFIX = "druid.kafka.ingestion.";

  public static final String DRUID_KAFKA_CONSUMER_PROPERTY_PREFIX = DRUID_KAFKA_INGESTION_PROPERTY_PREFIX + "consumer.";

  /* Kafka Ingestion state - valid values - START/STOP/RESET */
  public static final String DRUID_KAFKA_INGESTION = "druid.kafka.ingestion";

  //Druid storage timestamp column name
  public static final String DEFAULT_TIMESTAMP_COLUMN = "__time";

  public static final String DRUID_TIMESTAMP_FORMAT = "druid.timestamp.format";

  // Used when the field name in ingested data via streaming ingestion does not match
  // druid default timestamp column i.e `__time`
  public static final String DRUID_TIMESTAMP_COLUMN = "druid.timestamp.column";

  //Druid Json timestamp column name for GroupBy results
  public static final String EVENT_TIMESTAMP_COLUMN = "timestamp";

  // Druid ParseSpec Type - JSON/CSV/TSV/AVRO
  public static final String DRUID_PARSE_SPEC_FORMAT = "druid.parseSpec.format";

  public static final String AVRO_SCHEMA_LITERAL = "avro.schema.literal";

  // value delimiter for druid columns
  public static final String DRUID_PARSE_SPEC_DELIMITER = "druid.parseSpec.delimiter";

  // list delimiter for multi-valued columns
  public static final String DRUID_PARSE_SPEC_LIST_DELIMITER = "druid.parseSpec.listDelimiter";

  // order of columns for delimiter and csv parse specs.
  public static final String DRUID_PARSE_SPEC_COLUMNS = "druid.parseSpec.columns";

  public static final String DRUID_PARSE_SPEC_SKIP_HEADER_ROWS = "druid.parseSpec.skipHeaderRows";

  public static final String DRUID_PARSE_SPEC_HAS_HEADER_ROWS = "druid.parseSpec.hasHeaderRows";
}
