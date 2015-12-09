/**
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
package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.mapred.TextInputFormat;

import com.google.common.annotations.VisibleForTesting;

public final class IOConstants {
  public static final String COLUMNS = "columns";
  public static final String COLUMNS_TYPES = "columns.types";
  public static final String MAPRED_TASK_ID = "mapred.task.id";

  public static final String TEXTFILE = "TEXTFILE";
  public static final String SEQUENCEFILE = "SEQUENCEFILE";
  public static final String RCFILE = "RCFILE";
  public static final String ORC = "ORC";
  public static final String ORCFILE = "ORCFILE";
  public static final String PARQUET = "PARQUET";
  public static final String PARQUETFILE = "PARQUETFILE";
  public static final String AVRO = "AVRO";
  public static final String AVROFILE = "AVROFILE";

  /**
   * The desired TABLE column names and types for input format schema evolution.
   * This is different than COLUMNS and COLUMNS_TYPES, which are based on individual partition
   * metadata.
   *
   * Virtual columns and partition columns are not included
   *
   */
  public static final String SCHEMA_EVOLUTION_COLUMNS = "schema.evolution.columns";
  public static final String SCHEMA_EVOLUTION_COLUMNS_TYPES = "schema.evolution.columns.types";

  @VisibleForTesting
  public static final String CUSTOM_TEXT_SERDE = "CustomTextSerde";

  public static final String TEXTFILE_INPUT = TextInputFormat.class
      .getName();
  @SuppressWarnings("deprecation")
  public static final String TEXTFILE_OUTPUT = IgnoreKeyTextOutputFormat.class
      .getName();

  private IOConstants() {
    // prevent instantiation
  }
}
