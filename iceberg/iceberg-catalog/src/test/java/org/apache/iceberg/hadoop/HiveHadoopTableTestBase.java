/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hadoop;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import static org.apache.iceberg.types.Types.NestedField.required;

public class HiveHadoopTableTestBase {
  static final Schema SCHEMA =
        new Schema(
              required(3, "id", Types.IntegerType.get(), "unique ID"),
              required(4, "data", Types.StringType.get()));

  static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  static final HadoopTables TABLES = new HadoopTables(new Configuration());

  static final DataFile FILE_A =
        DataFiles.builder(SPEC)
              .withPath("/path/to/data-a.parquet")
              .withFileSizeInBytes(0)
              .withPartitionPath("data_bucket=0") // easy way to set partition data for now
              .withRecordCount(2) // needs at least one record or else metrics will filter it out
              .build();

  @TempDir
  File tableDir;

  String tableLocation = null;
  File metadataDir = null;
  File versionHintFile = null;
  Table table = null;

  @BeforeEach
  public void setupTable() {
    this.tableLocation = tableDir.toURI().toString();
    this.metadataDir = new File(tableDir, "metadata");
    this.versionHintFile = new File(metadataDir, "version-hint.text");
    this.table = TABLES.create(SCHEMA, SPEC, tableLocation);
  }
}
