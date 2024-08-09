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
package org.apache.iceberg.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.io.FileIO;

import java.util.Map;

/**
 * A Friend bridge to Iceberg.
 */
public class HiveUtil {

  public static final HiveTableOperations newTableOperations(Configuration conf, String catalogName, String database, String table) {
    return new HiveTableOperations(conf, null, null, catalogName, database, table);
  }

  public static final HiveTableOperations newTableOperations(Configuration conf, ClientPool metaClients, FileIO fileIO, String catalogName, String database, String table) {
    return new HiveTableOperations(conf, null, null, catalogName, database, table);
  }

  public static Database convertToDatabase(HiveCatalog catalog, Namespace ns, Map<String, String> meta) {
    return catalog.convertToDatabase(ns, meta);
  }

  public static void setSnapshotSummary(HiveTableOperations ops, Map<String, String> parameters, Snapshot snapshot) {
    ops.setSnapshotSummary(parameters, snapshot);
  }

  public static void setSnapshotStats(HiveTableOperations ops, TableMetadata metadata, Map<String, String> parameters) {
    ops.setSnapshotStats(metadata, parameters);
  }

  public static void setSchema(HiveTableOperations ops, TableMetadata metadata, Map<String, String> parameters) {
    ops.setSchema(metadata, parameters);
  }

  public static void setPartitionSpec(HiveTableOperations ops, TableMetadata metadata, Map<String, String> parameters) {
    ops.setPartitionSpec(metadata, parameters);
  }

  public static void setSortOrder(HiveTableOperations ops, TableMetadata metadata, Map<String, String> parameters) {
    ops.setSortOrder(metadata, parameters);
  }
}
