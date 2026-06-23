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

package org.apache.iceberg.mr.hive;

import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Hive-native bucketing metadata ({@code CLUSTERED BY}) stored in HMS for an Iceberg table.
 */
public final class HiveIcebergHiveBucketingMetadata {

  private final int numBuckets;
  private final List<String> bucketCols;
  private final int bucketingVersion;

  private HiveIcebergHiveBucketingMetadata(int numBuckets, List<String> bucketCols, int bucketingVersion) {
    this.numBuckets = numBuckets;
    this.bucketCols = bucketCols == null ? Collections.emptyList() : bucketCols;
    this.bucketingVersion = bucketingVersion;
  }

  public static HiveIcebergHiveBucketingMetadata fromHmsTable(org.apache.hadoop.hive.ql.metadata.Table table) {
    return new HiveIcebergHiveBucketingMetadata(
        table.getNumBuckets(), table.getBucketCols(), table.getBucketingVersion());
  }

  public static HiveIcebergHiveBucketingMetadata load(Configuration conf, String tableName)
      throws SerDeException {
    if (conf == null || tableName == null || tableName.isEmpty()) {
      throw new SerDeException("Cannot load Hive bucketing metadata without configuration and table name");
    }
    try {
      TableIdentifier tableId = TableIdentifier.parse(tableName);
      String dbName = !tableId.namespace().isEmpty() ?
          tableId.namespace().level(0) :
          Warehouse.DEFAULT_DATABASE_NAME;
      org.apache.hadoop.hive.ql.metadata.Table table =
          Hive.get(conf, HiveIcebergHiveBucketingMetadata.class).getTable(dbName, tableId.name());
      return fromHmsTable(table);
    } catch (HiveException e) {
      throw new SerDeException("Failed to load Hive bucketing metadata for table " + tableName, e);
    }
  }

  public boolean hasHiveBucketing() {
    return numBuckets > 0 && !bucketCols.isEmpty();
  }

  public int numBuckets() {
    return numBuckets;
  }

  public List<String> bucketCols() {
    return bucketCols;
  }

  public int bucketingVersion() {
    return bucketingVersion;
  }
}
