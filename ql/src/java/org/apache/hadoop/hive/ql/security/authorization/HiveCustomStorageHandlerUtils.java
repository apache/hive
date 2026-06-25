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
package org.apache.hadoop.hive.ql.security.authorization;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.common.StatsSetupConst;

public class HiveCustomStorageHandlerUtils {

  public static final String WRITE_OPERATION_CONFIG_PREFIX = "file.sink.write.operation.";
  public static final String WRITE_OPERATION_IS_SORTED = "file.sink.write.operation.sorted.";

  public static final String MERGE_TASK_ENABLED = "file.sink.merge.task.enabled.";

  // Iceberg: gate Hive-native bucketing (CLUSTERED BY) file routing for a specific write target.
  // When enabled, bucket metadata is propagated via jobconf (set by SDPO / FileSinkOperator).
  public static final String ICEBERG_HIVE_BUCKETING_ROUTE_ENABLED =
      "file.sink.iceberg.hive.bucketing.route.enabled.";
  public static final String ICEBERG_HIVE_BUCKETING_NUM_BUCKETS =
      "file.sink.iceberg.hive.bucketing.num.buckets.";
  public static final String ICEBERG_HIVE_BUCKETING_BUCKET_COLS =
      "file.sink.iceberg.hive.bucketing.bucket.cols.";
  public static final String ICEBERG_HIVE_BUCKETING_VERSION =
      "file.sink.iceberg.hive.bucketing.version.";

  /**
   * Hive {@code CLUSTERED BY} metadata for an Iceberg write target, carried in jobconf when available.
   */
  public static final class IcebergHiveBucketingConf {
    private final int numBuckets;
    private final List<String> bucketCols;
    private final int bucketingVersion;

    public IcebergHiveBucketingConf(int numBuckets, List<String> bucketCols, int bucketingVersion) {
      this.numBuckets = numBuckets;
      this.bucketCols = bucketCols == null ? Collections.emptyList() : bucketCols;
      this.bucketingVersion = bucketingVersion;
    }

    public static IcebergHiveBucketingConf fromTable(org.apache.hadoop.hive.ql.metadata.Table table) {
      return new IcebergHiveBucketingConf(
          table.getNumBuckets(), table.getBucketCols(), table.getBucketingVersion());
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

  public static String getTablePropsForCustomStorageHandler(Map<String, String> tableProperties) {
    StringBuilder properties = new StringBuilder();
    for (Map.Entry<String, String> serdeMap : tableProperties.entrySet()) {
      if (!serdeMap.getKey().equalsIgnoreCase(serdeConstants.SERIALIZATION_FORMAT) &&
            !serdeMap.getKey().equalsIgnoreCase(StatsSetupConst.COLUMN_STATS_ACCURATE)) {
        properties.append(
            //replace space and double quotes if any in the values to avoid URI syntax exception
            serdeMap.getValue().replaceAll("\\s", "").replaceAll("\"", ""));
        properties.append("/");
      }
    }
    return properties.toString();
  }

  /**
   * @param table the HMS table
   * @return a map of table properties combined with the serde properties, if any
   */
  public static Map<String, String> getTableProperties(Table table) {
    Map<String, String> tblProps = new HashMap<>(table.getParameters());
    Optional.ofNullable(table.getSd().getSerdeInfo().getParameters())
        .ifPresent(tblProps::putAll);
    return tblProps;
  }

  public static Context.Operation getWriteOperation(UnaryOperator<String> ops, String tableName) {
    String operation = ops.apply(WRITE_OPERATION_CONFIG_PREFIX + tableName);
    return operation == null ? null : Context.Operation.valueOf(operation);
  }

  public static void setWriteOperation(Configuration conf, String tableName, Context.Operation operation) {
    if (conf == null || tableName == null) {
      return;
    }
    conf.set(WRITE_OPERATION_CONFIG_PREFIX + tableName, operation.name());
  }

  public static void setWriteOperationIsSorted(Configuration conf, String tableName, boolean isSorted) {
    if (conf == null || tableName == null) {
      return;
    }
    conf.set(WRITE_OPERATION_IS_SORTED + tableName, Boolean.toString(isSorted));
  }

  public static boolean getWriteOperationIsSorted(UnaryOperator<String> ops, String tableName) {
    String operation = ops.apply(WRITE_OPERATION_IS_SORTED + tableName);
    return Boolean.parseBoolean(operation);
  }

  public static void setMergeTaskEnabled(Configuration conf, String tableName, boolean isMerge) {
    if (conf == null || tableName == null) {
      return;
    }
    conf.set(MERGE_TASK_ENABLED + tableName, Boolean.toString(isMerge));
  }

  public static boolean isMergeTaskEnabled(UnaryOperator<String> ops, String tableName) {
    String operation = ops.apply(MERGE_TASK_ENABLED + tableName);
    return Boolean.parseBoolean(operation);
  }

  public static void setIcebergHiveBucketingRouteEnabled(Configuration conf, String tableName, boolean enabled) {
    if (conf == null || tableName == null) {
      return;
    }
    conf.set(ICEBERG_HIVE_BUCKETING_ROUTE_ENABLED + tableName, Boolean.toString(enabled));
  }

  public static boolean getIcebergHiveBucketingRouteEnabled(UnaryOperator<String> ops, String tableName) {
    String value = ops.apply(ICEBERG_HIVE_BUCKETING_ROUTE_ENABLED + tableName);
    return Boolean.parseBoolean(value);
  }

  public static void writeIcebergHiveBucketingMetadata(BiConsumer<String, String> store, String tableName,
      IcebergHiveBucketingConf metadata) {
    if (store == null || tableName == null || metadata == null) {
      return;
    }
    store.accept(ICEBERG_HIVE_BUCKETING_NUM_BUCKETS + tableName, Integer.toString(metadata.numBuckets()));
    store.accept(ICEBERG_HIVE_BUCKETING_BUCKET_COLS + tableName, String.join(",", metadata.bucketCols()));
    store.accept(ICEBERG_HIVE_BUCKETING_VERSION + tableName, Integer.toString(metadata.bucketingVersion()));
  }

  public static Optional<IcebergHiveBucketingConf> readIcebergHiveBucketingMetadata(
      Function<String, String> lookup, String tableName) {
    if (lookup == null || tableName == null) {
      return Optional.empty();
    }
    String numBuckets = lookup.apply(ICEBERG_HIVE_BUCKETING_NUM_BUCKETS + tableName);
    if (numBuckets == null) {
      return Optional.empty();
    }
    String version = lookup.apply(ICEBERG_HIVE_BUCKETING_VERSION + tableName);
    return Optional.of(new IcebergHiveBucketingConf(
        Integer.parseInt(numBuckets),
        parseBucketCols(lookup.apply(ICEBERG_HIVE_BUCKETING_BUCKET_COLS + tableName)),
        version == null ? 2 : Integer.parseInt(version)));
  }

  /**
   * Copies Iceberg Hive bucketing route flag and metadata from FileSink table properties into the task JobConf.
   */
  public static void propagateIcebergHiveBucketingFromTableInfo(Properties tableInfoProps, Configuration conf,
      String tableName) {
    if (tableInfoProps == null || conf == null || tableName == null) {
      return;
    }
    String routeEnabledStr = tableInfoProps.getProperty(ICEBERG_HIVE_BUCKETING_ROUTE_ENABLED + tableName);
    if (routeEnabledStr != null) {
      setIcebergHiveBucketingRouteEnabled(conf, tableName, Boolean.parseBoolean(routeEnabledStr));
    }
    readIcebergHiveBucketingMetadata(tableInfoProps::getProperty, tableName)
        .ifPresent(metadata -> writeIcebergHiveBucketingMetadata(conf::set, tableName, metadata));
  }

  private static List<String> parseBucketCols(String cols) {
    if (cols == null || cols.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.stream(cols.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }
}
