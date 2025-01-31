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

package org.apache.iceberg.hive;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hive.iceberg.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.BiMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableBiMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.JsonUtil;
import org.apache.parquet.Strings;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.GC_ENABLED;

public class HMSTablePropertyHelper {
  private static final Logger LOG = LoggerFactory.getLogger(HMSTablePropertyHelper.class);
  public static final String HIVE_ICEBERG_STORAGE_HANDLER = "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler";

  private static final BiMap<String, String> ICEBERG_TO_HMS_TRANSLATION = ImmutableBiMap.of(
      // gc.enabled in Iceberg and external.table.purge in Hive are meant to do the same things
      // but with different names
      GC_ENABLED, "external.table.purge", TableProperties.PARQUET_COMPRESSION, ParquetOutputFormat.COMPRESSION,
      TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, ParquetOutputFormat.BLOCK_SIZE);

  public static final Set<String> PROPERTIES_TO_REMOVE = ImmutableSet
      // We don't want to push down the metadata location props to Iceberg from HMS,
      // since the snapshot pointer in HMS would always be one step ahead
      .of(BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
          BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP);

  public static final String NAME = "name";
  public static final String LOCATION = "location";

  private HMSTablePropertyHelper() {
  }

  /**
   * Provides key translation where necessary between Iceberg and HMS props. This translation is needed because some
   * properties control the same behaviour but are named differently in Iceberg and Hive. Therefore changes to these
   * property pairs should be synchronized.
   *
   * Example: Deleting data files upon DROP TABLE is enabled using gc.enabled=true in Iceberg and
   * external.table.purge=true in Hive. Hive and Iceberg users are unaware of each other's control flags, therefore
   * inconsistent behaviour can occur from e.g. a Hive user's point of view if external.table.purge=true is set on the
   * HMS table but gc.enabled=false is set on the Iceberg table, resulting in no data file deletion.
   *
   * @param hmsProp The HMS property that should be translated to Iceberg property
   * @return Iceberg property equivalent to the hmsProp. If no such translation exists, the original hmsProp is returned
   */
  public static String translateToIcebergProp(String hmsProp) {
    return ICEBERG_TO_HMS_TRANSLATION.inverse().getOrDefault(hmsProp, hmsProp);
  }
  /** Updates the HMS Table properties based on the Iceberg Table metadata. */
  public static void updateHmsTableForIcebergTable(
      String newMetadataLocation,
      Table tbl,
      TableMetadata metadata,
      Set<String> obsoleteProps,
      boolean hiveEngineEnabled,
      long maxHiveTablePropertySize,
      String currentLocation) {
    Map<String, String> parameters =
        Optional.ofNullable(tbl.getParameters()).orElseGet(Maps::newHashMap);
    Map<String, String> summary =
        Optional.ofNullable(metadata.currentSnapshot())
            .map(Snapshot::summary)
            .orElseGet(ImmutableMap::of);
    // push all Iceberg table properties into HMS
    metadata.properties().entrySet().stream()
        .filter(entry -> !entry.getKey().equalsIgnoreCase(HiveCatalog.HMS_TABLE_OWNER))
        .forEach(
            entry -> {
              String key = entry.getKey();
              // translate key names between Iceberg and HMS where needed
              String hmsKey = ICEBERG_TO_HMS_TRANSLATION.getOrDefault(key, key);
              parameters.put(hmsKey, entry.getValue());
            });
    setCommonParameters(
        newMetadataLocation,
        metadata.uuid(),
        obsoleteProps,
        currentLocation,
        parameters,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH),
        metadata.schema(),
        maxHiveTablePropertySize);
    setStorageHandler(parameters, hiveEngineEnabled);

    // Set the basic statistics
    if (summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP) != null) {
      parameters.put(StatsSetupConst.NUM_FILES, summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    }
    if (summary.get(SnapshotSummary.TOTAL_RECORDS_PROP) != null) {
      parameters.put(StatsSetupConst.ROW_COUNT, summary.get(SnapshotSummary.TOTAL_RECORDS_PROP));
    }
    if (summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP) != null) {
      parameters.put(StatsSetupConst.TOTAL_SIZE, summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP));
    }

    setSnapshotStats(metadata, parameters, maxHiveTablePropertySize);
    setPartitionSpec(metadata, parameters, maxHiveTablePropertySize);
    setSortOrder(metadata, parameters, maxHiveTablePropertySize);

    tbl.setParameters(parameters);
  }

  /**
   * Create {@link PartitionSpec} based on the partition information stored in
   * {@link TransformSpec}.
   * @param configuration a Hadoop configuration
   * @param schema iceberg table schema
   * @return iceberg partition spec, always non-null
   */
  public static PartitionSpec createPartitionSpec(Configuration configuration, Schema schema) {
    List<TransformSpec> partitionTransformSpecList = SessionStateUtil
        .getResource(configuration, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC)
        .map(o -> (List<TransformSpec>) o)
        .orElse(null);

    if (partitionTransformSpecList == null) {
      LOG.warn("Iceberg partition transform spec is not found in QueryState.");
      return null;
    }
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    partitionTransformSpecList.forEach(spec -> {
      switch (spec.getTransformType()) {
        case IDENTITY:
          builder.identity(spec.getColumnName().toLowerCase());
          break;
        case YEAR:
          builder.year(spec.getColumnName());
          break;
        case MONTH:
          builder.month(spec.getColumnName());
          break;
        case DAY:
          builder.day(spec.getColumnName());
          break;
        case HOUR:
          builder.hour(spec.getColumnName());
          break;
        case TRUNCATE:
          builder.truncate(spec.getColumnName(), spec.getTransformParam().get());
          break;
        case BUCKET:
          builder.bucket(spec.getColumnName(), spec.getTransformParam().get());
          break;
      }
    });
    return builder.build();
  }

  public static SortOrder getSortOrder(Properties props, Schema schema) {
    String sortOrderJsonString = props.getProperty(TableProperties.DEFAULT_SORT_ORDER);
    return Strings.isNullOrEmpty(sortOrderJsonString) ? SortOrder.unsorted() : SortOrderParser.fromJson(schema,
        sortOrderJsonString);
  }

  /**
   * Calculates the properties we would like to send to the catalog.
   * <ul>
   * <li>The base of the properties is the properties stored at the Hive Metastore for the given table
   * <li>We add the {@link HiveIcebergRESTCatalogClientAdapter#LOCATION} as the table location
   * <li>We add the {@link HiveIcebergRESTCatalogClientAdapter#NAME} as
   * TableIdentifier defined by the database name and table name
   * <li>We add the serdeProperties of the HMS table
   * <li>We remove some parameters that we don't want to push down to the Iceberg table props
   * </ul>
   * @param hmsTable Table for which we are calculating the properties
   * @return The properties we can provide for Iceberg functions
   */
  public static Properties getCatalogProperties(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    Properties properties = new Properties();

    hmsTable.getParameters().entrySet().stream().filter(e -> e.getKey() != null && e.getValue() != null).forEach(e -> {
      // translate key names between HMS and Iceberg where needed
      String icebergKey = HMSTablePropertyHelper.translateToIcebergProp(e.getKey());
      properties.put(icebergKey, e.getValue());
    });

    if (properties.get(LOCATION) == null &&
        hmsTable.getSd() != null && hmsTable.getSd().getLocation() != null) {
      properties.put(LOCATION, hmsTable.getSd().getLocation());
    }

    if (properties.get(NAME) == null) {
      properties.put(NAME, TableIdentifier.of(hmsTable.getDbName(), hmsTable.getTableName()).toString());
    }

    SerDeInfo serdeInfo = hmsTable.getSd().getSerdeInfo();
    if (serdeInfo != null) {
      serdeInfo.getParameters().entrySet().stream()
          .filter(e -> e.getKey() != null && e.getValue() != null).forEach(e -> {
            String icebergKey = HMSTablePropertyHelper.translateToIcebergProp(e.getKey());
            properties.put(icebergKey, e.getValue());
          });
    }

    // Remove HMS table parameters we don't want to propagate to Iceberg
    PROPERTIES_TO_REMOVE.forEach(properties::remove);

    return properties;
  }
  private static void setCommonParameters(
      String newMetadataLocation,
      String uuid,
      Set<String> obsoleteProps,
      String currentLocation,
      Map<String, String> parameters,
      String tableType,
      Schema schema,
      long maxHiveTablePropertySize) {
    if (uuid != null) {
      parameters.put(TableProperties.UUID, uuid);
    }
    if (obsoleteProps != null) {
      obsoleteProps.forEach(parameters::remove);
    }

    parameters.put(BaseMetastoreTableOperations.TABLE_TYPE_PROP, tableType);
    parameters.put(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, newMetadataLocation);

    if (currentLocation != null && !currentLocation.isEmpty()) {
      parameters.put(BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP, currentLocation);
    }

    setSchema(schema, parameters, maxHiveTablePropertySize);
  }

  @VisibleForTesting
  static void setStorageHandler(Map<String, String> parameters, boolean hiveEngineEnabled) {
    // If needed set the 'storage_handler' property to enable query from Hive
    if (hiveEngineEnabled) {
      parameters.put(hive_metastoreConstants.META_TABLE_STORAGE, HIVE_ICEBERG_STORAGE_HANDLER);
    } else {
      parameters.remove(hive_metastoreConstants.META_TABLE_STORAGE);
    }
  }

  @VisibleForTesting
  static void setSnapshotStats(TableMetadata metadata, Map<String, String> parameters, long maxHiveTablePropertySize) {
    parameters.remove(TableProperties.CURRENT_SNAPSHOT_ID);
    parameters.remove(TableProperties.CURRENT_SNAPSHOT_TIMESTAMP);
    parameters.remove(TableProperties.CURRENT_SNAPSHOT_SUMMARY);

    Snapshot currentSnapshot = metadata.currentSnapshot();
    if (exposeInHmsProperties(maxHiveTablePropertySize) && currentSnapshot != null) {
      parameters.put(TableProperties.CURRENT_SNAPSHOT_ID, String.valueOf(currentSnapshot.snapshotId()));
      parameters.put(TableProperties.CURRENT_SNAPSHOT_TIMESTAMP, String.valueOf(currentSnapshot.timestampMillis()));
      setSnapshotSummary(parameters, currentSnapshot, maxHiveTablePropertySize);
    }

    parameters.put(TableProperties.SNAPSHOT_COUNT, String.valueOf(metadata.snapshots().size()));
  }

  @VisibleForTesting
  static void setSnapshotSummary(
      Map<String, String> parameters,
      Snapshot currentSnapshot,
      long maxHiveTablePropertySize) {
    try {
      String summary = JsonUtil.mapper().writeValueAsString(currentSnapshot.summary());
      if (summary.length() <= maxHiveTablePropertySize) {
        parameters.put(TableProperties.CURRENT_SNAPSHOT_SUMMARY, summary);
      } else {
        LOG.warn("Not exposing the current snapshot({}) summary in HMS since it exceeds {} characters",
            currentSnapshot.snapshotId(), maxHiveTablePropertySize);
      }
    } catch (JsonProcessingException e) {
      LOG.warn("Failed to convert current snapshot({}) summary to a json string", currentSnapshot.snapshotId(), e);
    }
  }

  @VisibleForTesting
  static void setPartitionSpec(TableMetadata metadata, Map<String, String> parameters, long maxHiveTablePropertySize) {
    parameters.remove(TableProperties.DEFAULT_PARTITION_SPEC);
    if (exposeInHmsProperties(maxHiveTablePropertySize) && metadata.spec() != null && metadata.spec().isPartitioned()) {
      String spec = PartitionSpecParser.toJson(metadata.spec());
      setField(parameters, TableProperties.DEFAULT_PARTITION_SPEC, spec, maxHiveTablePropertySize);
    }
  }

  @VisibleForTesting
  static void setSortOrder(TableMetadata metadata, Map<String, String> parameters, long maxHiveTablePropertySize) {
    parameters.remove(TableProperties.DEFAULT_SORT_ORDER);
    if (exposeInHmsProperties(maxHiveTablePropertySize) &&
        metadata.sortOrder() != null &&
        metadata.sortOrder().isSorted()) {
      String sortOrder = SortOrderParser.toJson(metadata.sortOrder());
      setField(parameters, TableProperties.DEFAULT_SORT_ORDER, sortOrder, maxHiveTablePropertySize);
    }
  }

  public static void setSchema(Schema schema, Map<String, String> parameters, long maxHiveTablePropertySize) {
    parameters.remove(TableProperties.CURRENT_SCHEMA);
    if (exposeInHmsProperties(maxHiveTablePropertySize) && schema != null) {
      String jsonSchema = SchemaParser.toJson(schema);
      setField(parameters, TableProperties.CURRENT_SCHEMA, jsonSchema, maxHiveTablePropertySize);
    }
  }

  private static void setField(
      Map<String, String> parameters,
      String key, String value,
      long maxHiveTablePropertySize) {
    if (value.length() <= maxHiveTablePropertySize) {
      parameters.put(key, value);
    } else {
      LOG.warn("Not exposing {} in HMS since it exceeds {} characters", key, maxHiveTablePropertySize);
    }
  }

  private static boolean exposeInHmsProperties(long maxHiveTablePropertySize) {
    return maxHiveTablePropertySize > 0;
  }
}
