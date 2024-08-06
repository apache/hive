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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableUtil {

  public static final int SPEC_IDX = 1;
  public static final int PART_IDX = 0;
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableUtil.class);

  private IcebergTableUtil() {

  }

  /**
   * Constructs the table properties needed for the Iceberg table loading by retrieving the information from the
   * hmsTable. It then calls {@link IcebergTableUtil#getTable(Configuration, Properties)} with these properties.
   * @param configuration a Hadoop configuration
   * @param hmsTable the HMS table
   * @param skipCache if set to true there won't be an attempt to retrieve the table from SessionState
   * @return the Iceberg table
   */
  static Table getTable(Configuration configuration, org.apache.hadoop.hive.metastore.api.Table hmsTable,
      boolean skipCache) {
    Properties properties = new Properties();
    properties.setProperty(Catalogs.NAME, TableIdentifier.of(hmsTable.getDbName(), hmsTable.getTableName()).toString());
    properties.setProperty(Catalogs.LOCATION, hmsTable.getSd().getLocation());
    hmsTable.getParameters().computeIfPresent(InputFormatConfig.CATALOG_NAME,
        (k, v) -> {
          properties.setProperty(k, v);
          return v;
        });
    return getTable(configuration, properties, skipCache);
  }

  public static Table getTable(Configuration configuration, org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    return getTable(configuration, hmsTable, false);
  }

  /**
   * Load the iceberg table either from the {@link QueryState} or through the configured catalog. Look for the table
   * object stored in the query state. If it's null, it means the table was not loaded yet within the same query
   * therefore we claim it through the Catalogs API and then store it in query state.
   * @param configuration a Hadoop configuration
   * @param properties controlling properties
   * @param skipCache if set to true there won't be an attempt to retrieve the table from SessionState
   * @return an Iceberg table
   */
  static Table getTable(Configuration configuration, Properties properties, boolean skipCache) {
    String metaTable = properties.getProperty(IcebergAcidUtil.META_TABLE_PROPERTY);
    String tableName = properties.getProperty(Catalogs.NAME);
    String location = properties.getProperty(Catalogs.LOCATION);
    if (metaTable != null) {
      // HiveCatalog, HadoopCatalog uses NAME to identify the metadata table
      properties.setProperty(Catalogs.NAME, tableName + "." + metaTable);
      // HadoopTable uses LOCATION to identify the metadata table
      properties.setProperty(Catalogs.LOCATION, location + "#" + metaTable);
    }

    String tableIdentifier = properties.getProperty(Catalogs.NAME);
    Function<Void, Table> tableLoadFunc =
        unused -> {
          Table tab = Catalogs.loadTable(configuration, properties);
          SessionStateUtil.addResource(configuration, tableIdentifier, tab);
          return tab;
        };

    if (skipCache) {
      return tableLoadFunc.apply(null);
    } else {
      return SessionStateUtil.getResource(configuration, tableIdentifier).filter(o -> o instanceof Table)
          .map(o -> (Table) o).orElseGet(() -> {
            LOG.debug("Iceberg table {} is not found in QueryState. Loading table from configured catalog",
                tableIdentifier);
            return tableLoadFunc.apply(null);
          });
    }
  }

  static Table getTable(Configuration configuration, Properties properties) {
    return getTable(configuration, properties, false);
  }

  static Optional<Path> getColStatsPath(Table table) {
    return getColStatsPath(table, table.currentSnapshot().snapshotId());
  }

  static Optional<Path> getColStatsPath(Table table, long snapshotId) {
    return table.statisticsFiles().stream()
      .filter(stats -> stats.snapshotId() == snapshotId)
      .filter(stats -> stats.blobMetadata().stream()
        .anyMatch(metadata -> ColumnStatisticsObj.class.getSimpleName().equals(metadata.type()))
      )
      .map(stats -> new Path(stats.path()))
      .findAny();
  }

  /**
   * Create {@link PartitionSpec} based on the partition information stored in
   * {@link TransformSpec}.
   * @param configuration a Hadoop configuration
   * @param schema iceberg table schema
   * @return iceberg partition spec, always non-null
   */
  public static PartitionSpec spec(Configuration configuration, Schema schema) {
    List<TransformSpec> partitionTransformSpecList = SessionStateUtil
            .getResource(configuration, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC)
        .map(o -> (List<TransformSpec>) o).orElseGet(() -> null);

    if (partitionTransformSpecList == null) {
      LOG.debug("Iceberg partition transform spec is not found in QueryState.");
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

  public static void updateSpec(Configuration configuration, Table table) {
    // get the new partition transform spec
    PartitionSpec newPartitionSpec = spec(configuration, table.schema());
    if (newPartitionSpec == null) {
      LOG.debug("Iceberg Partition spec is not updated due to empty partition spec definition.");
      return;
    }

    // delete every field from the old partition spec
    UpdatePartitionSpec updatePartitionSpec = table.updateSpec().caseSensitive(false);
    table.spec().fields().forEach(field -> updatePartitionSpec.removeField(field.name()));

    List<TransformSpec> partitionTransformSpecList = SessionStateUtil
        .getResource(configuration, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC)
        .map(o -> (List<TransformSpec>) o).orElseGet(() -> null);

    partitionTransformSpecList.forEach(spec -> {
      switch (spec.getTransformType()) {
        case IDENTITY:
          updatePartitionSpec.addField(spec.getColumnName());
          break;
        case YEAR:
          updatePartitionSpec.addField(Expressions.year(spec.getColumnName()));
          break;
        case MONTH:
          updatePartitionSpec.addField(Expressions.month(spec.getColumnName()));
          break;
        case DAY:
          updatePartitionSpec.addField(Expressions.day(spec.getColumnName()));
          break;
        case HOUR:
          updatePartitionSpec.addField(Expressions.hour(spec.getColumnName()));
          break;
        case TRUNCATE:
          updatePartitionSpec.addField(Expressions.truncate(spec.getColumnName(), spec.getTransformParam().get()));
          break;
        case BUCKET:
          updatePartitionSpec.addField(Expressions.bucket(spec.getColumnName(), spec.getTransformParam().get()));
          break;
      }
    });

    updatePartitionSpec.commit();
  }

  public static boolean isBucketed(Table table) {
    return table.spec().fields().stream().anyMatch(f -> f.transform().toString().startsWith("bucket["));
  }

  /**
   * Roll an iceberg table's data back to a specific snapshot identified either by id or before a given timestamp.
   * @param table the iceberg table
   * @param type the type of the rollback, can be either time based or version based
   * @param value parameter of the rollback, that can be a timestamp in millis or a snapshot id
   */
  public static void rollback(Table table, AlterTableExecuteSpec.RollbackSpec.RollbackType type, Long value) {
    ManageSnapshots manageSnapshots = table.manageSnapshots();
    if (type == AlterTableExecuteSpec.RollbackSpec.RollbackType.TIME) {
      LOG.debug("Trying to rollback iceberg table to snapshot before timestamp {}", value);
      manageSnapshots.rollbackToTime(value);
    } else {
      LOG.debug("Trying to rollback iceberg table to snapshot ID {}", value);
      manageSnapshots.rollbackTo(value);
    }
    manageSnapshots.commit();
  }

  /**
   * Set the current snapshot for the iceberg table
   * @param table the iceberg table
   * @param value parameter of the rollback, that can be a snapshot id or a SnapshotRef name
   */
  public static void setCurrentSnapshot(Table table, String value) {
    ManageSnapshots manageSnapshots = table.manageSnapshots();
    long snapshotId;
    try {
      snapshotId = Long.parseLong(value);
      LOG.debug("Rolling the iceberg table {} from snapshot id {} to snapshot ID {}", table.name(),
          table.currentSnapshot().snapshotId(), snapshotId);
    } catch (NumberFormatException e) {
      String refName = PlanUtils.stripQuotes(value);
      snapshotId = Optional.ofNullable(table.refs().get(refName)).map(SnapshotRef::snapshotId).orElseThrow(() ->
          new IllegalArgumentException(String.format("SnapshotRef %s does not exist", refName)));
      LOG.debug("Rolling the iceberg table {} from snapshot id {} to the snapshot ID {} of SnapshotRef {}",
          table.name(), table.currentSnapshot().snapshotId(), snapshotId, refName);
    }
    manageSnapshots.setCurrentSnapshot(snapshotId);
    manageSnapshots.commit();
  }

  /**
   * Fast forwards a branch to another.
   * @param table the iceberg table
   * @param sourceBranch the source branch
   * @param targetBranch the target branch
   */
  public static void fastForwardBranch(Table table, String sourceBranch, String targetBranch) {
    LOG.debug("Fast Forwarding the iceberg table {} branch {} to {}", table.name(), sourceBranch, targetBranch);
    table.manageSnapshots().fastForwardBranch(sourceBranch, targetBranch).commit();
  }

  public static void cherryPick(Table table, long snapshotId) {
    LOG.debug("Cherry-Picking {} to {}", snapshotId, table.name());
    table.manageSnapshots().cherrypick(snapshotId).commit();
  }

  public static boolean isV2Table(Map<String, String> props) {
    return props == null ||
        "2".equals(props.get(TableProperties.FORMAT_VERSION)) || props.get(TableProperties.FORMAT_VERSION) == null;
  }

  public static boolean isCopyOnWriteMode(Context.Operation operation, BinaryOperator<String> props) {
    String mode = null;
    switch (operation) {
      case DELETE:
        mode = props.apply(TableProperties.DELETE_MODE,
            TableProperties.DELETE_MODE_DEFAULT);
        break;
      case UPDATE:
        mode = props.apply(TableProperties.UPDATE_MODE,
            TableProperties.UPDATE_MODE_DEFAULT);
        break;
      case MERGE:
        mode = props.apply(TableProperties.MERGE_MODE,
            TableProperties.MERGE_MODE_DEFAULT);
        break;
    }
    return RowLevelOperationMode.COPY_ON_WRITE.modeName().equalsIgnoreCase(mode);
  }

  public static void performMetadataDelete(Table icebergTable, String branchName, SearchArgument sarg) {
    Expression exp = HiveIcebergFilterFactory.generateFilterExpression(sarg);
    DeleteFiles deleteFiles = icebergTable.newDelete();
    if (StringUtils.isNotEmpty(branchName)) {
      deleteFiles = deleteFiles.toBranch(HiveUtils.getTableSnapshotRef(branchName));
    }
    deleteFiles.deleteFromRowFilter(exp).commit();
  }

  public static PartitionData toPartitionData(StructLike key, Types.StructType keyType) {
    PartitionData data = new PartitionData(keyType);
    for (int i = 0; i < keyType.fields().size(); i++) {
      Object val = key.get(i, keyType.fields().get(i).type().typeId().javaClass());
      if (val != null) {
        data.set(i, val);
      }
    }
    return data;
  }

  public static PartitionData toPartitionData(StructLike sourceKey, Types.StructType sourceKeyType,
      Types.StructType targetKeyType) {
    PartitionData data = new PartitionData(targetKeyType);
    for (int i = 0; i < targetKeyType.fields().size(); i++) {

      int fi = i;
      String fieldName = targetKeyType.fields().get(fi).name();
      Object val = sourceKeyType.fields().stream()
          .filter(f -> f.name().equals(fieldName)).findFirst()
          .map(sourceKeyElem -> sourceKey.get(sourceKeyType.fields().indexOf(sourceKeyElem),
              targetKeyType.fields().get(fi).type().typeId().javaClass()))
          .orElseThrow(() -> new RuntimeException(
              String.format("Error retrieving value of partition field %s", fieldName)));

      if (val != null) {
        data.set(fi, val);
      } else {
        throw new RuntimeException(String.format("Partition field's %s value is null", fieldName));
      }
    }
    return data;
  }

  /**
   * Returns list of data files filtered by specId and partitionPath as following:
   *  1. If matchBySpecId is true, then filters files by specId == file's specId, else by specId != file's specId
   *  2. If partitionPath is not null, then also filters files where partitionPath == file's partition path
   * @param table the iceberg table
   * @param specId partition spec id
   * @param partitionPath partition path
   * @param matchBySpecId filter that's applied on data files' spec ids
   */
  public static List<DataFile> getDataFiles(Table table, int specId, String partitionPath,
      Predicate<Object> matchBySpecId) {
    CloseableIterable<FileScanTask> fileScanTasks =
        table.newScan().useSnapshot(table.currentSnapshot().snapshotId()).ignoreResiduals().planFiles();
    CloseableIterable<FileScanTask> filteredFileScanTasks =
        CloseableIterable.filter(fileScanTasks, t -> {
          DataFile file = t.asFileScanTask().file();
          return matchBySpecId.test(file.specId()) && (partitionPath == null || (partitionPath != null &&
                  table.specs().get(specId).partitionToPath(file.partition()).equals(partitionPath)));
        });
    return Lists.newArrayList(CloseableIterable.transform(filteredFileScanTasks, t -> t.file()));
  }

  /**
   * Returns list of delete files filtered by specId and partitionPath as following:
   *  1. If matchBySpecId is true, then filters files by specId == file's specId, else by specId != file's specId
   *  2. If partitionPath is not null, then also filters files where partitionPath == file's partition path
   * @param table the iceberg table
   * @param specId partition spec id
   * @param partitionPath partition path
   * @param matchBySpecId filter that's applied on delete files' spec ids
   */
  public static List<DeleteFile> getDeleteFiles(Table table, int specId, String partitionPath,
      Predicate<Object> matchBySpecId) {
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.POSITION_DELETES);
    CloseableIterable<ScanTask> deletesScanTasks = deletesTable.newBatchScan().planFiles();
    CloseableIterable<ScanTask> filteredDeletesScanTasks =
        CloseableIterable.filter(deletesScanTasks, t -> {
          DeleteFile file = ((PositionDeletesScanTask) t).file();
          return matchBySpecId.test(file.specId()) && (partitionPath == null || (partitionPath != null &&
              table.specs().get(specId).partitionToPath(file.partition()).equals(partitionPath)));
        });
    return Lists.newArrayList(CloseableIterable.transform(filteredDeletesScanTasks,
        t -> ((PositionDeletesScanTask) t).file()));
  }

  public static Expression generateExpressionFromPartitionSpec(Table table, Map<String, String> partitionSpec)
      throws SemanticException {
    Map<String, PartitionField> partitionFieldMap = getPartitionFields(table).stream()
        .collect(Collectors.toMap(PartitionField::name, Function.identity()));
    Expression finalExp = Expressions.alwaysTrue();
    for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
      String partColName = entry.getKey();
      if (partitionFieldMap.containsKey(partColName)) {
        PartitionField partitionField = partitionFieldMap.get(partColName);
        Type resultType = partitionField.transform().getResultType(table.schema()
            .findField(partitionField.sourceId()).type());
        Object value = Conversions.fromPartitionString(resultType, entry.getValue());
        TransformSpec.TransformType transformType = TransformSpec.fromString(partitionField.transform().toString());
        Iterable<?> iterable = () -> Collections.singletonList(value).iterator();
        if (TransformSpec.TransformType.IDENTITY == transformType) {
          Expression boundPredicate = Expressions.in(partitionField.name(), iterable);
          finalExp = Expressions.and(finalExp, boundPredicate);
        } else {
          throw new SemanticException(
              String.format("Partition transforms are not supported via truncate operation: %s", partColName));
        }
      } else {
        throw new SemanticException(String.format("No partition column/transform by the name: %s", partColName));
      }
    }
    return finalExp;
  }

  public static List<FieldSchema> getPartitionKeys(Table table, int specId) {
    Schema schema = table.specs().get(specId).schema();
    List<FieldSchema> hiveSchema = HiveSchemaUtil.convert(schema);
    Map<String, String> colNameToColType = hiveSchema.stream()
        .collect(Collectors.toMap(FieldSchema::getName, FieldSchema::getType));
    return table.specs().get(specId).fields().stream().map(partField ->
        new FieldSchema(schema.findColumnName(partField.sourceId()),
            colNameToColType.get(schema.findColumnName(partField.sourceId())),
            String.format("Transform: %s", partField.transform().toString()))).collect(Collectors.toList());
  }

  public static List<PartitionField> getPartitionFields(Table table) {
    return table.specs().values().stream().flatMap(spec -> spec.fields()
        .stream()).distinct().collect(Collectors.toList());
  }

  public static Map<PartitionData, Integer> getPartitionInfo(Table icebergTable, Map<String, String> partSpecMap,
      boolean allowPartialSpec, boolean latestSpecOnly) throws SemanticException, IOException {
    Expression expression = IcebergTableUtil.generateExpressionFromPartitionSpec(icebergTable, partSpecMap);
    PartitionsTable partitionsTable = (PartitionsTable) MetadataTableUtils
        .createMetadataTableInstance(icebergTable, MetadataTableType.PARTITIONS);

    Map<PartitionData, Integer> result = Maps.newLinkedHashMap();
    try (CloseableIterable<FileScanTask> fileScanTasks = partitionsTable.newScan().planFiles()) {
      fileScanTasks.forEach(task ->
          CloseableIterable.filter(
              CloseableIterable.transform(task.asDataTask().rows(), row -> {
                StructProjection data = row.get(IcebergTableUtil.PART_IDX, StructProjection.class);
                Integer specId = row.get(IcebergTableUtil.SPEC_IDX, Integer.class);
                return Maps.immutableEntry(IcebergTableUtil.toPartitionData(data,
                    Partitioning.partitionType(icebergTable), icebergTable.specs().get(specId).partitionType()),
                    specId);
              }), entry -> {
                ResidualEvaluator resEval = ResidualEvaluator.of(icebergTable.specs().get(entry.getValue()),
                    expression, false);
                return resEval.residualFor(entry.getKey()).isEquivalentTo(Expressions.alwaysTrue()) &&
                    (entry.getKey().size() == partSpecMap.size() || allowPartialSpec) &&
                    (entry.getValue() == icebergTable.spec().specId() || !latestSpecOnly);
              }).forEach(entry -> result.put(entry.getKey(), entry.getValue())));
    }

    return result;
  }

  public static List<String> getPartitionNames(Table icebergTable, Map<String, String> partitionSpec,
      boolean latestSpecOnly) throws SemanticException {
    try {
      return IcebergTableUtil
          .getPartitionInfo(icebergTable, partitionSpec, true, latestSpecOnly).entrySet().stream()
          .map(e -> {
            PartitionData partitionData = e.getKey();
            int specId = e.getValue();
            return icebergTable.specs().get(specId).partitionToPath(partitionData);
          }).collect(Collectors.toList());
    } catch (IOException e) {
      throw new SemanticException(String.format("Error while fetching the partitions due to: %s", e));
    }
  }
}
