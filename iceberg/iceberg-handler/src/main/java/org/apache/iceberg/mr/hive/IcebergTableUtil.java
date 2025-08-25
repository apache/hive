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
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.utils.TableFetcher;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.hadoop.hive.ql.parse.TransformSpec.TransformType;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.util.Sets;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
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
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.StructProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.RowLevelOperationMode.COPY_ON_WRITE;
import static org.apache.iceberg.RowLevelOperationMode.MERGE_ON_READ;
import static org.apache.iceberg.mr.InputFormatConfig.CATALOG_NAME;
import static org.apache.iceberg.mr.InputFormatConfig.CATALOG_WAREHOUSE_TEMPLATE;

public class IcebergTableUtil {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableUtil.class);

  public static final String PARTITION_TRANSFORM_SPEC_NOT_FOUND =
      "Iceberg partition transform spec is not found in QueryState.";

  public static final int SPEC_IDX = 1;
  public static final int PART_IDX = 0;

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

    Properties props = new Properties(properties); // use input properties as default
    if (metaTable != null) {
      // HiveCatalog, HadoopCatalog uses NAME to identify the metadata table
      props.put(Catalogs.NAME, properties.get(Catalogs.NAME) + "." + metaTable);
      // HadoopTable uses LOCATION to identify the metadata table
      props.put(Catalogs.LOCATION, properties.get(Catalogs.LOCATION) + "#" + metaTable);
    }
    String tableIdentifier = props.getProperty(Catalogs.NAME);
    Function<Void, Table> tableLoadFunc =
        unused -> {
          Table tab = Catalogs.loadTable(configuration, props);
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

  static Snapshot getTableSnapshot(Table table, org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    long snapshotId = -1;

    if (hmsTable.getAsOfTimestamp() != null) {
      ZoneId timeZone = SessionState.get() == null ?
          new HiveConf().getLocalTimeZone() : SessionState.get().getConf().getLocalTimeZone();
      TimestampTZ time = TimestampTZUtil.parse(hmsTable.getAsOfTimestamp(), timeZone);
      snapshotId = SnapshotUtil.snapshotIdAsOfTime(table, time.toEpochMilli());

    } else if (hmsTable.getAsOfVersion() != null) {
      try {
        snapshotId = Long.parseLong(hmsTable.getAsOfVersion());
      } catch (NumberFormatException e) {
        SnapshotRef ref = table.refs().get(hmsTable.getAsOfVersion());
        if (ref == null) {
          throw new RuntimeException("Cannot find matching snapshot ID or reference name for version " +
              hmsTable.getAsOfVersion());
        }
        snapshotId = ref.snapshotId();
      }
    }
    if (snapshotId > 0) {
      return table.snapshot(snapshotId);
    }
    return getTableSnapshot(table, hmsTable.getSnapshotRef());
  }

  static Snapshot getTableSnapshot(Table table, String snapshotRef) {
    if (snapshotRef != null) {
      String ref = HiveUtils.getTableSnapshotRef(snapshotRef);
      return table.snapshot(ref);
    }
    return table.currentSnapshot();
  }

  static Path getColStatsPath(Table table) {
    return getColStatsPath(table, table.currentSnapshot().snapshotId());
  }

  static Path getColStatsPath(Table table, long snapshotId) {
    return table.statisticsFiles().stream()
      .filter(stats -> stats.snapshotId() == snapshotId)
      .filter(stats -> stats.blobMetadata().stream()
        .anyMatch(metadata -> ColumnStatisticsObj.class.getSimpleName().equals(metadata.type()))
      )
      .map(stats -> new Path(stats.path()))
      .findAny().orElse(null);
  }

  static PartitionStatisticsFile getPartitionStatsFile(Table table, long snapshotId) {
    return table.partitionStatisticsFiles().stream()
      .filter(stats -> stats.snapshotId() == snapshotId)
      .findAny().orElse(null);
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
        .map(o -> (List<TransformSpec>) o).orElse(null);

    if (partitionTransformSpecList == null) {
      LOG.warn(PARTITION_TRANSFORM_SPEC_NOT_FOUND);
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
          builder.truncate(spec.getColumnName(), spec.getTransformParam());
          break;
        case BUCKET:
          builder.bucket(spec.getColumnName(), spec.getTransformParam());
          break;
      }
    });
    return builder.build();
  }

  public static void updateSpec(Configuration configuration, Table table) {
    // get the new partition transform spec
    PartitionSpec newPartitionSpec = spec(configuration, table.schema());
    if (newPartitionSpec == null) {
      LOG.warn("Iceberg partition spec is not updated due to empty partition spec definition.");
      return;
    }

    // delete every field from the old partition spec
    UpdatePartitionSpec updatePartitionSpec = table.updateSpec().caseSensitive(false);
    table.spec().fields().forEach(field -> updatePartitionSpec.removeField(field.name()));

    List<TransformSpec> partitionTransformSpecList = SessionStateUtil
        .getResource(configuration, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC)
        .map(o -> (List<TransformSpec>) o).orElse(null);

    if (partitionTransformSpecList == null) {
      LOG.warn(PARTITION_TRANSFORM_SPEC_NOT_FOUND);
      return;
    }
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
          updatePartitionSpec.addField(Expressions.truncate(spec.getColumnName(), spec.getTransformParam()));
          break;
        case BUCKET:
          updatePartitionSpec.addField(Expressions.bucket(spec.getColumnName(), spec.getTransformParam()));
          break;
      }
    });

    updatePartitionSpec.commit();
  }

  public static boolean isBucketed(Table table) {
    return table.spec().fields().stream().anyMatch(f -> f.transform().toString().startsWith("bucket["));
  }

  public static boolean isBucket(TransformSpec spec) {
    // Iceberg's bucket transform requires a bucket number to be specified
    return spec.getTransformType() == TransformType.BUCKET && spec.getTransformParam() != null;
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

  public static boolean isV2TableOrAbove(Map<String, String> props) {
    return IcebergTableUtil.formatVersion(props) >= 2;
  }

  public static boolean isV2TableOrAbove(BinaryOperator<String> props) {
    return IcebergTableUtil.formatVersion(props) >= 2;
  }

  public static Integer formatVersion(Map<String, String> props) {
    if (props == null) {
      // TODO: switch to v3 once fully supported
      return 2; // default to v2
    }
    return IcebergTableUtil.formatVersion(props::getOrDefault);
  }

  private static Integer formatVersion(BinaryOperator<String> props) {
    String version = props.apply(TableProperties.FORMAT_VERSION, null);
    if (version == null) {
      // TODO: switch to v3 once fully supported
      return 2; // default to v2
    }
    try {
      return Integer.parseInt(version);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid format version: " + version, e);
    }
  }

  private static String getWriteModeDefault(BinaryOperator<String> props) {
    return (isV2TableOrAbove(props) ? MERGE_ON_READ : COPY_ON_WRITE).modeName();
  }

  public static boolean isCopyOnWriteMode(Context.Operation operation, BinaryOperator<String> props) {
    final String mode = switch (operation) {
      case DELETE -> props.apply(
          TableProperties.DELETE_MODE, getWriteModeDefault(props));
      case UPDATE -> props.apply(
          TableProperties.UPDATE_MODE, getWriteModeDefault(props));
      case MERGE -> props.apply(
          TableProperties.MERGE_MODE, getWriteModeDefault(props));
      default -> null;
    };
    return COPY_ON_WRITE.modeName().equalsIgnoreCase(mode);
  }

  public static boolean isFanoutEnabled(Map<String, String> props) {
    return PropertyUtil.propertyAsBoolean(props, InputFormatConfig.WRITE_FANOUT_ENABLED, true);
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
    PartitionData keyTemplate = new PartitionData(keyType);
    return keyTemplate.copyFor(key);
  }

  public static PartitionData toPartitionData(StructLike sourceKey, Types.StructType sourceKeyType,
      Types.StructType targetKeyType) {
    StructProjection projection = StructProjection.create(sourceKeyType, targetKeyType).wrap(sourceKey);
    return toPartitionData(projection, targetKeyType);
  }

  public static Expression generateExpressionFromPartitionSpec(Table table, Map<String, String> partitionSpec,
      boolean latestSpecOnly) throws SemanticException {
    Map<String, PartitionField> partitionFieldMap = getPartitionFields(table, latestSpecOnly).stream()
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
    return table.specs().get(specId).fields().stream()
        .map(partField -> new FieldSchema(
            schema.findColumnName(partField.sourceId()),
            colNameToColType.get(schema.findColumnName(partField.sourceId())),
            String.format("Transform: %s", partField.transform().toString()))
        )
        .collect(Collectors.toList());
  }

  public static List<PartitionField> getPartitionFields(Table table, boolean latestSpecOnly) {
    return latestSpecOnly ? table.spec().fields() :
      table.specs().values().stream()
        .flatMap(spec -> spec.fields().stream()
            .filter(f -> !f.transform().isVoid()))
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * Returns a list of partition names satisfying the provided partition spec.
   * @param table Iceberg table
   * @param partSpecMap Partition Spec used as the criteria for filtering
   * @param latestSpecOnly when True, returns partitions with the current spec only, else - any specs
   * @return List of partition names
   */
  public static List<String> getPartitionNames(Table table, Map<String, String> partSpecMap,
      boolean latestSpecOnly) throws SemanticException {
    Expression expression = IcebergTableUtil.generateExpressionFromPartitionSpec(
        table, partSpecMap, latestSpecOnly);
    PartitionsTable partitionsTable = (PartitionsTable) MetadataTableUtils.createMetadataTableInstance(
        table, MetadataTableType.PARTITIONS);

    try (CloseableIterable<FileScanTask> fileScanTasks = partitionsTable.newScan().planFiles()) {
      return FluentIterable.from(fileScanTasks)
          .transformAndConcat(task -> task.asDataTask().rows())
          .transform(row -> {
            StructLike data = row.get(IcebergTableUtil.PART_IDX, StructProjection.class);
            PartitionSpec spec = table.specs().get(row.get(IcebergTableUtil.SPEC_IDX, Integer.class));
            return Maps.immutableEntry(
                IcebergTableUtil.toPartitionData(
                    data, Partitioning.partitionType(table), spec.partitionType()),
                spec);
          }).filter(e -> {
            ResidualEvaluator resEval = ResidualEvaluator.of(e.getValue(),
                expression, false);
            return e.getValue().isPartitioned() &&
              resEval.residualFor(e.getKey()).isEquivalentTo(Expressions.alwaysTrue()) &&
              (e.getValue().specId() == table.spec().specId() || !latestSpecOnly);

          }).transform(e -> e.getValue().partitionToPath(e.getKey())).toSortedList(
            Comparator.naturalOrder());

    } catch (IOException e) {
      throw new SemanticException(
          String.format("Error while fetching the partitions due to: %s", e));
    }
  }

  public static PartitionSpec getPartitionSpec(Table icebergTable, String partitionPath)
      throws MetaException, HiveException {
    if (icebergTable == null || partitionPath == null || partitionPath.isEmpty()) {
      throw new HiveException("Table and partitionPath must not be null or empty.");
    }

    // Extract field names from the path: "field1=val1/field2=val2" → [field1, field2]
    List<String> fieldNames = Lists.newArrayList(Warehouse.makeSpecFromName(partitionPath).keySet());

    return icebergTable.specs().values().stream()
        .filter(spec -> {
          List<String> specFieldNames = spec.fields().stream()
              .map(PartitionField::name)
              .toList();
          return specFieldNames.equals(fieldNames);
        })
        .findFirst() // Supposed to be only one matching spec
        .orElseThrow(() -> new HiveException("No matching partition spec found for partition path: " + partitionPath));
  }

  public static TransformSpec getTransformSpec(Table table, String transformName, int sourceId) {
    TransformSpec spec = TransformSpec.fromString(transformName.toUpperCase(),
        table.schema().findColumnName(sourceId));
    return spec;
  }

  public static <T> List<T> readColStats(Table table, Long snapshotId, Predicate<BlobMetadata> filter) {
    List<T> colStats = Lists.newArrayList();

    Path statsPath  = IcebergTableUtil.getColStatsPath(table, snapshotId);
    if (statsPath == null) {
      return colStats;
    }
    try (PuffinReader reader = Puffin.read(table.io().newInputFile(statsPath.toString())).build()) {
      List<BlobMetadata> blobMetadata = reader.fileMetadata().blobs();

      if (filter != null) {
        blobMetadata = blobMetadata.stream().filter(filter)
          .toList();
      }
      Iterator<ByteBuffer> it = Iterables.transform(reader.readAll(blobMetadata), Pair::second).iterator();
      LOG.info("Using column stats from: {}", statsPath);

      while (it.hasNext()) {
        byte[] byteBuffer = ByteBuffers.toByteArray(it.next());
        colStats.add(SerializationUtils.deserialize(byteBuffer));
      }
    } catch (Exception e) {
      LOG.warn("Unable to read column stats: {}", e.getMessage());
    }
    return colStats;
  }

  public static ExecutorService newDeleteThreadPool(String completeName, int numThreads) {
    AtomicInteger deleteThreadsIndex = new AtomicInteger(0);
    return Executors.newFixedThreadPool(numThreads, runnable -> {
      Thread thread = new Thread(runnable);
      thread.setName("remove-snapshot-" + completeName + "-" + deleteThreadsIndex.getAndIncrement());
      return thread;
    });
  }

  public static boolean hasUndergonePartitionEvolution(Table table) {
    // The current spec is not necessary the latest which can happen when partition spec was changed to one of
    // table's past specs.
    return table.currentSnapshot() != null &&
        table.currentSnapshot().allManifests(table.io()).parallelStream()
            .map(ManifestFile::partitionSpecId)
            .anyMatch(id -> id != table.spec().specId());
  }

  public static <T extends ContentFile<?>> Set<String> getPartitionNames(Table icebergTable, Iterable<T> files,
      Boolean latestSpecOnly) {
    Set<String> partitions = Sets.newHashSet();
    int tableSpecId = icebergTable.spec().specId();
    for (T file : files) {
      if (latestSpecOnly == null || Boolean.TRUE.equals(latestSpecOnly) && file.specId() == tableSpecId ||
          Boolean.FALSE.equals(latestSpecOnly) && file.specId() != tableSpecId) {
        String partName = icebergTable.specs().get(file.specId()).partitionToPath(file.partition());
        partitions.add(partName);
      }
    }
    return partitions;
  }

  public static List<Partition> convertNameToMetastorePartition(org.apache.hadoop.hive.ql.metadata.Table hmsTable,
      Collection<String> partNames) {
    List<Partition> partitions = Lists.newArrayList();
    for (String partName : partNames) {
      Map<String, String> partSpecMap = Maps.newLinkedHashMap();
      Warehouse.makeSpecFromName(partSpecMap, new Path(partName), null);
      partitions.add(new DummyPartition(hmsTable, partName, partSpecMap));
    }
    return partitions;
  }

  public static TableFetcher getTableFetcher(IMetaStoreClient msc, String catalogName, String dbPattern,
      String tablePattern) {
    return new TableFetcher.Builder(msc, catalogName, dbPattern, tablePattern).tableTypes(
            "EXTERNAL_TABLE")
        .tableCondition(
            hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS + "table_type like \"ICEBERG\" ")
        .build();
  }

  public static String defaultWarehouseLocation(TableIdentifier tableIdentifier,
      Configuration conf, Properties catalogProperties) {
    StringBuilder sb = new StringBuilder();
    String warehouseLocation = conf.get(String.format(
        CATALOG_WAREHOUSE_TEMPLATE, catalogProperties.getProperty(CATALOG_NAME))
    );
    sb.append(warehouseLocation).append('/');
    for (String level : tableIdentifier.namespace().levels()) {
      sb.append(level).append('/');
    }
    sb.append(tableIdentifier.name());
    return sb.toString();
  }

}
