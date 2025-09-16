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
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.DropPartitionsExpr;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.PartitionTransform;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.hive.CachedClientPool;
import org.apache.iceberg.hive.CatalogUtils;
import org.apache.iceberg.hive.HiveLock;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.hive.MetastoreLock;
import org.apache.iceberg.hive.NoLock;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructProjection;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergMetaHook extends BaseHiveIcebergMetaHook {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergMetaHook.class);
  static final EnumSet<AlterTableType> SUPPORTED_ALTER_OPS = EnumSet.of(
      AlterTableType.ADDCOLS, AlterTableType.REPLACE_COLUMNS, AlterTableType.RENAME_COLUMN, AlterTableType.DROP_COLUMN,
      AlterTableType.ADDPROPS, AlterTableType.DROPPROPS, AlterTableType.SETPARTITIONSPEC,
      AlterTableType.UPDATE_COLUMNS, AlterTableType.RENAME, AlterTableType.EXECUTE, AlterTableType.CREATE_BRANCH,
      AlterTableType.CREATE_TAG, AlterTableType.DROP_BRANCH, AlterTableType.RENAME_BRANCH, AlterTableType.DROPPARTITION,
      AlterTableType.DROP_TAG, AlterTableType.COMPACT, AlterTableType.REPLACE_SNAPSHOTREF);
  private static final List<String> MIGRATION_ALLOWED_SOURCE_FORMATS = ImmutableList.of(
      FileFormat.PARQUET.name().toLowerCase(),
      FileFormat.ORC.name().toLowerCase(),
      FileFormat.AVRO.name().toLowerCase());
  private static final PartitionDropOptions DROP_OPTIONS = new PartitionDropOptions().deleteData(false).ifExists(true);
  private static final List<org.apache.commons.lang3.tuple.Pair<Integer, byte[]>> EMPTY_FILTER =
      Lists.newArrayList(org.apache.commons.lang3.tuple.Pair.of(1, new byte[0]));
  static final String MIGRATED_TO_ICEBERG = "MIGRATED_TO_ICEBERG";
  static final String DECIMAL64_VECTORIZATION = "iceberg.decimal64.vectorization";
  static final String MANUAL_ICEBERG_METADATA_LOCATION_CHANGE = "MANUAL_ICEBERG_METADATA_LOCATION_CHANGE";

  private boolean deleteIcebergTable;
  private FileIO deleteIo;
  private TableMetadata deleteMetadata;
  private boolean isTableMigration;
  private PreAlterTableProperties preAlterTableProperties;
  private UpdateSchema updateSchema;
  private Transaction transaction;
  private AlterTableType currentAlterTableOp;
  private HiveLock commitLock;

  public HiveIcebergMetaHook(Configuration conf) {
    super(conf);
  }

  @Override
  public void rollbackCreateTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    // do nothing
  }

  @Override
  public void commitCreateTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    if (icebergTable == null) {

      setFileFormat(catalogProperties.getProperty(TableProperties.DEFAULT_FILE_FORMAT));

      String metadataLocation = hmsTable.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
      Table table;
      if (metadataLocation != null) {
        table = Catalogs.registerTable(conf, catalogProperties, metadataLocation);
      } else {
        table = Catalogs.createTable(conf, catalogProperties);
      }

      if (!HiveTableUtil.isCtas(catalogProperties)) {
        return;
      }

      // set this in the query state so that we can rollback the table in the lifecycle hook in case of failures
      String tableIdentifier = catalogProperties.getProperty(Catalogs.NAME);
      SessionStateUtil.addResource(conf, InputFormatConfig.CTAS_TABLE_NAME, tableIdentifier);
      SessionStateUtil.addResource(conf, tableIdentifier, table);

      HiveTableUtil.createFileForTableObject(table, conf);
    }
  }

  @Override
  public void preDropTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    // do nothing
  }

  @Override
  public void preDropTable(org.apache.hadoop.hive.metastore.api.Table hmsTable, boolean deleteData) {
    this.catalogProperties = CatalogUtils.getCatalogProperties(hmsTable);
    this.deleteIcebergTable = hmsTable.getParameters() != null &&
        "TRUE".equalsIgnoreCase(hmsTable.getParameters().get(InputFormatConfig.EXTERNAL_TABLE_PURGE));

    if (deleteIcebergTable && Catalogs.hiveCatalog(conf, catalogProperties) && deleteData) {
      // Store the metadata and the io for deleting the actual table data
      try {
        String metadataLocation = hmsTable.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
        this.deleteIo = Catalogs.loadTable(conf, catalogProperties).io();
        this.deleteMetadata = TableMetadataParser.read(deleteIo, metadataLocation);
      } catch (Exception e) {
        LOG.error("preDropTable: Error during loading Iceberg table or parsing its metadata for HMS table: {}.{}. " +
                "In some cases, this might lead to undeleted metadata files under the table directory: {}. " +
                "Please double check and, if needed, manually delete any dangling files/folders, if any. " +
                "In spite of this error, the HMS table drop operation should proceed as normal.",
            hmsTable.getDbName(), hmsTable.getTableName(), hmsTable.getSd().getLocation(), e);
      }
    }
  }

  @Override
  public void rollbackDropTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    // do nothing
  }

  @Override
  public void commitDropTable(org.apache.hadoop.hive.metastore.api.Table hmsTable, boolean deleteData) {
    if (deleteData && deleteIcebergTable) {
      try {
        if (!Catalogs.hiveCatalog(conf, catalogProperties)) {
          LOG.info("Dropping with purge all the data for table {}.{}", hmsTable.getDbName(), hmsTable.getTableName());
          Catalogs.dropTable(conf, catalogProperties);
        } else {
          // do nothing if metadata folder has been deleted already (Hive 4 behaviour for purge=TRUE)
          if (deleteMetadata != null && deleteIo.newInputFile(deleteMetadata.location()).exists()) {
            CatalogUtil.dropTableData(deleteIo, deleteMetadata);
          }
        }
      } catch (Exception e) {
        // we want to successfully complete the Hive DROP TABLE command despite catalog-related exceptions here
        // e.g. we wish to successfully delete a Hive table even if the underlying Hadoop table has already been deleted
        LOG.warn("Exception during commitDropTable operation for table {}.{}.",
            hmsTable.getDbName(), hmsTable.getTableName(), e);
      }
    }
  }

  @Override
  public void preAlterTable(org.apache.hadoop.hive.metastore.api.Table hmsTable, EnvironmentContext context)
      throws MetaException {
    catalogProperties = CatalogUtils.getCatalogProperties(hmsTable);
    setupAlterOperationType(hmsTable, context);
    if (AlterTableType.RENAME.equals(currentAlterTableOp)) {
      catalogProperties.put(Catalogs.NAME, TableIdentifier.of(context.getProperties().get(OLD_DB_NAME),
          context.getProperties().get(OLD_TABLE_NAME)).toString());
    }
    if (commitLock == null) {
      commitLock = lockObject(hmsTable);
    }

    try {
      commitLock.lock();
      doPreAlterTable(hmsTable, context);
    } catch (Exception e) {
      commitLock.unlock();
      throw new MetaException(StringUtils.stringifyException(e));
    }
  }

  private HiveLock lockObject(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    if (!HiveTableOperations.hiveLockEnabled(hmsTable.getParameters(), conf) ||
        SessionStateUtil.getQueryState(conf)
            .map(QueryState::getHiveOperation)
            .filter(opType -> HiveOperation.QUERY == opType)
            .isPresent()) {
      return new NoLock();
    } else {
      return new MetastoreLock(
          conf,
          new CachedClientPool(conf, Maps.fromProperties(catalogProperties)),
          catalogProperties.getProperty(Catalogs.NAME), hmsTable.getDbName(),
          hmsTable.getTableName());
    }
  }

  private void doPreAlterTable(org.apache.hadoop.hive.metastore.api.Table hmsTable, EnvironmentContext context)
      throws MetaException {
    try {
      icebergTable = IcebergTableUtil.getTable(conf, catalogProperties, true);
    } catch (NoSuchTableException nte) {
      context.getProperties().put(MIGRATE_HIVE_TO_ICEBERG, "true");
      // If the iceberg table does not exist, then this is an ALTER command aimed at migrating the table to iceberg
      // First we must check whether it's eligible for migration to iceberg
      // If so, we will create the iceberg table in commitAlterTable and go ahead with the migration
      assertTableCanBeMigrated(hmsTable);
      isTableMigration = true;
      // Set whether the format is ORC, to be used during vectorization.
      setOrcOnlyFilesParam(hmsTable);

      StorageDescriptor sd = hmsTable.getSd();
      preAlterTableProperties = new PreAlterTableProperties();
      preAlterTableProperties.tableLocation = sd.getLocation();
      preAlterTableProperties.format = sd.getInputFormat();
      preAlterTableProperties.schema =
          schema(catalogProperties, hmsTable, Collections.emptySet(), Collections.emptyMap());
      preAlterTableProperties.partitionKeys = hmsTable.getPartitionKeys();

      context.getProperties().put(HiveMetaHook.ALLOW_PARTITION_KEY_CHANGE, "true");
      // If there are partition keys specified remove them from the HMS table and add them to the column list
      try {
        Hive db = SessionState.get().getHiveDb();
        preAlterTableProperties.partitionSpecProxy = db.getMSC().listPartitionSpecs(
            hmsTable.getCatName(), hmsTable.getDbName(), hmsTable.getTableName(), Integer.MAX_VALUE);
        if (hmsTable.isSetPartitionKeys() && !hmsTable.getPartitionKeys().isEmpty()) {
          db.dropPartitions(hmsTable.getDbName(), hmsTable.getTableName(), EMPTY_FILTER, DROP_OPTIONS);

          List<TransformSpec> spec = PartitionTransform.getPartitionTransformSpec(hmsTable.getPartitionKeys());
          SessionStateUtil.addResourceOrThrow(conf, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC, spec);
          hmsTable.getSd().getCols().addAll(hmsTable.getPartitionKeys());
          hmsTable.setPartitionKeysIsSet(false);
        }
      } catch (MetaException me) {
        throw me;
      } catch (HiveException | TException e) {
        throw new MetaException(StringUtils.stringifyException(e));
      }

      preAlterTableProperties.spec = spec(conf, preAlterTableProperties.schema, hmsTable);

      sd.setInputFormat(HiveIcebergInputFormat.class.getCanonicalName());
      sd.setOutputFormat(HiveIcebergOutputFormat.class.getCanonicalName());
      sd.setSerdeInfo(new SerDeInfo("icebergSerde", HiveIcebergSerDe.class.getCanonicalName(), Collections.emptyMap()));
      setCommonHmsTablePropertiesForIceberg(hmsTable);
      // set an additional table prop to designate that this table has been migrated to Iceberg, i.e.
      // all or some of its data files have not been written out using the Iceberg writer, and therefore those data
      // files do not contain Iceberg field IDs. This makes certain schema evolution operations problematic, so we
      // want to disable these ops for now using this new table prop
      hmsTable.getParameters().put(MIGRATED_TO_ICEBERG, "true");
      NameMapping nameMapping = MappingUtil.create(preAlterTableProperties.schema);
      hmsTable.getParameters().put(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(nameMapping));
    }

    if (AlterTableType.ADDCOLS.equals(currentAlterTableOp)) {
      handleAddColumns(hmsTable);
    } else if (AlterTableType.REPLACE_COLUMNS.equals(currentAlterTableOp)) {
      assertNotMigratedTable(hmsTable.getParameters(), currentAlterTableOp.getName().toUpperCase());
      handleReplaceColumns(hmsTable);
    } else if (AlterTableType.DROP_COLUMN.equals(currentAlterTableOp)) {
      assertNotMigratedTable(hmsTable.getParameters(), currentAlterTableOp.getName().toUpperCase());
      handleDropColumn(hmsTable);
    } else if (AlterTableType.RENAME_COLUMN.equals(currentAlterTableOp)) {
      // passing in the "CHANGE COLUMN" string instead, since RENAME COLUMN is not part of SQL syntax (not to mention
      // that users can change data types or reorder columns too with this alter op type, so its name is misleading..)
      assertNotMigratedTable(hmsTable.getParameters(), "CHANGE COLUMN");
      handleChangeColumn(hmsTable);
    } else {
      setWriteModeDefaults(icebergTable, hmsTable.getParameters(), context);
      assertNotCrossTableMetadataLocationChange(hmsTable.getParameters(), context);
    }

    // Migration case is already handled above, in case of migration we don't have all the properties set till this
    // point.
    if (!isTableMigration) {
      // Set whether the format is ORC, to be used during vectorization.
      setOrcOnlyFilesParam(hmsTable);
    }

  }

  /**
   * Perform a check on the current iceberg table whether a metadata change can be performed. A table is eligible if
   * the current metadata uuid and the new metadata uuid matches.
   * @param tblParams hms table properties, must be non-null
   */
  private void assertNotCrossTableMetadataLocationChange(Map<String, String> tblParams, EnvironmentContext context) {
    if (tblParams.containsKey(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)) {
      Preconditions.checkArgument(icebergTable != null,
          "Cannot perform table migration to Iceberg and setting the snapshot location in one step. " +
              "Please migrate the table first");
      String newMetadataLocation = tblParams.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
      FileIO io = ((BaseTable) icebergTable).operations().io();
      TableMetadata newMetadata = TableMetadataParser.read(io, newMetadataLocation);
      TableMetadata currentMetadata = ((BaseTable) icebergTable).operations().current();
      if (!currentMetadata.uuid().equals(newMetadata.uuid())) {
        throw new UnsupportedOperationException(
            String.format("Cannot change iceberg table %s metadata location pointing to another table's metadata %s",
                icebergTable.name(), newMetadataLocation)
        );
      }
      if (!currentMetadata.metadataFileLocation().equals(newMetadataLocation) &&
          !context.getProperties().containsKey(MANUAL_ICEBERG_METADATA_LOCATION_CHANGE)) {
        // If we got here then this is an alter table operation where the table to be changed had an Iceberg commit
        // meanwhile. The base metadata locations differ, while we know that this wasn't an intentional, manual
        // metadata_location set by a user. To protect the interim commit we need to refresh the metadata file location.
        tblParams.put(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, currentMetadata.metadataFileLocation());
        LOG.warn("Detected an alter table operation attempting to do alterations on an Iceberg table with a stale " +
            "metadata_location. Considered base metadata_location: {}, Actual metadata_location: {}. Will override " +
            "this request with the refreshed metadata_location in order to preserve the concurrent commit.",
            newMetadataLocation, currentMetadata.metadataFileLocation());
      }
    }
  }

  private void assertNotMigratedTable(Map<String, String> tblParams, String opType) {
    if (Boolean.parseBoolean(tblParams.get(MIGRATED_TO_ICEBERG))) {
      throw new UnsupportedOperationException(
          String.format("Cannot perform %s operation on a migrated Iceberg table.", opType));
    }
  }

  /**
   * Checks if the table can be migrated to iceberg format. An eligible table is:
   * - external
   * - not temporary
   * - not acid
   * - uses one of supported file formats
   * @param hmsTable the table which should be migrated to iceberg, if eligible
   * @throws MetaException if the table is not eligible for migration due to violating one of the conditions above
   */
  private void assertTableCanBeMigrated(org.apache.hadoop.hive.metastore.api.Table hmsTable)
      throws MetaException {
    StorageDescriptor sd = hmsTable.getSd();
    boolean hasCorrectTableType = MetaStoreUtils.isExternalTable(hmsTable) && !hmsTable.isTemporary() &&
        !AcidUtils.isTransactionalTable(hmsTable);
    if (!hasCorrectTableType) {
      throw new MetaException("Converting non-external, temporary or transactional hive table to iceberg " +
          "table is not allowed.");
    }
    boolean hasCorrectFileFormat = MIGRATION_ALLOWED_SOURCE_FORMATS.stream()
        .anyMatch(f -> sd.getInputFormat().toLowerCase().contains(f));
    if (!hasCorrectFileFormat) {
      throw new MetaException("Cannot convert hive table to iceberg with input format: " + sd.getInputFormat());
    }

    List<TypeInfo> typeInfos = hmsTable.getSd().getCols().stream()
        .map(f -> TypeInfoUtils.getTypeInfoFromTypeString(f.getType()))
        .toList();
    for (TypeInfo typeInfo : typeInfos) {
      validateColumnType(typeInfo);
    }
  }

  private void validateColumnType(TypeInfo typeInfo) throws MetaException {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
            ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
        if (primitiveCategory.equals(PrimitiveObjectInspector.PrimitiveCategory.CHAR) || primitiveCategory.equals(
            PrimitiveObjectInspector.PrimitiveCategory.VARCHAR)) {
          throw new MetaException(String.format(
              "Cannot convert hive table to iceberg that contains column type %s. " + "Use string type columns instead",
             primitiveCategory));
        }
        break;
      case STRUCT:
        List<TypeInfo> structTypeInfos = ((StructTypeInfo) typeInfo).getAllStructFieldTypeInfos();
        for (TypeInfo structTypeInfo : structTypeInfos) {
          validateColumnType(structTypeInfo);
        }
        break;
      case LIST:
        validateColumnType(((ListTypeInfo) typeInfo).getListElementTypeInfo());
        break;
      case MAP:
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        validateColumnType(mapTypeInfo.getMapKeyTypeInfo());
        validateColumnType(mapTypeInfo.getMapValueTypeInfo());
        break;
    }
  }


  @Override
  public void commitAlterTable(org.apache.hadoop.hive.metastore.api.Table hmsTable, EnvironmentContext context)
      throws MetaException {
    if (commitLock == null) {
      throw new IllegalStateException("Hive commit lock should already be set");
    }
    commitLock.unlock();
    if (isTableMigration) {
      catalogProperties = CatalogUtils.getCatalogProperties(hmsTable);
      catalogProperties.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(preAlterTableProperties.schema));
      catalogProperties.put(InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(preAlterTableProperties.spec));
      setFileFormat(preAlterTableProperties.format);
      HiveTableUtil.importFiles(preAlterTableProperties.tableLocation, preAlterTableProperties.format,
          preAlterTableProperties.partitionSpecProxy, preAlterTableProperties.partitionKeys, catalogProperties, conf);
    } else if (currentAlterTableOp != null) {
      switch (currentAlterTableOp) {
        case DROP_COLUMN:
        case REPLACE_COLUMNS:
        case RENAME_COLUMN:
        case ADDCOLS:
          if (transaction != null) {
            transaction.commitTransaction();
          }
          break;
        case ADDPROPS:
        case DROPPROPS:
          alterTableProperties(hmsTable, context.getProperties());
          break;
        case SETPARTITIONSPEC:
          IcebergTableUtil.updateSpec(conf, icebergTable);
          break;
        case RENAME:
          Catalogs.renameTable(conf, catalogProperties, TableIdentifier.of(hmsTable.getDbName(),
              hmsTable.getTableName()));
          break;
      }
    }
  }

  @Override
  public void rollbackAlterTable(org.apache.hadoop.hive.metastore.api.Table hmsTable, EnvironmentContext context) {
    if (commitLock == null) {
      throw new IllegalStateException("Hive commit lock should already be set");
    }
    commitLock.unlock();
    if (Boolean.parseBoolean(context.getProperties().getOrDefault(MIGRATE_HIVE_TO_ICEBERG, "false"))) {
      LOG.debug("Initiating rollback for table {} at location {}",
          hmsTable.getTableName(), hmsTable.getSd().getLocation());
      context.getProperties().put(INITIALIZE_ROLLBACK_MIGRATION, "true");
      this.catalogProperties = CatalogUtils.getCatalogProperties(hmsTable);
      try {
        this.icebergTable = Catalogs.loadTable(conf, catalogProperties);
      } catch (NoSuchTableException nte) {
        // iceberg table was not yet created, no need to delete the metadata dir separately
        return;
      }

      // we want to keep the data files but get rid of the metadata directory
      String metadataLocation = ((BaseTable) this.icebergTable).operations().current().metadataFileLocation();
      try {
        Path path = new Path(metadataLocation).getParent();
        FileSystem.get(path.toUri(), conf).delete(path, true);
        LOG.debug("Metadata directory of iceberg table {} at location {} was deleted",
            icebergTable.name(), path);
      } catch (IOException e) {
        // the file doesn't exists, do nothing
      }
    }
  }

  @Override
  public void preTruncateTable(org.apache.hadoop.hive.metastore.api.Table table, EnvironmentContext context,
      List<String> partNames)
      throws MetaException {
    this.catalogProperties = CatalogUtils.getCatalogProperties(table);
    this.icebergTable = Catalogs.loadTable(conf, catalogProperties);
    Map<String, PartitionField> partitionFieldMap = icebergTable.spec().fields().stream()
        .collect(Collectors.toMap(PartitionField::name, Function.identity()));
    Expression finalExp = CollectionUtils.isEmpty(partNames) ? Expressions.alwaysTrue() : Expressions.alwaysFalse();
    if (partNames != null) {
      for (String partName : partNames) {
        Map<String, String> specMap = Warehouse.makeSpecFromName(partName);
        Expression subExp = Expressions.alwaysTrue();
        for (Map.Entry<String, String> entry : specMap.entrySet()) {
          // Since Iceberg encodes the values in UTF-8, we need to decode it.
          String partColValue = URLDecoder.decode(entry.getValue(), StandardCharsets.UTF_8);

          if (partitionFieldMap.containsKey(entry.getKey())) {
            PartitionField partitionField = partitionFieldMap.get(entry.getKey());
            Type resultType = partitionField.transform().getResultType(icebergTable.schema()
                    .findField(partitionField.sourceId()).type());
            TransformSpec.TransformType transformType = TransformSpec.fromString(partitionField.transform().toString());
            Object value = Conversions.fromPartitionString(resultType, partColValue);
            Iterable iterable = () -> Collections.singletonList(value).iterator();
            if (TransformSpec.TransformType.IDENTITY.equals(transformType)) {
              Expression boundPredicate = Expressions.in(partitionField.name(), iterable);
              subExp = Expressions.and(subExp, boundPredicate);
            } else {
              throw new MetaException(
                  String.format("Partition transforms are not supported via truncate operation: %s", entry.getKey()));
            }
          } else {
            throw new MetaException(String.format("No partition column/transform name by the name: %s",
                entry.getKey()));
          }
        }
        finalExp = Expressions.or(finalExp, subExp);
      }
    }

    DeleteFiles delete = icebergTable.newDelete();
    String branchName = context.getProperties().get(Catalogs.SNAPSHOT_REF);
    if (branchName != null) {
      delete.toBranch(HiveUtils.getTableSnapshotRef(branchName));
    }
    delete.deleteFromRowFilter(finalExp);
    delete.commit();
    context.putToProperties("truncateSkipDataDeletion", "true");
  }

  @Override public boolean createHMSTableInHook() {
    return createHMSTableInHook;
  }

  private void alterTableProperties(org.apache.hadoop.hive.metastore.api.Table hmsTable,
      Map<String, String> contextProperties) {
    Map<String, String> hmsTableParameters = hmsTable.getParameters();
    Splitter splitter = Splitter.on(PROPERTIES_SEPARATOR);
    UpdateProperties icebergUpdateProperties = icebergTable.updateProperties();
    if (contextProperties.containsKey(SET_PROPERTIES)) {
      splitter.splitToList(contextProperties.get(SET_PROPERTIES))
          .forEach(k -> icebergUpdateProperties.set(k, hmsTableParameters.get(k)));
    } else if (contextProperties.containsKey(UNSET_PROPERTIES)) {
      splitter.splitToList(contextProperties.get(UNSET_PROPERTIES)).forEach(icebergUpdateProperties::remove);
    }
    icebergUpdateProperties.commit();
  }

  private void setupAlterOperationType(org.apache.hadoop.hive.metastore.api.Table hmsTable,
      EnvironmentContext context) throws MetaException {
    TableName tableName = new TableName(hmsTable.getCatName(), hmsTable.getDbName(), hmsTable.getTableName());
    if (context == null || context.getProperties() == null) {
      throw new MetaException("ALTER TABLE operation type on Iceberg table " + tableName +
          " could not be determined.");
    }
    String stringOpType = context.getProperties().get(ALTER_TABLE_OPERATION_TYPE);
    if (stringOpType != null) {
      currentAlterTableOp = AlterTableType.valueOf(stringOpType);
      if (SUPPORTED_ALTER_OPS.stream().noneMatch(op -> op.equals(currentAlterTableOp))) {
        throw new MetaException(
            "Unsupported ALTER TABLE operation type on Iceberg table " + tableName + ", must be one of: " +
                SUPPORTED_ALTER_OPS);
      }

      if (currentAlterTableOp != AlterTableType.ADDPROPS && Catalogs.hiveCatalog(conf, catalogProperties)) {
        context.getProperties().put(SKIP_METASTORE_ALTER, "true");
      }
    }
  }

  private void setFileFormat(String format) {
    if (format == null) {
      return;
    }

    String lowerCaseFormat = format.toLowerCase();
    for (FileFormat fileFormat : FileFormat.values()) {
      if (lowerCaseFormat.contains(fileFormat.getLabel())) {
        catalogProperties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.getLabel());
      }
    }
  }

  private void handleAddColumns(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    Collection<FieldSchema> addedCols =
        HiveSchemaUtil.getSchemaDiff(hmsTable.getSd().getCols(), HiveSchemaUtil.convert(icebergTable.schema()), false)
            .getMissingFromSecond();
    if (!addedCols.isEmpty()) {
      transaction = icebergTable.newTransaction();
      updateSchema = transaction.updateSchema();
    }
    for (FieldSchema addedCol : addedCols) {
      updateSchema.addColumn(addedCol.getName(),
          HiveSchemaUtil.convert(TypeInfoUtils.getTypeInfoFromTypeString(addedCol.getType())), addedCol.getComment());
    }
    updateSchema.commit();
  }

  private void handleDropColumn(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    List<FieldSchema> hmsCols = hmsTable.getSd().getCols();
    List<FieldSchema> icebergCols = HiveSchemaUtil.convert(icebergTable.schema());

    List<FieldSchema> removedCols = HiveSchemaUtil.getSchemaDiff(icebergCols, hmsCols, false).getMissingFromSecond();
    if (removedCols.isEmpty()) {
      return;
    }

    transaction = icebergTable.newTransaction();
    transaction.updateSchema().deleteColumn(removedCols.getFirst().getName()).commit();
    LOG.info("handleDropColumn: Dropping the following column for Iceberg table {}, col: {}", hmsTable.getTableName(),
        removedCols);
  }

  private void handleReplaceColumns(org.apache.hadoop.hive.metastore.api.Table hmsTable) throws MetaException {
    List<FieldSchema> hmsCols = hmsTable.getSd().getCols();
    List<FieldSchema> icebergCols = HiveSchemaUtil.convert(icebergTable.schema());
    HiveSchemaUtil.SchemaDifference schemaDifference = HiveSchemaUtil.getSchemaDiff(hmsCols, icebergCols, true);

    // if there are columns dropped, let's remove them from the iceberg schema as well so we can compare the order
    if (!schemaDifference.getMissingFromFirst().isEmpty()) {
      schemaDifference.getMissingFromFirst().forEach(icebergCols::remove);
    }

    Pair<String, Optional<String>> outOfOrder = HiveSchemaUtil.getReorderedColumn(
        hmsCols, icebergCols, ImmutableMap.of());

    // limit the scope of this operation to only dropping columns
    if (!schemaDifference.getMissingFromSecond().isEmpty() || !schemaDifference.getTypeChanged().isEmpty() ||
        !schemaDifference.getCommentChanged().isEmpty() || outOfOrder != null) {
      throw new MetaException("Unsupported operation to use REPLACE COLUMNS for adding a column, changing a " +
          "column type, column comment or reordering columns. Only use REPLACE COLUMNS for dropping columns. " +
          "For the other operations, consider using the ADD COLUMNS or CHANGE COLUMN commands.");
    }

    if (schemaDifference.getMissingFromFirst().isEmpty()) {
      throw new MetaException("No schema change detected from REPLACE COLUMNS operations. For rectifying any schema " +
          "mismatches between HMS and Iceberg, please consider the UPDATE COLUMNS command.");
    }

    transaction = icebergTable.newTransaction();
    updateSchema = transaction.updateSchema();
    LOG.info("handleReplaceColumns: Dropping the following columns for Iceberg table {}, cols: {}",
        hmsTable.getTableName(), schemaDifference.getMissingFromFirst());
    for (FieldSchema droppedCol : schemaDifference.getMissingFromFirst()) {
      updateSchema.deleteColumn(droppedCol.getName());
    }
    updateSchema.commit();
  }

  private void handleChangeColumn(org.apache.hadoop.hive.metastore.api.Table hmsTable) throws MetaException {
    List<FieldSchema> hmsCols = hmsTable.getSd().getCols();
    List<FieldSchema> icebergCols = HiveSchemaUtil.convert(icebergTable.schema());
    // compute schema difference for renames, type/comment changes
    HiveSchemaUtil.SchemaDifference schemaDifference = HiveSchemaUtil.getSchemaDiff(hmsCols, icebergCols, true);
    // check column reorder (which could happen even in the absence of any rename, type or comment change)
    Map<String, String> renameMapping = ImmutableMap.of();
    if (!schemaDifference.getMissingFromSecond().isEmpty()) {
      renameMapping = ImmutableMap.of(
          schemaDifference.getMissingFromSecond().getFirst().getName(),
          schemaDifference.getMissingFromFirst().getFirst().getName());
    }
    Pair<String, Optional<String>> outOfOrder = HiveSchemaUtil.getReorderedColumn(hmsCols, icebergCols, renameMapping);

    if (!schemaDifference.isEmpty() || outOfOrder != null) {
      transaction = icebergTable.newTransaction();
      updateSchema = transaction.updateSchema();
    } else {
      // we should get here if the user didn't change anything about the column
      // i.e. no changes to the name, type, comment or order
      LOG.info("Found no difference between new and old schema for ALTER TABLE CHANGE COLUMN for" +
          " table: {}. There will be no Iceberg commit.", hmsTable.getTableName());
      return;
    }

    // case 1: column name has been renamed
    if (!schemaDifference.getMissingFromSecond().isEmpty()) {
      FieldSchema updatedField = schemaDifference.getMissingFromSecond().getFirst();
      FieldSchema oldField = schemaDifference.getMissingFromFirst().getFirst();
      updateSchema.renameColumn(oldField.getName(), updatedField.getName());

      // check if type/comment changed too
      if (!Objects.equals(oldField.getType(), updatedField.getType())) {
        updateSchema.updateColumn(oldField.getName(), getPrimitiveTypeOrThrow(updatedField), updatedField.getComment());
      } else if (!Objects.equals(oldField.getComment(), updatedField.getComment())) {
        updateSchema.updateColumnDoc(oldField.getName(), updatedField.getComment());
      }

    // case 2: only column type and/or comment changed
    } else if (!schemaDifference.getTypeChanged().isEmpty()) {
      FieldSchema updatedField = schemaDifference.getTypeChanged().getFirst();
      updateSchema.updateColumn(updatedField.getName(), getPrimitiveTypeOrThrow(updatedField),
          updatedField.getComment());

    // case 3: only comment changed
    } else if (!schemaDifference.getCommentChanged().isEmpty()) {
      FieldSchema updatedField = schemaDifference.getCommentChanged().getFirst();
      updateSchema.updateColumnDoc(updatedField.getName(), updatedField.getComment());
    }

    // case 4: handle any order change
    if (outOfOrder != null) {
      if (outOfOrder.second().isPresent()) {
        updateSchema.moveAfter(outOfOrder.first(), outOfOrder.second().get());
      } else {
        updateSchema.moveFirst(outOfOrder.first());
      }
    }
    updateSchema.commit();

    handlePartitionRename(schemaDifference);
  }

  private void handlePartitionRename(HiveSchemaUtil.SchemaDifference schemaDifference) {
    // in case a partition column has been renamed, spec needs to be adjusted too
    if (!schemaDifference.getMissingFromSecond().isEmpty()) {
      FieldSchema oldField = schemaDifference.getMissingFromFirst().getFirst();
      FieldSchema updatedField = schemaDifference.getMissingFromSecond().getFirst();
      if (icebergTable.spec().fields().stream().anyMatch(pf -> pf.name().equals(oldField.getName()))) {
        UpdatePartitionSpec updatePartitionSpec = transaction.updateSpec();
        updatePartitionSpec.renameField(oldField.getName(), updatedField.getName());
        updatePartitionSpec.commit();
      }
    }
  }

  private Type.PrimitiveType getPrimitiveTypeOrThrow(FieldSchema field) throws MetaException {
    Type newType = HiveSchemaUtil.convert(TypeInfoUtils.getTypeInfoFromTypeString(field.getType()));
    if (!(newType instanceof Type.PrimitiveType)) {
      throw new MetaException(String.format("Cannot promote type of column: '%s' to a non-primitive type: %s.",
          field.getName(), newType));
    }
    return (Type.PrimitiveType) newType;
  }

  @Override
  public void preDropPartitions(org.apache.hadoop.hive.metastore.api.Table hmsTable,
      EnvironmentContext context,
      List<org.apache.commons.lang3.tuple.Pair<Integer, byte[]>> partExprs)
      throws MetaException {
    Table icebergTbl = IcebergTableUtil.getTable(conf, hmsTable);
    DeleteFiles deleteFiles = icebergTbl.newDelete();
    List<Expression> expressions = partExprs.stream().map(partExpr -> {
      ExprNodeDesc exprNodeDesc = SerializationUtilities.deserializeObjectWithTypeInformation(
          partExpr.getRight(), true);
      SearchArgument sarg = ConvertAstToSearchArg.create(conf, (ExprNodeGenericFuncDesc) exprNodeDesc);
      validatePartitionSpec(sarg, icebergTbl.spec());
      return HiveIcebergFilterFactory.generateFilterExpression(sarg);
    }).toList();
    PartitionsTable partitionsTable = (PartitionsTable) MetadataTableUtils.createMetadataTableInstance(
        icebergTbl, MetadataTableType.PARTITIONS);
    List<PartitionData> partitionList = Lists.newArrayList();
    Expression finalExpr = Expressions.alwaysFalse();
    PartitionSpec pSpec = icebergTbl.spec();
    for (Expression expr : expressions) {
      finalExpr = Expressions.or(finalExpr, expr);
    }
    ResidualEvaluator resEval = ResidualEvaluator.of(icebergTbl.spec(), finalExpr, false);
    try (CloseableIterable<FileScanTask> fileScanTasks = partitionsTable.newScan().planFiles()) {
      fileScanTasks.forEach(task -> {
        CloseableIterable<PartitionData> partitionData = CloseableIterable.transform(task.asDataTask().rows(), row -> {
          StructProjection data = row.get(0, StructProjection.class);
          return IcebergTableUtil.toPartitionData(data, pSpec.partitionType());
        });
        partitionData = CloseableIterable.filter(partitionData, pd ->
            resEval.residualFor(pd).isEquivalentTo(Expressions.alwaysTrue()));
        partitionList.addAll(Sets.newHashSet(partitionData));
      });

      Expression partitionSetFilter = Expressions.alwaysFalse();
      for (PartitionData partitionData : partitionList) {
        Expression partFilter = Expressions.alwaysTrue();

        for (int index = 0; index < pSpec.fields().size(); index++) {
          PartitionField field = icebergTbl.spec().fields().get(index);
          UnboundPredicate<Object> equal = getPartitionPredicate(partitionData, field, index, icebergTbl.schema());
          partFilter = Expressions.and(partFilter, equal);
        }
        partitionSetFilter = Expressions.or(partitionSetFilter, partFilter);
      }

      deleteFiles.deleteFromRowFilter(partitionSetFilter);
      deleteFiles.commit();
    } catch (IOException e) {
      throw new MetaException(String.format("Error while fetching the partitions due to: %s", e));
    }
  }

  @Override
  public void preDropPartitions(org.apache.hadoop.hive.metastore.api.Table hmsTable,
                                EnvironmentContext context,
                                RequestPartsSpec partsSpec) throws MetaException {
    if (partsSpec.isSetExprs()) {
      List<DropPartitionsExpr> exprs = partsSpec.getExprs();
      List<org.apache.commons.lang3.tuple.Pair<Integer, byte[]>> partExprs = Lists.newArrayList();
      for (DropPartitionsExpr expr : exprs) {
        partExprs.add(
            org.apache.commons.lang3.tuple.Pair.of(expr.getPartArchiveLevel(), expr.getExpr()));
      }
      preDropPartitions(hmsTable, context, partExprs);
    } else if (partsSpec.isSetNames()) {
      preTruncateTable(hmsTable, context, partsSpec.getNames());
    }
    context.putToProperties(ThriftHiveMetaStoreClient.SKIP_DROP_PARTITION, "true");
  }

  private static void validatePartitionSpec(SearchArgument sarg, PartitionSpec partitionSpec) {
    for (PredicateLeaf leaf : sarg.getLeaves()) {
      TransformSpec transformSpec = TransformSpec.fromStringWithColumnName(leaf.getColumnName());
      Types.NestedField column = partitionSpec.schema().findField(transformSpec.getColumnName());
      PartitionField partitionColumn = partitionSpec.fields().stream()
          .filter(pf -> pf.sourceId() == column.fieldId())
          .findFirst().get();

      if (!(partitionColumn.transform() == null && transformSpec.transformTypeString() == null) &&
          !partitionColumn.transform().toString().equalsIgnoreCase(transformSpec.transformTypeString())) {
        throw new UnsupportedOperationException(
            "Invalid transform for column: " + transformSpec.getColumnName() +
                " Expected: " + partitionColumn.transform() +
                " Found: " + transformSpec.getTransformType());
      }
    }
  }

  private static UnboundPredicate<Object> getPartitionPredicate(PartitionData partitionData, PartitionField field,
      int index, Schema schema) {
    String columName = schema.findField(field.sourceId()).name();
    TransformSpec transformSpec = TransformSpec.fromString(field.transform().toString(), columName);

    UnboundTerm<Object> partitionColumn =
        ObjectUtils.defaultIfNull(HiveIcebergFilterFactory.toTerm(columName, transformSpec),
            Expressions.ref(field.name()));

    return Expressions.equal(partitionColumn, partitionData.get(index, Object.class));
  }

  private static class PreAlterTableProperties {
    private String tableLocation;
    private String format;
    private Schema schema;
    private PartitionSpec spec;
    private List<FieldSchema> partitionKeys;
    private PartitionSpecProxy partitionSpecProxy;
  }

}
