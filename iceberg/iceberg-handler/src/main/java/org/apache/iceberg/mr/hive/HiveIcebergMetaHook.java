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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.parse.PartitionTransform;
import org.apache.hadoop.hive.ql.parse.PartitionTransformSpec;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
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
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.hive.HiveTableOperations;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergMetaHook implements HiveMetaHook {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergMetaHook.class);
  private static final Set<String> PARAMETERS_TO_REMOVE = ImmutableSet
      .of(InputFormatConfig.TABLE_SCHEMA, Catalogs.LOCATION, Catalogs.NAME, InputFormatConfig.PARTITION_SPEC);
  private static final Set<String> PROPERTIES_TO_REMOVE = ImmutableSet
      // We don't want to push down the metadata location props to Iceberg from HMS,
      // since the snapshot pointer in HMS would always be one step ahead
      .of(BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
      BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP);
  static final EnumSet<AlterTableType> SUPPORTED_ALTER_OPS = EnumSet.of(
      AlterTableType.ADDCOLS, AlterTableType.REPLACE_COLUMNS, AlterTableType.RENAME_COLUMN,
      AlterTableType.ADDPROPS, AlterTableType.DROPPROPS, AlterTableType.SETPARTITIONSPEC,
      AlterTableType.UPDATE_COLUMNS);
  private static final List<String> MIGRATION_ALLOWED_SOURCE_FORMATS = ImmutableList.of(
      FileFormat.PARQUET.name().toLowerCase(),
      FileFormat.ORC.name().toLowerCase(),
      FileFormat.AVRO.name().toLowerCase());
  static final String MIGRATED_TO_ICEBERG = "MIGRATED_TO_ICEBERG";

  private final Configuration conf;
  private Table icebergTable = null;
  private Properties catalogProperties;
  private boolean deleteIcebergTable;
  private FileIO deleteIo;
  private TableMetadata deleteMetadata;
  private boolean isTableMigration;
  private PreAlterTableProperties preAlterTableProperties;
  private UpdateSchema updateSchema;
  private UpdatePartitionSpec updatePartitionSpec;
  private Transaction transaction;
  private AlterTableType currentAlterTableOp;

  public HiveIcebergMetaHook(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void preCreateTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    this.catalogProperties = getCatalogProperties(hmsTable);

    // Set the table type even for non HiveCatalog based tables
    hmsTable.getParameters().put(BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase());

    if (!Catalogs.hiveCatalog(conf, catalogProperties)) {
      // For non-HiveCatalog tables too, we should set the input and output format
      // so that the table can be read by other engines like Impala
      hmsTable.getSd().setInputFormat(HiveIcebergInputFormat.class.getCanonicalName());
      hmsTable.getSd().setOutputFormat(HiveIcebergOutputFormat.class.getCanonicalName());

      // If not using HiveCatalog check for existing table
      try {

        this.icebergTable = IcebergTableUtil.getTable(conf, catalogProperties);

        Preconditions.checkArgument(catalogProperties.getProperty(InputFormatConfig.TABLE_SCHEMA) == null,
            "Iceberg table already created - can not use provided schema");
        Preconditions.checkArgument(catalogProperties.getProperty(InputFormatConfig.PARTITION_SPEC) == null,
            "Iceberg table already created - can not use provided partition specification");

        LOG.info("Iceberg table already exists {}", icebergTable);

        return;
      } catch (NoSuchTableException nte) {
        // If the table does not exist we will create it below
      }
    }

    // If the table does not exist collect data for table creation
    // - InputFormatConfig.TABLE_SCHEMA, InputFormatConfig.PARTITION_SPEC takes precedence so the user can override the
    // Iceberg schema and specification generated by the code

    Schema schema = schema(catalogProperties, hmsTable);
    PartitionSpec spec = spec(conf, schema, hmsTable);

    // If there are partition keys specified remove them from the HMS table and add them to the column list
    if (hmsTable.isSetPartitionKeys()) {
      hmsTable.getSd().getCols().addAll(hmsTable.getPartitionKeys());
      hmsTable.setPartitionKeysIsSet(false);
    }

    catalogProperties.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(schema));
    catalogProperties.put(InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(spec));
    setCommonHmsTablePropertiesForIceberg(hmsTable);
  }

  @Override
  public void rollbackCreateTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    // do nothing
  }

  @Override
  public void commitCreateTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    if (icebergTable == null) {
      if (Catalogs.hiveCatalog(conf, catalogProperties)) {
        catalogProperties.put(TableProperties.ENGINE_HIVE_ENABLED, true);
      }

      Catalogs.createTable(conf, catalogProperties);
    }
  }

  @Override
  public void preDropTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    // do nothing
  }

  @Override
  public void preDropTable(org.apache.hadoop.hive.metastore.api.Table hmsTable, boolean deleteData) {
    this.catalogProperties = getCatalogProperties(hmsTable);
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
    setupAlterOperationType(hmsTable, context);
    catalogProperties = getCatalogProperties(hmsTable);
    try {
      icebergTable = IcebergTableUtil.getTable(conf, catalogProperties);
    } catch (NoSuchTableException nte) {
      context.getProperties().put(MIGRATE_HIVE_TO_ICEBERG, "true");
      // If the iceberg table does not exist, then this is an ALTER command aimed at migrating the table to iceberg
      // First we must check whether it's eligible for migration to iceberg
      // If so, we will create the iceberg table in commitAlterTable and go ahead with the migration
      assertTableCanBeMigrated(hmsTable);
      isTableMigration = true;

      StorageDescriptor sd = hmsTable.getSd();
      preAlterTableProperties = new PreAlterTableProperties();
      preAlterTableProperties.tableLocation = sd.getLocation();
      preAlterTableProperties.format = sd.getInputFormat();
      preAlterTableProperties.schema = schema(catalogProperties, hmsTable);
      preAlterTableProperties.partitionKeys = hmsTable.getPartitionKeys();

      context.getProperties().put(HiveMetaHook.ALLOW_PARTITION_KEY_CHANGE, "true");
      // If there are partition keys specified remove them from the HMS table and add them to the column list
      if (hmsTable.isSetPartitionKeys() && !hmsTable.getPartitionKeys().isEmpty()) {
        List<PartitionTransformSpec> spec = PartitionTransform.getPartitionTransformSpec(hmsTable.getPartitionKeys());
        if (!SessionStateUtil.addResource(conf, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC, spec)) {
          throw new MetaException("Query state attached to Session state must be not null. " +
              "Partition transform metadata cannot be saved.");
        }
        hmsTable.getSd().getCols().addAll(hmsTable.getPartitionKeys());
        hmsTable.setPartitionKeysIsSet(false);
      }
      preAlterTableProperties.spec = spec(conf, preAlterTableProperties.schema, hmsTable);

      sd.setInputFormat(HiveIcebergInputFormat.class.getCanonicalName());
      sd.setOutputFormat(HiveIcebergOutputFormat.class.getCanonicalName());
      sd.setSerdeInfo(new SerDeInfo("icebergSerde", HiveIcebergSerDe.class.getCanonicalName(),
          Collections.emptyMap()));
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
    } else if (AlterTableType.RENAME_COLUMN.equals(currentAlterTableOp)) {
      // passing in the "CHANGE COLUMN" string instead, since RENAME COLUMN is not part of SQL syntax (not to mention
      // that users can change data types or reorder columns too with this alter op type, so its name is misleading..)
      assertNotMigratedTable(hmsTable.getParameters(), "CHANGE COLUMN");
      handleChangeColumn(hmsTable);
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
  }

  @Override
  public void commitAlterTable(org.apache.hadoop.hive.metastore.api.Table hmsTable, EnvironmentContext context,
      PartitionSpecProxy partitionSpecProxy) throws MetaException {
    if (isTableMigration) {
      catalogProperties = getCatalogProperties(hmsTable);
      catalogProperties.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(preAlterTableProperties.schema));
      catalogProperties.put(InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(preAlterTableProperties.spec));
      setFileFormat();
      if (Catalogs.hiveCatalog(conf, catalogProperties)) {
        catalogProperties.put(TableProperties.ENGINE_HIVE_ENABLED, true);
      }
      HiveTableUtil.importFiles(preAlterTableProperties.tableLocation, preAlterTableProperties.format,
          partitionSpecProxy, preAlterTableProperties.partitionKeys, catalogProperties, conf);
    } else if (currentAlterTableOp != null) {
      switch (currentAlterTableOp) {
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
      }
    }
  }

  @Override
  public void rollbackAlterTable(org.apache.hadoop.hive.metastore.api.Table hmsTable, EnvironmentContext context) {
    if (Boolean.parseBoolean(context.getProperties().getOrDefault(MIGRATE_HIVE_TO_ICEBERG, "false"))) {
      LOG.debug("Initiating rollback for table {} at location {}",
          hmsTable.getTableName(), hmsTable.getSd().getLocation());
      context.getProperties().put(INITIALIZE_ROLLBACK_MIGRATION, "true");
      this.catalogProperties = getCatalogProperties(hmsTable);
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
  public void preTruncateTable(org.apache.hadoop.hive.metastore.api.Table table, EnvironmentContext context)
      throws MetaException {
    this.catalogProperties = getCatalogProperties(table);
    this.icebergTable = Catalogs.loadTable(conf, catalogProperties);
    DeleteFiles delete = icebergTable.newDelete();
    delete.deleteFromRowFilter(Expressions.alwaysTrue());
    delete.commit();
    context.putToProperties("truncateSkipDataDeletion", "true");
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
    }
  }

  private void setFileFormat() {
    String format = preAlterTableProperties.format.toLowerCase();
    if (format.contains("orc")) {
      catalogProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "orc");
    } else if (format.contains("parquet")) {
      catalogProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
    } else if (format.contains("avro")) {
      catalogProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
    }
  }

  private void setCommonHmsTablePropertiesForIceberg(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    // Set the table type even for non HiveCatalog based tables
    hmsTable.getParameters().put(BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase());

    // Allow purging table data if the table is created now and not set otherwise
    hmsTable.getParameters().putIfAbsent(InputFormatConfig.EXTERNAL_TABLE_PURGE, "TRUE");

    // If the table is not managed by Hive catalog then the location should be set
    if (!Catalogs.hiveCatalog(conf, catalogProperties)) {
      Preconditions.checkArgument(hmsTable.getSd() != null && hmsTable.getSd().getLocation() != null,
          "Table location not set");
    }

    // Remove null values from hms table properties
    hmsTable.getParameters().entrySet().removeIf(e -> e.getKey() == null || e.getValue() == null);

    // Remove creation related properties
    PARAMETERS_TO_REMOVE.forEach(hmsTable.getParameters()::remove);
  }

  /**
   * Calculates the properties we would like to send to the catalog.
   * <ul>
   * <li>The base of the properties is the properties stored at the Hive Metastore for the given table
   * <li>We add the {@link Catalogs#LOCATION} as the table location
   * <li>We add the {@link Catalogs#NAME} as TableIdentifier defined by the database name and table name
   * <li>We add the serdeProperties of the HMS table
   * <li>We remove some parameters that we don't want to push down to the Iceberg table props
   * </ul>
   * @param hmsTable Table for which we are calculating the properties
   * @return The properties we can provide for Iceberg functions, like {@link Catalogs}
   */
  private static Properties getCatalogProperties(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    Properties properties = new Properties();

    hmsTable.getParameters().entrySet().stream().filter(e -> e.getKey() != null && e.getValue() != null).forEach(e -> {
      // translate key names between HMS and Iceberg where needed
      String icebergKey = HiveTableOperations.translateToIcebergProp(e.getKey());
      properties.put(icebergKey, e.getValue());
    });

    if (properties.get(Catalogs.LOCATION) == null &&
        hmsTable.getSd() != null && hmsTable.getSd().getLocation() != null) {
      properties.put(Catalogs.LOCATION, hmsTable.getSd().getLocation());
    }

    if (properties.get(Catalogs.NAME) == null) {
      properties.put(Catalogs.NAME, TableIdentifier.of(hmsTable.getDbName(), hmsTable.getTableName()).toString());
    }

    SerDeInfo serdeInfo = hmsTable.getSd().getSerdeInfo();
    if (serdeInfo != null) {
      serdeInfo.getParameters().entrySet().stream()
          .filter(e -> e.getKey() != null && e.getValue() != null).forEach(e -> {
            String icebergKey = HiveTableOperations.translateToIcebergProp(e.getKey());
            properties.put(icebergKey, e.getValue());
          });
    }

    // Remove HMS table parameters we don't want to propagate to Iceberg
    PROPERTIES_TO_REMOVE.forEach(properties::remove);

    return properties;
  }

  private Schema schema(Properties properties, org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    boolean autoConversion = conf.getBoolean(InputFormatConfig.SCHEMA_AUTO_CONVERSION, false);

    if (properties.getProperty(InputFormatConfig.TABLE_SCHEMA) != null) {
      return SchemaParser.fromJson(properties.getProperty(InputFormatConfig.TABLE_SCHEMA));
    } else if (hmsTable.isSetPartitionKeys() && !hmsTable.getPartitionKeys().isEmpty()) {
      // Add partitioning columns to the original column list before creating the Iceberg Schema
      List<FieldSchema> cols = Lists.newArrayList(hmsTable.getSd().getCols());
      cols.addAll(hmsTable.getPartitionKeys());
      return HiveSchemaUtil.convert(cols, autoConversion);
    } else {
      return HiveSchemaUtil.convert(hmsTable.getSd().getCols(), autoConversion);
    }
  }

  private static PartitionSpec spec(Configuration configuration, Schema schema,
      org.apache.hadoop.hive.metastore.api.Table hmsTable) {

    Preconditions.checkArgument(!hmsTable.isSetPartitionKeys() || hmsTable.getPartitionKeys().isEmpty(),
        "We can only handle non-partitioned Hive tables. The Iceberg schema should be in " +
            InputFormatConfig.PARTITION_SPEC + " or already converted to a partition transform ");

    PartitionSpec spec = IcebergTableUtil.spec(configuration, schema);
    if (spec != null) {
      Preconditions.checkArgument(hmsTable.getParameters().get(InputFormatConfig.PARTITION_SPEC) == null,
          "Provide only one of the following: Hive partition transform specification, or the " +
              InputFormatConfig.PARTITION_SPEC + " property");
      return spec;
    }

    if (hmsTable.getParameters().get(InputFormatConfig.PARTITION_SPEC) != null) {
      return PartitionSpecParser.fromJson(schema, hmsTable.getParameters().get(InputFormatConfig.PARTITION_SPEC));
    } else {
      return PartitionSpec.unpartitioned();
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
          schemaDifference.getMissingFromSecond().get(0).getName(),
          schemaDifference.getMissingFromFirst().get(0).getName());
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
      FieldSchema updatedField = schemaDifference.getMissingFromSecond().get(0);
      FieldSchema oldField = schemaDifference.getMissingFromFirst().get(0);
      updateSchema.renameColumn(oldField.getName(), updatedField.getName());

      // check if type/comment changed too
      if (!Objects.equals(oldField.getType(), updatedField.getType())) {
        updateSchema.updateColumn(oldField.getName(), getPrimitiveTypeOrThrow(updatedField), updatedField.getComment());
      } else if (!Objects.equals(oldField.getComment(), updatedField.getComment())) {
        updateSchema.updateColumnDoc(oldField.getName(), updatedField.getComment());
      }

    // case 2: only column type and/or comment changed
    } else if (!schemaDifference.getTypeChanged().isEmpty()) {
      FieldSchema updatedField = schemaDifference.getTypeChanged().get(0);
      updateSchema.updateColumn(updatedField.getName(), getPrimitiveTypeOrThrow(updatedField),
          updatedField.getComment());

    // case 3: only comment changed
    } else if (!schemaDifference.getCommentChanged().isEmpty()) {
      FieldSchema updatedField = schemaDifference.getCommentChanged().get(0);
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
      FieldSchema oldField = schemaDifference.getMissingFromFirst().get(0);
      FieldSchema updatedField = schemaDifference.getMissingFromSecond().get(0);
      if (icebergTable.spec().fields().stream().anyMatch(pf -> pf.name().equals(oldField.getName()))) {
        updatePartitionSpec = transaction.updateSpec();
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

  private class PreAlterTableProperties {
    private String tableLocation;
    private String format;
    private Schema schema;
    private PartitionSpec spec;
    private List<FieldSchema> partitionKeys;
  }
}
