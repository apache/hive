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
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergMetaHook implements HiveMetaHook {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergMetaHook.class);
  private static final Set<String> PARAMETERS_TO_REMOVE = ImmutableSet
      .of(InputFormatConfig.TABLE_SCHEMA, Catalogs.LOCATION, Catalogs.NAME);
  private static final Set<String> PROPERTIES_TO_REMOVE = ImmutableSet
      // We don't want to push down the metadata location props to Iceberg from HMS,
      // since the snapshot pointer in HMS would always be one step ahead
      .of(BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
      BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP,
      // Initially we'd like to cache the partition spec in HMS, but not push it down later to Iceberg during alter
      // table commands since by then the HMS info can be stale + Iceberg does not store its partition spec in the props
      InputFormatConfig.PARTITION_SPEC);
  private static final EnumSet<AlterTableType> SUPPORTED_ALTER_OPS = EnumSet.of(
      AlterTableType.ADDCOLS, AlterTableType.REPLACE_COLUMNS, AlterTableType.ADDPROPS, AlterTableType.DROPPROPS,
      AlterTableType.SETPARTITIONSPEC);

  private final Configuration conf;
  private Table icebergTable = null;
  private Properties catalogProperties;
  private boolean deleteIcebergTable;
  private FileIO deleteIo;
  private TableMetadata deleteMetadata;
  private boolean canMigrateHiveTable;
  private PreAlterTableProperties preAlterTableProperties;
  private UpdateSchema updateSchema;
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
    PartitionSpec spec = spec(conf, schema, catalogProperties, hmsTable);

    // If there are partition keys specified remove them from the HMS table and add them to the column list
    if (hmsTable.isSetPartitionKeys()) {
      hmsTable.getSd().getCols().addAll(hmsTable.getPartitionKeys());
      hmsTable.setPartitionKeysIsSet(false);
    }

    catalogProperties.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(schema));
    catalogProperties.put(InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(spec));
    updateHmsTableProperties(hmsTable);
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
      // Store the metadata and the id for deleting the actual table data
      String metadataLocation = hmsTable.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
      this.deleteIo = IcebergTableUtil.getTable(conf, catalogProperties).io();
      this.deleteMetadata = TableMetadataParser.read(deleteIo, metadataLocation);
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
          if (deleteIo.newInputFile(deleteMetadata.location()).exists()) {
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
      // If the iceberg table does not exist, and the hms table is external and not temporary and not acid
      // we will create it in commitAlterTable
      StorageDescriptor sd = hmsTable.getSd();
      canMigrateHiveTable = MetaStoreUtils.isExternalTable(hmsTable) && !hmsTable.isTemporary() &&
          !AcidUtils.isTransactionalTable(hmsTable);
      if (!canMigrateHiveTable) {
        throw new MetaException("Converting non-external, temporary or transactional hive table to iceberg " +
            "table is not allowed.");
      }

      preAlterTableProperties = new PreAlterTableProperties();
      preAlterTableProperties.tableLocation = sd.getLocation();
      preAlterTableProperties.format = sd.getInputFormat();
      preAlterTableProperties.schema = schema(catalogProperties, hmsTable);
      preAlterTableProperties.spec = spec(conf, preAlterTableProperties.schema, catalogProperties, hmsTable);
      preAlterTableProperties.partitionKeys = hmsTable.getPartitionKeys();

      context.getProperties().put(HiveMetaHook.ALLOW_PARTITION_KEY_CHANGE, "true");
      // If there are partition keys specified remove them from the HMS table and add them to the column list
      if (hmsTable.isSetPartitionKeys()) {
        hmsTable.getSd().getCols().addAll(hmsTable.getPartitionKeys());
        hmsTable.setPartitionKeysIsSet(false);
      }
      sd.setInputFormat(HiveIcebergInputFormat.class.getCanonicalName());
      sd.setOutputFormat(HiveIcebergOutputFormat.class.getCanonicalName());
      sd.setSerdeInfo(new SerDeInfo("icebergSerde", HiveIcebergSerDe.class.getCanonicalName(),
          Collections.emptyMap()));
      updateHmsTableProperties(hmsTable);
    }

    if (AlterTableType.ADDCOLS.equals(currentAlterTableOp)) {
      Collection<FieldSchema> addedCols = HiveSchemaUtil.getSchemaDiff(
          hmsTable.getSd().getCols(), HiveSchemaUtil.convert(icebergTable.schema()), false).getMissingFromSecond();
      if (!addedCols.isEmpty()) {
        updateSchema = icebergTable.updateSchema();
      }
      for (FieldSchema addedCol : addedCols) {
        updateSchema.addColumn(
            addedCol.getName(),
            HiveSchemaUtil.convert(TypeInfoUtils.getTypeInfoFromTypeString(addedCol.getType())),
            addedCol.getComment()
        );
      }
    } else if (AlterTableType.REPLACE_COLUMNS.equals(currentAlterTableOp)) {
      handleReplaceColumns(hmsTable);
    }
  }

  @Override
  public void commitAlterTable(org.apache.hadoop.hive.metastore.api.Table hmsTable, EnvironmentContext context,
      PartitionSpecProxy partitionSpecProxy) throws MetaException {
    if (canMigrateHiveTable) {
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
      Map<String, String> contextProperties = context.getProperties();
      switch (currentAlterTableOp) {
        case REPLACE_COLUMNS:
        case ADDCOLS:
          if (updateSchema != null) {
            updateSchema.commit();
          }
          break;
        case ADDPROPS:
        case DROPPROPS:
          alterTableProperties(hmsTable, contextProperties);
          break;
        case SETPARTITIONSPEC:
          IcebergTableUtil.updateSpec(conf, icebergTable);
          break;
      }
    }
  }

  @Override
  public void rollbackAlterTable(org.apache.hadoop.hive.metastore.api.Table hmsTable, EnvironmentContext context)
      throws MetaException {
    if (Boolean.valueOf(context.getProperties().getOrDefault(MIGRATE_HIVE_TO_ICEBERG, "false"))) {
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
                SUPPORTED_ALTER_OPS.toString());
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

  private void updateHmsTableProperties(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
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

  private static PartitionSpec spec(Configuration configuration, Schema schema, Properties properties,
      org.apache.hadoop.hive.metastore.api.Table hmsTable) {

    PartitionSpec spec = IcebergTableUtil.spec(configuration, schema);
    if (spec != null) {
      Preconditions.checkArgument(!hmsTable.isSetPartitionKeys() || hmsTable.getPartitionKeys().isEmpty(),
          "Provide only one of the following: Hive partition transform specification, or the " +
              InputFormatConfig.PARTITION_SPEC + " property");
      return spec;
    }

    if (hmsTable.getParameters().get(InputFormatConfig.PARTITION_SPEC) != null) {
      Preconditions.checkArgument(!hmsTable.isSetPartitionKeys() || hmsTable.getPartitionKeys().isEmpty(),
          "Provide only one of the following: Hive partition specification, or the " +
              InputFormatConfig.PARTITION_SPEC + " property");
      return PartitionSpecParser.fromJson(schema, hmsTable.getParameters().get(InputFormatConfig.PARTITION_SPEC));
    } else if (hmsTable.isSetPartitionKeys() && !hmsTable.getPartitionKeys().isEmpty()) {
      // If the table is partitioned then generate the identity partition definitions for the Iceberg table
      return HiveSchemaUtil.spec(schema, hmsTable.getPartitionKeys());
    } else {
      return PartitionSpec.unpartitioned();
    }
  }

  private void handleReplaceColumns(org.apache.hadoop.hive.metastore.api.Table hmsTable) throws MetaException {
    HiveSchemaUtil.SchemaDifference schemaDifference = HiveSchemaUtil.getSchemaDiff(hmsTable.getSd().getCols(),
        HiveSchemaUtil.convert(icebergTable.schema()), true);
    if (!schemaDifference.isEmpty()) {
      updateSchema = icebergTable.updateSchema();
    } else {
      // we should get here if the user restated the exactly the existing columns in the REPLACE COLUMNS command
      LOG.info("Found no difference between new and old schema for ALTER TABLE REPLACE COLUMNS for" +
          " table: {}. There will be no Iceberg commit.", hmsTable.getTableName());
      return;
    }

    for (FieldSchema droppedCol : schemaDifference.getMissingFromFirst()) {
      updateSchema.deleteColumn(droppedCol.getName());
    }

    for (FieldSchema addedCol : schemaDifference.getMissingFromSecond()) {
      updateSchema.addColumn(
          addedCol.getName(),
          HiveSchemaUtil.convert(TypeInfoUtils.getTypeInfoFromTypeString(addedCol.getType())),
          addedCol.getComment()
      );
    }

    for (FieldSchema updatedCol : schemaDifference.getTypeChanged()) {
      Type newType = HiveSchemaUtil.convert(TypeInfoUtils.getTypeInfoFromTypeString(updatedCol.getType()));
      if (!(newType instanceof Type.PrimitiveType)) {
        throw new MetaException(String.format("Cannot promote type of column: '%s' to a non-primitive type: %s.",
            updatedCol.getName(), newType));
      }
      updateSchema.updateColumn(updatedCol.getName(), (Type.PrimitiveType) newType, updatedCol.getComment());
    }

    for (FieldSchema updatedCol : schemaDifference.getCommentChanged()) {
      updateSchema.updateColumnDoc(updatedCol.getName(), updatedCol.getComment());
    }
  }

  private class PreAlterTableProperties {
    private String tableLocation;
    private String format;
    private Schema schema;
    private PartitionSpec spec;
    private List<FieldSchema> partitionKeys;
  }
}
