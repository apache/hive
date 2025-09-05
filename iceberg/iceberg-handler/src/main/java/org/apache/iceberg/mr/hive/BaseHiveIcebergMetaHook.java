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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.ddl.misc.sortoder.SortFieldDesc;
import org.apache.hadoop.hive.ql.ddl.misc.sortoder.SortFields;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hive.CatalogUtils;
import org.apache.iceberg.hive.HMSTablePropertyHelper;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.RowLevelOperationMode.MERGE_ON_READ;

public class BaseHiveIcebergMetaHook implements HiveMetaHook {
  private static final Logger LOG = LoggerFactory.getLogger(BaseHiveIcebergMetaHook.class);
  private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();
  public static final Map<String, String> COMMON_HMS_PROPERTIES = ImmutableMap.of(
      BaseMetastoreTableOperations.TABLE_TYPE_PROP, BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase()
  );
  private static final Set<String> PARAMETERS_TO_REMOVE = ImmutableSet
      .of(InputFormatConfig.TABLE_SCHEMA, Catalogs.LOCATION, Catalogs.NAME, InputFormatConfig.PARTITION_SPEC);
  static final String ORC_FILES_ONLY = "iceberg.orc.files.only";

  protected final Configuration conf;
  protected Table icebergTable = null;
  protected Properties catalogProperties;
  protected boolean createHMSTableInHook = false;

  public enum FileFormat {
    ORC("orc"), PARQUET("parquet"), AVRO("avro");

    private final String label;

    FileFormat(String label) {
      this.label = label;
    }

    public String getLabel() {
      return label;
    }
  }

  public BaseHiveIcebergMetaHook(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void preCreateTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    CreateTableRequest request = new CreateTableRequest(hmsTable);
    preCreateTable(request);
  }

  @Override
  public void preCreateTable(CreateTableRequest request) {
    org.apache.hadoop.hive.metastore.api.Table hmsTable = request.getTable();
    if (hmsTable.isTemporary()) {
      throw new UnsupportedOperationException("Creation of temporary iceberg tables is not supported.");
    }
    this.catalogProperties = CatalogUtils.getCatalogProperties(hmsTable);

    // Set the table type even for non HiveCatalog based tables
    hmsTable.getParameters().put(BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase());

    if (!Catalogs.hiveCatalog(conf, catalogProperties)) {
      if (Boolean.parseBoolean(this.catalogProperties.getProperty(hive_metastoreConstants.TABLE_IS_CTLT))) {
        throw new UnsupportedOperationException("CTLT target table must be a HiveCatalog table.");
      }
      // For non-HiveCatalog tables too, we should set the input and output format
      // so that the table can be read by other engines like Impala
      hmsTable.getSd().setInputFormat(HiveIcebergInputFormat.class.getCanonicalName());
      hmsTable.getSd().setOutputFormat(HiveIcebergOutputFormat.class.getCanonicalName());

      // If not using HiveCatalog check for existing table
      try {
        this.icebergTable = IcebergTableUtil.getTable(conf, catalogProperties, true);

        if (CatalogUtils.hadoopCatalog(conf, catalogProperties) && hmsTable.getSd() != null &&
                hmsTable.getSd().getLocation() == null) {
          hmsTable.getSd().setLocation(icebergTable.location());
        }
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

    Set<String> identifierFields = Optional.ofNullable(request.getPrimaryKeys())
        .map(primaryKeys ->
            primaryKeys.stream().map(SQLPrimaryKey::getColumn_name).collect(Collectors.toSet()))
        .orElse(Collections.emptySet());

    Schema schema = schema(catalogProperties, hmsTable, identifierFields);
    PartitionSpec spec = spec(conf, schema, hmsTable);

    // If there are partition keys specified remove them from the HMS table and add them to the column list
    if (hmsTable.isSetPartitionKeys()) {
      hmsTable.getSd().getCols().addAll(hmsTable.getPartitionKeys());
      hmsTable.setPartitionKeysIsSet(false);
    }

    catalogProperties.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(schema));
    String specString = PartitionSpecParser.toJson(spec);
    catalogProperties.put(InputFormatConfig.PARTITION_SPEC, specString);
    validateCatalogConfigsDefined();

    if (request.getEnvContext() == null) {
      request.setEnvContext(new EnvironmentContext());
    }
    request.getEnvContext().putToProperties(TableProperties.DEFAULT_PARTITION_SPEC, specString);
    setCommonHmsTablePropertiesForIceberg(hmsTable);

    if (hmsTable.getParameters().containsKey(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)) {
      createHMSTableInHook = true;
    }

    assertFileFormat(catalogProperties.getProperty(TableProperties.DEFAULT_FILE_FORMAT));

    // Set whether the format is ORC, to be used during vectorization.
    setOrcOnlyFilesParam(hmsTable);
    // Remove hive primary key columns from table request, as iceberg doesn't support hive primary key.
    request.setPrimaryKeys(null);
    setSortOrder(hmsTable, schema, catalogProperties);
  }

  /**
   * Method for verification that necessary catalog configs are defined in Session Conf.
   *
   * <p>If the catalog name is provided in 'iceberg.catalog' table property,
   * and the name is not the default catalog and not hadoop catalog, checks that one of the two configs
   * is defined in Session Conf: iceberg.catalog.<code>catalogName</code>.type
   * or iceberg.catalog.<code>catalogName</code>.catalog-impl. See description in Catalogs.java for more details.
   *
   */
  private void validateCatalogConfigsDefined() {
    String catalogName = catalogProperties.getProperty(InputFormatConfig.CATALOG_NAME);
    if (!StringUtils.isEmpty(catalogName) && !Catalogs.ICEBERG_HADOOP_TABLE_NAME.equals(catalogName)) {

      boolean configsExist = !StringUtils.isEmpty(CatalogUtils.getCatalogType(conf, catalogName)) ||
          !StringUtils.isEmpty(CatalogUtils.getCatalogImpl(conf, catalogName));

      Preconditions.checkArgument(configsExist, "Catalog type or impl must be set for catalog: %s", catalogName);
    }
  }

  private void setSortOrder(org.apache.hadoop.hive.metastore.api.Table hmsTable, Schema schema,
      Properties properties) {
    String sortOderJSONString = hmsTable.getParameters().get(TableProperties.DEFAULT_SORT_ORDER);
    SortFields sortFields = null;
    if (!Strings.isNullOrEmpty(sortOderJSONString)) {
      try {
        sortFields = JSON_OBJECT_MAPPER.reader().readValue(sortOderJSONString, SortFields.class);
      } catch (Exception e) {
        LOG.warn("Can not read write order json: {}", sortOderJSONString, e);
        return;
      }
      if (sortFields != null && !sortFields.getSortFields().isEmpty()) {
        SortOrder.Builder sortOderBuilder = SortOrder.builderFor(schema);
        sortFields.getSortFields().forEach(fieldDesc -> {
          NullOrder nullOrder = fieldDesc.getNullOrdering() == NullOrdering.NULLS_FIRST ?
              NullOrder.NULLS_FIRST : NullOrder.NULLS_LAST;
          SortDirection sortDirection = fieldDesc.getDirection() == SortFieldDesc.SortDirection.ASC ?
              SortDirection.ASC : SortDirection.DESC;
          sortOderBuilder.sortBy(fieldDesc.getColumnName(), sortDirection, nullOrder);
        });
        properties.put(TableProperties.DEFAULT_SORT_ORDER, SortOrderParser.toJson(sortOderBuilder.build()));
      }
    }
  }

  @Override
  public void rollbackCreateTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    // do nothing
  }

  @Override
  public void commitCreateTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    // do nothing
  }

  @Override
  public void preDropTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    // do nothing
  }

  @Override
  public void rollbackDropTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    // do nothing
  }

  @Override
  public void commitDropTable(org.apache.hadoop.hive.metastore.api.Table hmsTable, boolean deleteData) {
    // do nothing
  }

  @Override
  public boolean createHMSTableInHook() {
    return createHMSTableInHook;
  }

  private static void assertFileFormat(String format) {
    if (format == null) {
      return;
    }
    String lowerCaseFormat = format.toLowerCase();
    Preconditions.checkArgument(Arrays.stream(FileFormat.values()).anyMatch(v -> lowerCaseFormat.contains(v.label)),
        String.format("Unsupported fileformat %s", format));
  }

  protected void setCommonHmsTablePropertiesForIceberg(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    if (CatalogUtils.isHadoopTable(conf, catalogProperties)) {
      String location = (hmsTable.getSd() != null) ? hmsTable.getSd().getLocation() : null;
      if (location == null && CatalogUtils.hadoopCatalog(conf, catalogProperties)) {
        location = IcebergTableUtil.defaultWarehouseLocation(
            TableIdentifier.of(hmsTable.getDbName(), hmsTable.getTableName()),
            conf, catalogProperties);
        hmsTable.getSd().setLocation(location);
      }
      Preconditions.checkArgument(location != null, "Table location not set");
    }

    Map<String, String> hmsParams = hmsTable.getParameters();
    COMMON_HMS_PROPERTIES.forEach(hmsParams::putIfAbsent);

    // Remove null values from hms table properties
    hmsParams.entrySet().removeIf(e -> e.getKey() == null || e.getValue() == null);

    // Remove creation related properties
    PARAMETERS_TO_REMOVE.forEach(hmsParams::remove);

    setWriteModeDefaults(null, hmsParams, null);
  }

  protected Schema schema(Properties properties, org.apache.hadoop.hive.metastore.api.Table hmsTable,
                        Set<String> identifierFields) {
    boolean autoConversion = conf.getBoolean(InputFormatConfig.SCHEMA_AUTO_CONVERSION, false);

    if (properties.getProperty(InputFormatConfig.TABLE_SCHEMA) != null) {
      return SchemaParser.fromJson(properties.getProperty(InputFormatConfig.TABLE_SCHEMA));
    }
    List<FieldSchema> cols = Lists.newArrayList(hmsTable.getSd().getCols());
    if (hmsTable.isSetPartitionKeys() && !hmsTable.getPartitionKeys().isEmpty()) {
      cols.addAll(hmsTable.getPartitionKeys());
    }
    Schema schema = HiveSchemaUtil.convert(cols, autoConversion);

    return getSchemaWithIdentifierFields(schema, identifierFields);
  }

  private Schema getSchemaWithIdentifierFields(Schema schema, Set<String> identifierFields) {
    if (identifierFields == null || identifierFields.isEmpty()) {
      return schema;
    }
    Set<Integer> identifierFieldIds = identifierFields.stream()
            .map(column -> {
              Types.NestedField field = schema.findField(column);
              Preconditions.checkNotNull(field,
                      "Cannot find identifier field ID for the column %s in schema %s", column, schema);
              return field.fieldId();
            })
            .collect(Collectors.toSet());

    List<Types.NestedField> cols = schema.columns().stream()
            .map(column -> identifierFieldIds.contains(column.fieldId()) ? column.asRequired() : column)
            .toList();

    return new Schema(cols, identifierFieldIds);
  }

  protected static PartitionSpec spec(Configuration configuration, Schema schema,
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

    return HMSTablePropertyHelper.getPartitionSpec(hmsTable.getParameters(), schema);
  }

  protected void setOrcOnlyFilesParam(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    hmsTable.getParameters().put(ORC_FILES_ONLY, String.valueOf(isOrcOnlyFiles(hmsTable)));
  }

  protected boolean isOrcOnlyFiles(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    return !"FALSE".equalsIgnoreCase(hmsTable.getParameters().get(ORC_FILES_ONLY)) &&
        (hmsTable.getSd().getInputFormat() != null &&
            hmsTable.getSd().getInputFormat().toUpperCase().contains(org.apache.iceberg.FileFormat.ORC.name()) ||
            org.apache.iceberg.FileFormat.ORC.name()
                .equalsIgnoreCase(hmsTable.getSd().getSerdeInfo().getParameters()
                    .get(TableProperties.DEFAULT_FILE_FORMAT)) ||
            org.apache.iceberg.FileFormat.ORC.name()
                .equalsIgnoreCase(hmsTable.getParameters().get(TableProperties.DEFAULT_FILE_FORMAT)));
  }

  protected void setWriteModeDefaults(Table icebergTbl, Map<String, String> newProps, EnvironmentContext context) {
    if ((icebergTbl == null || ((BaseTable) icebergTbl).operations().current().formatVersion() == 1) &&
        IcebergTableUtil.isV2TableOrAbove(newProps)) {
      List<String> writeModeList = ImmutableList.of(
          TableProperties.DELETE_MODE, TableProperties.UPDATE_MODE, TableProperties.MERGE_MODE);
      writeModeList.stream()
          .filter(writeMode -> catalogProperties.get(writeMode) == null)
          .forEach(writeMode -> {
            catalogProperties.put(writeMode, MERGE_ON_READ.modeName());
            newProps.put(writeMode, MERGE_ON_READ.modeName());
          });

      if (context != null) {
        Splitter splitter = Splitter.on(PROPERTIES_SEPARATOR);
        Map<String, String> contextProperties = context.getProperties();
        if (contextProperties.containsKey(SET_PROPERTIES)) {
          String propValue = context.getProperties().get(SET_PROPERTIES);
          String writeModeStr = writeModeList.stream()
              .filter(writeMode -> !splitter.splitToList(propValue).contains(writeMode))
              .collect(Collectors.joining("'"));
          if (!writeModeStr.isEmpty()) {
            contextProperties.put(SET_PROPERTIES, propValue + "'" + writeModeStr);
          }
        }
      }
    }
  }

  @Override
  public void postGetTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    if (hmsTable != null) {
      try {
        Table tbl = IcebergTableUtil.getTable(conf, hmsTable);
        String formatVersion = String.valueOf(((BaseTable) tbl).operations().current().formatVersion());
        hmsTable.getParameters().put(TableProperties.FORMAT_VERSION, formatVersion);
        // Set the serde info
        hmsTable.getSd().setInputFormat(HiveIcebergInputFormat.class.getName());
        hmsTable.getSd().setOutputFormat(HiveIcebergOutputFormat.class.getName());
        hmsTable.getSd().getSerdeInfo().setSerializationLib(HiveIcebergSerDe.class.getName());
        String storageHandler = hmsTable.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE);
        // Check if META_TABLE_STORAGE is not present or is not an instance of ICEBERG_STORAGE_HANDLER
        if (storageHandler == null || !isHiveIcebergStorageHandler(storageHandler)) {
          hmsTable.getParameters()
              .put(hive_metastoreConstants.META_TABLE_STORAGE, HMSTablePropertyHelper.HIVE_ICEBERG_STORAGE_HANDLER);
        }
      } catch (NoSuchTableException | NotFoundException ex) {
        // If the table doesn't exist, ignore throwing exception from here
      }
    }
  }

  private static boolean isHiveIcebergStorageHandler(String storageHandler) {
    try {
      Class<?> storageHandlerClass = Class.forName(storageHandler);
      return Class.forName(HIVE_ICEBERG_STORAGE_HANDLER).isAssignableFrom(storageHandlerClass);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Error checking storage handler class", e);
    }
  }
}
