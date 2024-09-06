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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.security.authorization.HiveCustomStorageHandlerUtils;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.serde.objectinspector.IcebergObjectInspector;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergSerDe extends AbstractSerDe {
  public static final String CTAS_EXCEPTION_MSG = "CTAS target table must be a HiveCatalog table." +
      " For other catalog types, the target Iceberg table would be created successfully but the table will not be" +
      " registered in HMS. This means that even though the CTAS query succeeds, the new table wouldn't be immediately" +
      " queryable from Hive, since HMS does not know about it.";

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergSerDe.class);

  private ObjectInspector inspector;
  private Schema tableSchema;
  private Schema projectedSchema;
  private Collection<String> partitionColumns;
  private Map<ObjectInspector, Deserializer> deserializers = Maps.newHashMapWithExpectedSize(1);
  private Container<Record> row = new Container<>();
  private Map<String, String> jobConf =  Maps.newHashMap();

  @Override
  public void initialize(@Nullable Configuration configuration, Properties serDeProperties,
                         Properties partitionProperties) throws SerDeException {
    super.initialize(configuration, serDeProperties, partitionProperties);

    // HiveIcebergSerDe.initialize is called multiple places in Hive code:
    // - When we are trying to create a table - HiveDDL data is stored at the serDeProperties, but no Iceberg table
    // is created yet.
    // - When we are compiling the Hive query on HiveServer2 side - We only have table information (location/name),
    // and we have to read the schema using the table data. This is called multiple times so there is room for
    // optimizing here.
    // - When we are executing the Hive query in the execution engine - We do not want to load the table data on every
    // executor, but serDeProperties are populated by HiveIcebergStorageHandler.configureInputJobProperties() and
    // the resulting properties are serialized and distributed to the executors

    if (serDeProperties.get(InputFormatConfig.TABLE_SCHEMA) != null) {
      this.tableSchema = SchemaParser.fromJson(serDeProperties.getProperty(InputFormatConfig.TABLE_SCHEMA));
      if (serDeProperties.get(InputFormatConfig.PARTITION_SPEC) != null) {
        PartitionSpec spec =
            PartitionSpecParser.fromJson(tableSchema, serDeProperties.getProperty(InputFormatConfig.PARTITION_SPEC));
        this.partitionColumns = spec.fields().stream().map(PartitionField::name).collect(Collectors.toList());
      } else {
        this.partitionColumns = ImmutableList.of();
      }
    } else {
      try {
        Table table = IcebergTableUtil.getTable(configuration, serDeProperties);
        // always prefer the original table schema if there is one
        this.tableSchema = table.schema();
        this.partitionColumns = table.spec().fields().stream().map(PartitionField::name).collect(Collectors.toList());
        LOG.info("Using schema from existing table {}", SchemaParser.toJson(tableSchema));
      } catch (Exception e) {
        // During table creation we might not have the schema information from the Iceberg table, nor from the HMS
        // table. In this case we have to generate the schema using the serdeProperties which contains the info
        // provided in the CREATE TABLE query.

        if (serDeProperties.get("metadata_location") != null) {
          // If metadata location is provided, extract the schema details from it.
          try (FileIO fileIO = new HadoopFileIO(configuration)) {
            TableMetadata metadata = TableMetadataParser.read(fileIO, serDeProperties.getProperty("metadata_location"));
            this.tableSchema = metadata.schema();
            this.partitionColumns =
                metadata.spec().fields().stream().map(PartitionField::name).collect(Collectors.toList());
            // Validate no schema is provided via create command
            if (!getColumnNames().isEmpty() || !getPartitionColumnNames().isEmpty()) {
              throw new SerDeException("Column names can not be provided along with metadata location.");
            }
          }
        } else {
          boolean autoConversion = configuration.getBoolean(InputFormatConfig.SCHEMA_AUTO_CONVERSION, false);
          // If we can not load the table try the provided hive schema
          this.tableSchema = hiveSchemaOrThrow(e, autoConversion);
          // This is only for table creation, it is ok to have an empty partition column list
          this.partitionColumns = ImmutableList.of();
        }
        if (e instanceof NoSuchTableException &&
            HiveTableUtil.isCtas(serDeProperties) &&
            !Catalogs.hiveCatalog(configuration, serDeProperties)) {
          throw new SerDeException(CTAS_EXCEPTION_MSG);
        }
      }
    }

    this.projectedSchema =
        projectedSchema(configuration, serDeProperties.getProperty(Catalogs.NAME), tableSchema, jobConf);

    // Currently ClusteredWriter is used which requires that records are ordered by partition keys.
    // Here we ensure that SortedDynPartitionOptimizer will kick in and do the sorting.
    // TODO: remove once we have both Fanout and ClusteredWriter available: HIVE-25948
    HiveConf.setIntVar(configuration, HiveConf.ConfVars.HIVE_OPT_SORT_DYNAMIC_PARTITION_THRESHOLD, 1);
    HiveConf.setVar(configuration, HiveConf.ConfVars.DYNAMIC_PARTITIONING_MODE, "nonstrict");
    try {
      this.inspector = IcebergObjectInspector.create(projectedSchema);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  private static Schema projectedSchema(Configuration configuration, String tableName, Schema tableSchema,
      Map<String, String> jobConfs) {
    Context.Operation operation = HiveCustomStorageHandlerUtils.getWriteOperation(configuration::get, tableName);
    if (operation == null) {
      jobConfs.put(InputFormatConfig.CASE_SENSITIVE, "false");
      String[] selectedColumns = ColumnProjectionUtils.getReadColumnNames(configuration);
      // When same table is joined multiple times, it is possible some selected columns are duplicated,
      // in this case wrong recordStructField position leads wrong value or ArrayIndexOutOfBoundException
      String[] distinctSelectedColumns = Arrays.stream(selectedColumns).distinct().toArray(String[]::new);
      Schema projectedSchema = distinctSelectedColumns.length > 0 ?
          tableSchema.caseInsensitiveSelect(distinctSelectedColumns) : tableSchema;
      // the input split mapper handles does not belong to this table
      // it is necessary to ensure projectedSchema equals to tableSchema,
      // or we cannot find selectOperator's column from inspector
      if (projectedSchema.columns().size() != distinctSelectedColumns.length) {
        return tableSchema;
      } else {
        return projectedSchema;
      }
    }
    if (IcebergTableUtil.isCopyOnWriteMode(operation, configuration::get)) {
      return IcebergAcidUtil.createSerdeSchemaForDelete(tableSchema.columns());
    }
    switch (operation) {
      case DELETE:
        return IcebergAcidUtil.createSerdeSchemaForDelete(tableSchema.columns());
      case UPDATE:
        return IcebergAcidUtil.createSerdeSchemaForUpdate(tableSchema.columns());
      case OTHER:
        return tableSchema;
      default:
        throw new IllegalArgumentException("Unsupported operation " + operation);
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Container.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) {
    Deserializer deserializer = deserializers.get(objectInspector);
    if (deserializer == null) {
      deserializer = new Deserializer.Builder()
          .schema(projectedSchema)
          .sourceInspector((StructObjectInspector) objectInspector)
          .writerInspector((StructObjectInspector) inspector)
          .build();
      deserializers.put(objectInspector, deserializer);
    }

    row.set(deserializer.deserialize(o));
    return row;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public void handleJobLevelConfiguration(HiveConf conf) {
    for (Map.Entry<String, String> confs : jobConf.entrySet()) {
      conf.set(confs.getKey(), confs.getValue());
    }
  }

  @Override
  public Object deserialize(Writable writable) {
    return ((Container<?>) writable).get();
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }

  /**
   * Gets the hive schema and throws an exception if it is not provided. In the later case it adds the
   * previousException as a root cause.
   * @param previousException If we had an exception previously
   * @param autoConversion When <code>true</code>, convert unsupported types to more permissive ones, like tinyint to
   *                       int
   * @return The hive schema parsed from the serDeProperties provided when the SerDe was initialized
   * @throws SerDeException If there is no schema information in the serDeProperties
   */
  private Schema hiveSchemaOrThrow(Exception previousException, boolean autoConversion)
      throws SerDeException {
    List<String> names = Lists.newArrayList();
    names.addAll(getColumnNames());
    names.addAll(getPartitionColumnNames());

    List<TypeInfo> types = Lists.newArrayList();
    types.addAll(getColumnTypes());
    types.addAll(getPartitionColumnTypes());

    List<String> comments = Lists.newArrayList();
    comments.addAll(getColumnComments());
    comments.addAll(getPartitionColumnComments());

    if (!names.isEmpty() && !types.isEmpty()) {
      Schema hiveSchema = HiveSchemaUtil.convert(names, types, comments, autoConversion);
      LOG.info("Using hive schema {}", SchemaParser.toJson(hiveSchema));
      return hiveSchema;
    } else {
      throw new SerDeException("Please provide an existing table or a valid schema", previousException);
    }
  }

  /**
   * If the table already exists then returns the list of the current Iceberg partition column names.
   * If the table is not partitioned by Iceberg, or the table does not exists yet then returns an empty list.
   * @return The name of the Iceberg partition columns.
   */
  public Collection<String> partitionColumns() {
    return partitionColumns;
  }

  @Override
  public boolean shouldStoreFieldsInMetastore(Map<String, String> tableParams) {
    return true;
  }

  public Schema getTableSchema() {
    return tableSchema;
  }
}
