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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.serde.objectinspector.IcebergObjectInspector;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergSerDe extends AbstractSerDe {
  public static final String CTAS_EXCEPTION_MSG = "CTAS target table must be a HiveCatalog table." +
      " For other catalog types, the target Iceberg table would be created successfully but the table will not be" +
      " registered in HMS. This means that even though the CTAS query succeeds, the new table wouldn't be immediately" +
      " queryable from Hive, since HMS does not know about it.";

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergSerDe.class);
  private static final String LIST_COLUMN_COMMENT = "columns.comments";

  private ObjectInspector inspector;
  private Schema tableSchema;
  private Collection<String> partitionColumns;
  private Map<ObjectInspector, Deserializer> deserializers = new HashMap<>(1);
  private Container<Record> row = new Container<>();

  @Override
  public void initialize(@Nullable Configuration configuration, Properties serDeProperties,
                         Properties partitionProperties) throws SerDeException {
    // HiveIcebergSerDe.initialize is called multiple places in Hive code:
    // - When we are trying to create a table - HiveDDL data is stored at the serDeProperties, but no Iceberg table
    // is created yet.
    // - When we are compiling the Hive query on HiveServer2 side - We only have table information (location/name),
    // and we have to read the schema using the table data. This is called multiple times so there is room for
    // optimizing here.
    // - When we are executing the Hive query in the execution engine - We do not want to load the table data on every
    // executor, but serDeProperties are populated by HiveIcebergStorageHandler.configureInputJobProperties() and
    // the resulting properties are serialized and distributed to the executors

    // temporarily disabling vectorization in Tez, since it doesn't work with projection pruning (fix: TEZ-4248)
    // TODO: remove this once TEZ-4248 has been released and the Tez dependencies updated here
    assertNotVectorizedTez(configuration);

    if (serDeProperties.get(InputFormatConfig.TABLE_SCHEMA) != null) {
      this.tableSchema = SchemaParser.fromJson((String) serDeProperties.get(InputFormatConfig.TABLE_SCHEMA));
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
        boolean autoConversion = configuration.getBoolean(InputFormatConfig.SCHEMA_AUTO_CONVERSION, false);
        // If we can not load the table try the provided hive schema
        this.tableSchema = hiveSchemaOrThrow(serDeProperties, e, autoConversion);
        // This is only for table creation, it is ok to have an empty partition column list
        this.partitionColumns = ImmutableList.of();
        // create table for CTAS
        if (e instanceof NoSuchTableException &&
            Boolean.parseBoolean(serDeProperties.getProperty(hive_metastoreConstants.TABLE_IS_CTAS))) {
          if (!Catalogs.hiveCatalog(configuration, serDeProperties)) {
            throw new SerDeException(CTAS_EXCEPTION_MSG);
          }

          createTableForCTAS(configuration, serDeProperties);
        }
      }
    }

    Schema projectedSchema;
    if (serDeProperties.get(HiveIcebergStorageHandler.WRITE_KEY) != null) {
      // when writing out data, we should not do projection pushdown
      projectedSchema = tableSchema;
    } else {
      configuration.setBoolean(InputFormatConfig.CASE_SENSITIVE, false);
      String[] selectedColumns = ColumnProjectionUtils.getReadColumnNames(configuration);
      // When same table is joined multiple times, it is possible some selected columns are duplicated,
      // in this case wrong recordStructField position leads wrong value or ArrayIndexOutOfBoundException
      String[] distinctSelectedColumns = Arrays.stream(selectedColumns).distinct().toArray(String[]::new);
      projectedSchema = distinctSelectedColumns.length > 0 ?
              tableSchema.caseInsensitiveSelect(distinctSelectedColumns) : tableSchema;
      // the input split mapper handles does not belong to this table
      // it is necessary to ensure projectedSchema equals to tableSchema,
      // or we cannot find selectOperator's column from inspector
      if (projectedSchema.columns().size() != distinctSelectedColumns.length) {
        projectedSchema = tableSchema;
      }
    }

    try {
      this.inspector = IcebergObjectInspector.create(projectedSchema);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  private void createTableForCTAS(Configuration configuration, Properties serDeProperties) {
    serDeProperties.setProperty(TableProperties.ENGINE_HIVE_ENABLED, "true");
    serDeProperties.setProperty(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(tableSchema));

    // build partition spec, if any
    String partColsString = serDeProperties.getProperty(serdeConstants.LIST_PARTITION_COLUMNS);
    if (partColsString != null && !partColsString.isEmpty()) {
      String partColDelimiter = partColsString.contains(String.valueOf(ColumnType.COLUMN_COMMENTS_DELIMITER)) ?
          String.valueOf(ColumnType.COLUMN_COMMENTS_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
      String[] partCols = partColsString.split(partColDelimiter);
      String[] partColTypes = serDeProperties.getProperty(serdeConstants.LIST_PARTITION_COLUMN_TYPES)
          .split(String.valueOf(SerDeUtils.COLON));
      List<FieldSchema> partitionFields = IntStream.range(0, partCols.length)
          .mapToObj(i -> new FieldSchema(partCols[i], partColTypes[i], null))
          .collect(Collectors.toList());
      PartitionSpec spec = HiveSchemaUtil.spec(tableSchema, partitionFields);
      serDeProperties.put(InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(spec));
    }

    // create CTAS table
    LOG.info("Creating table {} for CTAS with schema: {}, and spec: {}",
        serDeProperties.get(Catalogs.NAME), tableSchema, serDeProperties.get(InputFormatConfig.PARTITION_SPEC));
    Catalogs.createTable(configuration, serDeProperties);

    // set this in the query state so that we can rollback the table in the lifecycle hook in case of failures
    SessionStateUtil.addResource(configuration, InputFormatConfig.CTAS_TABLE_NAME,
        serDeProperties.getProperty(Catalogs.NAME));
  }

  private void assertNotVectorizedTez(Configuration configuration) {
    if ("tez".equals(configuration.get("hive.execution.engine")) &&
        "true".equals(configuration.get("hive.vectorized.execution.enabled"))) {
      throw new UnsupportedOperationException("Vectorized execution on Tez is currently not supported when using " +
          "Iceberg tables. Please set hive.vectorized.execution.enabled=false and rerun the query.");
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
          .schema(tableSchema)
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
  public Object deserialize(Writable writable) {
    return ((Container<?>) writable).get();
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }

  /**
   * Gets the hive schema from the serDeProperties, and throws an exception if it is not provided. In the later case
   * it adds the previousException as a root cause.
   * @param serDeProperties The source of the hive schema
   * @param previousException If we had an exception previously
   * @param autoConversion When <code>true</code>, convert unsupported types to more permissive ones, like tinyint to
   *                       int
   * @return The hive schema parsed from the serDeProperties
   * @throws SerDeException If there is no schema information in the serDeProperties
   */
  private static Schema hiveSchemaOrThrow(Properties serDeProperties, Exception previousException,
                                          boolean autoConversion)
      throws SerDeException {
    // Read the configuration parameters
    String columnNames = serDeProperties.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypes = serDeProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    // No constant for column comments and column comments delimiter.
    String columnComments = serDeProperties.getProperty(LIST_COLUMN_COMMENT);
    String columnNameDelimiter = serDeProperties.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ?
        serDeProperties.getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
    if (columnNames != null && columnTypes != null && columnNameDelimiter != null &&
        !columnNames.isEmpty() && !columnTypes.isEmpty() && !columnNameDelimiter.isEmpty()) {
      // Parse the configuration parameters
      List<String> names = new ArrayList<>();
      Collections.addAll(names, columnNames.split(columnNameDelimiter));
      // check if there are partition columns as well
      String partColNames = serDeProperties.getProperty(serdeConstants.LIST_PARTITION_COLUMNS);
      if (partColNames != null && !partColNames.isEmpty()) {
        // add partition col names to regular col names
        String partColDelimiter = partColNames.contains(String.valueOf(ColumnType.COLUMN_COMMENTS_DELIMITER)) ?
            String.valueOf(ColumnType.COLUMN_COMMENTS_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
        Collections.addAll(names, partColNames.split(partColDelimiter));
        // add partition col types to regular col types
        columnTypes += SerDeUtils.COLON + serDeProperties.getProperty(serdeConstants.LIST_PARTITION_COLUMN_TYPES);
      }
      List<String> comments = new ArrayList<>();
      if (columnComments != null) {
        Collections.addAll(comments, columnComments.split(Character.toString(Character.MIN_VALUE)));
      }
      Schema hiveSchema = HiveSchemaUtil.convert(names, TypeInfoUtils.getTypeInfosFromTypeString(columnTypes),
              comments, autoConversion);
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
}
