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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.RowLineageUtils;
import org.apache.hadoop.hive.ql.security.authorization.HiveCustomStorageHandlerUtils;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.MetadataColumns;
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
import org.apache.iceberg.mr.hive.writer.BucketAwareContainer;
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
  private final Map<ObjectInspector, Deserializer> deserializers = Maps.newHashMapWithExpectedSize(1);
  private final Container<Record> row = new Container<>();
  private final BucketAwareContainer<Record> bucketAwareRow = new BucketAwareContainer<>();
  private final Map<String, String> jobConf =  Maps.newHashMap();

  private boolean hiveBucketingRouteEnabled = false;
  private int hiveBucketingNumBuckets = -1;
  private int hiveBucketingVersion = 2;
  private List<String> hiveBucketingBucketCols = ImmutableList.of();
  private final Map<ObjectInspector, HiveBucketComputer> bucketComputers = Maps.newHashMapWithExpectedSize(1);

  @Override
  public void initialize(Configuration conf, Properties serDeProperties,
      Properties partitionProperties) throws SerDeException {
    super.initialize(conf, serDeProperties, partitionProperties);

    // HiveIcebergSerDe.initialize is called multiple places in Hive code:
    // - When we are trying to create a table - HiveDDL data is stored at the serDeProperties, but no Iceberg table
    // is created yet.
    // - When we are compiling the Hive query on HiveServer2 side - We only have table information (location/name),
    // and we have to read the schema using the table data. This is called multiple times so there is room for
    // optimizing here.
    // - When we are executing the Hive query in the execution engine - We do not want to load the table data on every
    // executor, but serDeProperties are populated by HiveIcebergStorageHandler.configureInputJobProperties() and
    // the resulting properties are serialized and distributed to the executors

    initTableSchemaAndPartitionColumns(conf, serDeProperties);
    this.projectedSchema =
        projectedSchema(conf, serDeProperties.getProperty(Catalogs.NAME), tableSchema, jobConf);
    initHiveBucketingRoute(conf, serDeProperties);
    configureDynamicPartitionSorting(conf, serDeProperties);
    this.inspector = createInspector(projectedSchema);
  }

  private void initTableSchemaAndPartitionColumns(Configuration conf, Properties serDeProperties)
      throws SerDeException {
    if (serDeProperties.get(InputFormatConfig.TABLE_SCHEMA) != null) {
      initTableSchemaFromSerdeProperties(serDeProperties);
      return;
    }
    try {
      Table table = IcebergTableUtil.getTable(conf, serDeProperties);
      // always prefer the original table schema if there is one
      this.tableSchema = table.schema();
      this.partitionColumns = table.spec().fields().stream().map(PartitionField::name).collect(Collectors.toList());
      LOG.info("Using schema from existing table {}", SchemaParser.toJson(tableSchema));
    } catch (Exception e) {
      initTableSchemaOnLoadFailure(conf, serDeProperties, e);
    }
  }

  private void initTableSchemaFromSerdeProperties(Properties serDeProperties) {
    this.tableSchema = SchemaParser.fromJson(serDeProperties.getProperty(InputFormatConfig.TABLE_SCHEMA));
    if (serDeProperties.get(InputFormatConfig.PARTITION_SPEC) != null) {
      PartitionSpec spec =
          PartitionSpecParser.fromJson(tableSchema, serDeProperties.getProperty(InputFormatConfig.PARTITION_SPEC));
      this.partitionColumns = spec.fields().stream().map(PartitionField::name).collect(Collectors.toList());
    } else {
      this.partitionColumns = ImmutableList.of();
    }
  }

  private void initTableSchemaOnLoadFailure(Configuration conf, Properties serDeProperties, Exception loadException)
      throws SerDeException {
    // During table creation we might not have the schema information from the Iceberg table, nor from the HMS
    // table. In this case we have to generate the schema using the serdeProperties which contains the info
    // provided in the CREATE TABLE query.
    if (serDeProperties.get("metadata_location") != null) {
      initTableSchemaFromMetadataLocation(conf, serDeProperties);
    } else {
      boolean autoConversion = conf.getBoolean(InputFormatConfig.SCHEMA_AUTO_CONVERSION, false);
      // If we can not load the table try the provided hive schema
      this.tableSchema = hiveSchemaOrThrow(loadException, autoConversion);
      // This is only for table creation, it is ok to have an empty partition column list
      this.partitionColumns = ImmutableList.of();
    }
    if (loadException instanceof NoSuchTableException &&
        HiveTableUtil.isCtas(serDeProperties) && !Catalogs.hiveCatalog(conf, serDeProperties)) {
      throw new SerDeException(CTAS_EXCEPTION_MSG);
    }
  }

  private void initTableSchemaFromMetadataLocation(Configuration conf, Properties serDeProperties)
      throws SerDeException {
    // If metadata location is provided, extract the schema details from it.
    try (FileIO fileIO = new HadoopFileIO(conf)) {
      TableMetadata metadata = TableMetadataParser.read(fileIO, serDeProperties.getProperty("metadata_location"));
      this.tableSchema = metadata.schema();
      this.partitionColumns =
          metadata.spec().fields().stream().map(PartitionField::name).collect(Collectors.toList());
      // Validate no schema is provided via create command
      if (!getColumnNames().isEmpty() || !getPartitionColumnNames().isEmpty()) {
        throw new SerDeException("Column names can not be provided along with metadata location.");
      }
    } catch (SerDeException e) {
      throw e;
    } catch (Exception e) {
      throw new SerDeException("Failed to load schema from metadata location", e);
    }
  }

  private void initHiveBucketingRoute(Configuration conf, Properties serDeProperties) throws SerDeException {
    final String tableName = serDeProperties.getProperty(Catalogs.NAME);
    if (tableName == null) {
      return;
    }
    hiveBucketingRouteEnabled =
        HiveCustomStorageHandlerUtils.getIcebergHiveBucketingRouteEnabled(conf::get, tableName);
    if (!hiveBucketingRouteEnabled) {
      return;
    }
    HiveIcebergHiveBucketingMetadata bucketingMetadata = HiveIcebergHiveBucketingMetadata.load(conf, tableName);
    if (!bucketingMetadata.hasHiveBucketing()) {
      throw new SerDeException("Bucket routing enabled but HMS table has no CLUSTERED BY metadata: " + tableName);
    }
    hiveBucketingNumBuckets = bucketingMetadata.numBuckets();
    hiveBucketingVersion = bucketingMetadata.bucketingVersion();
    hiveBucketingBucketCols = bucketingMetadata.bucketCols();
  }

  private static void configureDynamicPartitionSorting(Configuration conf, Properties serDeProperties) {
    if (!IcebergTableUtil.isFanoutEnabled(Maps.fromProperties(serDeProperties))) {
      // ClusteredWriter requires that records are ordered by partition keys.
      // Here we ensure that SortedDynPartitionOptimizer will kick in and do the sorting.
      HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_OPT_SORT_DYNAMIC_PARTITION_THRESHOLD, 1);
    }
    HiveConf.setVar(conf, HiveConf.ConfVars.DYNAMIC_PARTITIONING_MODE, "nonstrict");
  }

  private static ObjectInspector createInspector(Schema schema) throws SerDeException {
    try {
      return IcebergObjectInspector.create(schema);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  private static Schema projectedSchema(Configuration conf, String tableName, Schema tableSchema,
      Map<String, String> jobConf) {
    Context.Operation operation = HiveCustomStorageHandlerUtils.getWriteOperation(conf::get, tableName);
    if (operation == null) {
      jobConf.put(InputFormatConfig.CASE_SENSITIVE, "false");
      String[] selectedColumns = ColumnProjectionUtils.getReadColumnNames(conf);
      // When same table is joined multiple times, it is possible some selected columns are duplicated,
      // in this case wrong recordStructField position leads wrong value or ArrayIndexOutOfBoundException
      String[] distinctSelectedColumns = Arrays.stream(selectedColumns).distinct().toArray(String[]::new);
      Schema projectedSchema = distinctSelectedColumns.length > 0 ?
          tableSchema.caseInsensitiveSelect(distinctSelectedColumns) : tableSchema;
      // the input split mapper handles does not belong to this table
      // it is necessary to ensure projectedSchema equals to tableSchema,
      // or we cannot find selectOperator's column from inspector
      if (projectedSchema.columns().size() != distinctSelectedColumns.length) {
        return RowLineageUtils.isRowLineageInsert(conf) ?
            MetadataColumns.schemaWithRowLineage(tableSchema) :
            tableSchema;
      } else {
        return projectedSchema;
      }
    }
    boolean isCOW = IcebergTableUtil.isCopyOnWriteMode(operation, conf::get);
    if (isCOW) {
      return getSchemaWithRowLineage(IcebergAcidUtil.createSerdeSchemaForDelete(tableSchema.columns()), conf);
    }
    switch (operation) {
      case DELETE:
        return IcebergAcidUtil.createSerdeSchemaForDelete(tableSchema.columns());
      case UPDATE:
        return IcebergAcidUtil.createSerdeSchemaForUpdate(tableSchema.columns());
      case OTHER:
        return getSchemaWithRowLineage(tableSchema, conf);
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
    if (hiveBucketingRouteEnabled) {
      HiveBucketComputer computer = bucketComputers.get(objectInspector);
      if (computer == null) {
        if (!(objectInspector instanceof StructObjectInspector)) {
          throw new IllegalStateException("Expected StructObjectInspector for bucketing route but got: " +
              objectInspector.getTypeName());
        }
        computer = new HiveBucketComputer((StructObjectInspector) objectInspector, hiveBucketingBucketCols,
            tableSchemaColumnNames(), hiveBucketingNumBuckets, hiveBucketingVersion);
        bucketComputers.put(objectInspector, computer);
      }
      int bucketId = computer.bucketId(o);
      bucketAwareRow.set(row.get(), bucketId);
      return bucketAwareRow;
    }
    return row;
  }

  @Override
  public void handleJobLevelConfiguration(HiveConf conf) {
    jobConf.forEach(conf::set);
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
   * If the table is not partitioned by Iceberg, or the table does not exist yet then returns an empty list.
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

  private List<String> tableSchemaColumnNames() {
    if (tableSchema == null) {
      return ImmutableList.of();
    }
    return tableSchema.columns().stream()
        .map(col -> col.name())
        .collect(Collectors.toList());
  }

  private static final class HiveBucketComputer {
    private final StructObjectInspector inspector;
    private final List<StructField> bucketFields;
    private final ObjectInspector[] bucketFieldInspectors;
    private final int numBuckets;
    private final int bucketingVersion;

    private HiveBucketComputer(
        StructObjectInspector inspector, List<String> bucketColNames, List<String> tableColumnNames,
        int numBuckets, int bucketingVersion) {
      this.inspector = inspector;
      this.numBuckets = numBuckets;
      this.bucketingVersion = bucketingVersion;
      this.bucketFields = Lists.newArrayListWithCapacity(bucketColNames.size());
      for (String bucketColName : bucketColNames) {
        bucketFields.add(resolveBucketField(inspector, bucketColName, tableColumnNames));
      }
      this.bucketFieldInspectors = new ObjectInspector[bucketFields.size()];
      for (int i = 0; i < bucketFields.size(); i++) {
        bucketFieldInspectors[i] = bucketFields.get(i).getFieldObjectInspector();
      }
    }

    private static StructField resolveBucketField(
        StructObjectInspector inspector, String bucketColName, List<String> tableColumnNames) {
      List<? extends StructField> rowFields = inspector.getAllStructFieldRefs();
      for (StructField rowField : rowFields) {
        if (bucketColName.equalsIgnoreCase(rowField.getFieldName())) {
          return rowField;
        }
      }
      // SDPO reduce path rows use internal names (_col0, ...); map logical bucket cols via table order.
      if (tableColumnNames != null && !tableColumnNames.isEmpty()) {
        for (int i = 0; i < tableColumnNames.size(); i++) {
          if (bucketColName.equalsIgnoreCase(tableColumnNames.get(i)) && i < rowFields.size()) {
            return rowFields.get(i);
          }
        }
      }
      throw new IllegalStateException(
          "Bucket column not found in row inspector: " + bucketColName + ", fields: " + rowFields);
    }

    int bucketId(Object row) {
      if (numBuckets <= 0) {
        throw new IllegalStateException("Invalid numBuckets for bucketing route: " + numBuckets);
      }
      Object[] values = new Object[bucketFields.size()];
      for (int i = 0; i < bucketFields.size(); i++) {
        values[i] = inspector.getStructFieldData(row, bucketFields.get(i));
      }
      if (bucketingVersion == 1) {
        return ObjectInspectorUtils.getBucketNumberOld(values, bucketFieldInspectors, numBuckets);
      }
      return ObjectInspectorUtils.getBucketNumber(values, bucketFieldInspectors, numBuckets);
    }
  }

  private static Schema getSchemaWithRowLineage(Schema schema, Configuration conf) {
    boolean rowLineage = Boolean.parseBoolean(conf.get(SessionStateUtil.ROW_LINEAGE));
    if (rowLineage) {
      return MetadataColumns.schemaWithRowLineage(schema);
    }
    return schema;
  }
}
