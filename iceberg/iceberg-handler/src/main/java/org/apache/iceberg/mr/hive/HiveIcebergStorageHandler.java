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
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.Context.Operation;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.hive.ql.stats.Partish;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.base.Throwables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergStorageHandler implements HiveStoragePredicateHandler, HiveStorageHandler {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergStorageHandler.class);

  private static final String ICEBERG_URI_PREFIX = "iceberg://";
  private static final Splitter TABLE_NAME_SPLITTER = Splitter.on("..");
  private static final String TABLE_NAME_SEPARATOR = "..";
  /**
   * Function template for producing a custom sort expression function:
   * Takes the source column index and the bucket count to creat a function where Iceberg bucket UDF is used to build
   * the sort expression, e.g. iceberg_bucket(_col2, 5)
   */
  private static final transient BiFunction<Integer, Integer, Function<List<ExprNodeDesc>, ExprNodeDesc>>
      BUCKET_SORT_EXPR =
          (idx, bucket) -> cols -> {
            try {
              ExprNodeDesc icebergBucketSourceCol = cols.get(idx);
              return ExprNodeGenericFuncDesc.newInstance(new GenericUDFIcebergBucket(), "iceberg_bucket",
                  Lists.newArrayList(
                      icebergBucketSourceCol,
                      new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, bucket)
                  ));
            } catch (UDFArgumentException e) {
              throw new RuntimeException(e);
            }
          };

  private static final List<VirtualColumn> ACID_VIRTUAL_COLS = ImmutableList.of(VirtualColumn.PARTITION_SPEC_ID,
      VirtualColumn.PARTITION_HASH, VirtualColumn.FILE_PATH, VirtualColumn.ROW_POSITION);
  private static final List<FieldSchema> ACID_VIRTUAL_COLS_AS_FIELD_SCHEMA = ACID_VIRTUAL_COLS.stream()
      .map(v -> new FieldSchema(v.getName(), v.getTypeInfo().getTypeName(), ""))
      .collect(Collectors.toList());


  private Configuration conf;

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveIcebergInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveIcebergOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return HiveIcebergSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    // Make sure to always return a new instance here, as HiveIcebergMetaHook might hold state relevant for the
    // operation.
    return new HiveIcebergMetaHook(conf);
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() {
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> map) {
    overlayTableProperties(conf, tableDesc, map);
    // Until the vectorized reader can handle delete files, let's fall back to non-vector mode for V2 tables
    fallbackToNonVectorizedModeBasedOnProperties(tableDesc.getProperties());
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> map) {
    overlayTableProperties(conf, tableDesc, map);
    // Until the vectorized reader can handle delete files, let's fall back to non-vector mode for V2 tables
    fallbackToNonVectorizedModeBasedOnProperties(tableDesc.getProperties());
    // For Tez, setting the committer here is enough to make sure it'll be part of the jobConf
    map.put("mapred.output.committer.class", HiveIcebergNoJobCommitter.class.getName());
    // For MR, the jobConf is set only in configureJobConf, so we're setting the write key here to detect it over there
    String opType = getOperationType();
    map.put(InputFormatConfig.OPERATION_TYPE_PREFIX + tableDesc.getTableName(), opType);
    // Putting the key into the table props as well, so that projection pushdown can be determined on a
    // table-level and skipped only for output tables in HiveIcebergSerde. Properties from the map will be present in
    // the serde config for all tables in the query, not just the output tables, so we can't rely on that in the serde.
    tableDesc.getProperties().put(InputFormatConfig.OPERATION_TYPE_PREFIX + tableDesc.getTableName(), opType);
  }

  /**
   * Committer with no-op job commit. We can pass this into the Tez AM to take care of task commits/aborts, as well
   * as aborting jobs reliably if an execution error occurred. However, we want to execute job commits on the
   * HS2-side during the MoveTask, so we will use the full-featured HiveIcebergOutputCommitter there.
   */
  static class HiveIcebergNoJobCommitter extends HiveIcebergOutputCommitter {
    @Override
    public void commitJob(JobContext originalContext) throws IOException {
      // do nothing
    }
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> map) {

  }

  // Override annotation commented out, since this interface method has been introduced only in Hive 3
  // @Override
  public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> secrets) {

  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    setCommonJobConf(jobConf);
    if (tableDesc != null && tableDesc.getProperties() != null &&
        tableDesc.getProperties().get(InputFormatConfig.OPERATION_TYPE_PREFIX + tableDesc.getTableName()) != null) {
      String tableName = tableDesc.getTableName();
      String opKey = InputFormatConfig.OPERATION_TYPE_PREFIX + tableName;
      // set operation type into job conf too
      jobConf.set(opKey, tableDesc.getProperties().getProperty(opKey));
      Preconditions.checkArgument(!tableName.contains(TABLE_NAME_SEPARATOR),
          "Can not handle table " + tableName + ". Its name contains '" + TABLE_NAME_SEPARATOR + "'");
      String tables = jobConf.get(InputFormatConfig.OUTPUT_TABLES);
      tables = tables == null ? tableName : tables + TABLE_NAME_SEPARATOR + tableName;
      jobConf.set(InputFormatConfig.OUTPUT_TABLES, tables);

      String catalogName = tableDesc.getProperties().getProperty(InputFormatConfig.CATALOG_NAME);
      if (catalogName != null) {
        jobConf.set(InputFormatConfig.TABLE_CATALOG_PREFIX + tableName, catalogName);
      }
    }
    try {
      if (!jobConf.getBoolean(HiveConf.ConfVars.HIVE_IN_TEST_IDE.varname, false)) {
        // For running unit test this won't work as maven surefire CP is different than what we have on a cluster:
        // it places the current projects' classes and test-classes to top instead of jars made from these...
        Utilities.addDependencyJars(jobConf, HiveIcebergStorageHandler.class);
      }
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Override
  public boolean directInsertCTAS() {
    return true;
  }

  @Override
  public boolean alwaysUnpartitioned() {
    return true;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }

  /**
   * @param jobConf Job configuration for InputFormat to access
   * @param deserializer Deserializer
   * @param exprNodeDesc Filter expression extracted by Hive
   * @return Entire filter to take advantage of Hive's pruning as well as Iceberg's pruning.
   */
  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer, ExprNodeDesc exprNodeDesc) {
    DecomposedPredicate predicate = new DecomposedPredicate();
    predicate.residualPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
    predicate.pushedPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
    return predicate;
  }

  @Override
  public boolean canProvideBasicStatistics() {
    return true;
  }

  @Override
  public Map<String, String> getBasicStatistics(Partish partish) {
    org.apache.hadoop.hive.ql.metadata.Table hmsTable = partish.getTable();
    TableDesc tableDesc = Utilities.getTableDesc(hmsTable);
    Table table = Catalogs.loadTable(conf, tableDesc.getProperties());
    Map<String, String> stats = Maps.newHashMap();
    if (table.currentSnapshot() != null) {
      Map<String, String> summary = table.currentSnapshot().summary();
      if (summary != null) {
        if (summary.containsKey(SnapshotSummary.TOTAL_DATA_FILES_PROP)) {
          stats.put(StatsSetupConst.NUM_FILES, summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
        }
        if (summary.containsKey(SnapshotSummary.TOTAL_RECORDS_PROP)) {
          stats.put(StatsSetupConst.ROW_COUNT, summary.get(SnapshotSummary.TOTAL_RECORDS_PROP));
        }
        if (summary.containsKey(SnapshotSummary.TOTAL_FILE_SIZE_PROP)) {
          stats.put(StatsSetupConst.TOTAL_SIZE, summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP));
        }
      }
    } else {
      stats.put(StatsSetupConst.NUM_FILES, "0");
      stats.put(StatsSetupConst.ROW_COUNT, "0");
      stats.put(StatsSetupConst.TOTAL_SIZE, "0");
    }
    return stats;
  }

  /**
   * No need for exclusive locks when writing, since Iceberg tables use optimistic concurrency when writing
   * and only lock the table during the commit operation.
   */
  @Override
  public LockType getLockType(WriteEntity writeEntity) {
    return LockType.SHARED_READ;
  }

  @Override
  public boolean supportsPartitionTransform() {
    return true;
  }

  @Override
  public List<TransformSpec> getPartitionTransformSpec(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    TableDesc tableDesc = Utilities.getTableDesc(hmsTable);
    Table table = IcebergTableUtil.getTable(conf, tableDesc.getProperties());
    return table.spec().fields().stream().map(f ->
      getTransformSpec(table, f.transform().toString().toUpperCase(), f.sourceId())
    ).collect(Collectors.toList());
  }

  private List<TransformSpec> getSortTransformSpec(Table table) {
    return table.sortOrder().fields().stream().map(s ->
      getTransformSpec(table, s.transform().toString().toUpperCase(), s.sourceId())
    ).collect(Collectors.toList());
  }

  private TransformSpec getTransformSpec(Table table, String transformName, int sourceId) {
    TransformSpec spec = new TransformSpec();
    spec.setColumnName(table.schema().findColumnName(sourceId));
    // if the transform name contains '[' it means it has some config params
    if (transformName.contains("[")) {
      spec.setTransformType(TransformSpec.TransformType
          .valueOf(transformName.substring(0, transformName.indexOf("["))));
      spec.setTransformParam(Optional.of(Integer
          .valueOf(transformName.substring(transformName.indexOf("[") + 1, transformName.indexOf("]")))));
    } else {
      spec.setTransformType(TransformSpec.TransformType.valueOf(transformName));
      spec.setTransformParam(Optional.empty());
    }

    return spec;
  }

  @Override
  public DynamicPartitionCtx createDPContext(
          HiveConf hiveConf, org.apache.hadoop.hive.ql.metadata.Table hmsTable, Operation writeOperation)
      throws SemanticException {
    // delete records are already clustered by partition spec id and the hash of the partition struct
    // there is no need to do any additional sorting based on partition columns
    if (writeOperation == Operation.DELETE) {
      return null;
    }

    TableDesc tableDesc = Utilities.getTableDesc(hmsTable);
    Table table = IcebergTableUtil.getTable(conf, tableDesc.getProperties());

    DynamicPartitionCtx dpCtx = new DynamicPartitionCtx(Maps.newLinkedHashMap(),
        hiveConf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME),
        hiveConf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTSPERNODE));
    List<Function<List<ExprNodeDesc>, ExprNodeDesc>> customSortExprs = Lists.newLinkedList();
    dpCtx.setCustomSortExpressions(customSortExprs);

    if (table.spec().isPartitioned()) {
      addCustomSortExpr(table, hmsTable, writeOperation, customSortExprs, getPartitionTransformSpec(hmsTable));
    }

    SortOrder sortOrder = table.sortOrder();
    if (sortOrder.isSorted()) {
      List<Integer> customSortPositions = Lists.newLinkedList();
      List<Integer> customSortOrder = Lists.newLinkedList();
      dpCtx.setCustomSortOrder(customSortOrder);
      List<Integer> customSortNullOrder = Lists.newLinkedList();
      dpCtx.setCustomSortNullOrder(customSortNullOrder);
      for (SortField sortField : sortOrder.fields()) {
        int pos = 0;
        for (Types.NestedField field : table.schema().columns()) {
          if (sortField.sourceId() == field.fieldId()) {
            customSortPositions.add(pos);
            customSortOrder.add(sortField.direction() == SortDirection.ASC ? 1 : 0);
            customSortNullOrder.add(sortField.nullOrder() == NullOrder.NULLS_FIRST ? 0 : 1);
            break;
          }
          pos++;
        }
      }

      addCustomSortExpr(table, hmsTable, writeOperation, customSortExprs, getSortTransformSpec(table));
    }

    return dpCtx;
  }

  private void addCustomSortExpr(Table table,  org.apache.hadoop.hive.ql.metadata.Table hmsTable,
      Operation writeOperation, List<Function<List<ExprNodeDesc>, ExprNodeDesc>> customSortExprs,
      List<TransformSpec> transformSpecs) {
    Map<String, Integer> fieldOrderMap = Maps.newHashMap();
    List<Types.NestedField> fields = table.schema().columns();
    for (int i = 0; i < fields.size(); ++i) {
      fieldOrderMap.put(fields.get(i).name(), i);
    }

    int offset = acidSelectColumns(hmsTable, writeOperation).size();

    for (TransformSpec spec : transformSpecs) {
      int order = fieldOrderMap.get(spec.getColumnName());
      if (TransformSpec.TransformType.BUCKET.equals(spec.getTransformType())) {
        customSortExprs.add(BUCKET_SORT_EXPR.apply(order + offset, spec.getTransformParam().get()));
      } else {
        customSortExprs.add(cols -> cols.get(order + offset).clone());
      }
    }
  }

  @Override
  public String getFileFormatPropertyKey() {
    return TableProperties.DEFAULT_FILE_FORMAT;
  }

  @Override
  public boolean commitInMoveTask() {
    return true;
  }

  @Override
  public void storageHandlerCommit(Properties commitProperties, boolean overwrite) throws HiveException {
    String tableName = commitProperties.getProperty(Catalogs.NAME);
    Configuration configuration = SessionState.getSessionConf();
    List<JobContext> jobContextList = generateJobContext(configuration, tableName, overwrite);
    if (jobContextList.isEmpty()) {
      return;
    }

    HiveIcebergOutputCommitter committer = new HiveIcebergOutputCommitter();
    try {
      committer.commitJobs(jobContextList);
    } catch (Throwable e) {
      String ids = jobContextList
          .stream().map(jobContext -> jobContext.getJobID().toString()).collect(Collectors.joining(", "));
      // Aborting the job if the commit has failed
      LOG.error("Error while trying to commit job: {}, starting rollback changes for table: {}",
          ids, tableName, e);
      try {
        committer.abortJobs(jobContextList);
      } catch (IOException ioe) {
        LOG.error("Error while trying to abort failed job. There might be uncleaned data files.", ioe);
        // no throwing here because the original exception should be propagated
      }
      throw new HiveException(
          "Error committing job: " + ids + " for table: " + tableName, e);
    }
  }

  @Override
  public boolean isAllowedAlterOperation(AlterTableType opType) {
    return HiveIcebergMetaHook.SUPPORTED_ALTER_OPS.contains(opType);
  }

  @Override
  public boolean supportsTruncateOnNonNativeTables() {
    return true;
  }

  @Override
  public boolean isTimeTravelAllowed() {
    return true;
  }

  @Override
  public boolean isMetadataTableSupported() {
    return true;
  }

  @Override
  public void executeOperation(org.apache.hadoop.hive.ql.metadata.Table hmsTable, AlterTableExecuteSpec executeSpec) {
    TableDesc tableDesc = Utilities.getTableDesc(hmsTable);
    Table icebergTable = IcebergTableUtil.getTable(conf, tableDesc.getProperties());
    switch (executeSpec.getOperationType()) {
      case ROLLBACK:
        LOG.info("Executing rollback operation on iceberg table. If you would like to revert rollback you could " +
              "try altering the metadata location to the current metadata location by executing the following query:" +
              "ALTER TABLE {}.{} SET TBLPROPERTIES('metadata_location'='{}'). This operation is supported for Hive " +
              "Catalog tables.", hmsTable.getDbName(), hmsTable.getTableName(),
            ((BaseTable) icebergTable).operations().current().metadataFileLocation());
        AlterTableExecuteSpec.RollbackSpec rollbackSpec =
            (AlterTableExecuteSpec.RollbackSpec) executeSpec.getOperationParams();
        IcebergTableUtil.rollback(icebergTable, rollbackSpec.getRollbackType(), rollbackSpec.getParam());
        break;
      case EXPIRE_SNAPSHOT:
        LOG.info("Executing expire snapshots operation on iceberg table {}.{}", hmsTable.getDbName(),
            hmsTable.getTableName());
        AlterTableExecuteSpec.ExpireSnapshotsSpec expireSnapshotsSpec =
            (AlterTableExecuteSpec.ExpireSnapshotsSpec) executeSpec.getOperationParams();
        icebergTable.expireSnapshots().expireOlderThan(expireSnapshotsSpec.getTimestampMillis()).commit();
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Operation type %s is not supported", executeSpec.getOperationType().name()));
    }
  }

  @Override
  public boolean isValidMetadataTable(String metaTableName) {
    return IcebergMetadataTables.isValidMetaTable(metaTableName);
  }

  @Override
  public URI getURIForAuth(org.apache.hadoop.hive.metastore.api.Table hmsTable) throws URISyntaxException {
    String dbName = hmsTable.getDbName();
    String tableName = hmsTable.getTableName();
    StringBuilder authURI = new StringBuilder(ICEBERG_URI_PREFIX).append(dbName).append("/").append(tableName)
        .append("?snapshot=");
    Optional<String> locationProperty = SessionStateUtil.getProperty(conf, hive_metastoreConstants.META_TABLE_LOCATION);
    if (locationProperty.isPresent()) {
      Preconditions.checkArgument(locationProperty.get() != null,
          "Table location is not set in SessionState. Authorization URI cannot be supplied.");
      // this property is set during the create operation before the hive table was created
      // we are returning a dummy iceberg metadata file
      authURI.append(URI.create(locationProperty.get()).getPath()).append("/metadata/dummy.metadata.json");
    } else {
      Table table = IcebergTableUtil.getTable(conf, hmsTable);
      authURI.append(URI.create(((BaseTable) table).operations().current().metadataFileLocation()).getPath());
    }
    LOG.debug("Iceberg storage handler authorization URI {}", authURI);
    return new URI(HiveConf.EncoderDecoderFactory.URL_ENCODER_DECODER.encode(authURI.toString()));
  }


  @Override
  public void validateSinkDesc(FileSinkDesc sinkDesc) throws SemanticException {
    HiveStorageHandler.super.validateSinkDesc(sinkDesc);
    if (sinkDesc.getInsertOverwrite()) {
      Table table = IcebergTableUtil.getTable(conf, sinkDesc.getTableInfo().getProperties());
      if (IcebergTableUtil.isBucketed(table)) {
        throw new SemanticException("Cannot perform insert overwrite query on bucket partitioned Iceberg table.");
      }
      if (table.currentSnapshot() != null) {
        if (table.currentSnapshot().allManifests().parallelStream().map(ManifestFile::partitionSpecId)
            .anyMatch(id -> id < table.spec().specId())) {
          throw new SemanticException(
              "Cannot perform insert overwrite query on Iceberg table where partition evolution happened. In order " +
              "to succesfully carry out any insert overwrite operation on this table, the data has to be rewritten " +
              "conforming to the latest spec. ");
        }
      }
    }
  }

  @Override
  public AcidSupportType supportsAcidOperations() {
    return AcidSupportType.WITHOUT_TRANSACTIONS;
  }

  @Override
  public List<VirtualColumn> acidVirtualColumns() {
    return ACID_VIRTUAL_COLS;
  }

  @Override
  public List<FieldSchema> acidSelectColumns(org.apache.hadoop.hive.ql.metadata.Table table, Operation operation) {
    switch (operation) {
      case DELETE:
      case UPDATE:
        // TODO: make it configurable whether we want to include the table columns in the select query.
        // It might make delete writes faster if we don't have to write out the row object
        return Stream.of(ACID_VIRTUAL_COLS_AS_FIELD_SCHEMA, table.getCols())
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
      default:
        return ImmutableList.of();
    }
  }

  @Override
  public List<FieldSchema> acidSortColumns(org.apache.hadoop.hive.ql.metadata.Table table, Operation operation) {
    switch (operation) {
      case DELETE:
        return ACID_VIRTUAL_COLS_AS_FIELD_SCHEMA;
      default:
        // For update operations we use the same sort order defined by
        // {@link #createDPContext(HiveConf, org.apache.hadoop.hive.ql.metadata.Table)}
        return ImmutableList.of();
    }
  }

  @Override
  public boolean supportsSortColumns() {
    return true;
  }

  @Override
  public List<FieldSchema> sortColumns(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    TableDesc tableDesc = Utilities.getTableDesc(hmsTable);
    Table table = IcebergTableUtil.getTable(conf, tableDesc.getProperties());
    if (table.sortOrder().isUnsorted()) {
      return Collections.emptyList();
    }

    Schema schema = table.schema();
    return table.sortOrder().fields().stream().map(s -> new FieldSchema(schema.findColumnName(s.sourceId()),
        schema.findType(s.sourceId()).toString(),
        String.format("Transform: %s, Sort direction: %s, Null sort order: %s",
        s.transform().toString(), s.direction().name(), s.nullOrder().name()))).collect(Collectors.toList());
  }

  private void setCommonJobConf(JobConf jobConf) {
    jobConf.set("tez.mrreader.config.update.properties", "hive.io.file.readcolumn.names,hive.io.file.readcolumn.ids");
  }

  public boolean addDynamicSplitPruningEdge(org.apache.hadoop.hive.ql.metadata.Table table,
                                            ExprNodeDesc syntheticFilterPredicate) {
    try {
      Collection<String> partitionColumns = ((HiveIcebergSerDe) table.getDeserializer()).partitionColumns();
      if (partitionColumns.size() > 0) {
        // The filter predicate contains ExprNodeDynamicListDesc object(s) for places where we will substitute
        // dynamic values later during execution. For Example:
        // GenericUDFIn(Column[ss_sold_date_sk], RS[5] <-- This is an ExprNodeDynamicListDesc)
        //
        // We would like to check if we will be able to convert these expressions to Iceberg filters when the
        // actual values will be available, so in this check we replace the ExprNodeDynamicListDesc with dummy
        // values and check whether the conversion will be possible or not.
        ExprNodeDesc clone = syntheticFilterPredicate.clone();

        String filterColumn = collectColumnAndReplaceDummyValues(clone, null);

        // If the filter is for a partition column then it could be worthwhile to try dynamic partition pruning
        if (partitionColumns.contains(filterColumn)) {
          // Check if we can convert the expression to a valid Iceberg filter
          SearchArgument sarg = ConvertAstToSearchArg.create(conf, (ExprNodeGenericFuncDesc) clone);
          HiveIcebergFilterFactory.generateFilterExpression(sarg);
          LOG.debug("Found Iceberg partition column to prune with predicate {}", syntheticFilterPredicate);
          return true;
        }
      }
    } catch (UnsupportedOperationException uoe) {
      // If we can not convert the filter, we do not prune
      LOG.debug("Unsupported predicate {}", syntheticFilterPredicate, uoe);
    }

    // There is nothing to prune, or we could not use the filter
    LOG.debug("Not found Iceberg partition columns to prune with predicate {}", syntheticFilterPredicate);
    return false;
  }

  /**
   * Returns the Table serialized to the configuration based on the table name.
   * If configuration is missing from the FileIO of the table, it will be populated with the input config.
   *
   * @param config The configuration used to get the data from
   * @param name The name of the table we need as returned by TableDesc.getTableName()
   * @return The Table
   */
  public static Table table(Configuration config, String name) {
    Table table = SerializationUtil.deserializeFromBase64(config.get(InputFormatConfig.SERIALIZED_TABLE_PREFIX + name));
    checkAndSetIoConfig(config, table);
    return table;
  }

  /**
   * If enabled, it populates the FileIO's hadoop configuration with the input config object.
   * This might be necessary when the table object was serialized without the FileIO config.
   *
   * @param config Configuration to set for FileIO, if enabled
   * @param table The Iceberg table object
   */
  public static void checkAndSetIoConfig(Configuration config, Table table) {
    if (table != null && config.getBoolean(InputFormatConfig.CONFIG_SERIALIZATION_DISABLED,
        InputFormatConfig.CONFIG_SERIALIZATION_DISABLED_DEFAULT) && table.io() instanceof HadoopConfigurable) {
      ((HadoopConfigurable) table.io()).setConf(config);
    }
  }

  /**
   * If enabled, it ensures that the FileIO's hadoop configuration will not be serialized.
   * This might be desirable for decreasing the overall size of serialized table objects.
   *
   * Note: Skipping FileIO config serialization in this fashion might in turn necessitate calling
   * {@link #checkAndSetIoConfig(Configuration, Table)} on the deserializer-side to enable subsequent use of the FileIO.
   *
   * @param config Configuration to set for FileIO in a transient manner, if enabled
   * @param table The Iceberg table object
   */
  public static void checkAndSkipIoConfigSerialization(Configuration config, Table table) {
    if (table != null && config.getBoolean(InputFormatConfig.CONFIG_SERIALIZATION_DISABLED,
        InputFormatConfig.CONFIG_SERIALIZATION_DISABLED_DEFAULT) && table.io() instanceof HadoopConfigurable) {
      ((HadoopConfigurable) table.io()).serializeConfWith(conf -> new NonSerializingConfig(config)::get);
    }
  }

  /**
   * Returns the names of the output tables stored in the configuration.
   * @param config The configuration used to get the data from
   * @return The collection of the table names as returned by TableDesc.getTableName()
   */
  public static Set<String> outputTables(Configuration config) {
    return Sets.newHashSet(TABLE_NAME_SPLITTER.split(config.get(InputFormatConfig.OUTPUT_TABLES)));
  }

  /**
   * Returns the catalog name serialized to the configuration.
   * @param config The configuration used to get the data from
   * @param name The name of the table we neeed as returned by TableDesc.getTableName()
   * @return catalog name
   */
  public static String catalogName(Configuration config, String name) {
    return config.get(InputFormatConfig.TABLE_CATALOG_PREFIX + name);
  }

  /**
   * Returns the Table Schema serialized to the configuration.
   * @param config The configuration used to get the data from
   * @return The Table Schema object
   */
  public static Schema schema(Configuration config) {
    return SchemaParser.fromJson(config.get(InputFormatConfig.TABLE_SCHEMA));
  }

  /**
   * Stores the serializable table data in the configuration.
   * Currently the following is handled:
   * <ul>
   *   <li>- Table - in case the table is serializable</li>
   *   <li>- Location</li>
   *   <li>- Schema</li>
   *   <li>- Partition specification</li>
   *   <li>- FileIO for handling table files</li>
   *   <li>- Location provider used for file generation</li>
   *   <li>- Encryption manager for encryption handling</li>
   * </ul>
   * @param configuration The configuration storing the catalog information
   * @param tableDesc The table which we want to store to the configuration
   * @param map The map of the configuration properties which we append with the serialized data
   */
  @VisibleForTesting
  static void overlayTableProperties(Configuration configuration, TableDesc tableDesc, Map<String, String> map) {
    Properties props = tableDesc.getProperties();
    Table table = IcebergTableUtil.getTable(configuration, props);
    String schemaJson = SchemaParser.toJson(table.schema());

    Maps.fromProperties(props).entrySet().stream()
        .filter(entry -> !map.containsKey(entry.getKey())) // map overrides tableDesc properties
        .forEach(entry -> map.put(entry.getKey(), entry.getValue()));

    map.put(InputFormatConfig.TABLE_IDENTIFIER, props.getProperty(Catalogs.NAME));
    map.put(InputFormatConfig.TABLE_LOCATION, table.location());
    map.put(InputFormatConfig.TABLE_SCHEMA, schemaJson);
    props.put(InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(table.spec()));

    // serialize table object into config
    Table serializableTable = SerializableTable.copyOf(table);
    checkAndSkipIoConfigSerialization(configuration, serializableTable);
    map.put(InputFormatConfig.SERIALIZED_TABLE_PREFIX + tableDesc.getTableName(),
        SerializationUtil.serializeToBase64(serializableTable));

    // We need to remove this otherwise the job.xml will be invalid as column comments are separated with '\0' and
    // the serialization utils fail to serialize this character
    map.remove("columns.comments");

    // save schema into table props as well to avoid repeatedly hitting the HMS during serde initializations
    // this is an exception to the interface documentation, but it's a safe operation to add this property
    props.put(InputFormatConfig.TABLE_SCHEMA, schemaJson);
  }

  /**
   * Recursively replaces the ExprNodeDynamicListDesc nodes by a dummy ExprNodeConstantDesc so we can test if we can
   * convert the predicate to an Iceberg predicate when pruning the partitions later. Also collects the column names
   * in the filter.
   * <p>
   * Please make sure that it is ok to change the input node (clone if needed)
   * @param node The node we are traversing
   * @param foundColumn The column we already found
   */
  private String collectColumnAndReplaceDummyValues(ExprNodeDesc node, String foundColumn) {
    String column = foundColumn;
    List<ExprNodeDesc> children = node.getChildren();
    if (children != null && !children.isEmpty()) {
      ListIterator<ExprNodeDesc> iterator = children.listIterator();
      while (iterator.hasNext()) {
        ExprNodeDesc child = iterator.next();
        if (child instanceof ExprNodeDynamicListDesc) {
          Object dummy;
          switch (((PrimitiveTypeInfo) child.getTypeInfo()).getPrimitiveCategory()) {
            case INT:
            case SHORT:
              dummy = 1;
              break;
            case LONG:
              dummy = 1L;
              break;
            case TIMESTAMP:
            case TIMESTAMPLOCALTZ:
              dummy = new Timestamp();
              break;
            case CHAR:
            case VARCHAR:
            case STRING:
              dummy = "1";
              break;
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
              dummy = 1.1;
              break;
            case DATE:
              dummy = new Date();
              break;
            case BOOLEAN:
              dummy = true;
              break;
            default:
              throw new UnsupportedOperationException("Not supported primitive type in partition pruning: " +
                  child.getTypeInfo());
          }

          iterator.set(new ExprNodeConstantDesc(child.getTypeInfo(), dummy));
        } else {
          String newColumn;
          if (child instanceof ExprNodeColumnDesc) {
            newColumn = ((ExprNodeColumnDesc) child).getColumn();
          } else {
            newColumn = collectColumnAndReplaceDummyValues(child, column);
          }

          if (column != null && newColumn != null && !newColumn.equals(column)) {
            throw new UnsupportedOperationException("Partition pruning does not support filtering for more columns");
          }

          if (column == null) {
            column = newColumn;
          }
        }
      }
    }

    return column;
  }

  /**
   * If any of the following checks is true we fall back to non vectorized mode:
   * <ul>
   *   <li>iceberg format-version is "2"</li>
   *   <li>fileformat is set to avro</li>
   *   <li>querying metadata tables</li>
   *   <li>fileformat is set to ORC, and table schema has time type column</li>
   *   <li>fileformat is set to PARQUET, and table schema has a list type column, that has a complex type element</li>
   * </ul>
   * @param tableProps table properties, must be not null
   */
  private void fallbackToNonVectorizedModeBasedOnProperties(Properties tableProps) {
    Schema tableSchema = SchemaParser.fromJson(tableProps.getProperty(InputFormatConfig.TABLE_SCHEMA));
    if ("2".equals(tableProps.get(TableProperties.FORMAT_VERSION)) ||
        FileFormat.AVRO.name().equalsIgnoreCase(tableProps.getProperty(TableProperties.DEFAULT_FILE_FORMAT)) ||
        (tableProps.containsKey("metaTable") && isValidMetadataTable(tableProps.getProperty("metaTable"))) ||
        hasOrcTimeInSchema(tableProps, tableSchema) ||
        !hasParquetNestedTypeWithinListOrMap(tableProps, tableSchema)) {
      conf.setBoolean(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED.varname, false);
    }
  }

  /**
   * Iceberg Time type columns are written as longs into ORC files. There is no Time type in Hive, so it is represented
   * as String instead. For ORC there's no automatic conversion from long to string during vectorized reading such as
   * for example in Parquet (in Parquet files Time type is an int64 with 'time' logical annotation).
   * @param tableProps iceberg table properties
   * @param tableSchema iceberg table schema
   * @return
   */
  private static boolean hasOrcTimeInSchema(Properties tableProps, Schema tableSchema) {
    if (!FileFormat.ORC.name().equalsIgnoreCase(tableProps.getProperty(TableProperties.DEFAULT_FILE_FORMAT))) {
      return false;
    }
    return tableSchema.columns().stream().anyMatch(f -> Types.TimeType.get().typeId() == f.type().typeId());
  }

  /**
   * Vectorized reads of parquet files from columns with list or map type is only supported if the nested types are of
   * primitive type category
   * check {@link VectorizedParquetRecordReader#checkListColumnSupport} for details on nested types under lists
   * @param tableProps iceberg table properties
   * @param tableSchema iceberg table schema
   * @return
   */
  private static boolean hasParquetNestedTypeWithinListOrMap(Properties tableProps, Schema tableSchema) {
    if (!FileFormat.PARQUET.name().equalsIgnoreCase(tableProps.getProperty(TableProperties.DEFAULT_FILE_FORMAT))) {
      return true;
    }

    for (Types.NestedField field : tableSchema.columns()) {
      if (field.type().isListType() || field.type().isMapType()) {
        for (Types.NestedField nestedField : field.type().asNestedType().fields()) {
          if (!nestedField.type().isPrimitiveType()) {
            return false;
          }
        }
      }
    }

    return true;
  }

  /**
   * Generates {@link JobContext}s for the OutputCommitter for the specific table.
   * @param configuration The configuration used for as a base of the JobConf
   * @param tableName The name of the table we are planning to commit
   * @param overwrite If we have to overwrite the existing table or just add the new data
   * @return The generated Optional JobContext list or empty if not presents.
   */
  private List<JobContext> generateJobContext(Configuration configuration, String tableName,
      boolean overwrite) {
    JobConf jobConf = new JobConf(configuration);
    Optional<Map<String, SessionStateUtil.CommitInfo>> commitInfoMap =
        SessionStateUtil.getCommitInfo(jobConf, tableName);
    if (commitInfoMap.isPresent()) {
      List<JobContext> jobContextList = Lists.newLinkedList();
      for (SessionStateUtil.CommitInfo commitInfo : commitInfoMap.get().values()) {
        JobID jobID = JobID.forName(commitInfo.getJobIdStr());
        commitInfo.getProps().forEach(jobConf::set);
        jobConf.setBoolean(InputFormatConfig.IS_OVERWRITE, overwrite);

        // we should only commit this current table because
        // for multi-table inserts, this hook method will be called sequentially for each target table
        jobConf.set(InputFormatConfig.OUTPUT_TABLES, tableName);

        jobContextList.add(new JobContextImpl(jobConf, jobID, null));
      }
      return jobContextList;
    } else {
      // most likely empty write scenario
      LOG.debug("Unable to find commit information in query state for table: {}", tableName);
      return Collections.emptyList();
    }
  }

  private String getOperationType() {
    return SessionStateUtil.getProperty(conf, Operation.class.getSimpleName())
        .orElse(Operation.OTHER.name());
  }

  private static class NonSerializingConfig implements Serializable {

    private final transient Configuration conf;

    NonSerializingConfig(Configuration conf) {
      this.conf = conf;
    }

    public Configuration get() {
      if (conf == null) {
        throw new IllegalStateException("Configuration was not serialized on purpose but was not set manually either");
      }

      return conf;
    }
  }

  public String getCurrentSnapshot(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    TableDesc tableDesc = Utilities.getTableDesc(hmsTable);
    Table table = IcebergTableUtil.getTable(conf, tableDesc.getProperties());
    return Long.toString(table.currentSnapshot().sequenceNumber());
  }
}
