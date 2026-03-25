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

package org.apache.iceberg.mr.hive.compaction;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.Context.RewritePolicy;
import org.apache.hadoop.hive.ql.DriverUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.RowLineageUtils;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorContext;
import org.apache.hadoop.hive.ql.txn.compactor.QueryCompactor;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hive.iceberg.org.apache.orc.storage.common.TableName;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.mr.hive.HiveIcebergFilterFactory;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.mr.hive.compaction.evaluator.CompactionEvaluator;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergQueryCompactor extends QueryCompactor  {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergQueryCompactor.class.getName());

  @Override
  public boolean run(CompactorContext context) throws IOException, HiveException, InterruptedException {

    String compactTableName = TableName.getDbTable(context.getTable().getDbName(), context.getTable().getTableName());
    Map<String, String> tblProperties = context.getTable().getParameters();
    LOG.debug("Initiating compaction for the {} table", compactTableName);

    HiveConf conf = new HiveConf(context.getConf());
    CompactionInfo ci = context.getCompactionInfo();
    org.apache.hadoop.hive.ql.metadata.Table hiveTable =
        new org.apache.hadoop.hive.ql.metadata.Table(context.getTable());
    boolean rowLineageEnabled = RowLineageUtils.supportsRowLineage(hiveTable);
    String compactionQuery = buildCompactionQuery(context, compactTableName, conf, rowLineageEnabled);

    SessionState sessionState = setupQueryCompactionSession(conf, ci, tblProperties);

    if (rowLineageEnabled) {
      RowLineageUtils.enableRowLineage(sessionState);
      LOG.debug("Row lineage flag set for compaction of table {}", compactTableName);
    }

    String compactionTarget = "table " + HiveUtils.unparseIdentifier(compactTableName) +
        (ci.partName != null ? ", partition " + HiveUtils.unparseIdentifier(ci.partName) : "");

    try {
      DriverUtils.runOnDriver(sessionState.getConf(), sessionState, compactionQuery);
      LOG.info("Completed compaction for {}", compactionTarget);
      return true;
    } catch (HiveException e) {
      LOG.error("Failed compacting {}", compactionTarget, e);
      throw e;
    } finally {
      RowLineageUtils.disableRowLineage(sessionState);
      sessionState.setCompaction(false);
    }
  }

  private String buildCompactionQuery(CompactorContext context, String compactTableName, HiveConf conf,
      boolean rowLineageEnabled)
      throws HiveException {
    CompactionInfo ci = context.getCompactionInfo();
    String rowLineageColumns = RowLineageUtils.getRowLineageSelectColumns(rowLineageEnabled);
    org.apache.hadoop.hive.ql.metadata.Table table = Hive.get(conf).getTable(context.getTable().getDbName(),
        context.getTable().getTableName());
    Table icebergTable = IcebergTableUtil.getTable(conf, table.getTTable());
    String orderBy = ci.orderByClause == null ? "" : ci.orderByClause;
    String fileSizePredicate = buildMinorFileSizePredicate(ci, compactTableName, conf, table);

    String compactionQuery = (ci.partName == null) ?
        buildFullTableCompactionQuery(compactTableName, conf, icebergTable,
            rowLineageColumns, fileSizePredicate, orderBy) :
        buildPartitionCompactionQuery(ci, compactTableName, conf, icebergTable,
            rowLineageColumns, fileSizePredicate, orderBy);

    LOG.info("Compaction query: {}", compactionQuery);
    return compactionQuery;
  }

  private static String buildMinorFileSizePredicate(
      CompactionInfo ci, String compactTableName, HiveConf conf, org.apache.hadoop.hive.ql.metadata.Table table) {
    if (ci.type != CompactionType.MINOR) {
      return null;
    }

    long fileSizeInBytesThreshold = CompactionEvaluator.getFragmentSizeBytes(table.getParameters());
    conf.setLong(CompactorContext.COMPACTION_FILE_SIZE_THRESHOLD, fileSizeInBytesThreshold);
    // IOW query containing a join with Iceberg .files metadata table fails with exception that Iceberg AVRO format
    // doesn't support vectorization, hence disabling it in this case.
    conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED, false);

    return String.format("%1$s in (select file_path from %2$s.files where file_size_in_bytes < %3$d)",
        VirtualColumn.FILE_PATH.getName(), compactTableName, fileSizeInBytesThreshold);
  }

  private String buildFullTableCompactionQuery(
      String compactTableName,
      HiveConf conf,
      Table icebergTable,
      String rowLineageColumns,
      String fileSizePredicate,
      String orderBy) throws HiveException {
    String selectColumns = buildSelectColumnList(icebergTable, conf);

    if (!icebergTable.spec().isPartitioned()) {
      HiveConf.setVar(conf, ConfVars.REWRITE_POLICY, RewritePolicy.FULL_TABLE.name());
      return String.format("insert overwrite table %1$s select %2$s%3$s from %1$s %4$s %5$s",
          compactTableName, selectColumns, rowLineageColumns,
          fileSizePredicate == null ? "" : "where " + fileSizePredicate, orderBy);
    }

    if (icebergTable.specs().size() > 1) {
      // Compacting partitions of old partition specs on a partitioned table with partition evolution
      HiveConf.setVar(conf, ConfVars.REWRITE_POLICY, RewritePolicy.PARTITION.name());
      // A single filter on a virtual column causes errors during compilation,
      // added another filter on file_path as a workaround.
      return String.format("insert overwrite table %1$s select %2$s%3$s from %1$s " +
              "where %4$s != %5$d and %6$s is not null %7$s %8$s",
          compactTableName, selectColumns, rowLineageColumns,
          VirtualColumn.PARTITION_SPEC_ID.getName(), icebergTable.spec().specId(),
          VirtualColumn.FILE_PATH.getName(), fileSizePredicate == null ? "" : "and " + fileSizePredicate, orderBy);
    }

    // Partitioned table without partition evolution with partition spec as null in the compaction request - this
    // code branch is not supposed to be reachable
    throw new HiveException(ErrorMsg.COMPACTION_NO_PARTITION);
  }

  private String buildPartitionCompactionQuery(
      CompactionInfo ci,
      String compactTableName,
      HiveConf conf,
      Table icebergTable,
      String rowLineageColumns,
      String fileSizePredicate,
      String orderBy) throws HiveException {
    HiveConf.setBoolVar(conf, ConfVars.HIVE_CONVERT_JOIN, false);
    conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    HiveConf.setVar(conf, ConfVars.REWRITE_POLICY, RewritePolicy.PARTITION.name());
    conf.set(IcebergCompactionService.PARTITION_PATH, new Path(ci.partName).toString());

    PartitionSpec spec;
    String partitionPredicate;
    try {
      spec = IcebergTableUtil.getPartitionSpec(icebergTable, ci.partName);
      partitionPredicate = buildPartitionPredicate(ci, spec);
    } catch (MetaException e) {
      throw new HiveException(e);
    }

    return String.format("INSERT OVERWRITE TABLE %1$s SELECT *%2$s FROM %1$s WHERE %3$s IN " +
            "(SELECT FILE_PATH FROM %1$s.FILES WHERE %4$s AND SPEC_ID = %5$d) %6$s %7$s",
        compactTableName, rowLineageColumns, VirtualColumn.FILE_PATH.getName(), partitionPredicate, spec.specId(),
        fileSizePredicate == null ? "" : "AND " + fileSizePredicate, orderBy);
  }

  /**
   * Builds a comma-separated SELECT list from the Iceberg table schema.
   */
  private static String buildSelectColumnList(Table icebergTable, HiveConf conf) {
    return icebergTable.schema().columns().stream()
        .map(Types.NestedField::name)
        .map(col -> HiveUtils.unparseIdentifier(col, conf))
        .collect(Collectors.joining(", "));
  }

  private String buildPartitionPredicate(CompactionInfo ci, PartitionSpec spec) throws MetaException {
    Map<String, String> partSpecMap = Warehouse.makeSpecFromName(ci.partName);
    Map<String, PartitionField> partitionFieldMap = spec.fields().stream()
        .collect(Collectors.toMap(PartitionField::name, Function.identity()));

    Types.StructType partitionType = spec.partitionType();
    return  partitionType.fields().stream().map(field -> {
      String value = partSpecMap.get(field.name());
      String literal = "NULL";

      if (value != null && !value.equals("null")) {
        String type = HiveSchemaUtil.convertToTypeString(field.type());
        PartitionField partitionField = partitionFieldMap.get(field.name());
        TransformSpec transformSpec = TransformSpec.fromString(partitionField.transform().toString(), field.name());
        literal = TypeInfoUtils.convertStringToLiteralForSQL(
            HiveIcebergFilterFactory.convertPartitionLiteral(value, transformSpec).toString(),
            ((PrimitiveTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(type)).getPrimitiveCategory());
      }

      return String.format("`partition`.%s %s %s", HiveUtils.unparseIdentifier(field.name()),
          Objects.equals(literal, "NULL") ? "IS" : "=", literal);
    }).collect(Collectors.joining(" AND "));
  }
}
