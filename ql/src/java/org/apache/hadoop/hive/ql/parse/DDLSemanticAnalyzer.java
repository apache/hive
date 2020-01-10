/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse;

import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.table.info.DescTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.info.ShowTablePropertiesDesc;
import org.apache.hadoop.hive.ql.ddl.table.info.ShowTableStatusDesc;
import org.apache.hadoop.hive.ql.ddl.table.info.ShowTablesDesc;
import org.apache.hadoop.hive.ql.ddl.table.lock.LockTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.lock.ShowLocksDesc;
import org.apache.hadoop.hive.ql.ddl.table.lock.UnlockTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.AlterTableRenameDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.AlterTableSetOwnerDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.AlterTableSetPropertiesDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.AlterTableTouchDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.AlterTableUnsetPropertiesDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.TruncateTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.partition.AlterTableAddPartitionDesc;
import org.apache.hadoop.hive.ql.ddl.table.partition.AlterTableAlterPartitionDesc;
import org.apache.hadoop.hive.ql.ddl.table.partition.AlterTableDropPartitionDesc;
import org.apache.hadoop.hive.ql.ddl.table.partition.AlterTableExchangePartitionsDesc;
import org.apache.hadoop.hive.ql.ddl.table.partition.AlterTableRenamePartitionDesc;
import org.apache.hadoop.hive.ql.ddl.table.partition.ShowPartitionsDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableArchiveDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableClusteredByDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableCompactDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableIntoBucketsDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableNotClusteredDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableNotSkewedDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableNotSortedDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableConcatenateDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableSetFileFormatDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableSetLocationDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableSetSerdeDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableSetSerdePropsDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableSetSkewedLocationDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableSkewedByDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.AlterTableUnarchiveDesc;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils;
import org.apache.hadoop.hive.ql.exec.ColumnStatsUpdateTask;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.lockmgr.TxnManagerFactory;
import org.apache.hadoop.hive.ql.metadata.DefaultConstraint;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.NotNullConstraint;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.authorization.AuthorizationParseUtils;
import org.apache.hadoop.hive.ql.plan.BasicStatsWork;
import org.apache.hadoop.hive.ql.plan.ColumnStatsUpdateWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.ValidationUtility;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * DDLSemanticAnalyzer.
 *
 */
public class DDLSemanticAnalyzer extends BaseSemanticAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(DDLSemanticAnalyzer.class);
  private static final Map<Integer, String> TokenToTypeName = new HashMap<Integer, String>();

  private final Set<String> reservedPartitionValues;
  // Equivalent to acidSinks, but for DDL operations that change data.
  private DDLDescWithWriteId ddlDescWithWriteId;

  static {
    TokenToTypeName.put(HiveParser.TOK_BOOLEAN, serdeConstants.BOOLEAN_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_TINYINT, serdeConstants.TINYINT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_SMALLINT, serdeConstants.SMALLINT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_INT, serdeConstants.INT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_BIGINT, serdeConstants.BIGINT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_FLOAT, serdeConstants.FLOAT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DOUBLE, serdeConstants.DOUBLE_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_STRING, serdeConstants.STRING_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_CHAR, serdeConstants.CHAR_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_VARCHAR, serdeConstants.VARCHAR_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_BINARY, serdeConstants.BINARY_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DATE, serdeConstants.DATE_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DATETIME, serdeConstants.DATETIME_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_TIMESTAMP, serdeConstants.TIMESTAMP_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_TIMESTAMPLOCALTZ, serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_INTERVAL_YEAR_MONTH, serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_INTERVAL_DAY_TIME, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DECIMAL, serdeConstants.DECIMAL_TYPE_NAME);
  }

  public static String getTypeName(ASTNode node) throws SemanticException {
    int token = node.getType();
    String typeName;

    // datetime type isn't currently supported
    if (token == HiveParser.TOK_DATETIME) {
      throw new SemanticException(ErrorMsg.UNSUPPORTED_TYPE.getMsg());
    }

    switch (token) {
    case HiveParser.TOK_CHAR:
      CharTypeInfo charTypeInfo = ParseUtils.getCharTypeInfo(node);
      typeName = charTypeInfo.getQualifiedName();
      break;
    case HiveParser.TOK_VARCHAR:
      VarcharTypeInfo varcharTypeInfo = ParseUtils.getVarcharTypeInfo(node);
      typeName = varcharTypeInfo.getQualifiedName();
      break;
    case HiveParser.TOK_TIMESTAMPLOCALTZ:
      TimestampLocalTZTypeInfo timestampLocalTZTypeInfo =
          TypeInfoFactory.getTimestampTZTypeInfo(null);
      typeName = timestampLocalTZTypeInfo.getQualifiedName();
      break;
    case HiveParser.TOK_DECIMAL:
      DecimalTypeInfo decTypeInfo = ParseUtils.getDecimalTypeTypeInfo(node);
      typeName = decTypeInfo.getQualifiedName();
      break;
    default:
      typeName = TokenToTypeName.get(token);
    }
    return typeName;
  }

  public DDLSemanticAnalyzer(QueryState queryState) throws SemanticException {
    this(queryState, createHiveDB(queryState.getConf()));
  }

  public DDLSemanticAnalyzer(QueryState queryState, Hive db) throws SemanticException {
    super(queryState, db);
    reservedPartitionValues = new HashSet<String>();
    // Partition can't have this name
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.DEFAULTPARTITIONNAME));
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.DEFAULT_ZOOKEEPER_PARTITION_NAME));
    // Partition value can't end in this suffix
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.METASTORE_INT_ORIGINAL));
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.METASTORE_INT_ARCHIVED));
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.METASTORE_INT_EXTRACTED));
  }

  @Override
  public void analyzeInternal(ASTNode input) throws SemanticException {

    ASTNode ast = input;
    switch (ast.getType()) {
    case HiveParser.TOK_ALTERTABLE: {
      ast = (ASTNode) input.getChild(1);
      final TableName tName =
          getQualifiedTableName((ASTNode) input.getChild(0), MetaStoreUtils.getDefaultCatalog(conf));
      // TODO CAT - for now always use the default catalog.  Eventually will want to see if
      // the user specified a catalog
      Map<String, String> partSpec = null;
      ASTNode partSpecNode = (ASTNode)input.getChild(2);
      if (partSpecNode != null) {
        //  We can use alter table partition rename to convert/normalize the legacy partition
        //  column values. In so, we should not enable the validation to the old partition spec
        //  passed in this command.
        if (ast.getType() == HiveParser.TOK_ALTERTABLE_RENAMEPART) {
          partSpec = getPartSpec(partSpecNode);
        } else {
          partSpec = getValidatedPartSpec(getTable(tName), partSpecNode, conf, false);
        }
      }

      if (ast.getType() == HiveParser.TOK_ALTERTABLE_RENAME) {
        analyzeAlterTableRename(tName, ast, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_TOUCH) {
        analyzeAlterTableTouch(tName, ast);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_ARCHIVE) {
        analyzeAlterTableArchive(tName, ast, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_UNARCHIVE) {
        analyzeAlterTableArchive(tName, ast, true);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_ADDPARTS) {
        analyzeAlterTableAddParts(tName, ast, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_DROPPARTS) {
        analyzeAlterTableDropParts(tName, ast, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_PARTCOLTYPE) {
        analyzeAlterTablePartColType(tName, ast);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_PROPERTIES) {
        analyzeAlterTableProps(tName, null, ast, false, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_DROPPROPERTIES) {
        analyzeAlterTableProps(tName, null, ast, false, true);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_UPDATESTATS ||
          ast.getType() == HiveParser.TOK_ALTERPARTITION_UPDATESTATS) {
        analyzeAlterTableProps(tName, partSpec, ast, false, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_SKEWED) {
        analyzeAlterTableSkewedby(tName, ast);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_EXCHANGEPARTITION) {
        analyzeExchangePartition(tName, ast);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_FILEFORMAT ||
          ast.getToken().getType() == HiveParser.TOK_ALTERPARTITION_FILEFORMAT) {
        analyzeAlterTableFileFormat(ast, tName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_LOCATION ||
          ast.getToken().getType() == HiveParser.TOK_ALTERPARTITION_LOCATION) {
        analyzeAlterTableLocation(ast, tName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_MERGEFILES ||
          ast.getToken().getType() == HiveParser.TOK_ALTERPARTITION_MERGEFILES) {
        analyzeAlterTablePartMergeFiles(ast, tName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SERIALIZER ||
          ast.getToken().getType() == HiveParser.TOK_ALTERPARTITION_SERIALIZER) {
        analyzeAlterTableSerde(ast, tName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES ||
          ast.getToken().getType() == HiveParser.TOK_ALTERPARTITION_SERDEPROPERTIES) {
        analyzeAlterTableSerdeProps(ast, tName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_RENAMEPART) {
        analyzeAlterTableRenamePart(ast, tName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SKEWED_LOCATION) {
        analyzeAlterTableSkewedLocation(ast, tName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_BUCKETS ||
          ast.getToken().getType() == HiveParser.TOK_ALTERPARTITION_BUCKETS) {
        analyzeAlterTableBucketNum(ast, tName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_CLUSTER_SORT) {
        analyzeAlterTableClusterSort(ast, tName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_COMPACT) {
        analyzeAlterTableCompact(ast, tName, partSpec);
      } else if(ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_UPDATECOLSTATS ||
          ast.getToken().getType() == HiveParser.TOK_ALTERPARTITION_UPDATECOLSTATS){
        analyzeAlterTableUpdateStats(ast, tName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_OWNER) {
        analyzeAlterTableOwner(ast, tName);
      }
      break;
    }
    case HiveParser.TOK_TRUNCATETABLE:
      analyzeTruncateTable(ast);
      break;
    case HiveParser.TOK_DESCTABLE:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeDescribeTable(ast);
      break;
    case HiveParser.TOK_SHOWTABLES:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowTables(ast);
      break;
    case HiveParser.TOK_SHOW_TABLESTATUS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowTableStatus(ast);
      break;
    case HiveParser.TOK_SHOW_TBLPROPERTIES:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowTableProperties(ast);
      break;
    case HiveParser.TOK_SHOWLOCKS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowLocks(ast);
      break;
    case HiveParser.TOK_SHOWDBLOCKS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowDbLocks(ast);
      break;
    case HiveParser.TOK_SHOWVIEWS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowViews(ast);
      break;
    case HiveParser.TOK_SHOWMATERIALIZEDVIEWS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowMaterializedViews(ast);
      break;
    case HiveParser.TOK_ALTERVIEW: {
      final TableName tName = getQualifiedTableName((ASTNode) ast.getChild(0));
      ast = (ASTNode) ast.getChild(1);
      if (ast.getType() == HiveParser.TOK_ALTERVIEW_PROPERTIES) {
        analyzeAlterTableProps(tName, null, ast, true, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERVIEW_DROPPROPERTIES) {
        analyzeAlterTableProps(tName, null, ast, true, true);
      } else if (ast.getType() == HiveParser.TOK_ALTERVIEW_ADDPARTS) {
        analyzeAlterTableAddParts(tName, ast, true);
      } else if (ast.getType() == HiveParser.TOK_ALTERVIEW_DROPPARTS) {
        analyzeAlterTableDropParts(tName, ast, true);
      } else if (ast.getType() == HiveParser.TOK_ALTERVIEW_RENAME) {
        analyzeAlterTableRename(tName, ast, true);
      }
      break;
    }
    case HiveParser.TOK_SHOWPARTITIONS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowPartitions(ast);
      break;
    case HiveParser.TOK_LOCKTABLE:
      analyzeLockTable(ast);
      break;
    case HiveParser.TOK_UNLOCKTABLE:
      analyzeUnlockTable(ast);
      break;
   default:
      throw new SemanticException("Unsupported command: " + ast);
    }
    if (fetchTask != null && !rootTasks.isEmpty()) {
      rootTasks.get(rootTasks.size() - 1).setFetchSource(true);
    }
  }

  private void analyzeAlterTableUpdateStats(ASTNode ast, TableName tblName, Map<String, String> partSpec)
      throws SemanticException {
    String colName = getUnescapedName((ASTNode) ast.getChild(0));
    Map<String, String> mapProp = getProps((ASTNode) (ast.getChild(1)).getChild(0));

    Table tbl = getTable(tblName);
    String partName = null;
    if (partSpec != null) {
      try {
        partName = Warehouse.makePartName(partSpec, false);
      } catch (MetaException e) {
        throw new SemanticException("partition " + partSpec.toString()
            + " not found");
      }
    }

    String colType = null;
    List<FieldSchema> cols = tbl.getCols();
    for (FieldSchema col : cols) {
      if (colName.equalsIgnoreCase(col.getName())) {
        colType = col.getType();
        break;
      }
    }

    if (colType == null) {
      throw new SemanticException("column type not found");
    }

    ColumnStatsUpdateWork columnStatsUpdateWork =
        new ColumnStatsUpdateWork(partName, mapProp, tbl.getDbName(), tbl.getTableName(), colName, colType);
    ColumnStatsUpdateTask cStatsUpdateTask = (ColumnStatsUpdateTask) TaskFactory
        .get(columnStatsUpdateWork);
    // TODO: doesn't look like this path is actually ever exercised. Maybe this needs to be removed.
    addInputsOutputsAlterTable(tblName, partSpec, null, AlterTableType.UPDATESTATS, false);
    if (AcidUtils.isTransactionalTable(tbl)) {
      setAcidDdlDesc(columnStatsUpdateWork);
    }
    rootTasks.add(cStatsUpdateTask);
  }

  private void analyzeExchangePartition(TableName tName, ASTNode ast) throws SemanticException {
    Table destTable = getTable(tName);
    Table sourceTable = getTable(getUnescapedName((ASTNode)ast.getChild(1)));

    // Get the partition specs
    Map<String, String> partSpecs = getValidatedPartSpec(sourceTable, (ASTNode)ast.getChild(0), conf, false);
    validatePartitionValues(partSpecs);
    boolean sameColumns = MetaStoreUtils.compareFieldColumns(
        destTable.getAllCols(), sourceTable.getAllCols());
    boolean samePartitions = MetaStoreUtils.compareFieldColumns(
        destTable.getPartitionKeys(), sourceTable.getPartitionKeys());
    if (!sameColumns || !samePartitions) {
      throw new SemanticException(ErrorMsg.TABLES_INCOMPATIBLE_SCHEMAS.getMsg());
    }

    // Exchange partition is not allowed with transactional tables.
    // If only source is transactional table, then target will see deleted rows too as no snapshot
    // isolation applicable for non-acid tables.
    // If only target is transactional table, then data would be visible to all ongoing transactions
    // affecting the snapshot isolation.
    // If both source and targets are transactional tables, then target partition may have delta/base
    // files with write IDs may not be valid. It may affect snapshot isolation for on-going txns as well.
    if (AcidUtils.isTransactionalTable(sourceTable) || AcidUtils.isTransactionalTable(destTable)) {
      throw new SemanticException(ErrorMsg.EXCHANGE_PARTITION_NOT_ALLOWED_WITH_TRANSACTIONAL_TABLES.getMsg());
    }

    // check if source partition exists
    getPartitions(sourceTable, partSpecs, true);

    // Verify that the partitions specified are continuous
    // If a subpartition value is specified without specifying a partition's value
    // then we throw an exception
    int counter = isPartitionValueContinuous(sourceTable.getPartitionKeys(), partSpecs);
    if (counter < 0) {
      throw new SemanticException(
          ErrorMsg.PARTITION_VALUE_NOT_CONTINUOUS.getMsg(partSpecs.toString()));
    }
    List<Partition> destPartitions = null;
    try {
      destPartitions = getPartitions(destTable, partSpecs, true);
    } catch (SemanticException ex) {
      // We should expect a semantic exception being throw as this partition
      // should not be present.
    }
    if (destPartitions != null) {
      // If any destination partition is present then throw a Semantic Exception.
      throw new SemanticException(ErrorMsg.PARTITION_EXISTS.getMsg(destPartitions.toString()));
    }
    AlterTableExchangePartitionsDesc alterTableExchangePartition =
        new AlterTableExchangePartitionsDesc(sourceTable, destTable, partSpecs);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTableExchangePartition)));

    inputs.add(new ReadEntity(sourceTable));
    outputs.add(new WriteEntity(destTable, WriteType.DDL_SHARED));
  }

  /**
   * @param partitionKeys the list of partition keys of the table
   * @param partSpecs the partition specs given by the user
   * @return >=0 if no subpartition value is specified without a partition's
   *         value being specified else it returns -1
   */
  private int isPartitionValueContinuous(List<FieldSchema> partitionKeys,
      Map<String, String> partSpecs) {
    int counter = 0;
    for (FieldSchema partitionKey : partitionKeys) {
      if (partSpecs.containsKey(partitionKey.getName())) {
        counter++;
        continue;
      }
      return partSpecs.size() == counter ? counter : -1;
    }
    return counter;
  }

  private void analyzeTruncateTable(ASTNode ast) throws SemanticException {
    ASTNode root = (ASTNode) ast.getChild(0); // TOK_TABLE_PARTITION
    final String tableName = getUnescapedName((ASTNode) root.getChild(0));

    Table table = getTable(tableName, true);
    final TableName tName = HiveTableName.of(table);
    checkTruncateEligibility(ast, root, tableName, table);

    Map<String, String> partSpec = getPartSpec((ASTNode) root.getChild(1));
    addTruncateTableOutputs(root, table, partSpec);

    Task<?> truncateTask = null;

    // Is this a truncate column command
    ASTNode colNamesNode = (ASTNode) ast.getFirstChildWithType(HiveParser.TOK_TABCOLNAME);
    if (colNamesNode == null) {
      truncateTask = getTruncateTaskWithoutColumnNames(tName, partSpec, table);
    } else {
      truncateTask = getTruncateTaskWithColumnNames(root, tName, table, partSpec, colNamesNode);
    }

    rootTasks.add(truncateTask);
  }

  private void checkTruncateEligibility(ASTNode ast, ASTNode root, String tableName, Table table)
      throws SemanticException {
    boolean isForce = ast.getFirstChildWithType(HiveParser.TOK_FORCE) != null;
    if (!isForce) {
      if (table.getTableType() != TableType.MANAGED_TABLE &&
          (table.getParameters().getOrDefault(MetaStoreUtils.EXTERNAL_TABLE_PURGE, "FALSE"))
              .equalsIgnoreCase("FALSE")) {
        throw new SemanticException(ErrorMsg.TRUNCATE_FOR_NON_MANAGED_TABLE.format(tableName));
      }
    }
    if (table.isNonNative()) {
      throw new SemanticException(ErrorMsg.TRUNCATE_FOR_NON_NATIVE_TABLE.format(tableName)); //TODO
    }
    if (!table.isPartitioned() && root.getChildCount() > 1) {
      throw new SemanticException(ErrorMsg.PARTSPEC_FOR_NON_PARTITIONED_TABLE.format(tableName));
    }
  }

  private void addTruncateTableOutputs(ASTNode root, Table table, Map<String, String> partSpec)
      throws SemanticException {
    if (partSpec == null) {
      if (!table.isPartitioned()) {
        outputs.add(new WriteEntity(table, WriteEntity.WriteType.DDL_EXCLUSIVE));
      } else {
        for (Partition partition : getPartitions(table, null, false)) {
          outputs.add(new WriteEntity(partition, WriteEntity.WriteType.DDL_EXCLUSIVE));
        }
      }
    } else {
      if (isFullSpec(table, partSpec)) {
        validatePartSpec(table, partSpec, (ASTNode) root.getChild(1), conf, true);
        Partition partition = getPartition(table, partSpec, true);
        outputs.add(new WriteEntity(partition, WriteEntity.WriteType.DDL_EXCLUSIVE));
      } else {
        validatePartSpec(table, partSpec, (ASTNode) root.getChild(1), conf, false);
        for (Partition partition : getPartitions(table, partSpec, false)) {
          outputs.add(new WriteEntity(partition, WriteEntity.WriteType.DDL_EXCLUSIVE));
        }
      }
    }
  }

  private Task<?> getTruncateTaskWithoutColumnNames(TableName tableName, Map<String, String> partSpec, Table table) {
    TruncateTableDesc truncateTblDesc = new TruncateTableDesc(tableName, partSpec, null, table);
    if (truncateTblDesc.mayNeedWriteId()) {
      setAcidDdlDesc(truncateTblDesc);
    }

    DDLWork ddlWork = new DDLWork(getInputs(), getOutputs(), truncateTblDesc);
    return TaskFactory.get(ddlWork);
  }

  private Task<?> getTruncateTaskWithColumnNames(ASTNode root, TableName tName, Table table,
      Map<String, String> partSpec, ASTNode colNamesNode) throws SemanticException {
    try {
      List<String> columnNames = getColumnNames(colNamesNode);

      // It would be possible to support this, but this is such a pointless command.
      if (AcidUtils.isInsertOnlyTable(table.getParameters())) {
        throw new SemanticException("Truncating MM table columns not presently supported");
      }

      List<String> bucketCols = null;
      Class<? extends InputFormat> inputFormatClass = null;
      boolean isArchived = false;
      Path newTblPartLoc = null;
      Path oldTblPartLoc = null;
      List<FieldSchema> cols = null;
      ListBucketingCtx lbCtx = null;
      boolean isListBucketed = false;
      List<String> listBucketColNames = null;

      if (table.isPartitioned()) {
        Partition part = db.getPartition(table, partSpec, false);

        Path tabPath = table.getPath();
        Path partPath = part.getDataLocation();

        // if the table is in a different dfs than the partition,
        // replace the partition's dfs with the table's dfs.
        newTblPartLoc = new Path(tabPath.toUri().getScheme(), tabPath.toUri()
            .getAuthority(), partPath.toUri().getPath());

        oldTblPartLoc = partPath;

        cols = part.getCols();
        bucketCols = part.getBucketCols();
        inputFormatClass = part.getInputFormatClass();
        isArchived = ArchiveUtils.isArchived(part);
        lbCtx = constructListBucketingCtx(part.getSkewedColNames(), part.getSkewedColValues(),
            part.getSkewedColValueLocationMaps(), part.isStoredAsSubDirectories());
        isListBucketed = part.isStoredAsSubDirectories();
        listBucketColNames = part.getSkewedColNames();
      } else {
        // input and output are the same
        oldTblPartLoc = table.getPath();
        newTblPartLoc = table.getPath();
        cols  = table.getCols();
        bucketCols = table.getBucketCols();
        inputFormatClass = table.getInputFormatClass();
        lbCtx = constructListBucketingCtx(table.getSkewedColNames(), table.getSkewedColValues(),
            table.getSkewedColValueLocationMaps(), table.isStoredAsSubDirectories());
        isListBucketed = table.isStoredAsSubDirectories();
        listBucketColNames = table.getSkewedColNames();
      }

      // throw a HiveException for non-rcfile.
      if (!inputFormatClass.equals(RCFileInputFormat.class)) {
        throw new SemanticException(ErrorMsg.TRUNCATE_COLUMN_NOT_RC.getMsg());
      }

      // throw a HiveException if the table/partition is archived
      if (isArchived) {
        throw new SemanticException(ErrorMsg.TRUNCATE_COLUMN_ARCHIVED.getMsg());
      }

      Set<Integer> columnIndexes = new HashSet<Integer>();
      for (String columnName : columnNames) {
        boolean found = false;
        for (int columnIndex = 0; columnIndex < cols.size(); columnIndex++) {
          if (columnName.equalsIgnoreCase(cols.get(columnIndex).getName())) {
            columnIndexes.add(columnIndex);
            found = true;
            break;
          }
        }
        // Throw an exception if the user is trying to truncate a column which doesn't exist
        if (!found) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(columnName));
        }
        // Throw an exception if the table/partition is bucketed on one of the columns
        for (String bucketCol : bucketCols) {
          if (bucketCol.equalsIgnoreCase(columnName)) {
            throw new SemanticException(ErrorMsg.TRUNCATE_BUCKETED_COLUMN.getMsg(columnName));
          }
        }
        if (isListBucketed) {
          for (String listBucketCol : listBucketColNames) {
            if (listBucketCol.equalsIgnoreCase(columnName)) {
              throw new SemanticException(
                  ErrorMsg.TRUNCATE_LIST_BUCKETED_COLUMN.getMsg(columnName));
            }
          }
        }
      }

      Path queryTmpdir = ctx.getExternalTmpPath(newTblPartLoc);
      TruncateTableDesc truncateTblDesc = new TruncateTableDesc(tName, partSpec, null, table,
          new ArrayList<Integer>(columnIndexes), oldTblPartLoc, queryTmpdir, lbCtx);
      if (truncateTblDesc.mayNeedWriteId()) {
        setAcidDdlDesc(truncateTblDesc);
      }

      DDLWork ddlWork = new DDLWork(getInputs(), getOutputs(), truncateTblDesc);
      Task<?> truncateTask = TaskFactory.get(ddlWork);

      addInputsOutputsAlterTable(tName, partSpec, null, AlterTableType.TRUNCATE, false);
      ddlWork.setNeedLock(true);
      TableDesc tblDesc = Utilities.getTableDesc(table);
      // Write the output to temporary directory and move it to the final location at the end
      // so the operation is atomic.
      LoadTableDesc ltd = new LoadTableDesc(queryTmpdir, tblDesc, partSpec == null ? new HashMap<>() : partSpec);
      ltd.setLbCtx(lbCtx);
      Task<MoveWork> moveTsk = TaskFactory.get(new MoveWork(null, null, ltd, null, false));
      truncateTask.addDependentTask(moveTsk);

      // Recalculate the HDFS stats if auto gather stats is set
      if (conf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
        BasicStatsWork basicStatsWork;
        if (oldTblPartLoc.equals(newTblPartLoc)) {
          // If we're merging to the same location, we can avoid some metastore calls
          TableSpec tablepart = new TableSpec(this.db, conf, root);
          basicStatsWork = new BasicStatsWork(tablepart);
        } else {
          basicStatsWork = new BasicStatsWork(ltd);
        }
        basicStatsWork.setNoStatsAggregator(true);
        basicStatsWork.setClearAggregatorStats(true);
        StatsWork columnStatsWork = new StatsWork(table, basicStatsWork, conf);

        Task<?> statTask = TaskFactory.get(columnStatsWork);
        moveTsk.addDependentTask(statTask);
      }

      return truncateTask;
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  public static boolean isFullSpec(Table table, Map<String, String> partSpec) {
    for (FieldSchema partCol : table.getPartCols()) {
      if (partSpec.get(partCol.getName()) == null) {
        return false;
      }
    }
    return true;
  }

  private void validateAlterTableType(Table tbl, AlterTableType op) throws SemanticException {
    validateAlterTableType(tbl, op, false);
  }

  private void validateAlterTableType(Table tbl, AlterTableType op, boolean expectView)
      throws SemanticException {
    if (tbl.isView()) {
      if (!expectView) {
        throw new SemanticException(ErrorMsg.ALTER_COMMAND_FOR_VIEWS.getMsg());
      }

      switch (op) {
      case ADDPARTITION:
      case DROPPARTITION:
      case RENAMEPARTITION:
      case ADDPROPS:
      case DROPPROPS:
      case RENAME:
        // allow this form
        break;
      default:
        throw new SemanticException(ErrorMsg.ALTER_VIEW_DISALLOWED_OP.getMsg(op.toString()));
      }
    } else {
      if (expectView) {
        throw new SemanticException(ErrorMsg.ALTER_COMMAND_FOR_TABLES.getMsg());
      }
    }
    if (tbl.isNonNative() && !AlterTableType.NON_NATIVE_TABLE_ALLOWED.contains(op)) {
      throw new SemanticException(ErrorMsg.ALTER_TABLE_NON_NATIVE.getMsg(tbl.getTableName()));
    }
  }

  private boolean hasConstraintsEnabled(final String tblName) throws SemanticException{

    NotNullConstraint nnc = null;
    DefaultConstraint dc = null;
    try {
      // retrieve enabled NOT NULL constraint from metastore
      nnc = Hive.get().getEnabledNotNullConstraints(
          db.getDatabaseCurrent().getName(), tblName);
      dc = Hive.get().getEnabledDefaultConstraints(
          db.getDatabaseCurrent().getName(), tblName);
    } catch (Exception e) {
      if (e instanceof SemanticException) {
        throw (SemanticException) e;
      } else {
        throw (new RuntimeException(e));
      }
    }
    if((nnc != null  && !nnc.getNotNullConstraints().isEmpty())
        || (dc != null && !dc.getDefaultConstraints().isEmpty())) {
      return true;
    }
    return false;
  }

  private void analyzeAlterTableProps(TableName tableName, Map<String, String> partSpec, ASTNode ast,
      boolean expectView, boolean isUnset) throws SemanticException {

    Map<String, String> mapProp = getProps((ASTNode) (ast.getChild(0)).getChild(0));
    EnvironmentContext environmentContext = null;
    // we need to check if the properties are valid, especially for stats.
    // they might be changed via alter table .. update statistics or
    // alter table .. set tblproperties. If the property is not row_count
    // or raw_data_size, it could not be changed through update statistics
    boolean changeStatsSucceeded = false;
    for (Entry<String, String> entry : mapProp.entrySet()) {
      // we make sure that we do not change anything if there is anything
      // wrong.
      if (entry.getKey().equals(StatsSetupConst.ROW_COUNT)
          || entry.getKey().equals(StatsSetupConst.RAW_DATA_SIZE)) {
        try {
          Long.parseLong(entry.getValue());
          changeStatsSucceeded = true;
        } catch (Exception e) {
          throw new SemanticException("AlterTable " + entry.getKey() + " failed with value "
              + entry.getValue());
        }
      }
      // if table is being modified to be external we need to make sure existing table
      // doesn't have enabled constraint since constraints are disallowed with such tables
      else if (entry.getKey().equals("external") && entry.getValue().equals("true")) {
        if (hasConstraintsEnabled(tableName.getTable())) {
          throw new SemanticException(
              ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Table: " + tableName.getDbTable() + " has constraints enabled."
                  + "Please remove those constraints to change this property."));
        }
      }
      else {
        if (queryState.getCommandType()
            .equals(HiveOperation.ALTERTABLE_UPDATETABLESTATS.getOperationName())
            || queryState.getCommandType()
                .equals(HiveOperation.ALTERTABLE_UPDATEPARTSTATS.getOperationName())) {
          throw new SemanticException("AlterTable UpdateStats " + entry.getKey()
              + " failed because the only valid keys are " + StatsSetupConst.ROW_COUNT + " and "
              + StatsSetupConst.RAW_DATA_SIZE);
        }
      }

      if (changeStatsSucceeded) {
        environmentContext = new EnvironmentContext();
        environmentContext.putToProperties(StatsSetupConst.STATS_GENERATED, StatsSetupConst.USER);
      }
    }
    boolean isToTxn = AcidUtils.isTablePropertyTransactional(mapProp)
        || mapProp.containsKey(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);
    boolean isExplicitStatsUpdate = changeStatsSucceeded && AcidUtils.isTransactionalTable(getTable(tableName, true));
    AbstractAlterTableDesc alterTblDesc = null;
    DDLWork ddlWork = null;

    if (isUnset) {
      boolean dropIfExists = ast.getChild(1) != null;
      // validate Unset Non Existed Table Properties
      if (!dropIfExists) {
        Table tab = getTable(tableName, true);
        Map<String, String> tableParams = tab.getTTable().getParameters();
        for (String currKey : mapProp.keySet()) {
          if (!tableParams.containsKey(currKey)) {
            String errorMsg = "The following property " + currKey + " does not exist in " + tab.getTableName();
            throw new SemanticException(
              ErrorMsg.ALTER_TBL_UNSET_NON_EXIST_PROPERTY.getMsg(errorMsg));
          }
        }
      }

      alterTblDesc = new AlterTableUnsetPropertiesDesc(tableName, partSpec, null, expectView, mapProp,
          isExplicitStatsUpdate, environmentContext);
      addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc, alterTblDesc.getType(), isToTxn);
      ddlWork = new DDLWork(getInputs(), getOutputs(), alterTblDesc);
    } else {
      addPropertyReadEntry(mapProp, inputs);
      boolean isAcidConversion = isToTxn && AcidUtils.isFullAcidTable(mapProp)
          && !AcidUtils.isFullAcidTable(getTable(tableName, true));
      alterTblDesc = new AlterTableSetPropertiesDesc(tableName, partSpec, null, expectView, mapProp,
          isExplicitStatsUpdate, isAcidConversion, environmentContext);
      addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc, alterTblDesc.getType(), isToTxn);
      ddlWork = new DDLWork(getInputs(), getOutputs(), alterTblDesc);
    }
    if (isToTxn) {
      ddlWork.setNeedLock(true); // Hmm... why don't many other operations here need locks?
    }
    if (isToTxn || isExplicitStatsUpdate) {
      setAcidDdlDesc(alterTblDesc);
    }

    rootTasks.add(TaskFactory.get(ddlWork));
  }

  private void setAcidDdlDesc(DDLDescWithWriteId descWithWriteId) {
    if(this.ddlDescWithWriteId != null) {
      throw new IllegalStateException("ddlDescWithWriteId is already set: " + this.ddlDescWithWriteId);
    }
    this.ddlDescWithWriteId = descWithWriteId;
  }

  @Override
  public DDLDescWithWriteId getAcidDdlDesc() {
    return ddlDescWithWriteId;
  }

  private void analyzeAlterTableSerdeProps(ASTNode ast, TableName tableName, Map<String, String> partSpec)
      throws SemanticException {
    Map<String, String> mapProp = getProps((ASTNode) (ast.getChild(0)).getChild(0));
    AlterTableSetSerdePropsDesc alterTblDesc = new AlterTableSetSerdePropsDesc(tableName, partSpec, mapProp);

    addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc, AlterTableType.SET_SERDE_PROPS, false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblDesc)));
  }

  private void analyzeAlterTableSerde(ASTNode ast, TableName tableName, Map<String, String> partSpec)
      throws SemanticException {
    String serdeName = unescapeSQLString(ast.getChild(0).getText());
    Map<String, String> props = (ast.getChildCount() > 1) ? getProps((ASTNode) (ast.getChild(1)).getChild(0)) : null;
    AlterTableSetSerdeDesc alterTblDesc = new AlterTableSetSerdeDesc(tableName, partSpec, props, serdeName);

    addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc, AlterTableType.SET_SERDE, false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblDesc)));
  }

  private void analyzeAlterTableFileFormat(ASTNode ast, TableName tableName, Map<String, String> partSpec)
      throws SemanticException {
    StorageFormat format = new StorageFormat(conf);
    ASTNode child = (ASTNode) ast.getChild(0);
    if (!format.fillStorageFormat(child)) {
      throw new AssertionError("Unknown token " + child.getText());
    }

    AlterTableSetFileFormatDesc alterTblDesc = new AlterTableSetFileFormatDesc(tableName, partSpec,
        format.getInputFormat(), format.getOutputFormat(), format.getSerde());

    addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc, AlterTableType.SET_FILE_FORMAT, false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblDesc)));
  }

  // For the time while all the alter table operations are getting migrated there is a duplication of this method here
  private WriteType determineAlterTableWriteType(Table tab, AbstractAlterTableDesc desc, AlterTableType op) {
    boolean convertingToAcid = false;
    if (desc != null && desc.getProps() != null &&
        Boolean.parseBoolean(desc.getProps().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL))) {
      convertingToAcid = true;
    }
    if(!AcidUtils.isTransactionalTable(tab) && convertingToAcid) {
      //non-acid to transactional conversion (property itself) must be mutexed to prevent concurrent writes.
      // See HIVE-16688 for use cases.
      return WriteType.DDL_EXCLUSIVE;
    }
    return WriteEntity.determineAlterTableWriteType(op);
  }

  private void addInputsOutputsAlterTable(TableName tableName, Map<String, String> partSpec,
      AbstractAlterTableDesc desc, AlterTableType op, boolean doForceExclusive) throws SemanticException {
    boolean isCascade = desc != null && desc.isCascade();
    boolean alterPartitions = partSpec != null && !partSpec.isEmpty();
    //cascade only occurs at table level then cascade to partition level
    if (isCascade && alterPartitions) {
      throw new SemanticException(
          ErrorMsg.ALTER_TABLE_PARTITION_CASCADE_NOT_SUPPORTED, op.getName());
    }

    Table tab = getTable(tableName, true);
    // cascade only occurs with partitioned table
    if (isCascade && !tab.isPartitioned()) {
      throw new SemanticException(
          ErrorMsg.ALTER_TABLE_NON_PARTITIONED_TABLE_CASCADE_NOT_SUPPORTED);
    }

    // Determine the lock type to acquire
    WriteEntity.WriteType writeType = doForceExclusive
        ? WriteType.DDL_EXCLUSIVE : determineAlterTableWriteType(tab, desc, op);

    if (!alterPartitions) {
      inputs.add(new ReadEntity(tab));
      WriteEntity alterTableOutput = new WriteEntity(tab, writeType);
      outputs.add(alterTableOutput);
      //do not need the lock for partitions since they are covered by the table lock
      if (isCascade) {
        for (Partition part : getPartitions(tab, partSpec, false)) {
          outputs.add(new WriteEntity(part, WriteEntity.WriteType.DDL_NO_LOCK));
        }
      }
    } else {
      ReadEntity re = new ReadEntity(tab);
      // In the case of altering a table for its partitions we don't need to lock the table
      // itself, just the partitions.  But the table will have a ReadEntity.  So mark that
      // ReadEntity as no lock.
      re.noLockNeeded();
      inputs.add(re);

      if (isFullSpec(tab, partSpec)) {
        // Fully specified partition spec
        Partition part = getPartition(tab, partSpec, true);
        outputs.add(new WriteEntity(part, writeType));
      } else {
        // Partial partition spec supplied. Make sure this is allowed.
        if (!AlterTableType.SUPPORT_PARTIAL_PARTITION_SPEC.contains(op)) {
          throw new SemanticException(
              ErrorMsg.ALTER_TABLE_TYPE_PARTIAL_PARTITION_SPEC_NO_SUPPORTED, op.getName());
        } else if (!conf.getBoolVar(HiveConf.ConfVars.DYNAMICPARTITIONING)) {
          throw new SemanticException(ErrorMsg.DYNAMIC_PARTITION_DISABLED);
        }

        for (Partition part : getPartitions(tab, partSpec, true)) {
          outputs.add(new WriteEntity(part, writeType));
        }
      }
    }

    if (desc != null) {
      validateAlterTableType(tab, op, desc.expectView());
    }
  }

  private void analyzeAlterTableOwner(ASTNode ast, TableName tableName) throws SemanticException {
    PrincipalDesc ownerPrincipal = AuthorizationParseUtils.getPrincipalDesc((ASTNode) ast.getChild(0));

    if (ownerPrincipal.getType() == null) {
      throw new SemanticException("Owner type can't be null in alter table set owner command");
    }

    if (ownerPrincipal.getName() == null) {
      throw new SemanticException("Owner name can't be null in alter table set owner command");
    }

    AlterTableSetOwnerDesc alterTblDesc  = new AlterTableSetOwnerDesc(tableName, ownerPrincipal);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblDesc), conf));
  }

  private void analyzeAlterTableLocation(ASTNode ast, TableName tableName, Map<String, String> partSpec)
      throws SemanticException {

    String newLocation = unescapeSQLString(ast.getChild(0).getText());
    try {
      // To make sure host/port pair is valid, the status of the location does not matter
      FileSystem.get(new URI(newLocation), conf).getFileStatus(new Path(newLocation));
    } catch (FileNotFoundException e) {
      // Only check host/port pair is valid, whether the file exist or not does not matter
    } catch (Exception e) {
      throw new SemanticException("Cannot connect to namenode, please check if host/port pair for " + newLocation + " is valid", e);
    }

    addLocationToOutputs(newLocation);
    AlterTableSetLocationDesc alterTblDesc = new AlterTableSetLocationDesc(tableName, partSpec, newLocation);
    Table tbl = getTable(tableName);
    if (AcidUtils.isTransactionalTable(tbl)) {
      setAcidDdlDesc(alterTblDesc);
    }

    addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc, AlterTableType.ALTERLOCATION, false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblDesc)));
  }

  private void analyzeAlterTablePartMergeFiles(ASTNode ast, TableName tableName, Map<String, String> partSpec)
      throws SemanticException {

    Path oldTblPartLoc = null;
    Path newTblPartLoc = null;
    Table tblObj = null;
    ListBucketingCtx lbCtx = null;

    tblObj = getTable(tableName);
    if(AcidUtils.isTransactionalTable(tblObj)) {
      LinkedHashMap<String, String> newPartSpec = null;
      if (partSpec != null) {
        newPartSpec = new LinkedHashMap<>(partSpec);
      }

      boolean isBlocking = !HiveConf.getBoolVar(conf,
          ConfVars.TRANSACTIONAL_CONCATENATE_NOBLOCK, false);
      AlterTableCompactDesc desc = new AlterTableCompactDesc(tableName, newPartSpec, "MAJOR", isBlocking, null);

      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
      return;
    }

    List<String> bucketCols = null;
    Class<? extends InputFormat> inputFormatClass = null;
    boolean isArchived = false;
    if (tblObj.isPartitioned()) {
      if (partSpec == null) {
        throw new SemanticException("source table " + tableName
            + " is partitioned but no partition desc found.");
      } else {
        Partition part = getPartition(tblObj, partSpec, false);
        if (part == null) {
          throw new SemanticException("source table " + tableName
              + " is partitioned but partition not found.");
        }
        bucketCols = part.getBucketCols();
        try {
          inputFormatClass = part.getInputFormatClass();
        } catch (HiveException e) {
          throw new SemanticException(e);
        }
        isArchived = ArchiveUtils.isArchived(part);

        Path tabPath = tblObj.getPath();
        Path partPath = part.getDataLocation();

        // if the table is in a different dfs than the partition,
        // replace the partition's dfs with the table's dfs.
        newTblPartLoc = new Path(tabPath.toUri().getScheme(), tabPath.toUri()
            .getAuthority(), partPath.toUri().getPath());

        oldTblPartLoc = partPath;

        lbCtx = constructListBucketingCtx(part.getSkewedColNames(), part.getSkewedColValues(),
            part.getSkewedColValueLocationMaps(), part.isStoredAsSubDirectories());
      }
    } else {
      inputFormatClass = tblObj.getInputFormatClass();
      bucketCols = tblObj.getBucketCols();

      // input and output are the same
      oldTblPartLoc = tblObj.getPath();
      newTblPartLoc = tblObj.getPath();

      lbCtx = constructListBucketingCtx(tblObj.getSkewedColNames(), tblObj.getSkewedColValues(),
          tblObj.getSkewedColValueLocationMaps(), tblObj.isStoredAsSubDirectories());
    }

    // throw a HiveException for other than rcfile and orcfile.
    if (!(inputFormatClass.equals(RCFileInputFormat.class) || inputFormatClass.equals(OrcInputFormat.class))) {
      throw new SemanticException(ErrorMsg.CONCATENATE_UNSUPPORTED_FILE_FORMAT.getMsg());
    }

    // throw a HiveException if the table/partition is bucketized
    if (bucketCols != null && bucketCols.size() > 0) {
      throw new SemanticException(ErrorMsg.CONCATENATE_UNSUPPORTED_TABLE_BUCKETED.getMsg());
    }

    // throw a HiveException if the table/partition is archived
    if (isArchived) {
      throw new SemanticException(ErrorMsg.CONCATENATE_UNSUPPORTED_PARTITION_ARCHIVED.getMsg());
    }

    // non-native and non-managed tables are not supported as MoveTask requires filenames to be in specific format,
    // violating which can cause data loss
    if (tblObj.isNonNative()) {
      throw new SemanticException(ErrorMsg.CONCATENATE_UNSUPPORTED_TABLE_NON_NATIVE.getMsg());
    }

    if (tblObj.getTableType() != TableType.MANAGED_TABLE) {
      throw new SemanticException(ErrorMsg.CONCATENATE_UNSUPPORTED_TABLE_NOT_MANAGED.getMsg());
    }

    addInputsOutputsAlterTable(tableName, partSpec, null, AlterTableType.MERGEFILES, false);
    TableDesc tblDesc = Utilities.getTableDesc(tblObj);
    Path queryTmpdir = ctx.getExternalTmpPath(newTblPartLoc);
    AlterTableConcatenateDesc mergeDesc = new AlterTableConcatenateDesc(tableName, partSpec, lbCtx, oldTblPartLoc,
        queryTmpdir, inputFormatClass, Utilities.getTableDesc(tblObj));
    DDLWork ddlWork = new DDLWork(getInputs(), getOutputs(), mergeDesc);
    ddlWork.setNeedLock(true);
    Task<?> mergeTask = TaskFactory.get(ddlWork);
    // No need to handle MM tables - unsupported path.
    LoadTableDesc ltd = new LoadTableDesc(queryTmpdir, tblDesc,
        partSpec == null ? new HashMap<>() : partSpec);
    ltd.setLbCtx(lbCtx);
    ltd.setInheritTableSpecs(true);
    Task<MoveWork> moveTsk =
        TaskFactory.get(new MoveWork(null, null, ltd, null, false));
    mergeTask.addDependentTask(moveTsk);

    if (conf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
      BasicStatsWork basicStatsWork;
      if (oldTblPartLoc.equals(newTblPartLoc)) {
        // If we're merging to the same location, we can avoid some metastore calls
        try{
          TableSpec tableSpec = new TableSpec(db, tableName, partSpec);
          basicStatsWork = new BasicStatsWork(tableSpec);
        } catch (HiveException e){
          throw new SemanticException(e);
        }
      } else {
        basicStatsWork = new BasicStatsWork(ltd);
      }
      basicStatsWork.setNoStatsAggregator(true);
      basicStatsWork.setClearAggregatorStats(true);
      StatsWork columnStatsWork = new StatsWork(tblObj, basicStatsWork, conf);

      Task<?> statTask = TaskFactory.get(columnStatsWork);
      moveTsk.addDependentTask(statTask);
    }

    rootTasks.add(mergeTask);
  }

  private void analyzeAlterTableClusterSort(ASTNode ast, TableName tableName, Map<String, String> partSpec)
      throws SemanticException {

    AbstractAlterTableDesc alterTblDesc;
    switch (ast.getChild(0).getType()) {
    case HiveParser.TOK_NOT_CLUSTERED:
      alterTblDesc = new AlterTableNotClusteredDesc(tableName, partSpec);
      break;
    case HiveParser.TOK_NOT_SORTED:
      alterTblDesc = new AlterTableNotSortedDesc(tableName, partSpec);
      break;
    case HiveParser.TOK_ALTERTABLE_BUCKETS:
      ASTNode buckets = (ASTNode) ast.getChild(0);
      List<String> bucketCols = getColumnNames((ASTNode) buckets.getChild(0));
      List<Order> sortCols = new ArrayList<Order>();
      int numBuckets = -1;
      if (buckets.getChildCount() == 2) {
        numBuckets = Integer.parseInt(buckets.getChild(1).getText());
      } else {
        sortCols = getColumnNamesOrder((ASTNode) buckets.getChild(1));
        numBuckets = Integer.parseInt(buckets.getChild(2).getText());
      }
      if (numBuckets <= 0) {
        throw new SemanticException(ErrorMsg.INVALID_BUCKET_NUMBER.getMsg());
      }

      alterTblDesc = new AlterTableClusteredByDesc(tableName, partSpec, numBuckets, bucketCols, sortCols);
      break;
    default:
      throw new SemanticException("Invalid operation " + ast.getChild(0).getType());
    }
    addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc, alterTblDesc.getType(), false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblDesc)));
  }

  private void analyzeAlterTableCompact(ASTNode ast, TableName tableName,
      Map<String, String> partSpec) throws SemanticException {

    String type = unescapeSQLString(ast.getChild(0).getText()).toLowerCase();

    if (!type.equals("minor") && !type.equals("major")) {
      throw new SemanticException(ErrorMsg.INVALID_COMPACTION_TYPE.getMsg());
    }

    LinkedHashMap<String, String> newPartSpec = null;
    if (partSpec != null) {
      newPartSpec = new LinkedHashMap<String, String>(partSpec);
    }

    Map<String, String> mapProp = null;
    boolean isBlocking = false;

    for(int i = 0; i < ast.getChildCount(); i++) {
      switch(ast.getChild(i).getType()) {
      case HiveParser.TOK_TABLEPROPERTIES:
        mapProp = getProps((ASTNode) (ast.getChild(i)).getChild(0));
        break;
      case HiveParser.TOK_BLOCKING:
        isBlocking = true;
        break;
      default:
        break;
      }
    }
    AlterTableCompactDesc desc = new AlterTableCompactDesc(tableName, newPartSpec, type, isBlocking, mapProp);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }

  /**
   * Utility class to resolve QualifiedName
   */
  static class QualifiedNameUtil {

    // delimiter to check DOT delimited qualified names
    static final String delimiter = "\\.";

    /**
     * Get the fully qualified name in the ast. e.g. the ast of the form ^(DOT
     * ^(DOT a b) c) will generate a name of the form a.b.c
     *
     * @param ast
     *          The AST from which the qualified name has to be extracted
     * @return String
     */
    static public String getFullyQualifiedName(ASTNode ast) {
      if (ast.getChildCount() == 0) {
        return ast.getText();
      } else if (ast.getChildCount() == 2) {
        return getFullyQualifiedName((ASTNode) ast.getChild(0)) + "."
        + getFullyQualifiedName((ASTNode) ast.getChild(1));
      } else if (ast.getChildCount() == 3) {
        return getFullyQualifiedName((ASTNode) ast.getChild(0)) + "."
        + getFullyQualifiedName((ASTNode) ast.getChild(1)) + "."
        + getFullyQualifiedName((ASTNode) ast.getChild(2));
      } else {
        return null;
      }
    }

    // get the column path
    // return column name if exists, column could be DOT separated.
    // example: lintString.$elem$.myint
    // return table name for column name if no column has been specified.
    static public String getColPath(Hive db, ASTNode node, TableName tableName, Map<String, String> partSpec)
        throws SemanticException {

      // if this ast has only one child, then no column name specified.
      if (node.getChildCount() == 1) {
        return null;
      }

      ASTNode columnNode = null;
      // Second child node could be partitionspec or column
      if (node.getChildCount() > 1) {
        if (partSpec == null) {
          columnNode = (ASTNode) node.getChild(1);
        } else {
          columnNode = (ASTNode) node.getChild(2);
        }
      }

      if (columnNode != null) {
        return String.join(".", tableName.getNotEmptyDbTable(), QualifiedNameUtil.getFullyQualifiedName(columnNode));
      } else {
        return null;
      }
    }

    // get partition metadata
    static Map<String, String> getPartitionSpec(Hive db, ASTNode ast, TableName tableName)
      throws SemanticException {
      ASTNode partNode = null;
      // if this ast has only one child, then no partition spec specified.
      if (ast.getChildCount() == 1) {
        return null;
      }

      // if ast has two children
      // the 2nd child could be partition spec or columnName
      // if the ast has 3 children, the second *has to* be partition spec
      if (ast.getChildCount() > 2 && (((ASTNode) ast.getChild(1)).getType() != HiveParser.TOK_PARTSPEC)) {
        throw new SemanticException(((ASTNode) ast.getChild(1)).getType() + " is not a partition specification");
      }

      if (((ASTNode) ast.getChild(1)).getType() == HiveParser.TOK_PARTSPEC) {
        partNode = (ASTNode) ast.getChild(1);
      }

      if (partNode != null) {
        Table tab = null;
        try {
          tab = db.getTable(tableName.getNotEmptyDbTable());
        }
        catch (InvalidTableException e) {
          throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName.getNotEmptyDbTable()), e);
        }
        catch (HiveException e) {
          throw new SemanticException(e.getMessage(), e);
        }

        Map<String, String> partSpec = null;
        try {
          partSpec = getValidatedPartSpec(tab, partNode, db.getConf(), false);
        } catch (SemanticException e) {
          // get exception in resolving partition
          // it could be DESCRIBE table key
          // return null
          // continue processing for DESCRIBE table key
          return null;
        }

        if (partSpec != null) {
          Partition part = null;
          try {
            part = db.getPartition(tab, partSpec, false);
          } catch (HiveException e) {
            // if get exception in finding partition
            // it could be DESCRIBE table key
            // return null
            // continue processing for DESCRIBE table key
            return null;
          }

          // if partition is not found
          // it is DESCRIBE table partition
          // invalid partition exception
          if (part == null) {
            throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partSpec.toString()));
          }

          // it is DESCRIBE table partition
          // return partition metadata
          return partSpec;
        }
      }

      return null;
    }

  }

  private void validateDatabase(String databaseName) throws SemanticException {
    try {
      if (!db.databaseExists(databaseName)) {
        throw new SemanticException(ErrorMsg.DATABASE_NOT_EXISTS.getMsg(databaseName));
      }
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.DATABASE_NOT_EXISTS.getMsg(databaseName), e);
    }
  }

  private void validateTable(TableName tableName, Map<String, String> partSpec)
      throws SemanticException {
    Table tab = getTable(tableName);
    if (partSpec != null) {
      getPartition(tab, partSpec, true);
    }
  }

  /**
   * A query like this will generate a tree as follows
   *   "describe formatted default.maptable partition (b=100) id;"
   * TOK_TABTYPE
   *   TOK_TABNAME --> root for tablename, 2 child nodes mean DB specified
   *     default
   *     maptable
   *   TOK_PARTSPEC  --> root node for partition spec. else columnName
   *     TOK_PARTVAL
   *       b
   *       100
   *   id           --> root node for columnName
   * formatted
   */
  private void analyzeDescribeTable(ASTNode ast) throws SemanticException {
    ASTNode tableTypeExpr = (ASTNode) ast.getChild(0);

    final TableName tableName;
    String colPath   = null;
    Map<String, String> partSpec = null;

    ASTNode tableNode = null;

    // process the first node to extract tablename
    // tablename is either TABLENAME or DBNAME.TABLENAME if db is given
    if (((ASTNode) tableTypeExpr.getChild(0)).getType() == HiveParser.TOK_TABNAME) {
      tableNode = (ASTNode) tableTypeExpr.getChild(0);
      if (tableNode.getChildCount() == 1) {
        tableName = HiveTableName.of(((ASTNode) tableNode.getChild(0)).getText());
      } else {
        tableName = TableName.fromString(((ASTNode) tableNode.getChild(1)).getText(),
            SessionState.get().getCurrentCatalog(), ((ASTNode) tableNode.getChild(0)).getText());
      }
    } else {
      throw new SemanticException(((ASTNode) tableTypeExpr.getChild(0)).getText() + " is not an expected token type");
    }

    // process the second child,if exists, node to get partition spec(s)
    partSpec = QualifiedNameUtil.getPartitionSpec(db, tableTypeExpr, tableName);

    // process the third child node,if exists, to get partition spec(s)
    colPath  = QualifiedNameUtil.getColPath(db, tableTypeExpr, tableName, partSpec);

    // if database is not the one currently using
    // validate database
    if (tableName.getDb() != null) {
      validateDatabase(tableName.getDb());
    }
    if (partSpec != null) {
      validateTable(tableName, partSpec);
    }

    boolean showColStats = false;
    boolean isFormatted = false;
    boolean isExt = false;
    if (ast.getChildCount() == 2) {
      int descOptions = ast.getChild(1).getType();
      isFormatted = descOptions == HiveParser.KW_FORMATTED;
      isExt = descOptions == HiveParser.KW_EXTENDED;
      // in case of "DESCRIBE FORMATTED tablename column_name" statement, colPath
      // will contain tablename.column_name. If column_name is not specified
      // colPath will be equal to tableName. This is how we can differentiate
      // if we are describing a table or column
      if (colPath != null && isFormatted) {
        showColStats = true;
      }
    }

    inputs.add(new ReadEntity(getTable(tableName)));

    DescTableDesc descTblDesc = new DescTableDesc(ctx.getResFile(), tableName, partSpec, colPath, isExt, isFormatted);
    Task<?> ddlTask = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), descTblDesc));
    rootTasks.add(ddlTask);
    String schema = showColStats ? DescTableDesc.COLUMN_STATISTICS_SCHEMA : DescTableDesc.SCHEMA;
    setFetchTask(createFetchTask(schema));
    LOG.info("analyzeDescribeTable done");
  }

  private void analyzeShowPartitions(ASTNode ast) throws SemanticException {
    ShowPartitionsDesc showPartsDesc;
    String tableName = getUnescapedName((ASTNode) ast.getChild(0));
    List<Map<String, String>> partSpecs = getPartitionSpecs(getTable(tableName), ast);
    // We only can have a single partition spec
    assert (partSpecs.size() <= 1);
    Map<String, String> partSpec = null;
    if (partSpecs.size() > 0) {
      partSpec = partSpecs.get(0);
    }

    validateTable(HiveTableName.ofNullableWithNoDefault(tableName), null);

    showPartsDesc = new ShowPartitionsDesc(tableName, ctx.getResFile(), partSpec);
    inputs.add(new ReadEntity(getTable(tableName)));
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), showPartsDesc)));
    setFetchTask(createFetchTask(ShowPartitionsDesc.SCHEMA));
  }

  private void analyzeShowTables(ASTNode ast) throws SemanticException {
    ShowTablesDesc showTblsDesc;
    String dbName = SessionState.get().getCurrentDatabase();
    String tableNames = null;
    TableType tableTypeFilter = null;
    boolean isExtended = false;

    if (ast.getChildCount() > 4) {
      throw new SemanticException(ErrorMsg.INVALID_AST_TREE.getMsg(ast.toStringTree()));
    }

    for (int i = 0; i < ast.getChildCount(); i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      if (child.getType() == HiveParser.TOK_FROM) { // Specifies a DB
        dbName = unescapeIdentifier(ast.getChild(++i).getText());
        validateDatabase(dbName);
      } else if (child.getType() == HiveParser.TOK_TABLE_TYPE) { // Filter on table type
        String tableType = unescapeIdentifier(child.getChild(0).getText());
        if (!tableType.equalsIgnoreCase("table_type")) {
          throw new SemanticException("SHOW TABLES statement only allows equality filter on table_type value");
        }
        tableTypeFilter = TableType.valueOf(unescapeSQLString(child.getChild(1).getText()));
      } else if (child.getType() == HiveParser.KW_EXTENDED) { // Include table type
        isExtended = true;
      } else { // Uses a pattern
        tableNames = unescapeSQLString(child.getText());
      }
    }

    showTblsDesc = new ShowTablesDesc(ctx.getResFile(), dbName, tableNames, tableTypeFilter, isExtended);
    inputs.add(new ReadEntity(getDatabase(dbName)));
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), showTblsDesc)));
    setFetchTask(createFetchTask(showTblsDesc.getSchema()));
  }

  private void analyzeShowTableStatus(ASTNode ast) throws SemanticException {
    ShowTableStatusDesc showTblStatusDesc;
    String tableNames = getUnescapedName((ASTNode) ast.getChild(0));
    String dbName = SessionState.get().getCurrentDatabase();
    int children = ast.getChildCount();
    Map<String, String> partSpec = null;
    if (children >= 2) {
      if (children > 3) {
        throw new SemanticException(ErrorMsg.INVALID_AST_TREE.getMsg());
      }
      for (int i = 1; i < children; i++) {
        ASTNode child = (ASTNode) ast.getChild(i);
        if (child.getToken().getType() == HiveParser.Identifier) {
          dbName = unescapeIdentifier(child.getText());
        } else if (child.getToken().getType() == HiveParser.TOK_PARTSPEC) {
          partSpec = getValidatedPartSpec(getTable(tableNames), child, conf, false);
        } else {
          throw new SemanticException(ErrorMsg.INVALID_AST_TREE.getMsg(child.toStringTree() +
            " , Invalid token " + child.getToken().getType()));
        }
      }
    }

    if (partSpec != null) {
      validateTable(HiveTableName.ofNullableWithNoDefault(tableNames), partSpec);
    }

    showTblStatusDesc = new ShowTableStatusDesc(ctx.getResFile().toString(), dbName, tableNames, partSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), showTblStatusDesc)));
    setFetchTask(createFetchTask(ShowTableStatusDesc.SCHEMA));
  }

  private void analyzeShowTableProperties(ASTNode ast) throws SemanticException {
    ShowTablePropertiesDesc showTblPropertiesDesc;
    TableName qualified = getQualifiedTableName((ASTNode) ast.getChild(0));
    String propertyName = null;
    if (ast.getChildCount() > 1) {
      propertyName = unescapeSQLString(ast.getChild(1).getText());
    }

    validateTable(qualified, null);

    showTblPropertiesDesc = new ShowTablePropertiesDesc(ctx.getResFile().toString(), qualified, propertyName);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), showTblPropertiesDesc)));
    setFetchTask(createFetchTask(ShowTablePropertiesDesc.SCHEMA));
  }

  /**
   * Add the task according to the parsed command tree. This is used for the CLI
   * command "SHOW LOCKS;".
   *
   * @param ast
   *          The parsed command tree.
   * @throws SemanticException
   *           Parsing failed
   */
  private void analyzeShowLocks(ASTNode ast) throws SemanticException {
    String tableName = null;
    Map<String, String> partSpec = null;
    boolean isExtended = false;

    if (ast.getChildCount() >= 1) {
      // table for which show locks is being executed
      for (int i = 0; i < ast.getChildCount(); i++) {
        ASTNode child = (ASTNode) ast.getChild(i);
        if (child.getType() == HiveParser.TOK_TABTYPE) {
          ASTNode tableTypeExpr = child;
          tableName =
            QualifiedNameUtil.getFullyQualifiedName((ASTNode) tableTypeExpr.getChild(0));
          // get partition metadata if partition specified
          if (tableTypeExpr.getChildCount() == 2) {
            ASTNode partSpecNode = (ASTNode) tableTypeExpr.getChild(1);
            partSpec = getValidatedPartSpec(getTable(tableName), partSpecNode, conf, false);
          }
        } else if (child.getType() == HiveParser.KW_EXTENDED) {
          isExtended = true;
        }
      }
    }

    HiveTxnManager txnManager = null;
    try {
      txnManager = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    } catch (LockException e) {
      throw new SemanticException(e.getMessage());
    }

    ShowLocksDesc showLocksDesc = new ShowLocksDesc(ctx.getResFile(), tableName,
        partSpec, isExtended, txnManager.useNewShowLocksFormat());
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), showLocksDesc)));
    setFetchTask(createFetchTask(showLocksDesc.getSchema()));

    // Need to initialize the lock manager
    ctx.setNeedLockMgr(true);
  }

   /**
    * Add the task according to the parsed command tree. This is used for the CLI
   * command "SHOW LOCKS DATABASE database [extended];".
   *
   * @param ast
   *          The parsed command tree.
   * @throws SemanticException
   *           Parsing failed
   */
  private void analyzeShowDbLocks(ASTNode ast) throws SemanticException {
    boolean isExtended = (ast.getChildCount() > 1);
    String dbName = stripQuotes(ast.getChild(0).getText());

    HiveTxnManager txnManager = null;
    try {
      txnManager = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    } catch (LockException e) {
      throw new SemanticException(e.getMessage());
    }

    ShowLocksDesc showLocksDesc = new ShowLocksDesc(ctx.getResFile(), dbName,
                                                    isExtended, txnManager.useNewShowLocksFormat());
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), showLocksDesc)));
    setFetchTask(createFetchTask(showLocksDesc.getSchema()));

    // Need to initialize the lock manager
    ctx.setNeedLockMgr(true);
  }

  private void analyzeShowViews(ASTNode ast) throws SemanticException {
    ShowTablesDesc showViewsDesc;
    String dbName = SessionState.get().getCurrentDatabase();
    String viewNames = null;

    if (ast.getChildCount() > 3) {
      throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
    }

    switch (ast.getChildCount()) {
    case 1: // Uses a pattern
      viewNames = unescapeSQLString(ast.getChild(0).getText());
      showViewsDesc = new ShowTablesDesc(ctx.getResFile(), dbName, viewNames, TableType.VIRTUAL_VIEW);
      break;
    case 2: // Specifies a DB
      assert (ast.getChild(0).getType() == HiveParser.TOK_FROM);
      dbName = unescapeIdentifier(ast.getChild(1).getText());
      validateDatabase(dbName);
      showViewsDesc = new ShowTablesDesc(ctx.getResFile(), dbName, TableType.VIRTUAL_VIEW);
      break;
    case 3: // Uses a pattern and specifies a DB
      assert (ast.getChild(0).getType() == HiveParser.TOK_FROM);
      dbName = unescapeIdentifier(ast.getChild(1).getText());
      viewNames = unescapeSQLString(ast.getChild(2).getText());
      validateDatabase(dbName);
      showViewsDesc = new ShowTablesDesc(ctx.getResFile(), dbName, viewNames, TableType.VIRTUAL_VIEW);
      break;
    default: // No pattern or DB
      showViewsDesc = new ShowTablesDesc(ctx.getResFile(), dbName, TableType.VIRTUAL_VIEW);
      break;
    }

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), showViewsDesc)));
    setFetchTask(createFetchTask(showViewsDesc.getSchema()));
  }

  private void analyzeShowMaterializedViews(ASTNode ast) throws SemanticException {
    ShowTablesDesc showMaterializedViewsDesc;
    String dbName = SessionState.get().getCurrentDatabase();
    String materializedViewNames = null;

    if (ast.getChildCount() > 3) {
      throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
    }

    switch (ast.getChildCount()) {
    case 1: // Uses a pattern
      materializedViewNames = unescapeSQLString(ast.getChild(0).getText());
      showMaterializedViewsDesc = new ShowTablesDesc(
          ctx.getResFile(), dbName, materializedViewNames, TableType.MATERIALIZED_VIEW);
      break;
    case 2: // Specifies a DB
      assert (ast.getChild(0).getType() == HiveParser.TOK_FROM);
      dbName = unescapeIdentifier(ast.getChild(1).getText());
      validateDatabase(dbName);
      showMaterializedViewsDesc = new ShowTablesDesc(ctx.getResFile(), dbName, TableType.MATERIALIZED_VIEW);
      break;
    case 3: // Uses a pattern and specifies a DB
      assert (ast.getChild(0).getType() == HiveParser.TOK_FROM);
      dbName = unescapeIdentifier(ast.getChild(1).getText());
      materializedViewNames = unescapeSQLString(ast.getChild(2).getText());
      validateDatabase(dbName);
      showMaterializedViewsDesc = new ShowTablesDesc(
          ctx.getResFile(), dbName, materializedViewNames, TableType.MATERIALIZED_VIEW);
      break;
    default: // No pattern or DB
      showMaterializedViewsDesc = new ShowTablesDesc(ctx.getResFile(), dbName, TableType.MATERIALIZED_VIEW);
      break;
    }

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), showMaterializedViewsDesc)));
    setFetchTask(createFetchTask(showMaterializedViewsDesc.getSchema()));
  }

  /**
   * Add the task according to the parsed command tree. This is used for the CLI
   * command "LOCK TABLE ..;".
   *
   * @param ast
   *          The parsed command tree.
   * @throws SemanticException
   *           Parsing failed
   */
  private void analyzeLockTable(ASTNode ast)
      throws SemanticException {
    String tableName = getUnescapedName((ASTNode) ast.getChild(0)).toLowerCase();
    String mode = unescapeIdentifier(ast.getChild(1).getText().toUpperCase());
    List<Map<String, String>> partSpecs = getPartitionSpecs(getTable(tableName), ast);

    // We only can have a single partition spec
    assert (partSpecs.size() <= 1);
    Map<String, String> partSpec = null;
    if (partSpecs.size() > 0) {
      partSpec = partSpecs.get(0);
    }

    LockTableDesc lockTblDesc = new LockTableDesc(tableName, mode, partSpec,
        HiveConf.getVar(conf, ConfVars.HIVEQUERYID), ctx.getCmd());
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), lockTblDesc)));

    // Need to initialize the lock manager
    ctx.setNeedLockMgr(true);
  }

  /**
   * Add the task according to the parsed command tree. This is used for the CLI
   * command "UNLOCK TABLE ..;".
   *
   * @param ast
   *          The parsed command tree.
   * @throws SemanticException
   *           Parsing failed
   */
  private void analyzeUnlockTable(ASTNode ast)
      throws SemanticException {
    String tableName = getUnescapedName((ASTNode) ast.getChild(0));
    List<Map<String, String>> partSpecs = getPartitionSpecs(getTable(tableName), ast);

    // We only can have a single partition spec
    assert (partSpecs.size() <= 1);
    Map<String, String> partSpec = null;
    if (partSpecs.size() > 0) {
      partSpec = partSpecs.get(0);
    }

    UnlockTableDesc unlockTblDesc = new UnlockTableDesc(tableName, partSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), unlockTblDesc)));

    // Need to initialize the lock manager
    ctx.setNeedLockMgr(true);
  }

  private void analyzeAlterTableRename(TableName source, ASTNode ast, boolean expectView)
      throws SemanticException {
    final TableName target = getQualifiedTableName((ASTNode) ast.getChild(0));

    AlterTableRenameDesc alterTblDesc = new AlterTableRenameDesc(source, null, expectView, target.getDbTable());
    Table table = getTable(source.getDbTable(), true);
    if (AcidUtils.isTransactionalTable(table)) {
      setAcidDdlDesc(alterTblDesc);
    }
    addInputsOutputsAlterTable(source, null, alterTblDesc, alterTblDesc.getType(), false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblDesc)));
  }

  private void analyzeAlterTableRenamePart(ASTNode ast, TableName tblName,
      Map<String, String> oldPartSpec) throws SemanticException {
    Table tab = getTable(tblName, true);
    validateAlterTableType(tab, AlterTableType.RENAMEPARTITION);
    Map<String, String> newPartSpec =
        getValidatedPartSpec(tab, (ASTNode)ast.getChild(0), conf, false);
    if (newPartSpec == null) {
      throw new SemanticException("RENAME PARTITION Missing Destination" + ast);
    }
    ReadEntity re = new ReadEntity(tab);
    re.noLockNeeded();
    inputs.add(re);

    List<Map<String, String>> partSpecs = new ArrayList<Map<String, String>>();
    partSpecs.add(oldPartSpec);
    partSpecs.add(newPartSpec);
    addTablePartsOutputs(tab, partSpecs, WriteEntity.WriteType.DDL_EXCLUSIVE);
    AlterTableRenamePartitionDesc renamePartitionDesc = new AlterTableRenamePartitionDesc(tblName, oldPartSpec,
        newPartSpec, null, tab);
    if (AcidUtils.isTransactionalTable(tab)) {
      setAcidDdlDesc(renamePartitionDesc);
    }
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), renamePartitionDesc)));
  }

  private void analyzeAlterTableBucketNum(ASTNode ast, TableName tblName, Map<String, String> partSpec)
      throws SemanticException {
    Table tab = getTable(tblName, true);
    if (CollectionUtils.isEmpty(tab.getBucketCols())) {
      throw new SemanticException(ErrorMsg.ALTER_BUCKETNUM_NONBUCKETIZED_TBL.getMsg());
    }
    validateAlterTableType(tab, AlterTableType.INTO_BUCKETS);
    inputs.add(new ReadEntity(tab));

    int numberOfBuckets = Integer.parseInt(ast.getChild(0).getText());
    AlterTableIntoBucketsDesc alterBucketNum = new AlterTableIntoBucketsDesc(tblName, partSpec, numberOfBuckets);

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterBucketNum)));
  }

  private void analyzeAlterTableDropParts(TableName tName, ASTNode ast, boolean expectView) throws SemanticException {

    boolean ifExists = (ast.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null)
        || HiveConf.getBoolVar(conf, ConfVars.DROP_IGNORES_NON_EXISTENT);
    // If the drop has to fail on non-existent partitions, we cannot batch expressions.
    // That is because we actually have to check each separate expression for existence.
    // We could do a small optimization for the case where expr has all columns and all
    // operators are equality, if we assume those would always match one partition (which
    // may not be true with legacy, non-normalized column values). This is probably a
    // popular case but that's kinda hacky. Let's not do it for now.
    boolean canGroupExprs = ifExists;

    boolean mustPurge = (ast.getFirstChildWithType(HiveParser.KW_PURGE) != null);
    ReplicationSpec replicationSpec = new ReplicationSpec(ast);

    Table tab = null;
    try {
      tab = getTable(tName);
    } catch (SemanticException se){
      if (replicationSpec.isInReplicationScope() &&
            (
                (se.getCause() instanceof InvalidTableException)
                ||  (se.getMessage().contains(ErrorMsg.INVALID_TABLE.getMsg()))
            )){
        // If we're inside a replication scope, then the table not existing is not an error.
        // We just return in that case, no drop needed.
        return;
        // TODO : the contains message check is fragile, we should refactor SemanticException to be
        // queriable for error code, and not simply have a message
        // NOTE : IF_EXISTS might also want to invoke this, but there's a good possibility
        // that IF_EXISTS is stricter about table existence, and applies only to the ptn.
        // Therefore, ignoring IF_EXISTS here.
      } else {
        throw se;
      }
    }
    Map<Integer, List<ExprNodeGenericFuncDesc>> partSpecs =
        getFullPartitionSpecs(ast, tab, canGroupExprs);
    if (partSpecs.isEmpty())
     {
      return; // nothing to do
    }

    validateAlterTableType(tab, AlterTableType.DROPPARTITION, expectView);
    ReadEntity re = new ReadEntity(tab);
    re.noLockNeeded();
    inputs.add(re);

    addTableDropPartsOutputs(tab, partSpecs.values(), !ifExists);

    AlterTableDropPartitionDesc dropTblDesc =
        new AlterTableDropPartitionDesc(tName, partSpecs, mustPurge, replicationSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), dropTblDesc)));
  }

  private void analyzeAlterTablePartColType(TableName tableName, ASTNode ast)
      throws SemanticException {


    // check if table exists.
    Table tab = getTable(tableName);
    inputs.add(new ReadEntity(tab));

    // validate the DDL is a valid operation on the table.
    validateAlterTableType(tab, AlterTableType.ALTERPARTITION, false);

    // Alter table ... partition column ( column newtype) only takes one column at a time.
    // It must have a column name followed with type.
    ASTNode colAst = (ASTNode) ast.getChild(0);

    FieldSchema newCol = new FieldSchema();

    // get column name
    String name = colAst.getChild(0).getText().toLowerCase();
    newCol.setName(unescapeIdentifier(name));

    // get column type
    ASTNode typeChild = (ASTNode) (colAst.getChild(1));
    newCol.setType(getTypeStringFromAST(typeChild));

    if (colAst.getChildCount() == 3) {
      newCol.setComment(unescapeSQLString(colAst.getChild(2).getText()));
    }

    // check if column is defined or not
    boolean fFoundColumn = false;
    for( FieldSchema col : tab.getTTable().getPartitionKeys()) {
      if (col.getName().compareTo(newCol.getName()) == 0) {
        fFoundColumn = true;
      }
    }

    // raise error if we could not find the column
    if (!fFoundColumn) {
      throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(newCol.getName()));
    }

    AlterTableAlterPartitionDesc alterTblAlterPartDesc =
            new AlterTableAlterPartitionDesc(tableName.getDbTable(), newCol);
    if (AcidUtils.isTransactionalTable(tab)) {
      setAcidDdlDesc(alterTblAlterPartDesc);
    }

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblAlterPartDesc)));
  }

  /**
   * Add one or more partitions to a table. Useful when the data has been copied
   * to the right location by some other process.
   *
   * @param ast
   *          The parsed command tree.
   *
   * @param expectView
   *          True for ALTER VIEW, false for ALTER TABLE.
   *
   * @throws SemanticException
   *           Parsing failed
   */
  private void analyzeAlterTableAddParts(TableName tName, CommonTree ast, boolean expectView) throws SemanticException {

    // ^(TOK_ALTERTABLE_ADDPARTS identifier ifNotExists? alterStatementSuffixAddPartitionsElement+)
    boolean ifNotExists = ast.getChild(0).getType() == HiveParser.TOK_IFNOTEXISTS;

    Table table = getTable(tName);
    boolean isView = table.isView();
    validateAlterTableType(table, AlterTableType.ADDPARTITION, expectView);
    outputs.add(new WriteEntity(table,
        /*use DDL_EXCLUSIVE to cause X lock to prevent races between concurrent add partition calls
        with IF NOT EXISTS.  w/o this 2 concurrent calls to add the same partition may both add
        data since for transactional tables creating partition metadata and moving data there are
        2 separate actions. */
        ifNotExists && AcidUtils.isTransactionalTable(table) ? WriteType.DDL_EXCLUSIVE
        : WriteEntity.WriteType.DDL_SHARED));

    int numCh = ast.getChildCount();
    int start = ifNotExists ? 1 : 0;

    String currentLocation = null;
    Map<String, String> currentPart = null;
    // Parser has done some verification, so the order of tokens doesn't need to be verified here.

    List<AlterTableAddPartitionDesc.PartitionDesc> partitions = new ArrayList<>();
    for (int num = start; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);
      switch (child.getToken().getType()) {
      case HiveParser.TOK_PARTSPEC:
        if (currentPart != null) {
          partitions.add(createPartitionDesc(table, currentLocation, currentPart));
          currentLocation = null;
        }
        currentPart = getValidatedPartSpec(table, child, conf, true);
        validatePartitionValues(currentPart); // validate reserved values
        break;
      case HiveParser.TOK_PARTITIONLOCATION:
        // if location specified, set in partition
        if (isView) {
          throw new SemanticException("LOCATION clause illegal for view partition");
        }
        currentLocation = unescapeSQLString(child.getChild(0).getText());
        inputs.add(toReadEntity(currentLocation));
        break;
      default:
        throw new SemanticException("Unknown child: " + child);
      }
    }

    // add the last one
    if (currentPart != null) {
      partitions.add(createPartitionDesc(table, currentLocation, currentPart));
    }

    if (partitions.isEmpty()) {
      // nothing to do
      return;
    }

    AlterTableAddPartitionDesc addPartitionDesc = new AlterTableAddPartitionDesc(table.getDbName(),
        table.getTableName(), ifNotExists, partitions);

    Task<DDLWork> ddlTask =
        TaskFactory.get(new DDLWork(getInputs(), getOutputs(), addPartitionDesc));
    rootTasks.add(ddlTask);
    handleTransactionalTable(table, addPartitionDesc, ddlTask);

    if (isView) {
      // Compile internal query to capture underlying table partition dependencies
      StringBuilder cmd = new StringBuilder();
      cmd.append("SELECT * FROM ");
      cmd.append(HiveUtils.unparseIdentifier(tName.getDb()));
      cmd.append(".");
      cmd.append(HiveUtils.unparseIdentifier(tName.getTable()));
      cmd.append(" WHERE ");
      boolean firstOr = true;
      for (AlterTableAddPartitionDesc.PartitionDesc partitionDesc : partitions) {
        if (firstOr) {
          firstOr = false;
        } else {
          cmd.append(" OR ");
        }
        boolean firstAnd = true;
        cmd.append("(");
        for (Map.Entry<String, String> entry : partitionDesc.getPartSpec().entrySet()) {
          if (firstAnd) {
            firstAnd = false;
          } else {
            cmd.append(" AND ");
          }
          cmd.append(HiveUtils.unparseIdentifier(entry.getKey()));
          cmd.append(" = '");
          cmd.append(HiveUtils.escapeString(entry.getValue()));
          cmd.append("'");
        }
        cmd.append(")");
      }
      // FIXME: is it ok to have a completely new querystate?
      QueryState newQueryState = QueryState.getNewQueryState(conf, queryState.getLineageState());
      // FIXME: this driver instance is never closed
      Driver driver = new Driver(newQueryState);
      int rc = driver.compile(cmd.toString(), false);
      if (rc != 0) {
        throw new SemanticException(ErrorMsg.NO_VALID_PARTN.getMsg());
      }
      inputs.addAll(driver.getPlan().getInputs());
    }
  }

  private AlterTableAddPartitionDesc.PartitionDesc createPartitionDesc(Table table, String currentLocation,
      Map<String, String> currentPart) {
    Map<String, String> params = null;
    if (conf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER) && currentLocation == null) {
      params = new HashMap<String, String>();
      StatsSetupConst.setStatsStateForCreateTable(params,
          MetaStoreUtils.getColumnNames(table.getCols()), StatsSetupConst.TRUE);
    }
    return new AlterTableAddPartitionDesc.PartitionDesc(currentPart, currentLocation, params);
  }

  /**
   * Add partition for Transactional tables needs to add (copy/rename) the data so that it lands
   * in a delta_x_x/ folder in the partition dir.
   */
  private void handleTransactionalTable(Table tab, AlterTableAddPartitionDesc addPartitionDesc,
      Task ddlTask) throws SemanticException {
    if(!AcidUtils.isTransactionalTable(tab)) {
      return;
    }
    Long writeId = null;
    int stmtId = 0;

    for (AlterTableAddPartitionDesc.PartitionDesc partitonDesc : addPartitionDesc.getPartitions()) {
      if (partitonDesc.getLocation() != null) {
        AcidUtils.validateAcidPartitionLocation(partitonDesc.getLocation(), conf);
        if(addPartitionDesc.isIfNotExists()) {
          //Don't add partition data if it already exists
          Partition oldPart = getPartition(tab, partitonDesc.getPartSpec(), false);
          if(oldPart != null) {
            continue;
          }
        }
        if(writeId == null) {
          //so that we only allocate a writeId only if actually adding data
          // (vs. adding a partition w/o data)
          try {
            writeId = getTxnMgr().getTableWriteId(tab.getDbName(),
                tab.getTableName());
          } catch (LockException ex) {
            throw new SemanticException("Failed to allocate the write id", ex);
          }
          stmtId = getTxnMgr().getStmtIdAndIncrement();
        }
        LoadTableDesc loadTableWork = new LoadTableDesc(new Path(partitonDesc.getLocation()),
            Utilities.getTableDesc(tab), partitonDesc.getPartSpec(),
            LoadTableDesc.LoadFileType.KEEP_EXISTING, //not relevant - creating new partition
            writeId);
        loadTableWork.setStmtId(stmtId);
        loadTableWork.setInheritTableSpecs(true);
        try {
          partitonDesc.setLocation(new Path(tab.getDataLocation(),
              Warehouse.makePartPath(partitonDesc.getPartSpec())).toString());
        }
        catch (MetaException ex) {
          throw new SemanticException("Could not determine partition path due to: "
              + ex.getMessage(), ex);
        }
        Task<MoveWork> moveTask = TaskFactory.get(
            new MoveWork(getInputs(), getOutputs(), loadTableWork, null,
                true,//make sure to check format
                false));//is this right?
        ddlTask.addDependentTask(moveTask);
      }
    }
  }
  /**
   * Rewrite the metadata for one or more partitions in a table. Useful when
   * an external process modifies files on HDFS and you want the pre/post
   * hooks to be fired for the specified partition.
   *
   * @param ast
   *          The parsed command tree.
   * @throws SemanticException
   *           Parsing failed
   */
  private void analyzeAlterTableTouch(TableName tName, CommonTree ast) throws SemanticException {

    Table tab = getTable(tName);
    validateAlterTableType(tab, AlterTableType.TOUCH);
    inputs.add(new ReadEntity(tab));

    // partition name to value
    List<Map<String, String>> partSpecs = getPartitionSpecs(tab, ast);

    if (partSpecs.isEmpty()) {
      AlterTableTouchDesc touchDesc = new AlterTableTouchDesc(tName.getDbTable(), null);
      outputs.add(new WriteEntity(tab, WriteEntity.WriteType.DDL_NO_LOCK));
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), touchDesc)));
    } else {
      addTablePartsOutputs(tab, partSpecs, WriteEntity.WriteType.DDL_NO_LOCK);
      for (Map<String, String> partSpec : partSpecs) {
        AlterTableTouchDesc touchDesc = new AlterTableTouchDesc(tName.getDbTable(), partSpec);
        rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), touchDesc)));
      }
    }
  }

  private void analyzeAlterTableArchive(TableName tName, CommonTree ast, boolean isUnArchive) throws SemanticException {

    if (!conf.getBoolVar(HiveConf.ConfVars.HIVEARCHIVEENABLED)) {
      throw new SemanticException(ErrorMsg.ARCHIVE_METHODS_DISABLED.getMsg());

    }
    Table tab = getTable(tName);
    // partition name to value
    List<Map<String, String>> partSpecs = getPartitionSpecs(tab, ast);

    addTablePartsOutputs(tab, partSpecs, true, WriteEntity.WriteType.DDL_NO_LOCK);
    validateAlterTableType(tab, AlterTableType.ARCHIVE);
    inputs.add(new ReadEntity(tab));

    if (partSpecs.size() > 1) {
      throw new SemanticException(isUnArchive ?
          ErrorMsg.UNARCHIVE_ON_MULI_PARTS.getMsg() :
          ErrorMsg.ARCHIVE_ON_MULI_PARTS.getMsg());
    }
    if (partSpecs.size() == 0) {
      throw new SemanticException(ErrorMsg.ARCHIVE_ON_TABLE.getMsg());
    }

    Map<String, String> partSpec = partSpecs.get(0);
    try {
      isValidPrefixSpec(tab, partSpec);
    } catch (HiveException e) {
      throw new SemanticException(e.getMessage(), e);
    }
    DDLDesc archiveDesc = null;
    if (isUnArchive) {
      archiveDesc = new AlterTableUnarchiveDesc(tName.getDbTable(), partSpec);
    } else {
      archiveDesc = new AlterTableArchiveDesc(tName.getDbTable(), partSpec);
    }
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), archiveDesc)));
  }

  /**
   * Get the partition specs from the tree. This stores the full specification
   * with the comparator operator into the output list.
   *
   * @param ast Tree to extract partitions from.
   * @param tab Table.
   * @return    Map of partitions by prefix length. Most of the time prefix length will
   *            be the same for all partition specs, so we can just OR the expressions.
   */
  private Map<Integer, List<ExprNodeGenericFuncDesc>> getFullPartitionSpecs(
      CommonTree ast, Table tab, boolean canGroupExprs) throws SemanticException {
    String defaultPartitionName = HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULTPARTITIONNAME);
    Map<String, String> colTypes = new HashMap<String, String>();
    for (FieldSchema fs : tab.getPartitionKeys()) {
      colTypes.put(fs.getName().toLowerCase(), fs.getType());
    }

    Map<Integer, List<ExprNodeGenericFuncDesc>> result =
        new HashMap<Integer, List<ExprNodeGenericFuncDesc>>();
    for (int childIndex = 0; childIndex < ast.getChildCount(); childIndex++) {
      Tree partSpecTree = ast.getChild(childIndex);
      if (partSpecTree.getType() != HiveParser.TOK_PARTSPEC) {
        continue;
      }
      ExprNodeGenericFuncDesc expr = null;
      HashSet<String> names = new HashSet<String>(partSpecTree.getChildCount());
      for (int i = 0; i < partSpecTree.getChildCount(); ++i) {
        CommonTree partSpecSingleKey = (CommonTree) partSpecTree.getChild(i);
        assert (partSpecSingleKey.getType() == HiveParser.TOK_PARTVAL);
        String key = stripIdentifierQuotes(partSpecSingleKey.getChild(0).getText()).toLowerCase();
        String operator = partSpecSingleKey.getChild(1).getText();
        ASTNode partValNode = (ASTNode)partSpecSingleKey.getChild(2);
        TypeCheckCtx typeCheckCtx = new TypeCheckCtx(null);
        ExprNodeConstantDesc valExpr = (ExprNodeConstantDesc)TypeCheckProcFactory
            .genExprNode(partValNode, typeCheckCtx).get(partValNode);
        Object val = valExpr.getValue();

        boolean isDefaultPartitionName =  val.equals(defaultPartitionName);

        String type = colTypes.get(key);
        PrimitiveTypeInfo pti = TypeInfoFactory.getPrimitiveTypeInfo(type);
        if (type == null) {
          throw new SemanticException("Column " + key + " not found");
        }
        // Create the corresponding hive expression to filter on partition columns.
        if (!isDefaultPartitionName) {
          if (!valExpr.getTypeString().equals(type)) {
            Converter converter = ObjectInspectorConverters.getConverter(
              TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(valExpr.getTypeInfo()),
              TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(pti));
            val = converter.convert(valExpr.getValue());
          }
        }

        ExprNodeColumnDesc column = new ExprNodeColumnDesc(pti, key, null, true);
        ExprNodeGenericFuncDesc op;
        if (!isDefaultPartitionName) {
          op = makeBinaryPredicate(operator, column, new ExprNodeConstantDesc(pti, val));
        } else {
          GenericUDF originalOp = FunctionRegistry.getFunctionInfo(operator).getGenericUDF();
          String fnName;
          if (FunctionRegistry.isEq(originalOp)) {
            fnName = "isnull";
          } else if (FunctionRegistry.isNeq(originalOp)) {
            fnName = "isnotnull";
          } else {
            throw new SemanticException("Cannot use " + operator
                + " in a default partition spec; only '=' and '!=' are allowed.");
          }
          op = makeUnaryPredicate(fnName, column);
        }
        // If it's multi-expr filter (e.g. a='5', b='2012-01-02'), AND with previous exprs.
        expr = (expr == null) ? op : makeBinaryPredicate("and", expr, op);
        names.add(key);
      }
      if (expr == null) {
        continue;
      }
      // We got the expr for one full partition spec. Determine the prefix length.
      int prefixLength = calculatePartPrefix(tab, names);
      List<ExprNodeGenericFuncDesc> orExpr = result.get(prefixLength);
      // We have to tell apart partitions resulting from spec with different prefix lengths.
      // So, if we already have smth for the same prefix length, we can OR the two.
      // If we don't, create a new separate filter. In most cases there will only be one.
      if (orExpr == null) {
        result.put(prefixLength, Lists.newArrayList(expr));
      } else if (canGroupExprs) {
        orExpr.set(0, makeBinaryPredicate("or", expr, orExpr.get(0)));
      } else {
        orExpr.add(expr);
      }
    }
    return result;
  }

  public static ExprNodeGenericFuncDesc makeBinaryPredicate(
      String fn, ExprNodeDesc left, ExprNodeDesc right) throws SemanticException {
      return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
          FunctionRegistry.getFunctionInfo(fn).getGenericUDF(), Lists.newArrayList(left, right));
  }
  public static ExprNodeGenericFuncDesc makeUnaryPredicate(
      String fn, ExprNodeDesc arg) throws SemanticException {
      return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
          FunctionRegistry.getFunctionInfo(fn).getGenericUDF(), Lists.newArrayList(arg));
  }
  /**
   * Calculates the partition prefix length based on the drop spec.
   * This is used to avoid deleting archived partitions with lower level.
   * For example, if, for A and B key cols, drop spec is A=5, B=6, we shouldn't drop
   * archived A=5/, because it can contain B-s other than 6.
   * @param tbl Table
   * @param partSpecKeys Keys present in drop partition spec.
   */
  private int calculatePartPrefix(Table tbl, HashSet<String> partSpecKeys) {
    int partPrefixToDrop = 0;
    for (FieldSchema fs : tbl.getPartCols()) {
      if (!partSpecKeys.contains(fs.getName())) {
        break;
      }
      ++partPrefixToDrop;
    }
    return partPrefixToDrop;
  }

  /**
   * Certain partition values are are used by hive. e.g. the default partition
   * in dynamic partitioning and the intermediate partition values used in the
   * archiving process. Naturally, prohibit the user from creating partitions
   * with these reserved values. The check that this function is more
   * restrictive than the actual limitation, but it's simpler. Should be okay
   * since the reserved names are fairly long and uncommon.
   */
  private void validatePartitionValues(Map<String, String> partSpec)
      throws SemanticException {

    for (Entry<String, String> e : partSpec.entrySet()) {
      for (String s : reservedPartitionValues) {
        String value = e.getValue();
        if (value != null && value.contains(s)) {
          throw new SemanticException(ErrorMsg.RESERVED_PART_VAL.getMsg(
              "(User value: " + e.getValue() + " Reserved substring: " + s + ")"));
        }
      }
    }
  }

  /**
   * Add the table partitions to be modified in the output, so that it is available for the
   * pre-execution hook. If the partition does not exist, no error is thrown.
   */
  private void addTablePartsOutputs(Table table, List<Map<String, String>> partSpecs,
                                    WriteEntity.WriteType writeType)
      throws SemanticException {
    addTablePartsOutputs(table, partSpecs, false, false, null, writeType);
  }

  /**
   * Add the table partitions to be modified in the output, so that it is available for the
   * pre-execution hook. If the partition does not exist, no error is thrown.
   */
  private void addTablePartsOutputs(Table table, List<Map<String, String>> partSpecs,
      boolean allowMany, WriteEntity.WriteType writeType)
      throws SemanticException {
    addTablePartsOutputs(table, partSpecs, false, allowMany, null, writeType);
  }

  /**
   * Add the table partitions to be modified in the output, so that it is available for the
   * pre-execution hook. If the partition does not exist, throw an error if
   * throwIfNonExistent is true, otherwise ignore it.
   */
  private void addTablePartsOutputs(Table table, List<Map<String, String>> partSpecs,
      boolean throwIfNonExistent, boolean allowMany, ASTNode ast, WriteEntity.WriteType writeType)
      throws SemanticException {

    Iterator<Map<String, String>> i;
    int index;
    for (i = partSpecs.iterator(), index = 1; i.hasNext(); ++index) {
      Map<String, String> partSpec = i.next();
      List<Partition> parts = null;
      if (allowMany) {
        try {
          parts = db.getPartitions(table, partSpec);
        } catch (HiveException e) {
          LOG.error("Got HiveException during obtaining list of partitions"
              + StringUtils.stringifyException(e));
          throw new SemanticException(e.getMessage(), e);
        }
      } else {
        parts = new ArrayList<Partition>();
        try {
          Partition p = db.getPartition(table, partSpec, false);
          if (p != null) {
            parts.add(p);
          }
        } catch (HiveException e) {
          LOG.debug("Wrong specification" + StringUtils.stringifyException(e));
          throw new SemanticException(e.getMessage(), e);
        }
      }
      if (parts.isEmpty()) {
        if (throwIfNonExistent) {
          throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(ast.getChild(index)));
        }
      }
      for (Partition p : parts) {
        // Don't request any locks here, as the table has already been locked.
        outputs.add(new WriteEntity(p, writeType));
      }
    }
  }

  /**
   * Add the table partitions to be modified in the output, so that it is available for the
   * pre-execution hook. If the partition does not exist, throw an error if
   * throwIfNonExistent is true, otherwise ignore it.
   */
  private void addTableDropPartsOutputs(Table tab,
                                        Collection<List<ExprNodeGenericFuncDesc>> partSpecs,
                                        boolean throwIfNonExistent) throws SemanticException {
    for (List<ExprNodeGenericFuncDesc> specs : partSpecs) {
      for (ExprNodeGenericFuncDesc partSpec : specs) {
        List<Partition> parts = new ArrayList<Partition>();
        boolean hasUnknown = false;
        try {
          hasUnknown = db.getPartitionsByExpr(tab, partSpec, conf, parts);
        } catch (Exception e) {
          throw new SemanticException(
              ErrorMsg.INVALID_PARTITION.getMsg(partSpec.getExprString()), e);
        }
        if (hasUnknown) {
          throw new SemanticException(
              "Unexpected unknown partitions for " + partSpec.getExprString());
        }

        // TODO: ifExists could be moved to metastore. In fact it already supports that. Check it
        //       for now since we get parts for output anyway, so we can get the error message
        //       earlier... If we get rid of output, we can get rid of this.
        if (parts.isEmpty()) {
          if (throwIfNonExistent) {
            throw new SemanticException(
                ErrorMsg.INVALID_PARTITION.getMsg(partSpec.getExprString()));
          }
        }
        for (Partition p : parts) {
          outputs.add(new WriteEntity(p, WriteEntity.WriteType.DDL_EXCLUSIVE));
        }
      }
    }
  }

  /**
   * Analyze alter table's skewed table
   *
   * @param ast
   *          node
   * @throws SemanticException
   */
  private void analyzeAlterTableSkewedby(TableName tName, ASTNode ast) throws SemanticException {
    /**
     * Throw an error if the user tries to use the DDL with
     * hive.internal.ddl.list.bucketing.enable set to false.
     */
    SessionState.get().getConf();

    Table tab = getTable(tName);

    inputs.add(new ReadEntity(tab));
    outputs.add(new WriteEntity(tab, WriteEntity.WriteType.DDL_EXCLUSIVE));

    validateAlterTableType(tab, AlterTableType.SKEWED_BY);

    if (ast.getChildCount() == 0) {
      /* Convert a skewed table to non-skewed table. */
      AlterTableNotSkewedDesc alterTblDesc = new AlterTableNotSkewedDesc(tName);
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblDesc)));
    } else {
      switch (((ASTNode) ast.getChild(0)).getToken().getType()) {
      case HiveParser.TOK_TABLESKEWED:
        handleAlterTableSkewedBy(ast, tName, tab);
        break;
      case HiveParser.TOK_STOREDASDIRS:
        handleAlterTableDisableStoredAsDirs(tName, tab);
        break;
      default:
        assert false;
      }
    }
  }

  /**
   * Handle alter table <name> not stored as directories
   *
   * @param tableName
   * @param tab
   * @throws SemanticException
   */
  private void handleAlterTableDisableStoredAsDirs(TableName tableName, Table tab)
      throws SemanticException {
    List<String> skewedColNames = tab.getSkewedColNames();
    List<List<String>> skewedColValues = tab.getSkewedColValues();
    if (CollectionUtils.isEmpty(skewedColNames) || CollectionUtils.isEmpty(skewedColValues)) {
      throw new SemanticException(ErrorMsg.ALTER_TBL_STOREDASDIR_NOT_SKEWED.getMsg(tableName.getNotEmptyDbTable()));
    }

    AlterTableSkewedByDesc alterTblDesc = new AlterTableSkewedByDesc(tableName, skewedColNames, skewedColValues, false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblDesc)));
  }

  /**
   * Process "alter table <name> skewed by .. on .. stored as directories
   * @param ast
   * @param tableName
   * @param tab
   * @throws SemanticException
   */
  private void handleAlterTableSkewedBy(ASTNode ast, TableName tableName, Table tab) throws SemanticException {
    List<String> skewedColNames = new ArrayList<String>();
    List<List<String>> skewedValues = new ArrayList<List<String>>();
    /* skewed column names. */
    ASTNode skewedNode = (ASTNode) ast.getChild(0);
    skewedColNames = analyzeSkewedTablDDLColNames(skewedColNames, skewedNode);
    /* skewed value. */
    analyzeDDLSkewedValues(skewedValues, skewedNode);
    // stored as directories
    boolean storedAsDirs = analyzeStoredAdDirs(skewedNode);

    if (tab != null) {
      /* Validate skewed information. */
      ValidationUtility.validateSkewedInformation(
          ParseUtils.validateColumnNameUniqueness(tab.getCols()), skewedColNames, skewedValues);
    }

    AlterTableSkewedByDesc alterTblDesc = new AlterTableSkewedByDesc(tableName, skewedColNames, skewedValues,
        storedAsDirs);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblDesc)));
  }

  /**
   * Analyze alter table's skewed location
   *
   * @param ast
   * @param tableName
   * @param partSpec
   * @throws SemanticException
   */
  private void analyzeAlterTableSkewedLocation(ASTNode ast, TableName tableName,
      Map<String, String> partSpec) throws SemanticException {
    /**
     * Throw an error if the user tries to use the DDL with
     * hive.internal.ddl.list.bucketing.enable set to false.
     */
    SessionState.get().getConf();
    /**
     * Retrieve mappings from parser
     */
    Map<List<String>, String> locations = new HashMap<List<String>, String>();
    ArrayList<Node> locNodes = ast.getChildren();
    if (null == locNodes) {
      throw new SemanticException(ErrorMsg.ALTER_TBL_SKEWED_LOC_NO_LOC.getMsg());
    } else {
      for (Node locNode : locNodes) {
        // TOK_SKEWED_LOCATIONS
        ASTNode locAstNode = (ASTNode) locNode;
        ArrayList<Node> locListNodes = locAstNode.getChildren();
        if (null == locListNodes) {
          throw new SemanticException(ErrorMsg.ALTER_TBL_SKEWED_LOC_NO_LOC.getMsg());
        } else {
          for (Node locListNode : locListNodes) {
            // TOK_SKEWED_LOCATION_LIST
            ASTNode locListAstNode = (ASTNode) locListNode;
            ArrayList<Node> locMapNodes = locListAstNode.getChildren();
            if (null == locMapNodes) {
              throw new SemanticException(ErrorMsg.ALTER_TBL_SKEWED_LOC_NO_LOC.getMsg());
            } else {
              for (Node locMapNode : locMapNodes) {
                // TOK_SKEWED_LOCATION_MAP
                ASTNode locMapAstNode = (ASTNode) locMapNode;
                ArrayList<Node> locMapAstNodeMaps = locMapAstNode.getChildren();
                if ((null == locMapAstNodeMaps) || (locMapAstNodeMaps.size() != 2)) {
                  throw new SemanticException(ErrorMsg.ALTER_TBL_SKEWED_LOC_NO_MAP.getMsg());
                } else {
                  List<String> keyList = new LinkedList<String>();
                  ASTNode node = (ASTNode) locMapAstNodeMaps.get(0);
                  if (node.getToken().getType() == HiveParser.TOK_TABCOLVALUES) {
                    keyList = getSkewedValuesFromASTNode(node);
                  } else if (isConstant(node)) {
                    keyList.add(PlanUtils
                        .stripQuotes(node.getText()));
                  } else {
                    throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
                  }
                  String newLocation = PlanUtils
                      .stripQuotes(unescapeSQLString(((ASTNode) locMapAstNodeMaps.get(1))
                          .getText()));
                  validateSkewedLocationString(newLocation);
                  locations.put(keyList, newLocation);
                  addLocationToOutputs(newLocation);
                }
              }
            }
          }
        }
      }
    }
    AlterTableSetSkewedLocationDesc alterTblDesc = new AlterTableSetSkewedLocationDesc(tableName, partSpec, locations);
    addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc, AlterTableType.SET_SKEWED_LOCATION, false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblDesc)));
  }

  private void addLocationToOutputs(String newLocation) throws SemanticException {
    outputs.add(toWriteEntity(newLocation));
  }

  /**
   * Check if the node is constant.
   *
   * @param node
   * @return
   */
  private boolean isConstant(ASTNode node) {
    switch(node.getToken().getType()) {
      case HiveParser.Number:
      case HiveParser.StringLiteral:
      case HiveParser.IntegralLiteral:
      case HiveParser.NumberLiteral:
      case HiveParser.CharSetName:
      case HiveParser.KW_TRUE:
      case HiveParser.KW_FALSE:
        return true;
      default:
        return false;
    }
  }

  private void validateSkewedLocationString(String newLocation) throws SemanticException {
    /* Validate location string. */
    try {
      URI locUri = new URI(newLocation);
      if (!locUri.isAbsolute() || locUri.getScheme() == null
          || locUri.getScheme().trim().equals("")) {
        throw new SemanticException(
            newLocation
                + " is not absolute or has no scheme information. "
                + "Please specify a complete absolute uri with scheme information.");
      }
    } catch (URISyntaxException e) {
      throw new SemanticException(e);
    }
  }
}
