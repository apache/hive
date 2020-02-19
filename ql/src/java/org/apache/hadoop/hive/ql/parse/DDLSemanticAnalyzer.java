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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.table.misc.AlterTableRenameDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.AlterTableSetOwnerDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.AlterTableSetPropertiesDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.AlterTableTouchDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.AlterTableUnsetPropertiesDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.TruncateTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils;
import org.apache.hadoop.hive.ql.exec.ColumnStatsUpdateTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.metadata.DefaultConstraint;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.NotNullConstraint;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.authorization.AuthorizationParseUtils;
import org.apache.hadoop.hive.ql.plan.BasicStatsWork;
import org.apache.hadoop.hive.ql.plan.ColumnStatsUpdateWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.mapred.InputFormat;

/**
 * DDLSemanticAnalyzer.
 *
 */
public class DDLSemanticAnalyzer extends BaseSemanticAnalyzer {
  private static final Map<Integer, String> TokenToTypeName = new HashMap<Integer, String>();

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
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_PROPERTIES) {
        analyzeAlterTableProps(tName, null, ast, false, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_DROPPROPERTIES) {
        analyzeAlterTableProps(tName, null, ast, false, true);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_UPDATESTATS ||
          ast.getType() == HiveParser.TOK_ALTERPARTITION_UPDATESTATS) {
        analyzeAlterTableProps(tName, partSpec, ast, false, false);
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
    case HiveParser.TOK_ALTERVIEW: {
      final TableName tName = getQualifiedTableName((ASTNode) ast.getChild(0));
      ast = (ASTNode) ast.getChild(1);
      if (ast.getType() == HiveParser.TOK_ALTERVIEW_PROPERTIES) {
        analyzeAlterTableProps(tName, null, ast, true, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERVIEW_DROPPROPERTIES) {
        analyzeAlterTableProps(tName, null, ast, true, true);
      } else if (ast.getType() == HiveParser.TOK_ALTERVIEW_RENAME) {
        analyzeAlterTableRename(tName, ast, true);
      }
      break;
    }
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
        for (Partition partition : PartitionUtils.getPartitions(db, table, null, false)) {
          outputs.add(new WriteEntity(partition, WriteEntity.WriteType.DDL_EXCLUSIVE));
        }
      }
    } else {
      if (isFullSpec(table, partSpec)) {
        validatePartSpec(table, partSpec, (ASTNode) root.getChild(1), conf, true);
        Partition partition = PartitionUtils.getPartition(db, table, partSpec, true);
        outputs.add(new WriteEntity(partition, WriteEntity.WriteType.DDL_EXCLUSIVE));
      } else {
        validatePartSpec(table, partSpec, (ASTNode) root.getChild(1), conf, false);
        for (Partition partition : PartitionUtils.getPartitions(db, table, partSpec, false)) {
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
      throw new SemanticException(ErrorMsg.ALTER_TABLE_NON_NATIVE.format(
          AlterTableType.NON_NATIVE_TABLE_ALLOWED.toString(), tbl.getTableName()));
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
        for (Partition part : PartitionUtils.getPartitions(db, tab, partSpec, false)) {
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
        Partition part = PartitionUtils.getPartition(db, tab, partSpec, true);
        outputs.add(new WriteEntity(part, writeType));
      } else {
        // Partial partition spec supplied. Make sure this is allowed.
        if (!AlterTableType.SUPPORT_PARTIAL_PARTITION_SPEC.contains(op)) {
          throw new SemanticException(
              ErrorMsg.ALTER_TABLE_TYPE_PARTIAL_PARTITION_SPEC_NO_SUPPORTED, op.getName());
        } else if (!conf.getBoolVar(HiveConf.ConfVars.DYNAMICPARTITIONING)) {
          throw new SemanticException(ErrorMsg.DYNAMIC_PARTITION_DISABLED);
        }

        for (Partition part : PartitionUtils.getPartitions(db, tab, partSpec, true)) {
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

  /**
   * Utility class to resolve QualifiedName
   */
  static class QualifiedNameUtil {

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
      PartitionUtils.addTablePartsOutputs(db, outputs, tab, partSpecs, false, WriteEntity.WriteType.DDL_NO_LOCK);
      for (Map<String, String> partSpec : partSpecs) {
        AlterTableTouchDesc touchDesc = new AlterTableTouchDesc(tName.getDbTable(), partSpec);
        rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), touchDesc)));
      }
    }
  }
}
