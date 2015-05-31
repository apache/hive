/**
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

import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_DATABASELOCATION;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_DATABASEPROPERTIES;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils;
import org.apache.hadoop.hive.ql.exec.ColumnStatsUpdateTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.index.HiveIndex;
import org.apache.hadoop.hive.ql.index.HiveIndex.IndexType;
import org.apache.hadoop.hive.ql.index.HiveIndexHandler;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.lockmgr.TxnManagerFactory;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.authorization.AuthorizationParseUtils;
import org.apache.hadoop.hive.ql.parse.authorization.HiveAuthorizationTaskFactory;
import org.apache.hadoop.hive.ql.parse.authorization.HiveAuthorizationTaskFactoryImpl;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.AlterDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.AlterIndexDesc;
import org.apache.hadoop.hive.ql.plan.AlterIndexDesc.AlterIndexTypes;
import org.apache.hadoop.hive.ql.plan.AlterTableAlterPartDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes;
import org.apache.hadoop.hive.ql.plan.AlterTableExchangePartition;
import org.apache.hadoop.hive.ql.plan.AlterTableSimpleDesc;
import org.apache.hadoop.hive.ql.plan.ColumnStatsDesc;
import org.apache.hadoop.hive.ql.plan.ColumnStatsUpdateWork;
import org.apache.hadoop.hive.ql.plan.CreateDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.CreateIndexDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DescDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DescFunctionDesc;
import org.apache.hadoop.hive.ql.plan.DescTableDesc;
import org.apache.hadoop.hive.ql.plan.DropDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DropIndexDesc;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LockDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.LockTableDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.MsckDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.RenamePartitionDesc;
import org.apache.hadoop.hive.ql.plan.RoleDDLDesc;
import org.apache.hadoop.hive.ql.plan.ShowColumnsDesc;
import org.apache.hadoop.hive.ql.plan.ShowCompactionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowConfDesc;
import org.apache.hadoop.hive.ql.plan.ShowCreateTableDesc;
import org.apache.hadoop.hive.ql.plan.ShowDatabasesDesc;
import org.apache.hadoop.hive.ql.plan.ShowFunctionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowGrantDesc;
import org.apache.hadoop.hive.ql.plan.ShowIndexesDesc;
import org.apache.hadoop.hive.ql.plan.ShowLocksDesc;
import org.apache.hadoop.hive.ql.plan.ShowPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowTableStatusDesc;
import org.apache.hadoop.hive.ql.plan.ShowTablesDesc;
import org.apache.hadoop.hive.ql.plan.ShowTblPropertiesDesc;
import org.apache.hadoop.hive.ql.plan.ShowTxnsDesc;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.SwitchDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TruncateTableDesc;
import org.apache.hadoop.hive.ql.plan.UnlockDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.UnlockTableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Lists;

/**
 * DDLSemanticAnalyzer.
 *
 */
public class DDLSemanticAnalyzer extends BaseSemanticAnalyzer {
  private static final Log LOG = LogFactory.getLog(DDLSemanticAnalyzer.class);
  private static final Map<Integer, String> TokenToTypeName = new HashMap<Integer, String>();

  private final Set<String> reservedPartitionValues;
  private final HiveAuthorizationTaskFactory hiveAuthorizationTaskFactory;

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
    case HiveParser.TOK_DECIMAL:
        DecimalTypeInfo decTypeInfo = ParseUtils.getDecimalTypeTypeInfo(node);
        typeName = decTypeInfo.getQualifiedName();
        break;
    default:
      typeName = TokenToTypeName.get(token);
    }
    return typeName;
  }

  public DDLSemanticAnalyzer(HiveConf conf) throws SemanticException {
    this(conf, createHiveDB(conf));
  }

  public DDLSemanticAnalyzer(HiveConf conf, Hive db) throws SemanticException {
    super(conf, db);
    reservedPartitionValues = new HashSet<String>();
    // Partition can't have this name
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.DEFAULTPARTITIONNAME));
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.DEFAULT_ZOOKEEPER_PARTITION_NAME));
    // Partition value can't end in this suffix
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.METASTORE_INT_ORIGINAL));
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.METASTORE_INT_ARCHIVED));
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.METASTORE_INT_EXTRACTED));
    hiveAuthorizationTaskFactory = createAuthorizationTaskFactory(conf, db);
  }

  @Override
  public void analyzeInternal(ASTNode input) throws SemanticException {

    ASTNode ast = input;
    switch (ast.getType()) {
    case HiveParser.TOK_ALTERTABLE: {
      ast = (ASTNode) input.getChild(1);
      String[] qualified = getQualifiedTableName((ASTNode) input.getChild(0));
      String tableName = getDotName(qualified);
      HashMap<String, String> partSpec = null;
      ASTNode partSpecNode = (ASTNode)input.getChild(2);
      if (partSpecNode != null) {
        //  We can use alter table partition rename to convert/normalize the legacy partition
        //  column values. In so, we should not enable the validation to the old partition spec
        //  passed in this command.
        if (ast.getType() == HiveParser.TOK_ALTERTABLE_RENAMEPART) {
          partSpec = getPartSpec(partSpecNode);
        } else {
          partSpec = getValidatedPartSpec(getTable(tableName), partSpecNode, conf, false);
        }
      }

      if (ast.getType() == HiveParser.TOK_ALTERTABLE_RENAME) {
        analyzeAlterTableRename(qualified, ast, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_TOUCH) {
        analyzeAlterTableTouch(qualified, ast);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_ARCHIVE) {
        analyzeAlterTableArchive(qualified, ast, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_UNARCHIVE) {
        analyzeAlterTableArchive(qualified, ast, true);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_ADDCOLS) {
        analyzeAlterTableModifyCols(qualified, ast, partSpec, AlterTableTypes.ADDCOLS);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_REPLACECOLS) {
        analyzeAlterTableModifyCols(qualified, ast, partSpec, AlterTableTypes.REPLACECOLS);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_RENAMECOL) {
        analyzeAlterTableRenameCol(qualified, ast, partSpec);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_ADDPARTS) {
        analyzeAlterTableAddParts(qualified, ast, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_DROPPARTS) {
        analyzeAlterTableDropParts(qualified, ast, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_PARTCOLTYPE) {
        analyzeAlterTablePartColType(qualified, ast);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_PROPERTIES) {
        analyzeAlterTableProps(qualified, ast, false, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_DROPPROPERTIES) {
        analyzeAlterTableProps(qualified, ast, false, true);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_SKEWED) {
        analyzeAltertableSkewedby(qualified, ast);
      } else if (ast.getType() == HiveParser.TOK_ALTERTABLE_EXCHANGEPARTITION) {
        analyzeExchangePartition(qualified, ast);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_FILEFORMAT) {
        analyzeAlterTableFileFormat(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_PROTECTMODE) {
        analyzeAlterTableProtectMode(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_LOCATION) {
        analyzeAlterTableLocation(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_MERGEFILES) {
        analyzeAlterTablePartMergeFiles(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SERIALIZER) {
        analyzeAlterTableSerde(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES) {
        analyzeAlterTableSerdeProps(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_RENAMEPART) {
        analyzeAlterTableRenamePart(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SKEWED_LOCATION) {
        analyzeAlterTableSkewedLocation(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_BUCKETS) {
        analyzeAlterTableBucketNum(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_CLUSTER_SORT) {
        analyzeAlterTableClusterSort(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_COMPACT) {
        analyzeAlterTableCompact(ast, tableName, partSpec);
      } else if(ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_UPDATECOLSTATS){
        analyzeAlterTableUpdateStats(ast, tableName, partSpec);
      }
      break;
    }
    case HiveParser.TOK_DROPTABLE:
      analyzeDropTable(ast, false);
      break;
    case HiveParser.TOK_TRUNCATETABLE:
      analyzeTruncateTable(ast);
      break;
    case HiveParser.TOK_CREATEINDEX:
      analyzeCreateIndex(ast);
      break;
    case HiveParser.TOK_DROPINDEX:
      analyzeDropIndex(ast);
      break;
    case HiveParser.TOK_DESCTABLE:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeDescribeTable(ast);
      break;
    case HiveParser.TOK_SHOWDATABASES:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowDatabases(ast);
      break;
    case HiveParser.TOK_SHOWTABLES:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowTables(ast);
      break;
    case HiveParser.TOK_SHOWCOLUMNS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowColumns(ast);
      break;
    case HiveParser.TOK_SHOW_TABLESTATUS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowTableStatus(ast);
      break;
    case HiveParser.TOK_SHOW_TBLPROPERTIES:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowTableProperties(ast);
      break;
    case HiveParser.TOK_SHOWFUNCTIONS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowFunctions(ast);
      break;
    case HiveParser.TOK_SHOWLOCKS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowLocks(ast);
      break;
    case HiveParser.TOK_SHOWDBLOCKS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowDbLocks(ast);
      break;
    case HiveParser.TOK_SHOW_COMPACTIONS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowCompactions(ast);
      break;
    case HiveParser.TOK_SHOW_TRANSACTIONS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowTxns(ast);
      break;
    case HiveParser.TOK_SHOWCONF:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowConf(ast);
      break;
    case HiveParser.TOK_DESCFUNCTION:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeDescFunction(ast);
      break;
    case HiveParser.TOK_DESCDATABASE:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeDescDatabase(ast);
      break;
    case HiveParser.TOK_MSCK:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeMetastoreCheck(ast);
      break;
    case HiveParser.TOK_DROPVIEW:
      analyzeDropTable(ast, true);
      break;
    case HiveParser.TOK_ALTERVIEW: {
      String[] qualified = getQualifiedTableName((ASTNode) ast.getChild(0));
      ast = (ASTNode) ast.getChild(1);
      if (ast.getType() == HiveParser.TOK_ALTERVIEW_PROPERTIES) {
        analyzeAlterTableProps(qualified, ast, true, false);
      } else if (ast.getType() == HiveParser.TOK_ALTERVIEW_DROPPROPERTIES) {
        analyzeAlterTableProps(qualified, ast, true, true);
      } else if (ast.getType() == HiveParser.TOK_ALTERVIEW_ADDPARTS) {
        analyzeAlterTableAddParts(qualified, ast, true);
      } else if (ast.getType() == HiveParser.TOK_ALTERVIEW_DROPPARTS) {
        analyzeAlterTableDropParts(qualified, ast, true);
      } else if (ast.getType() == HiveParser.TOK_ALTERVIEW_RENAME) {
        analyzeAlterTableRename(qualified, ast, true);
      }
      break;
    }
    case HiveParser.TOK_ALTERINDEX_REBUILD:
      analyzeAlterIndexRebuild(ast);
      break;
    case HiveParser.TOK_ALTERINDEX_PROPERTIES:
      analyzeAlterIndexProps(ast);
      break;
    case HiveParser.TOK_SHOWPARTITIONS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowPartitions(ast);
      break;
    case HiveParser.TOK_SHOW_CREATETABLE:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowCreateTable(ast);
      break;
    case HiveParser.TOK_SHOWINDEXES:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowIndexes(ast);
      break;
    case HiveParser.TOK_LOCKTABLE:
      analyzeLockTable(ast);
      break;
    case HiveParser.TOK_UNLOCKTABLE:
      analyzeUnlockTable(ast);
      break;
    case HiveParser.TOK_LOCKDB:
      analyzeLockDatabase(ast);
      break;
    case HiveParser.TOK_UNLOCKDB:
      analyzeUnlockDatabase(ast);
      break;
    case HiveParser.TOK_CREATEDATABASE:
      analyzeCreateDatabase(ast);
      break;
    case HiveParser.TOK_DROPDATABASE:
      analyzeDropDatabase(ast);
      break;
    case HiveParser.TOK_SWITCHDATABASE:
      analyzeSwitchDatabase(ast);
      break;
    case HiveParser.TOK_ALTERDATABASE_PROPERTIES:
      analyzeAlterDatabaseProperties(ast);
      break;
    case HiveParser.TOK_ALTERDATABASE_OWNER:
      analyzeAlterDatabaseOwner(ast);
      break;
    case HiveParser.TOK_CREATEROLE:
      analyzeCreateRole(ast);
      break;
    case HiveParser.TOK_DROPROLE:
      analyzeDropRole(ast);
      break;
    case HiveParser.TOK_SHOW_ROLE_GRANT:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowRoleGrant(ast);
      break;
    case HiveParser.TOK_SHOW_ROLE_PRINCIPALS:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowRolePrincipals(ast);
      break;
    case HiveParser.TOK_SHOW_ROLES:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowRoles(ast);
      break;
    case HiveParser.TOK_GRANT_ROLE:
      analyzeGrantRevokeRole(true, ast);
      break;
    case HiveParser.TOK_REVOKE_ROLE:
      analyzeGrantRevokeRole(false, ast);
      break;
    case HiveParser.TOK_GRANT:
      analyzeGrant(ast);
      break;
    case HiveParser.TOK_SHOW_GRANT:
      ctx.setResFile(ctx.getLocalTmpPath());
      analyzeShowGrant(ast);
      break;
    case HiveParser.TOK_REVOKE:
      analyzeRevoke(ast);
      break;
   case HiveParser.TOK_SHOW_SET_ROLE:
     analyzeSetShowRole(ast);
     break;
    default:
      throw new SemanticException("Unsupported command.");
    }
    if (fetchTask != null && !rootTasks.isEmpty()) {
      rootTasks.get(rootTasks.size() - 1).setFetchSource(true);
    }
  }

  private void analyzeAlterTableUpdateStats(ASTNode ast, String tblName, Map<String, String> partSpec)
      throws SemanticException {
    String colName = getUnescapedName((ASTNode) ast.getChild(0));
    Map<String, String> mapProp = getProps((ASTNode) (ast.getChild(1)).getChild(0));

    Table tbl = getTable(tblName);
    String partName = null;
    if (partSpec != null) {
      try {
        partName = Warehouse.makePartName(partSpec, false);
      } catch (MetaException e) {
        // TODO Auto-generated catch block
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

    if (colType == null)
      throw new SemanticException("column type not found");

    ColumnStatsDesc cStatsDesc = new ColumnStatsDesc(tbl.getDbName() + "." + tbl.getTableName(),
        Arrays.asList(colName), Arrays.asList(colType), partSpec == null);
    ColumnStatsUpdateTask cStatsUpdateTask = (ColumnStatsUpdateTask) TaskFactory
        .get(new ColumnStatsUpdateWork(cStatsDesc, partName, mapProp), conf);
    rootTasks.add(cStatsUpdateTask);
  }

  private void analyzeSetShowRole(ASTNode ast) throws SemanticException {
    switch (ast.getChildCount()) {
      case 0:
        ctx.setResFile(ctx.getLocalTmpPath());
        rootTasks.add(hiveAuthorizationTaskFactory.createShowCurrentRoleTask(
        getInputs(), getOutputs(), ctx.getResFile()));
        setFetchTask(createFetchTask(RoleDDLDesc.getRoleNameSchema()));
        break;
      case 1:
        rootTasks.add(hiveAuthorizationTaskFactory.createSetRoleTask(
        BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText()),
        getInputs(), getOutputs()));
        break;
      default:
        throw new SemanticException("Internal error. ASTNode expected to have 0 or 1 child. "
        + ast.dump());
    }
  }

  private void analyzeGrantRevokeRole(boolean grant, ASTNode ast) throws SemanticException {
    Task<? extends Serializable> task;
    if(grant) {
      task = hiveAuthorizationTaskFactory.createGrantRoleTask(ast, getInputs(), getOutputs());
    } else {
      task = hiveAuthorizationTaskFactory.createRevokeRoleTask(ast, getInputs(), getOutputs());
    }
    if(task != null) {
      rootTasks.add(task);
    }
  }

  private void analyzeShowGrant(ASTNode ast) throws SemanticException {
    Task<? extends Serializable> task = hiveAuthorizationTaskFactory.
        createShowGrantTask(ast, ctx.getResFile(), getInputs(), getOutputs());
    if(task != null) {
      rootTasks.add(task);
      setFetchTask(createFetchTask(ShowGrantDesc.getSchema()));
    }
  }

  private void analyzeGrant(ASTNode ast) throws SemanticException {
    Task<? extends Serializable> task = hiveAuthorizationTaskFactory.
        createGrantTask(ast, getInputs(), getOutputs());
    if(task != null) {
      rootTasks.add(task);
    }
  }

  private void analyzeRevoke(ASTNode ast) throws SemanticException {
    Task<? extends Serializable> task = hiveAuthorizationTaskFactory.
        createRevokeTask(ast, getInputs(), getOutputs());
    if(task != null) {
      rootTasks.add(task);
    }
  }

  private void analyzeCreateRole(ASTNode ast) throws SemanticException {
    Task<? extends Serializable> task = hiveAuthorizationTaskFactory.
        createCreateRoleTask(ast, getInputs(), getOutputs());
    if(task != null) {
      rootTasks.add(task);
    }
  }

  private void analyzeDropRole(ASTNode ast) throws SemanticException {
    Task<? extends Serializable> task = hiveAuthorizationTaskFactory.
        createDropRoleTask(ast, getInputs(), getOutputs());
    if(task != null) {
      rootTasks.add(task);
    }
  }

  private void analyzeShowRoleGrant(ASTNode ast) throws SemanticException {
    Task<? extends Serializable> task = hiveAuthorizationTaskFactory.
        createShowRoleGrantTask(ast, ctx.getResFile(), getInputs(), getOutputs());
    if(task != null) {
      rootTasks.add(task);
      setFetchTask(createFetchTask(RoleDDLDesc.getRoleShowGrantSchema()));
    }
  }

  private void analyzeShowRolePrincipals(ASTNode ast) throws SemanticException {
    Task<DDLWork> roleDDLTask = (Task<DDLWork>) hiveAuthorizationTaskFactory
        .createShowRolePrincipalsTask(ast, ctx.getResFile(), getInputs(), getOutputs());

    if (roleDDLTask != null) {
      rootTasks.add(roleDDLTask);
      setFetchTask(createFetchTask(RoleDDLDesc.getShowRolePrincipalsSchema()));
    }
  }

  private void analyzeShowRoles(ASTNode ast) throws SemanticException {
    Task<DDLWork> roleDDLTask = (Task<DDLWork>) hiveAuthorizationTaskFactory
        .createShowRolesTask(ast, ctx.getResFile(), getInputs(), getOutputs());

    if (roleDDLTask != null) {
      rootTasks.add(roleDDLTask);
      setFetchTask(createFetchTask(RoleDDLDesc.getRoleNameSchema()));
    }
  }

  private void analyzeAlterDatabaseProperties(ASTNode ast) throws SemanticException {

    String dbName = unescapeIdentifier(ast.getChild(0).getText());
    Map<String, String> dbProps = null;

    for (int i = 1; i < ast.getChildCount(); i++) {
      ASTNode childNode = (ASTNode) ast.getChild(i);
      switch (childNode.getToken().getType()) {
      case HiveParser.TOK_DATABASEPROPERTIES:
        dbProps = DDLSemanticAnalyzer.getProps((ASTNode) childNode.getChild(0));
        break;
      default:
        throw new SemanticException("Unrecognized token in CREATE DATABASE statement");
      }
    }
    AlterDatabaseDesc alterDesc = new AlterDatabaseDesc(dbName, dbProps);
    addAlterDbDesc(alterDesc);
  }

  private void addAlterDbDesc(AlterDatabaseDesc alterDesc) throws SemanticException {
    Database database = getDatabase(alterDesc.getDatabaseName());
    outputs.add(new WriteEntity(database, WriteEntity.WriteType.DDL_NO_LOCK));
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterDesc), conf));
  }

  private void analyzeAlterDatabaseOwner(ASTNode ast) throws SemanticException {
    String dbName = getUnescapedName((ASTNode) ast.getChild(0));
    PrincipalDesc principalDesc = AuthorizationParseUtils.getPrincipalDesc((ASTNode) ast
        .getChild(1));

    // The syntax should not allow these fields to be null, but lets verify
    String nullCmdMsg = "can't be null in alter database set owner command";
    if(principalDesc.getName() == null){
      throw new SemanticException("Owner name " + nullCmdMsg);
    }
    if(principalDesc.getType() == null){
      throw new SemanticException("Owner type " + nullCmdMsg);
    }

    AlterDatabaseDesc alterDesc = new AlterDatabaseDesc(dbName, principalDesc);
    addAlterDbDesc(alterDesc);
  }

  private void analyzeExchangePartition(String[] qualified, ASTNode ast) throws SemanticException {
    Table destTable = getTable(qualified);
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
    AlterTableExchangePartition alterTableExchangePartition =
      new AlterTableExchangePartition(sourceTable, destTable, partSpecs);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
      alterTableExchangePartition), conf));
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

  private void analyzeCreateDatabase(ASTNode ast) throws SemanticException {
    String dbName = unescapeIdentifier(ast.getChild(0).getText());
    boolean ifNotExists = false;
    String dbComment = null;
    String dbLocation = null;
    Map<String, String> dbProps = null;

    for (int i = 1; i < ast.getChildCount(); i++) {
      ASTNode childNode = (ASTNode) ast.getChild(i);
      switch (childNode.getToken().getType()) {
      case HiveParser.TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      case HiveParser.TOK_DATABASECOMMENT:
        dbComment = unescapeSQLString(childNode.getChild(0).getText());
        break;
      case TOK_DATABASEPROPERTIES:
        dbProps = DDLSemanticAnalyzer.getProps((ASTNode) childNode.getChild(0));
        break;
      case TOK_DATABASELOCATION:
        dbLocation = unescapeSQLString(childNode.getChild(0).getText());
        addLocationToOutputs(dbLocation);
        break;
      default:
        throw new SemanticException("Unrecognized token in CREATE DATABASE statement");
      }
    }

    CreateDatabaseDesc createDatabaseDesc =
        new CreateDatabaseDesc(dbName, dbComment, dbLocation, ifNotExists);
    if (dbProps != null) {
      createDatabaseDesc.setDatabaseProperties(dbProps);
    }
    Database database = new Database(dbName, dbComment, dbLocation, dbProps);
    outputs.add(new WriteEntity(database, WriteEntity.WriteType.DDL_NO_LOCK));

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        createDatabaseDesc), conf));
  }

  private void analyzeDropDatabase(ASTNode ast) throws SemanticException {
    String dbName = unescapeIdentifier(ast.getChild(0).getText());
    boolean ifExists = false;
    boolean ifCascade = false;

    if (null != ast.getFirstChildWithType(HiveParser.TOK_IFEXISTS)) {
      ifExists = true;
    }

    if (null != ast.getFirstChildWithType(HiveParser.TOK_CASCADE)) {
      ifCascade = true;
    }

    Database database = getDatabase(dbName, !ifExists);
    if (database == null) {
      return;
    }

    // if cascade=true, then we need to authorize the drop table action as well
    if (ifCascade) {
      // add the tables as well to outputs
      List<String> tableNames;
      // get names of all tables under this dbName
      try {
        tableNames = db.getAllTables(dbName);
      } catch (HiveException e) {
        throw new SemanticException(e);
      }
      // add tables to outputs
      if (tableNames != null) {
        for (String tableName : tableNames) {
          Table table = getTable(dbName, tableName, true);
          // We want no lock here, as the database lock will cover the tables,
          // and putting a lock will actually cause us to deadlock on ourselves.
          outputs.add(new WriteEntity(table, WriteEntity.WriteType.DDL_NO_LOCK));
        }
      }
    }
    inputs.add(new ReadEntity(database));
    outputs.add(new WriteEntity(database, WriteEntity.WriteType.DDL_EXCLUSIVE));

    DropDatabaseDesc dropDatabaseDesc = new DropDatabaseDesc(dbName, ifExists, ifCascade);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), dropDatabaseDesc), conf));
  }

  private void analyzeSwitchDatabase(ASTNode ast) throws SemanticException {
    String dbName = unescapeIdentifier(ast.getChild(0).getText());
    Database database = getDatabase(dbName, true);
    ReadEntity dbReadEntity = new ReadEntity(database);
    dbReadEntity.noLockNeeded();
    inputs.add(dbReadEntity);
    SwitchDatabaseDesc switchDatabaseDesc = new SwitchDatabaseDesc(dbName);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        switchDatabaseDesc), conf));
  }



  private void analyzeDropTable(ASTNode ast, boolean expectView)
      throws SemanticException {
    String tableName = getUnescapedName((ASTNode) ast.getChild(0));
    boolean ifExists = (ast.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null);
    // we want to signal an error if the table/view doesn't exist and we're
    // configured not to fail silently
    boolean throwException =
        !ifExists && !HiveConf.getBoolVar(conf, ConfVars.DROPIGNORESNONEXISTENT);

    ReplicationSpec replicationSpec = new ReplicationSpec(ast);

    Table tab = getTable(tableName, throwException);
    if (tab != null) {
      inputs.add(new ReadEntity(tab));
      outputs.add(new WriteEntity(tab, WriteEntity.WriteType.DDL_EXCLUSIVE));
    }

    boolean ifPurge = (ast.getFirstChildWithType(HiveParser.KW_PURGE) != null);
    DropTableDesc dropTblDesc = new DropTableDesc(tableName, expectView, ifExists, ifPurge, replicationSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        dropTblDesc), conf));
  }

  private void analyzeTruncateTable(ASTNode ast) throws SemanticException {
    ASTNode root = (ASTNode) ast.getChild(0); // TOK_TABLE_PARTITION
    String tableName = getUnescapedName((ASTNode) root.getChild(0));

    Table table = getTable(tableName, true);
    if (table.getTableType() != TableType.MANAGED_TABLE) {
      throw new SemanticException(ErrorMsg.TRUNCATE_FOR_NON_MANAGED_TABLE.format(tableName));
    }
    if (table.isNonNative()) {
      throw new SemanticException(ErrorMsg.TRUNCATE_FOR_NON_NATIVE_TABLE.format(tableName)); //TODO
    }
    if (!table.isPartitioned() && root.getChildCount() > 1) {
      throw new SemanticException(ErrorMsg.PARTSPEC_FOR_NON_PARTITIONED_TABLE.format(tableName));
    }
    Map<String, String> partSpec = getPartSpec((ASTNode) root.getChild(1));
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

    TruncateTableDesc truncateTblDesc = new TruncateTableDesc(tableName, partSpec);

    DDLWork ddlWork = new DDLWork(getInputs(), getOutputs(), truncateTblDesc);
    Task<? extends Serializable> truncateTask = TaskFactory.get(ddlWork, conf);

    // Is this a truncate column command
    List<String> columnNames = null;
    if (ast.getChildCount() == 2) {
      try {
        columnNames = getColumnNames((ASTNode)ast.getChild(1));

        // Throw an error if the table is indexed
        List<Index> indexes = db.getIndexes(table.getDbName(), tableName, (short)1);
        if (indexes != null && indexes.size() > 0) {
          throw new SemanticException(ErrorMsg.TRUNCATE_COLUMN_INDEXED_TABLE.getMsg());
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
              part.getSkewedColValueLocationMaps(), part.isStoredAsSubDirectories(), conf);
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
              table.getSkewedColValueLocationMaps(), table.isStoredAsSubDirectories(), conf);
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

        truncateTblDesc.setColumnIndexes(new ArrayList<Integer>(columnIndexes));
        truncateTblDesc.setInputDir(oldTblPartLoc);
        truncateTblDesc.setLbCtx(lbCtx);

        addInputsOutputsAlterTable(tableName, partSpec, AlterTableTypes.TRUNCATE);

        ddlWork.setNeedLock(true);
        TableDesc tblDesc = Utilities.getTableDesc(table);
        // Write the output to temporary directory and move it to the final location at the end
        // so the operation is atomic.
        Path queryTmpdir = ctx.getExternalTmpPath(newTblPartLoc);
        truncateTblDesc.setOutputDir(queryTmpdir);
        LoadTableDesc ltd = new LoadTableDesc(queryTmpdir, tblDesc,
            partSpec == null ? new HashMap<String, String>() : partSpec);
        ltd.setLbCtx(lbCtx);
        Task<MoveWork> moveTsk = TaskFactory.get(new MoveWork(null, null, ltd, null, false),
            conf);
        truncateTask.addDependentTask(moveTsk);

        // Recalculate the HDFS stats if auto gather stats is set
        if (conf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
          StatsWork statDesc;
          if (oldTblPartLoc.equals(newTblPartLoc)) {
            // If we're merging to the same location, we can avoid some metastore calls
            TableSpec tablepart = new TableSpec(this.db, conf, root);
            statDesc = new StatsWork(tablepart);
          } else {
            statDesc = new StatsWork(ltd);
          }
          statDesc.setNoStatsAggregator(true);
          statDesc.setClearAggregatorStats(true);
          statDesc.setStatsReliable(conf.getBoolVar(HiveConf.ConfVars.HIVE_STATS_RELIABLE));
          Task<? extends Serializable> statTask = TaskFactory.get(statDesc, conf);
          moveTsk.addDependentTask(statTask);
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }
    }

    rootTasks.add(truncateTask);
  }

  public static boolean isFullSpec(Table table, Map<String, String> partSpec) {
    for (FieldSchema partCol : table.getPartCols()) {
      if (partSpec.get(partCol.getName()) == null) {
        return false;
      }
    }
    return true;
  }

  private void analyzeCreateIndex(ASTNode ast) throws SemanticException {
    String indexName = unescapeIdentifier(ast.getChild(0).getText());
    String typeName = unescapeSQLString(ast.getChild(1).getText());
    String[] qTabName = getQualifiedTableName((ASTNode) ast.getChild(2));
    List<String> indexedCols = getColumnNames((ASTNode) ast.getChild(3));

    IndexType indexType = HiveIndex.getIndexType(typeName);
    if (indexType != null) {
      typeName = indexType.getHandlerClsName();
    } else {
      try {
        JavaUtils.loadClass(typeName);
      } catch (Exception e) {
        throw new SemanticException("class name provided for index handler not found.", e);
      }
    }

    String indexTableName = null;
    boolean deferredRebuild = false;
    String location = null;
    Map<String, String> tblProps = null;
    Map<String, String> idxProps = null;
    String indexComment = null;

    RowFormatParams rowFormatParams = new RowFormatParams();
    StorageFormat storageFormat = new StorageFormat(conf);

    for (int idx = 4; idx < ast.getChildCount(); idx++) {
      ASTNode child = (ASTNode) ast.getChild(idx);
      if (storageFormat.fillStorageFormat(child)) {
        continue;
      }
      switch (child.getToken().getType()) {
      case HiveParser.TOK_TABLEROWFORMAT:
        rowFormatParams.analyzeRowFormat(child);
        break;
      case HiveParser.TOK_CREATEINDEX_INDEXTBLNAME:
        ASTNode ch = (ASTNode) child.getChild(0);
        indexTableName = getUnescapedName(ch);
        break;
      case HiveParser.TOK_DEFERRED_REBUILDINDEX:
        deferredRebuild = true;
        break;
      case HiveParser.TOK_TABLELOCATION:
        location = unescapeSQLString(child.getChild(0).getText());
        addLocationToOutputs(location);
        break;
      case HiveParser.TOK_TABLEPROPERTIES:
        tblProps = DDLSemanticAnalyzer.getProps((ASTNode) child.getChild(0));
        break;
      case HiveParser.TOK_INDEXPROPERTIES:
        idxProps = DDLSemanticAnalyzer.getProps((ASTNode) child.getChild(0));
        break;
      case HiveParser.TOK_TABLESERIALIZER:
        child = (ASTNode) child.getChild(0);
        storageFormat.setSerde(unescapeSQLString(child.getChild(0).getText()));
        if (child.getChildCount() == 2) {
          readProps((ASTNode) (child.getChild(1).getChild(0)),
              storageFormat.getSerdeProps());
        }
        break;
      case HiveParser.TOK_INDEXCOMMENT:
        child = (ASTNode) child.getChild(0);
        indexComment = unescapeSQLString(child.getText());
      }
    }

    storageFormat.fillDefaultStorageFormat(false);
    if (indexTableName == null) {
      indexTableName = MetaStoreUtils.getIndexTableName(qTabName[0], qTabName[1], indexName);
      indexTableName = qTabName[0] + "." + indexTableName; // on same database with base table
    } else {
      indexTableName = getDotName(Utilities.getDbTableName(indexTableName));
    }
    inputs.add(new ReadEntity(getTable(qTabName)));

    CreateIndexDesc crtIndexDesc = new CreateIndexDesc(getDotName(qTabName), indexName,
        indexedCols, indexTableName, deferredRebuild, storageFormat.getInputFormat(),
        storageFormat.getOutputFormat(),
        storageFormat.getStorageHandler(), typeName, location, idxProps, tblProps,
        storageFormat.getSerde(), storageFormat.getSerdeProps(), rowFormatParams.collItemDelim,
        rowFormatParams.fieldDelim, rowFormatParams.fieldEscape,
        rowFormatParams.lineDelim, rowFormatParams.mapKeyDelim, indexComment);
    Task<?> createIndex =
        TaskFactory.get(new DDLWork(getInputs(), getOutputs(), crtIndexDesc), conf);
    rootTasks.add(createIndex);
  }

  private void analyzeDropIndex(ASTNode ast) throws SemanticException {
    String indexName = unescapeIdentifier(ast.getChild(0).getText());
    String tableName = getUnescapedName((ASTNode) ast.getChild(1));
    boolean ifExists = (ast.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null);
    // we want to signal an error if the index doesn't exist and we're
    // configured not to ignore this
    boolean throwException =
        !ifExists && !HiveConf.getBoolVar(conf, ConfVars.DROPIGNORESNONEXISTENT);
    Table tbl = getTable(tableName, false);
    if (throwException && tbl == null) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName));
    }
    try {
      Index idx = db.getIndex(tableName, indexName);
    } catch (HiveException e) {
      if (!(e.getCause() instanceof NoSuchObjectException)) {
        throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg("dropping index"), e);
      }
      if (throwException) {
        throw new SemanticException(ErrorMsg.INVALID_INDEX.getMsg(indexName));
      }
    }
    if (tbl != null) {
      inputs.add(new ReadEntity(tbl));
    }

    DropIndexDesc dropIdxDesc = new DropIndexDesc(indexName, tableName, throwException);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        dropIdxDesc), conf));
  }

  private void analyzeAlterIndexRebuild(ASTNode ast) throws SemanticException {
    String[] qualified = getQualifiedTableName((ASTNode) ast.getChild(0));
    String indexName = unescapeIdentifier(ast.getChild(1).getText());
    HashMap<String, String> partSpec = null;
    Tree part = ast.getChild(2);
    if (part != null) {
      partSpec = getValidatedPartSpec(getTable(qualified), (ASTNode)part, conf, false);
    }
    List<Task<?>> indexBuilder = getIndexBuilderMapRed(qualified, indexName, partSpec);
    rootTasks.addAll(indexBuilder);

    // Handle updating index timestamps
    AlterIndexDesc alterIdxDesc = new AlterIndexDesc(AlterIndexTypes.UPDATETIMESTAMP);
    alterIdxDesc.setIndexName(indexName);
    alterIdxDesc.setBaseTableName(getDotName(qualified));
    alterIdxDesc.setSpec(partSpec);

    Task<?> tsTask = TaskFactory.get(new DDLWork(alterIdxDesc), conf);
    for (Task<?> t : indexBuilder) {
      t.addDependentTask(tsTask);
    }
  }

  private void analyzeAlterIndexProps(ASTNode ast)
      throws SemanticException {

    String[] qualified = getQualifiedTableName((ASTNode) ast.getChild(0));
    String indexName = unescapeIdentifier(ast.getChild(1).getText());
    HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(2))
        .getChild(0));

    AlterIndexDesc alterIdxDesc = new AlterIndexDesc(AlterIndexTypes.ADDPROPS);
    alterIdxDesc.setProps(mapProp);
    alterIdxDesc.setIndexName(indexName);
    alterIdxDesc.setBaseTableName(getDotName(qualified));

    rootTasks.add(TaskFactory.get(new DDLWork(alterIdxDesc), conf));
  }

  private List<Task<?>> getIndexBuilderMapRed(String[] names, String indexName,
      HashMap<String, String> partSpec) throws SemanticException {
    try {
      Index index = db.getIndex(names[0], names[1], indexName);
      Table indexTbl = null;
      String indexTableName = index.getIndexTableName();
      if (indexTableName != null) {
        indexTbl = getTable(Utilities.getDbTableName(index.getDbName(), indexTableName));
      }
      Table baseTbl = getTable(new String[] {index.getDbName(), index.getOrigTableName()});

      String handlerCls = index.getIndexHandlerClass();
      HiveIndexHandler handler = HiveUtils.getIndexHandler(conf, handlerCls);

      List<Partition> indexTblPartitions = null;
      List<Partition> baseTblPartitions = null;
      if (indexTbl != null) {
        indexTblPartitions = new ArrayList<Partition>();
        baseTblPartitions = preparePartitions(baseTbl, partSpec,
            indexTbl, db, indexTblPartitions);
      }

      List<Task<?>> ret = handler.generateIndexBuildTaskList(baseTbl,
          index, indexTblPartitions, baseTblPartitions, indexTbl, getInputs(), getOutputs());
      return ret;
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private List<Partition> preparePartitions(
      org.apache.hadoop.hive.ql.metadata.Table baseTbl,
      HashMap<String, String> partSpec,
      org.apache.hadoop.hive.ql.metadata.Table indexTbl, Hive db,
      List<Partition> indexTblPartitions)
      throws HiveException, MetaException {
    List<Partition> baseTblPartitions = new ArrayList<Partition>();
    if (partSpec != null) {
      // if partspec is specified, then only producing index for that
      // partition
      Partition part = db.getPartition(baseTbl, partSpec, false);
      if (part == null) {
        throw new HiveException("Partition "
            + Warehouse.makePartName(partSpec, false)
            + " does not exist in table "
            + baseTbl.getTableName());
      }
      baseTblPartitions.add(part);
      Partition indexPart = db.getPartition(indexTbl, partSpec, false);
      if (indexPart == null) {
        indexPart = db.createPartition(indexTbl, partSpec);
      }
      indexTblPartitions.add(indexPart);
    } else if (baseTbl.isPartitioned()) {
      // if no partition is specified, create indexes for all partitions one
      // by one.
      baseTblPartitions = db.getPartitions(baseTbl);
      for (Partition basePart : baseTblPartitions) {
        HashMap<String, String> pSpec = basePart.getSpec();
        Partition indexPart = db.getPartition(indexTbl, pSpec, false);
        if (indexPart == null) {
          indexPart = db.createPartition(indexTbl, pSpec);
        }
        indexTblPartitions.add(indexPart);
      }
    }
    return baseTblPartitions;
  }

  private void validateAlterTableType(Table tbl, AlterTableTypes op) throws SemanticException {
    validateAlterTableType(tbl, op, false);
  }

  private void validateAlterTableType(Table tbl, AlterTableTypes op, boolean expectView)
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
    if (tbl.isNonNative()) {
      throw new SemanticException(ErrorMsg.ALTER_TABLE_NON_NATIVE.getMsg(tbl.getTableName()));
    }
  }

  private void analyzeAlterTableProps(String[] qualified, ASTNode ast, boolean expectView, boolean isUnset)
      throws SemanticException {

    String tableName = getDotName(qualified);
    HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(0))
        .getChild(0));
    AlterTableDesc alterTblDesc = null;
    if (isUnset == true) {
      alterTblDesc = new AlterTableDesc(AlterTableTypes.DROPPROPS, expectView);
      if (ast.getChild(1) != null) {
        alterTblDesc.setDropIfExists(true);
      }
    } else {
      alterTblDesc = new AlterTableDesc(AlterTableTypes.ADDPROPS, expectView);
    }
    alterTblDesc.setProps(mapProp);
    alterTblDesc.setOldName(tableName);

    addInputsOutputsAlterTable(tableName, null, alterTblDesc);

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableSerdeProps(ASTNode ast, String tableName,
      HashMap<String, String> partSpec)
      throws SemanticException {
    HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(0))
        .getChild(0));
    AlterTableDesc alterTblDesc = new AlterTableDesc(
        AlterTableTypes.ADDSERDEPROPS);
    alterTblDesc.setProps(mapProp);
    alterTblDesc.setOldName(tableName);
    alterTblDesc.setPartSpec(partSpec);

    addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableSerde(ASTNode ast, String tableName,
      HashMap<String, String> partSpec)
      throws SemanticException {

    String serdeName = unescapeSQLString(ast.getChild(0).getText());
    AlterTableDesc alterTblDesc = new AlterTableDesc(AlterTableTypes.ADDSERDE);
    if (ast.getChildCount() > 1) {
      HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(1))
          .getChild(0));
      alterTblDesc.setProps(mapProp);
    }
    alterTblDesc.setOldName(tableName);
    alterTblDesc.setSerdeName(serdeName);
    alterTblDesc.setPartSpec(partSpec);

    addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableFileFormat(ASTNode ast, String tableName,
      HashMap<String, String> partSpec)
      throws SemanticException {

    StorageFormat format = new StorageFormat(conf);
    ASTNode child = (ASTNode) ast.getChild(0);

    if (!format.fillStorageFormat(child)) {
      throw new AssertionError("Unknown token " + child.getText());
    }

    AlterTableDesc alterTblDesc = new AlterTableDesc(tableName, format.getInputFormat(),
        format.getOutputFormat(), format.getSerde(), format.getStorageHandler(), partSpec);

    addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void addInputsOutputsAlterTable(String tableName, Map<String, String> partSpec,
      AlterTableTypes op) throws SemanticException {
    addInputsOutputsAlterTable(tableName, partSpec, null, op);
  }

  private void addInputsOutputsAlterTable(String tableName, Map<String, String> partSpec,
     AlterTableDesc desc) throws SemanticException {
    addInputsOutputsAlterTable(tableName, partSpec, desc, desc.getOp());
  }

  private void addInputsOutputsAlterTable(String tableName, Map<String, String> partSpec,
      AlterTableDesc desc, AlterTableTypes op) throws SemanticException {
    boolean isCascade = desc != null && desc.getIsCascade();
    boolean alterPartitions = partSpec != null && !partSpec.isEmpty();
    //cascade only occurs at table level then cascade to partition level
    if (isCascade && alterPartitions) {
      throw new SemanticException(
          ErrorMsg.ALTER_TABLE_PARTITION_CASCADE_NOT_SUPPORTED, op.getName());
    }

    Table tab = getTable(tableName, true);
    // Determine the lock type to acquire
    WriteEntity.WriteType writeType = WriteEntity.determineAlterTableWriteType(op);

    if (!alterPartitions) {
      inputs.add(new ReadEntity(tab));
      outputs.add(new WriteEntity(tab, writeType));
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
        if (!AlterTableDesc.doesAlterTableTypeSupportPartialPartitionSpec(op)) {
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
      validateAlterTableType(tab, op, desc.getExpectView());

      // validate Unset Non Existed Table Properties
      if (op == AlterTableDesc.AlterTableTypes.DROPPROPS && !desc.getIsDropIfExists()) {
        Map<String, String> tableParams = tab.getTTable().getParameters();
        for (String currKey : desc.getProps().keySet()) {
          if (!tableParams.containsKey(currKey)) {
            String errorMsg =
                "The following property " + currKey +
                " does not exist in " + tab.getTableName();
            throw new SemanticException(
              ErrorMsg.ALTER_TBL_UNSET_NON_EXIST_PROPERTY.getMsg(errorMsg));
          }
        }
      }
    }
  }

  private void analyzeAlterTableLocation(ASTNode ast, String tableName,
      HashMap<String, String> partSpec) throws SemanticException {

    String newLocation = unescapeSQLString(ast.getChild(0).getText());
    addLocationToOutputs(newLocation);
    AlterTableDesc alterTblDesc = new AlterTableDesc(tableName, newLocation, partSpec);

    addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));

  }

  private void analyzeAlterTableProtectMode(ASTNode ast, String tableName,
      HashMap<String, String> partSpec)
      throws SemanticException {

    AlterTableDesc alterTblDesc =
        new AlterTableDesc(AlterTableTypes.ALTERPROTECTMODE);

    alterTblDesc.setOldName(tableName);
    alterTblDesc.setPartSpec(partSpec);

    ASTNode child = (ASTNode) ast.getChild(0);

    switch (child.getToken().getType()) {
    case HiveParser.TOK_ENABLE:
      alterTblDesc.setProtectModeEnable(true);
      break;
    case HiveParser.TOK_DISABLE:
      alterTblDesc.setProtectModeEnable(false);
      break;
    default:
      throw new SemanticException(
          "Set Protect mode Syntax parsing error.");
    }

    ASTNode grandChild = (ASTNode) child.getChild(0);
    switch (grandChild.getToken().getType()) {
    case HiveParser.TOK_OFFLINE:
      alterTblDesc.setProtectModeType(AlterTableDesc.ProtectModeType.OFFLINE);
      break;
    case HiveParser.TOK_NO_DROP:
      if (grandChild.getChildCount() > 0) {
        alterTblDesc.setProtectModeType(AlterTableDesc.ProtectModeType.NO_DROP_CASCADE);
      }
      else {
        alterTblDesc.setProtectModeType(AlterTableDesc.ProtectModeType.NO_DROP);
      }
      break;
    case HiveParser.TOK_READONLY:
      throw new SemanticException(
          "Potect mode READONLY is not implemented");
    default:
      throw new SemanticException(
          "Only protect mode NO_DROP or OFFLINE supported");
    }

    addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTablePartMergeFiles(ASTNode ast,
      String tableName, HashMap<String, String> partSpec)
      throws SemanticException {
    AlterTablePartMergeFilesDesc mergeDesc = new AlterTablePartMergeFilesDesc(
        tableName, partSpec);

    List<Path> inputDir = new ArrayList<Path>();
    Path oldTblPartLoc = null;
    Path newTblPartLoc = null;
    Table tblObj = null;
    ListBucketingCtx lbCtx = null;

    try {
      tblObj = getTable(tableName);

      List<String> bucketCols = null;
      Class<? extends InputFormat> inputFormatClass = null;
      boolean isArchived = false;
      boolean checkIndex = HiveConf.getBoolVar(conf,
          HiveConf.ConfVars.HIVE_CONCATENATE_CHECK_INDEX);
      if (checkIndex) {
        List<Index> indexes = db.getIndexes(tblObj.getDbName(), tblObj.getTableName(),
            Short.MAX_VALUE);
        if (indexes != null && indexes.size() > 0) {
          throw new SemanticException("can not do merge because source table "
              + tableName + " is indexed.");
        }
      }

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
          inputFormatClass = part.getInputFormatClass();
          isArchived = ArchiveUtils.isArchived(part);

          Path tabPath = tblObj.getPath();
          Path partPath = part.getDataLocation();

          // if the table is in a different dfs than the partition,
          // replace the partition's dfs with the table's dfs.
          newTblPartLoc = new Path(tabPath.toUri().getScheme(), tabPath.toUri()
              .getAuthority(), partPath.toUri().getPath());

          oldTblPartLoc = partPath;

          lbCtx = constructListBucketingCtx(part.getSkewedColNames(), part.getSkewedColValues(),
              part.getSkewedColValueLocationMaps(), part.isStoredAsSubDirectories(), conf);
        }
      } else {
        inputFormatClass = tblObj.getInputFormatClass();
        bucketCols = tblObj.getBucketCols();

        // input and output are the same
        oldTblPartLoc = tblObj.getPath();
        newTblPartLoc = tblObj.getPath();

        lbCtx = constructListBucketingCtx(tblObj.getSkewedColNames(), tblObj.getSkewedColValues(),
            tblObj.getSkewedColValueLocationMaps(), tblObj.isStoredAsSubDirectories(), conf);
      }

      // throw a HiveException for other than rcfile and orcfile.
      if (!((inputFormatClass.equals(RCFileInputFormat.class) ||
          (inputFormatClass.equals(OrcInputFormat.class))))) {
        throw new SemanticException(
            "Only RCFile and ORCFile Formats are supportted right now.");
      }
      mergeDesc.setInputFormatClass(inputFormatClass);

      // throw a HiveException if the table/partition is bucketized
      if (bucketCols != null && bucketCols.size() > 0) {
        throw new SemanticException(
            "Merge can not perform on bucketized partition/table.");
      }

      // throw a HiveException if the table/partition is archived
      if (isArchived) {
        throw new SemanticException(
            "Merge can not perform on archived partitions.");
      }

      inputDir.add(oldTblPartLoc);

      mergeDesc.setInputDir(inputDir);

      mergeDesc.setLbCtx(lbCtx);

      addInputsOutputsAlterTable(tableName, partSpec, AlterTableTypes.MERGEFILES);
      DDLWork ddlWork = new DDLWork(getInputs(), getOutputs(), mergeDesc);
      ddlWork.setNeedLock(true);
      Task<? extends Serializable> mergeTask = TaskFactory.get(ddlWork, conf);
      TableDesc tblDesc = Utilities.getTableDesc(tblObj);
      Path queryTmpdir = ctx.getExternalTmpPath(newTblPartLoc);
      mergeDesc.setOutputDir(queryTmpdir);
      LoadTableDesc ltd = new LoadTableDesc(queryTmpdir, tblDesc,
          partSpec == null ? new HashMap<String, String>() : partSpec);
      ltd.setLbCtx(lbCtx);
      Task<MoveWork> moveTsk = TaskFactory.get(new MoveWork(null, null, ltd, null, false),
          conf);
      mergeTask.addDependentTask(moveTsk);

      if (conf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
        StatsWork statDesc;
        if (oldTblPartLoc.equals(newTblPartLoc)) {
          // If we're merging to the same location, we can avoid some metastore calls
          TableSpec tablepart = new TableSpec(db, conf, tableName, partSpec);
          statDesc = new StatsWork(tablepart);
        } else {
          statDesc = new StatsWork(ltd);
        }
        statDesc.setNoStatsAggregator(true);
        statDesc.setClearAggregatorStats(true);
        statDesc.setStatsReliable(conf.getBoolVar(HiveConf.ConfVars.HIVE_STATS_RELIABLE));
        Task<? extends Serializable> statTask = TaskFactory.get(statDesc, conf);
        moveTsk.addDependentTask(statTask);
      }

      rootTasks.add(mergeTask);
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private void analyzeAlterTableClusterSort(ASTNode ast, String tableName,
      HashMap<String, String> partSpec) throws SemanticException {

    AlterTableDesc alterTblDesc;
    switch (ast.getChild(0).getType()) {
    case HiveParser.TOK_NOT_CLUSTERED:
      alterTblDesc = new AlterTableDesc(tableName, -1, new ArrayList<String>(),
          new ArrayList<Order>(), partSpec);
      break;
    case HiveParser.TOK_NOT_SORTED:
      alterTblDesc = new AlterTableDesc(tableName, true, partSpec);
      break;
    case HiveParser.TOK_ALTERTABLE_BUCKETS:
      ASTNode buckets = (ASTNode) ast.getChild(0);
      List<String> bucketCols = getColumnNames((ASTNode) buckets.getChild(0));
      List<Order> sortCols = new ArrayList<Order>();
      int numBuckets = -1;
      if (buckets.getChildCount() == 2) {
        numBuckets = (Integer.valueOf(buckets.getChild(1).getText())).intValue();
      } else {
        sortCols = getColumnNamesOrder((ASTNode) buckets.getChild(1));
        numBuckets = (Integer.valueOf(buckets.getChild(2).getText())).intValue();
      }
      if (numBuckets <= 0) {
        throw new SemanticException(ErrorMsg.INVALID_BUCKET_NUMBER.getMsg());
      }

      alterTblDesc = new AlterTableDesc(tableName, numBuckets,
          bucketCols, sortCols, partSpec);
      break;
    default:
      throw new SemanticException("Invalid operation " + ast.getChild(0).getType());
    }
    addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblDesc), conf));
  }

  private void analyzeAlterTableCompact(ASTNode ast, String tableName,
      HashMap<String, String> partSpec) throws SemanticException {

    String type = unescapeSQLString(ast.getChild(0).getText()).toLowerCase();

    if (!type.equals("minor") && !type.equals("major")) {
      throw new SemanticException(ErrorMsg.INVALID_COMPACTION_TYPE.getMsg());
    }

    LinkedHashMap<String, String> newPartSpec = null;
    if (partSpec != null) newPartSpec = new LinkedHashMap<String, String>(partSpec);

    AlterTableSimpleDesc desc = new AlterTableSimpleDesc(
        tableName, newPartSpec, type);

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc), conf));
  }

  static HashMap<String, String> getProps(ASTNode prop) {
    // Must be deterministic order map for consistent q-test output across Java versions
    HashMap<String, String> mapProp = new LinkedHashMap<String, String>();
    readProps(prop, mapProp);
    return mapProp;
  }

  /**
   * Utility class to resolve QualifiedName
   */
  static class QualifiedNameUtil {

    // delimiter to check DOT delimited qualified names
    static String delimiter = "\\.";

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

    // assume the first component of DOT delimited name is tableName
    // get the attemptTableName
    static public String getAttemptTableName(Hive db, String qualifiedName, boolean isColumn)
        throws SemanticException {
      // check whether the name starts with table
      // DESCRIBE table
      // DESCRIBE table.column
      // DESCRIBE table column
      String tableName = qualifiedName.substring(0,
        qualifiedName.indexOf('.') == -1 ?
        qualifiedName.length() : qualifiedName.indexOf('.'));
      try {
        Table tab = db.getTable(tableName);
        if (tab != null) {
          if (isColumn) {
            // if attempt to get columnPath
            // return the whole qualifiedName(table.column or table)
            return qualifiedName;
          } else {
            // if attempt to get tableName
            // return table
            return tableName;
          }
        }
      } catch (InvalidTableException e) {
        // assume the first DOT delimited component is tableName
        // OK if it is not
        // do nothing when having exception
        return null;
      } catch (HiveException e) {
        throw new SemanticException(e.getMessage(), e);
      }
      return null;
    }

    // get Database Name
    static public String getDBName(Hive db, ASTNode ast) {
      String dbName = null;
      String fullyQualifiedName = getFullyQualifiedName(ast);

      // if database.table or database.table.column or table.column
      // first try the first component of the DOT separated name
      if (ast.getChildCount() >= 2) {
        dbName = fullyQualifiedName.substring(0,
          fullyQualifiedName.indexOf('.') == -1 ?
          fullyQualifiedName.length() :
          fullyQualifiedName.indexOf('.'));
        try {
          // if the database name is not valid
          // it is table.column
          // return null as dbName
          if (!db.databaseExists(dbName)) {
            return null;
          }
        } catch (HiveException e) {
          return null;
        }
      } else {
        // in other cases, return null
        // database is not validated if null
        return null;
      }
      return dbName;
    }

    // get Table Name
    static public String getTableName(Hive db, ASTNode ast)
      throws SemanticException {
      String tableName = null;
      String fullyQualifiedName = getFullyQualifiedName(ast);

      // assume the first component of DOT delimited name is tableName
      String attemptTableName = getAttemptTableName(db, fullyQualifiedName, false);
      if (attemptTableName != null) {
        return attemptTableName;
      }

      // if the name does not start with table
      // it should start with database
      // DESCRIBE database.table
      // DESCRIBE database.table column
      if (fullyQualifiedName.split(delimiter).length == 3) {
        // if DESCRIBE database.table.column
        // invalid syntax exception
        if (ast.getChildCount() == 2) {
          throw new SemanticException(ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(fullyQualifiedName));
        } else {
          // if DESCRIBE database.table column
          // return database.table as tableName
          tableName = fullyQualifiedName.substring(0,
            fullyQualifiedName.lastIndexOf('.'));
        }
      } else if (fullyQualifiedName.split(delimiter).length == 2) {
        // if DESCRIBE database.table
        // return database.table as tableName
        tableName = fullyQualifiedName;
      } else {
        // if fullyQualifiedName only have one component
        // it is an invalid table
        throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(fullyQualifiedName));
      }

      return tableName;
    }

    // get column path
    static public String getColPath(
      Hive db,
      ASTNode parentAst,
      ASTNode ast,
      String tableName,
      Map<String, String> partSpec) throws SemanticException {

      // if parent has two children
      // it could be DESCRIBE table key
      // or DESCRIBE table partition
      if (parentAst.getChildCount() == 2 && partSpec == null) {
        // if partitionSpec is null
        // it is DESCRIBE table key
        // return table as columnPath
        return getFullyQualifiedName(parentAst);
      }

      // assume the first component of DOT delimited name is tableName
      String attemptTableName = getAttemptTableName(db, tableName, true);
      if (attemptTableName != null) {
        return attemptTableName;
      }

      // if the name does not start with table
      // it should start with database
      // DESCRIBE database.table
      // DESCRIBE database.table column
      if (tableName.split(delimiter).length == 3) {
        // if DESCRIBE database.table column
        // return table.column as column path
        return tableName.substring(
          tableName.indexOf(".") + 1, tableName.length());
      }

      // in other cases, column path is the same as tableName
      return tableName;
    }

    // get partition metadata
    static public Map<String, String> getPartitionSpec(Hive db, ASTNode ast, String tableName)
      throws SemanticException {
      // if ast has two children
      // it could be DESCRIBE table key
      // or DESCRIBE table partition
      // check whether it is DESCRIBE table partition
      if (ast.getChildCount() == 2) {
        Table tab = null;
        try {
          tab = db.getTable(tableName);
        }
        catch (InvalidTableException e) {
          throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName), e);
        }
        catch (HiveException e) {
          throw new SemanticException(e.getMessage(), e);
        }

        ASTNode partNode = (ASTNode) ast.getChild(1);
        HashMap<String, String> partSpec = null;
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

  /**
   * Create a FetchTask for a given thrift ddl schema.
   *
   * @param schema
   *          thrift ddl
   */
  private FetchTask createFetchTask(String schema) {
    Properties prop = new Properties();

    prop.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
    prop.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, " ");
    String[] colTypes = schema.split("#");
    prop.setProperty("columns", colTypes[0]);
    prop.setProperty("columns.types", colTypes[1]);
    prop.setProperty(serdeConstants.SERIALIZATION_LIB, LazySimpleSerDe.class.getName());
    FetchWork fetch = new FetchWork(ctx.getResFile(), new TableDesc(
        TextInputFormat.class,IgnoreKeyTextOutputFormat.class, prop), -1);
    fetch.setSerializationNullFormat(" ");
    return (FetchTask) TaskFactory.get(fetch, conf);
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

  private void validateTable(String tableName, Map<String, String> partSpec)
      throws SemanticException {
    Table tab = getTable(tableName);
    if (partSpec != null) {
      getPartition(tab, partSpec, true);
    }
  }

  private void analyzeDescribeTable(ASTNode ast) throws SemanticException {
    ASTNode tableTypeExpr = (ASTNode) ast.getChild(0);

    String qualifiedName =
      QualifiedNameUtil.getFullyQualifiedName((ASTNode) tableTypeExpr.getChild(0));
    String tableName =
      QualifiedNameUtil.getTableName(db, (ASTNode)(tableTypeExpr.getChild(0)));
    String dbName =
      QualifiedNameUtil.getDBName(db, (ASTNode)(tableTypeExpr.getChild(0)));

    Map<String, String> partSpec =
      QualifiedNameUtil.getPartitionSpec(db, tableTypeExpr, tableName);

    String colPath = QualifiedNameUtil.getColPath(
        db, tableTypeExpr, (ASTNode) tableTypeExpr.getChild(0), qualifiedName, partSpec);

    // if database is not the one currently using
    // validate database
    if (dbName != null) {
      validateDatabase(dbName);
    }
    if (partSpec != null) {
      validateTable(tableName, partSpec);
    }

    DescTableDesc descTblDesc = new DescTableDesc(
      ctx.getResFile(), tableName, partSpec, colPath);

    boolean showColStats = false;
    if (ast.getChildCount() == 2) {
      int descOptions = ast.getChild(1).getType();
      descTblDesc.setFormatted(descOptions == HiveParser.KW_FORMATTED);
      descTblDesc.setExt(descOptions == HiveParser.KW_EXTENDED);
      descTblDesc.setPretty(descOptions == HiveParser.KW_PRETTY);
      // in case of "DESCRIBE FORMATTED tablename column_name" statement, colPath
      // will contain tablename.column_name. If column_name is not specified
      // colPath will be equal to tableName. This is how we can differentiate
      // if we are describing a table or column
      if (!colPath.equalsIgnoreCase(tableName) && descTblDesc.isFormatted()) {
        showColStats = true;
      }
    }

    inputs.add(new ReadEntity(getTable(tableName)));
    Task ddlTask = TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        descTblDesc), conf);
    rootTasks.add(ddlTask);
    String schema = DescTableDesc.getSchema(showColStats);
    setFetchTask(createFetchTask(schema));
    LOG.info("analyzeDescribeTable done");
  }

  /**
   * Describe database.
   *
   * @param ast
   * @throws SemanticException
   */
  private void analyzeDescDatabase(ASTNode ast) throws SemanticException {

    boolean isExtended;
    String dbName;

    if (ast.getChildCount() == 1) {
      dbName = stripQuotes(ast.getChild(0).getText());
      isExtended = false;
    } else if (ast.getChildCount() == 2) {
      dbName = stripQuotes(ast.getChild(0).getText());
      isExtended = true;
    } else {
      throw new SemanticException("Unexpected Tokens at DESCRIBE DATABASE");
    }

    DescDatabaseDesc descDbDesc = new DescDatabaseDesc(ctx.getResFile(),
        dbName, isExtended);
    inputs.add(new ReadEntity(getDatabase(dbName)));
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), descDbDesc), conf));
    setFetchTask(createFetchTask(descDbDesc.getSchema()));
  }

  public static HashMap<String, String> getPartSpec(ASTNode partspec)
      throws SemanticException {
    if (partspec == null) {
      return null;
    }
    HashMap<String, String> partSpec = new LinkedHashMap<String, String>();
    for (int i = 0; i < partspec.getChildCount(); ++i) {
      ASTNode partspec_val = (ASTNode) partspec.getChild(i);
      String key = partspec_val.getChild(0).getText();
      String val = null;
      if (partspec_val.getChildCount() > 1) {
        val = stripQuotes(partspec_val.getChild(1).getText());
      }
      partSpec.put(key.toLowerCase(), val);
    }
    return partSpec;
  }

  public static HashMap<String, String> getValidatedPartSpec(Table table, ASTNode astNode,
      HiveConf conf, boolean shouldBeFull) throws SemanticException {
    HashMap<String, String> partSpec = getPartSpec(astNode);
    if (partSpec != null && !partSpec.isEmpty()) {
      validatePartSpec(table, partSpec, astNode, conf, shouldBeFull);
    }
    return partSpec;
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

    validateTable(tableName, null);

    showPartsDesc = new ShowPartitionsDesc(tableName, ctx.getResFile(), partSpec);
    inputs.add(new ReadEntity(getTable(tableName)));
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showPartsDesc), conf));
    setFetchTask(createFetchTask(showPartsDesc.getSchema()));
  }

  private void analyzeShowCreateTable(ASTNode ast) throws SemanticException {
    ShowCreateTableDesc showCreateTblDesc;
    String tableName = getUnescapedName((ASTNode)ast.getChild(0));
    showCreateTblDesc = new ShowCreateTableDesc(tableName, ctx.getResFile().toString());

    Table tab = getTable(tableName);
    if (tab.getTableType() == org.apache.hadoop.hive.metastore.TableType.INDEX_TABLE) {
      throw new SemanticException(ErrorMsg.SHOW_CREATETABLE_INDEX.getMsg(tableName
          + " has table type INDEX_TABLE"));
    }
    inputs.add(new ReadEntity(tab));
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showCreateTblDesc), conf));
    setFetchTask(createFetchTask(showCreateTblDesc.getSchema()));
  }

  private void analyzeShowDatabases(ASTNode ast) throws SemanticException {
    ShowDatabasesDesc showDatabasesDesc;
    if (ast.getChildCount() == 1) {
      String databasePattern = unescapeSQLString(ast.getChild(0).getText());
      showDatabasesDesc = new ShowDatabasesDesc(ctx.getResFile(), databasePattern);
    } else {
      showDatabasesDesc = new ShowDatabasesDesc(ctx.getResFile());
    }
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), showDatabasesDesc), conf));
    setFetchTask(createFetchTask(showDatabasesDesc.getSchema()));
  }

  private void analyzeShowTables(ASTNode ast) throws SemanticException {
    ShowTablesDesc showTblsDesc;
    String dbName = SessionState.get().getCurrentDatabase();
    String tableNames = null;

    if (ast.getChildCount() > 3) {
      throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
    }

    switch (ast.getChildCount()) {
    case 1: // Uses a pattern
      tableNames = unescapeSQLString(ast.getChild(0).getText());
      showTblsDesc = new ShowTablesDesc(ctx.getResFile(), dbName, tableNames);
      break;
    case 2: // Specifies a DB
      assert (ast.getChild(0).getType() == HiveParser.TOK_FROM);
      dbName = unescapeIdentifier(ast.getChild(1).getText());
      validateDatabase(dbName);
      showTblsDesc = new ShowTablesDesc(ctx.getResFile(), dbName);
      break;
    case 3: // Uses a pattern and specifies a DB
      assert (ast.getChild(0).getType() == HiveParser.TOK_FROM);
      dbName = unescapeIdentifier(ast.getChild(1).getText());
      tableNames = unescapeSQLString(ast.getChild(2).getText());
      validateDatabase(dbName);
      showTblsDesc = new ShowTablesDesc(ctx.getResFile(), dbName, tableNames);
      break;
    default: // No pattern or DB
      showTblsDesc = new ShowTablesDesc(ctx.getResFile(), dbName);
      break;
    }
    inputs.add(new ReadEntity(getDatabase(dbName)));
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showTblsDesc), conf));
    setFetchTask(createFetchTask(showTblsDesc.getSchema()));
  }

  private void analyzeShowColumns(ASTNode ast) throws SemanticException {
    String tableName = getUnescapedName((ASTNode) ast.getChild(0));
    if (ast.getChildCount() > 1) {
      if (tableName.contains(".")) {
        throw new SemanticException("Duplicates declaration for database name");
      }
      tableName = getUnescapedName((ASTNode) ast.getChild(1)) + "." + tableName;
    }
    Table tab = getTable(tableName);
    inputs.add(new ReadEntity(tab));

    ShowColumnsDesc showColumnsDesc = new ShowColumnsDesc(ctx.getResFile(), tableName);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showColumnsDesc), conf));
    setFetchTask(createFetchTask(showColumnsDesc.getSchema()));
  }

  private void analyzeShowTableStatus(ASTNode ast) throws SemanticException {
    ShowTableStatusDesc showTblStatusDesc;
    String tableNames = getUnescapedName((ASTNode) ast.getChild(0));
    String dbName = SessionState.get().getCurrentDatabase();
    int children = ast.getChildCount();
    HashMap<String, String> partSpec = null;
    if (children >= 2) {
      if (children > 3) {
        throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
      }
      for (int i = 1; i < children; i++) {
        ASTNode child = (ASTNode) ast.getChild(i);
        if (child.getToken().getType() == HiveParser.Identifier) {
          dbName = unescapeIdentifier(child.getText());
        } else if (child.getToken().getType() == HiveParser.TOK_PARTSPEC) {
          partSpec = getValidatedPartSpec(getTable(tableNames), child, conf, false);
        } else {
          throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
        }
      }
    }

    if (partSpec != null) {
      validateTable(tableNames, partSpec);
    }

    showTblStatusDesc = new ShowTableStatusDesc(ctx.getResFile().toString(), dbName,
        tableNames, partSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showTblStatusDesc), conf));
    setFetchTask(createFetchTask(showTblStatusDesc.getSchema()));
  }

  private void analyzeShowTableProperties(ASTNode ast) throws SemanticException {
    ShowTblPropertiesDesc showTblPropertiesDesc;
    String[] qualified = getQualifiedTableName((ASTNode) ast.getChild(0));
    String propertyName = null;
    if (ast.getChildCount() > 1) {
      propertyName = unescapeSQLString(ast.getChild(1).getText());
    }

    String tableNames = getDotName(qualified);
    validateTable(tableNames, null);

    showTblPropertiesDesc = new ShowTblPropertiesDesc(ctx.getResFile().toString(), tableNames,
        propertyName);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showTblPropertiesDesc), conf));
    setFetchTask(createFetchTask(showTblPropertiesDesc.getSchema()));
  }

  private void analyzeShowIndexes(ASTNode ast) throws SemanticException {
    ShowIndexesDesc showIndexesDesc;
    String tableName = getUnescapedName((ASTNode) ast.getChild(0));
    showIndexesDesc = new ShowIndexesDesc(tableName, ctx.getResFile());

    if (ast.getChildCount() == 2) {
      int descOptions = ast.getChild(1).getType();
      showIndexesDesc.setFormatted(descOptions == HiveParser.KW_FORMATTED);
    }

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showIndexesDesc), conf));
    setFetchTask(createFetchTask(showIndexesDesc.getSchema()));
  }

  /**
   * Add the task according to the parsed command tree. This is used for the CLI
   * command "SHOW FUNCTIONS;".
   *
   * @param ast
   *          The parsed command tree.
   * @throws SemanticException
   *           Parsin failed
   */
  private void analyzeShowFunctions(ASTNode ast) throws SemanticException {
    ShowFunctionsDesc showFuncsDesc;
    if (ast.getChildCount() == 1) {
      String funcNames = stripQuotes(ast.getChild(0).getText());
      showFuncsDesc = new ShowFunctionsDesc(ctx.getResFile(), funcNames);
    } else if (ast.getChildCount() == 2) {
      assert (ast.getChild(0).getType() == HiveParser.KW_LIKE);
      String funcNames = stripQuotes(ast.getChild(1).getText());
      showFuncsDesc = new ShowFunctionsDesc(ctx.getResFile(), funcNames, true);
    } else {
      showFuncsDesc = new ShowFunctionsDesc(ctx.getResFile());
    }
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showFuncsDesc), conf));
    setFetchTask(createFetchTask(showFuncsDesc.getSchema()));
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
    HashMap<String, String> partSpec = null;
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
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showLocksDesc), conf));
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
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showLocksDesc), conf));
    setFetchTask(createFetchTask(showLocksDesc.getSchema()));

    // Need to initialize the lock manager
    ctx.setNeedLockMgr(true);
  }

  private void analyzeShowConf(ASTNode ast) throws SemanticException {
    String confName = stripQuotes(ast.getChild(0).getText());
    ShowConfDesc showConfDesc = new ShowConfDesc(ctx.getResFile(), confName);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showConfDesc), conf));
    setFetchTask(createFetchTask(showConfDesc.getSchema()));
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
        HiveConf.getVar(conf, ConfVars.HIVEQUERYID));
    lockTblDesc.setQueryStr(this.ctx.getCmd());
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        lockTblDesc), conf));

    // Need to initialize the lock manager
    ctx.setNeedLockMgr(true);
  }

  /**
   * Add a task to execute "SHOW COMPACTIONS"
   * @param ast The parsed command tree.
   * @throws SemanticException Parsing failed.
   */
  private void analyzeShowCompactions(ASTNode ast) throws SemanticException {
    ShowCompactionsDesc desc = new ShowCompactionsDesc(ctx.getResFile());
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc), conf));
    setFetchTask(createFetchTask(desc.getSchema()));
  }

  /**
   * Add a task to execute "SHOW COMPACTIONS"
   * @param ast The parsed command tree.
   * @throws SemanticException Parsing failed.
   */
  private void analyzeShowTxns(ASTNode ast) throws SemanticException {
    ShowTxnsDesc desc = new ShowTxnsDesc(ctx.getResFile());
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc), conf));
    setFetchTask(createFetchTask(desc.getSchema()));
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
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        unlockTblDesc), conf));

    // Need to initialize the lock manager
    ctx.setNeedLockMgr(true);
  }

  private void analyzeLockDatabase(ASTNode ast) throws SemanticException {
    String dbName = unescapeIdentifier(ast.getChild(0).getText());
    String mode  = unescapeIdentifier(ast.getChild(1).getText().toUpperCase());

    //inputs.add(new ReadEntity(dbName));
    //outputs.add(new WriteEntity(dbName));
    LockDatabaseDesc lockDatabaseDesc = new LockDatabaseDesc(dbName, mode,
                        HiveConf.getVar(conf, ConfVars.HIVEQUERYID));
    lockDatabaseDesc.setQueryStr(ctx.getCmd());
    DDLWork work = new DDLWork(getInputs(), getOutputs(), lockDatabaseDesc);
    rootTasks.add(TaskFactory.get(work, conf));
    ctx.setNeedLockMgr(true);
  }

  private void analyzeUnlockDatabase(ASTNode ast) throws SemanticException {
    String dbName = unescapeIdentifier(ast.getChild(0).getText());

    UnlockDatabaseDesc unlockDatabaseDesc = new UnlockDatabaseDesc(dbName);
    DDLWork work = new DDLWork(getInputs(), getOutputs(), unlockDatabaseDesc);
    rootTasks.add(TaskFactory.get(work, conf));
    // Need to initialize the lock manager
    ctx.setNeedLockMgr(true);
  }

  /**
   * Add the task according to the parsed command tree. This is used for the CLI
   * command "DESCRIBE FUNCTION;".
   *
   * @param ast
   *          The parsed command tree.
   * @throws SemanticException
   *           Parsing failed
   */
  private void analyzeDescFunction(ASTNode ast) throws SemanticException {
    String funcName;
    boolean isExtended;

    if (ast.getChildCount() == 1) {
      funcName = stripQuotes(ast.getChild(0).getText());
      isExtended = false;
    } else if (ast.getChildCount() == 2) {
      funcName = stripQuotes(ast.getChild(0).getText());
      isExtended = true;
    } else {
      throw new SemanticException("Unexpected Tokens at DESCRIBE FUNCTION");
    }

    DescFunctionDesc descFuncDesc = new DescFunctionDesc(ctx.getResFile(),
        funcName, isExtended);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        descFuncDesc), conf));
    setFetchTask(createFetchTask(descFuncDesc.getSchema()));
  }


  private void analyzeAlterTableRename(String[] source, ASTNode ast, boolean expectView)
      throws SemanticException {
    String[] target = getQualifiedTableName((ASTNode) ast.getChild(0));

    String sourceName = getDotName(source);
    String targetName = getDotName(target);

    AlterTableDesc alterTblDesc = new AlterTableDesc(sourceName, targetName, expectView);
    addInputsOutputsAlterTable(sourceName, null, alterTblDesc);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableRenameCol(String[] qualified, ASTNode ast,
      HashMap<String, String> partSpec) throws SemanticException {
    String newComment = null;
    boolean first = false;
    String flagCol = null;
    boolean isCascade = false;
    //col_old_name col_new_name column_type [COMMENT col_comment] [FIRST|AFTER column_name] [CASCADE|RESTRICT]
    String oldColName = ast.getChild(0).getText();
    String newColName = ast.getChild(1).getText();
    String newType = getTypeStringFromAST((ASTNode) ast.getChild(2));
    int childCount = ast.getChildCount();
    for (int i = 3; i < childCount; i++) {
      ASTNode child = (ASTNode)ast.getChild(i);
      switch (child.getToken().getType()) {
        case HiveParser.StringLiteral:
          newComment = unescapeSQLString(child.getText());
          break;
        case HiveParser.TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION:
          flagCol = unescapeIdentifier(child.getChild(0).getText());
          break;
        case HiveParser.KW_FIRST:
          first = true;
          break;
        case HiveParser.TOK_CASCADE:
          isCascade = true;
          break;
        case HiveParser.TOK_RESTRICT:
          break;
        default:
          throw new SemanticException("Unsupported token: " + child.getToken()
              + " for alter table");
      }
    }

    /* Validate the operation of renaming a column name. */
    Table tab = getTable(qualified);

    SkewedInfo skewInfo = tab.getTTable().getSd().getSkewedInfo();
    if ((null != skewInfo)
        && (null != skewInfo.getSkewedColNames())
        && skewInfo.getSkewedColNames().contains(oldColName)) {
      throw new SemanticException(oldColName
          + ErrorMsg.ALTER_TABLE_NOT_ALLOWED_RENAME_SKEWED_COLUMN.getMsg());
    }

    String tblName = getDotName(qualified);
    AlterTableDesc alterTblDesc = new AlterTableDesc(tblName, partSpec,
        unescapeIdentifier(oldColName), unescapeIdentifier(newColName),
        newType, newComment, first, flagCol, isCascade);
    addInputsOutputsAlterTable(tblName, partSpec, alterTblDesc);

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableRenamePart(ASTNode ast, String tblName,
      HashMap<String, String> oldPartSpec) throws SemanticException {
    Table tab = getTable(tblName, true);
    validateAlterTableType(tab, AlterTableTypes.RENAMEPARTITION);
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
    RenamePartitionDesc renamePartitionDesc = new RenamePartitionDesc(tblName, oldPartSpec, newPartSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        renamePartitionDesc), conf));
  }

  private void analyzeAlterTableBucketNum(ASTNode ast, String tblName,
      HashMap<String, String> partSpec) throws SemanticException {
    Table tab = getTable(tblName, true);
    if (tab.getBucketCols() == null || tab.getBucketCols().isEmpty()) {
      throw new SemanticException(ErrorMsg.ALTER_BUCKETNUM_NONBUCKETIZED_TBL.getMsg());
    }
    validateAlterTableType(tab, AlterTableTypes.ALTERBUCKETNUM);
    inputs.add(new ReadEntity(tab));

    int bucketNum = Integer.parseInt(ast.getChild(0).getText());
    AlterTableDesc alterBucketNum = new AlterTableDesc(tblName, partSpec, bucketNum);

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterBucketNum), conf));
  }

  private void analyzeAlterTableModifyCols(String[] qualified, ASTNode ast,
      HashMap<String, String> partSpec, AlterTableTypes alterType) throws SemanticException {

    String tblName = getDotName(qualified);
    List<FieldSchema> newCols = getColumns((ASTNode) ast.getChild(0));
    boolean isCascade = false;
    if (null != ast.getFirstChildWithType(HiveParser.TOK_CASCADE)) {
      isCascade = true;
    }

    AlterTableDesc alterTblDesc = new AlterTableDesc(tblName, partSpec, newCols,
        alterType, isCascade);

    addInputsOutputsAlterTable(tblName, partSpec, alterTblDesc);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableDropParts(String[] qualified, ASTNode ast, boolean expectView)
      throws SemanticException {

    boolean ifExists = (ast.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null)
        || HiveConf.getBoolVar(conf, ConfVars.DROPIGNORESNONEXISTENT);
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
      tab = getTable(qualified);
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
    if (partSpecs.isEmpty()) return; // nothing to do

    validateAlterTableType(tab, AlterTableTypes.DROPPARTITION, expectView);
    ReadEntity re = new ReadEntity(tab);
    re.noLockNeeded();
    inputs.add(re);

    boolean ignoreProtection = ast.getFirstChildWithType(HiveParser.TOK_IGNOREPROTECTION) != null;
    addTableDropPartsOutputs(tab, partSpecs.values(), !ifExists, ignoreProtection);

    DropTableDesc dropTblDesc =
        new DropTableDesc(getDotName(qualified), partSpecs, expectView, ignoreProtection, mustPurge, replicationSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), dropTblDesc), conf));
  }

  private void analyzeAlterTablePartColType(String[] qualified, ASTNode ast)
      throws SemanticException {


    // check if table exists.
    Table tab = getTable(qualified);
    inputs.add(new ReadEntity(tab));

    // validate the DDL is a valid operation on the table.
    validateAlterTableType(tab, AlterTableTypes.ALTERPARTITION, false);

    // Alter table ... partition column ( column newtype) only takes one column at a time.
    // It must have a column name followed with type.
    ASTNode colAst = (ASTNode) ast.getChild(0);
    assert(colAst.getChildCount() == 2);

    FieldSchema newCol = new FieldSchema();

    // get column name
    String name = colAst.getChild(0).getText().toLowerCase();
    newCol.setName(unescapeIdentifier(name));

    // get column type
    ASTNode typeChild = (ASTNode) (colAst.getChild(1));
    newCol.setType(getTypeStringFromAST(typeChild));

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

    AlterTableAlterPartDesc alterTblAlterPartDesc =
            new AlterTableAlterPartDesc(getDotName(qualified), newCol);

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
            alterTblAlterPartDesc), conf));
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
  private void analyzeAlterTableAddParts(String[] qualified, CommonTree ast, boolean expectView)
      throws SemanticException {

    // ^(TOK_ALTERTABLE_ADDPARTS identifier ifNotExists? alterStatementSuffixAddPartitionsElement+)
    boolean ifNotExists = ast.getChild(0).getType() == HiveParser.TOK_IFNOTEXISTS;

    Table tab = getTable(qualified);
    boolean isView = tab.isView();
    validateAlterTableType(tab, AlterTableTypes.ADDPARTITION, expectView);
    outputs.add(new WriteEntity(tab, WriteEntity.WriteType.DDL_SHARED));

    int numCh = ast.getChildCount();
    int start = ifNotExists ? 1 : 0;

    String currentLocation = null;
    Map<String, String> currentPart = null;
    // Parser has done some verification, so the order of tokens doesn't need to be verified here.
    AddPartitionDesc addPartitionDesc =
        new AddPartitionDesc(tab.getDbName(), tab.getTableName(), ifNotExists);
    for (int num = start; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);
      switch (child.getToken().getType()) {
      case HiveParser.TOK_PARTSPEC:
        if (currentPart != null) {
          addPartitionDesc.addPartition(currentPart, currentLocation);
          currentLocation = null;
        }
        currentPart = getValidatedPartSpec(tab, child, conf, true);
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
      addPartitionDesc.addPartition(currentPart, currentLocation);
    }

    if (addPartitionDesc.getPartitionCount() == 0) {
      // nothing to do
      return;
    }

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), addPartitionDesc), conf));

    if (isView) {
      // Compile internal query to capture underlying table partition dependencies
      StringBuilder cmd = new StringBuilder();
      cmd.append("SELECT * FROM ");
      cmd.append(HiveUtils.unparseIdentifier(getDotName(qualified)));
      cmd.append(" WHERE ");
      boolean firstOr = true;
      for (int i = 0; i < addPartitionDesc.getPartitionCount(); ++i) {
        AddPartitionDesc.OnePartitionDesc partitionDesc = addPartitionDesc.getPartition(i);
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
          cmd.append(HiveUtils.unparseIdentifier(entry.getKey(), conf));
          cmd.append(" = '");
          cmd.append(HiveUtils.escapeString(entry.getValue()));
          cmd.append("'");
        }
        cmd.append(")");
      }
      Driver driver = new Driver(conf);
      int rc = driver.compile(cmd.toString(), false);
      if (rc != 0) {
        throw new SemanticException(ErrorMsg.NO_VALID_PARTN.getMsg());
      }
      inputs.addAll(driver.getPlan().getInputs());
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
  private void analyzeAlterTableTouch(String[] qualified, CommonTree ast)
      throws SemanticException {

    Table tab = getTable(qualified);
    validateAlterTableType(tab, AlterTableTypes.TOUCH);
    inputs.add(new ReadEntity(tab));

    // partition name to value
    List<Map<String, String>> partSpecs = getPartitionSpecs(tab, ast);

    if (partSpecs.size() == 0) {
      AlterTableSimpleDesc touchDesc = new AlterTableSimpleDesc(
          getDotName(qualified), null,
          AlterTableDesc.AlterTableTypes.TOUCH);
      outputs.add(new WriteEntity(tab, WriteEntity.WriteType.DDL_NO_LOCK));
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          touchDesc), conf));
    } else {
      addTablePartsOutputs(tab, partSpecs, WriteEntity.WriteType.DDL_NO_LOCK);
      for (Map<String, String> partSpec : partSpecs) {
        AlterTableSimpleDesc touchDesc = new AlterTableSimpleDesc(
            getDotName(qualified), partSpec,
            AlterTableDesc.AlterTableTypes.TOUCH);
        rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
            touchDesc), conf));
      }
    }
  }

  private void analyzeAlterTableArchive(String[] qualified, CommonTree ast, boolean isUnArchive)
      throws SemanticException {

    if (!conf.getBoolVar(HiveConf.ConfVars.HIVEARCHIVEENABLED)) {
      throw new SemanticException(ErrorMsg.ARCHIVE_METHODS_DISABLED.getMsg());

    }
    Table tab = getTable(qualified);
    // partition name to value
    List<Map<String, String>> partSpecs = getPartitionSpecs(tab, ast);

    addTablePartsOutputs(tab, partSpecs, true, WriteEntity.WriteType.DDL_NO_LOCK);
    validateAlterTableType(tab, AlterTableTypes.ARCHIVE);
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
    AlterTableSimpleDesc archiveDesc = new AlterTableSimpleDesc(
        getDotName(qualified), partSpec,
        (isUnArchive ? AlterTableTypes.UNARCHIVE : AlterTableTypes.ARCHIVE));
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        archiveDesc), conf));

  }

  /**
   * Verify that the information in the metastore matches up with the data on
   * the fs.
   *
   * @param ast
   *          Query tree.
   * @throws SemanticException
   */
  private void analyzeMetastoreCheck(CommonTree ast) throws SemanticException {
    String tableName = null;
    boolean repair = false;
    if (ast.getChildCount() > 0) {
      repair = ast.getChild(0).getType() == HiveParser.KW_REPAIR;
      if (!repair) {
        tableName = getUnescapedName((ASTNode) ast.getChild(0));
      } else if (ast.getChildCount() > 1) {
        tableName = getUnescapedName((ASTNode) ast.getChild(1));
      }
    }
    List<Map<String, String>> specs = getPartitionSpecs(getTable(tableName), ast);
    MsckDesc checkDesc = new MsckDesc(tableName, specs, ctx.getResFile(),
        repair);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        checkDesc), conf));
  }

  /**
   * Get the partition specs from the tree.
   *
   * @param ast
   *          Tree to extract partitions from.
   * @return A list of partition name to value mappings.
   * @throws SemanticException
   */
  private List<Map<String, String>> getPartitionSpecs(Table tbl, CommonTree ast)
      throws SemanticException {
    List<Map<String, String>> partSpecs = new ArrayList<Map<String, String>>();
    int childIndex = 0;
    // get partition metadata if partition specified
    for (childIndex = 0; childIndex < ast.getChildCount(); childIndex++) {
      ASTNode partSpecNode = (ASTNode)ast.getChild(childIndex);
      // sanity check
      if (partSpecNode.getType() == HiveParser.TOK_PARTSPEC) {
        Map<String,String> partSpec = getValidatedPartSpec(tbl, partSpecNode, conf, false);
        partSpecs.add(partSpec);
      }
    }
    return partSpecs;
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
    Map<String, String> colTypes = new HashMap<String, String>();
    for (FieldSchema fs : tab.getPartitionKeys()) {
      colTypes.put(fs.getName().toLowerCase(), fs.getType());
    }

    Map<Integer, List<ExprNodeGenericFuncDesc>> result =
        new HashMap<Integer, List<ExprNodeGenericFuncDesc>>();
    for (int childIndex = 0; childIndex < ast.getChildCount(); childIndex++) {
      Tree partSpecTree = ast.getChild(childIndex);
      if (partSpecTree.getType() != HiveParser.TOK_PARTSPEC) continue;
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

        String type = colTypes.get(key);
        if (type == null) {
          throw new SemanticException("Column " + key + " not found");
        }
        // Create the corresponding hive expression to filter on partition columns.
        PrimitiveTypeInfo pti = TypeInfoFactory.getPrimitiveTypeInfo(type);
        Object val = valExpr.getValue();
        if (!valExpr.getTypeString().equals(type)) {
          Converter converter = ObjectInspectorConverters.getConverter(
            TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(valExpr.getTypeInfo()),
            TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(pti));
          val = converter.convert(valExpr.getValue());
        }
        ExprNodeColumnDesc column = new ExprNodeColumnDesc(pti, key, null, true);
        ExprNodeGenericFuncDesc op = makeBinaryPredicate(operator, column,
            new ExprNodeConstantDesc(pti, val));
        // If it's multi-expr filter (e.g. a='5', b='2012-01-02'), AND with previous exprs.
        expr = (expr == null) ? op : makeBinaryPredicate("and", expr, op);
        names.add(key);
      }
      if (expr == null) continue;
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

  private static ExprNodeGenericFuncDesc makeBinaryPredicate(
      String fn, ExprNodeDesc left, ExprNodeDesc right) throws SemanticException {
      return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
          FunctionRegistry.getFunctionInfo(fn).getGenericUDF(), Lists.newArrayList(left, right));
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
      if (!partSpecKeys.contains(fs.getName())) break;
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
      Collection<List<ExprNodeGenericFuncDesc>> partSpecs, boolean throwIfNonExistent,
      boolean ignoreProtection) throws SemanticException {

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
          // TODO: same thing, metastore already checks this but check here if we can.
          if (!ignoreProtection && !p.canDrop()) {
            throw new SemanticException(
              ErrorMsg.DROP_COMMAND_NOT_ALLOWED_FOR_PARTITION.getMsg(p.getCompleteName()));
          }
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
  private void analyzeAltertableSkewedby(String[] qualified, ASTNode ast) throws SemanticException {
    /**
     * Throw an error if the user tries to use the DDL with
     * hive.internal.ddl.list.bucketing.enable set to false.
     */
    HiveConf hiveConf = SessionState.get().getConf();

    Table tab = getTable(qualified);

    inputs.add(new ReadEntity(tab));
    outputs.add(new WriteEntity(tab, WriteEntity.WriteType.DDL_EXCLUSIVE));

    validateAlterTableType(tab, AlterTableTypes.ADDSKEWEDBY);

    String tableName = getDotName(qualified);
    if (ast.getChildCount() == 0) {
      /* Convert a skewed table to non-skewed table. */
      AlterTableDesc alterTblDesc = new AlterTableDesc(tableName, true,
          new ArrayList<String>(), new ArrayList<List<String>>());
      alterTblDesc.setStoredAsSubDirectories(false);
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          alterTblDesc), conf));
    } else {
      switch (((ASTNode) ast.getChild(0)).getToken().getType()) {
      case HiveParser.TOK_TABLESKEWED:
        handleAlterTableSkewedBy(ast, tableName, tab);
        break;
      case HiveParser.TOK_STOREDASDIRS:
        handleAlterTableDisableStoredAsDirs(tableName, tab);
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
  private void handleAlterTableDisableStoredAsDirs(String tableName, Table tab)
      throws SemanticException {
  List<String> skewedColNames = tab.getSkewedColNames();
    List<List<String>> skewedColValues = tab.getSkewedColValues();
    if ((skewedColNames == null) || (skewedColNames.size() == 0) || (skewedColValues == null)
        || (skewedColValues.size() == 0)) {
      throw new SemanticException(ErrorMsg.ALTER_TBL_STOREDASDIR_NOT_SKEWED.getMsg(tableName));
    }
    AlterTableDesc alterTblDesc = new AlterTableDesc(tableName, false,
        skewedColNames, skewedColValues);
    alterTblDesc.setStoredAsSubDirectories(false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  /**
   * Process "alter table <name> skewed by .. on .. stored as directories
   * @param ast
   * @param tableName
   * @param tab
   * @throws SemanticException
   */
  private void handleAlterTableSkewedBy(ASTNode ast, String tableName, Table tab)
      throws SemanticException {
    List<String> skewedColNames = new ArrayList<String>();
    List<List<String>> skewedValues = new ArrayList<List<String>>();
    /* skewed column names. */
    ASTNode skewedNode = (ASTNode) ast.getChild(0);
    skewedColNames = analyzeSkewedTablDDLColNames(skewedColNames, skewedNode);
    /* skewed value. */
    analyzeDDLSkewedValues(skewedValues, skewedNode);
    // stored as directories
    boolean storedAsDirs = analyzeStoredAdDirs(skewedNode);


    AlterTableDesc alterTblDesc = new AlterTableDesc(tableName, false,
        skewedColNames, skewedValues);
    alterTblDesc.setStoredAsSubDirectories(storedAsDirs);
    /**
     * Validate information about skewed table
     */
    alterTblDesc.setTable(tab);
    alterTblDesc.validate();
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  /**
   * Analyze skewed column names
   *
   * @param skewedColNames
   * @param child
   * @return
   * @throws SemanticException
   */
  private List<String> analyzeAlterTableSkewedColNames(List<String> skewedColNames,
      ASTNode child) throws SemanticException {
    Tree nNode = child.getChild(0);
    if (nNode == null) {
      throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_NAME.getMsg());
    } else {
      ASTNode nAstNode = (ASTNode) nNode;
      if (nAstNode.getToken().getType() != HiveParser.TOK_TABCOLNAME) {
        throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_NAME.getMsg());
      } else {
        skewedColNames = getColumnNames(nAstNode);
      }
    }
    return skewedColNames;
  }

  /**
   * Given a ASTNode, return list of values.
   *
   * use case:
   * create table xyz list bucketed (col1) with skew (1,2,5)
   * AST Node is for (1,2,5)
   *
   * @param ast
   * @return
   */
  private List<String> getColumnValues(ASTNode ast) {
    List<String> colList = new ArrayList<String>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      colList.add(stripQuotes(child.getText()).toLowerCase());
    }
    return colList;
  }


  /**
   * Analyze alter table's skewed location
   *
   * @param ast
   * @param tableName
   * @param partSpec
   * @throws SemanticException
   */
  private void analyzeAlterTableSkewedLocation(ASTNode ast, String tableName,
      HashMap<String, String> partSpec) throws SemanticException {
    /**
     * Throw an error if the user tries to use the DDL with
     * hive.internal.ddl.list.bucketing.enable set to false.
     */
    HiveConf hiveConf = SessionState.get().getConf();
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
    AlterTableDesc alterTblDesc = new AlterTableDesc(tableName, locations, partSpec);
    addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
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
    boolean result = false;
    switch(node.getToken().getType()) {
      case HiveParser.Number:
        result = true;
        break;
      case HiveParser.StringLiteral:
        result = true;
        break;
      case HiveParser.BigintLiteral:
        result = true;
        break;
      case HiveParser.SmallintLiteral:
        result = true;
        break;
      case HiveParser.TinyintLiteral:
        result = true;
        break;
      case HiveParser.DecimalLiteral:
        result = true;
        break;
      case HiveParser.CharSetName:
        result = true;
        break;
      case HiveParser.KW_TRUE:
      case HiveParser.KW_FALSE:
        result = true;
        break;
      default:
          break;
    }
    return result;
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

  private HiveAuthorizationTaskFactory createAuthorizationTaskFactory(HiveConf conf, Hive db) {
    Class<? extends HiveAuthorizationTaskFactory> authProviderClass = conf.
        getClass(HiveConf.ConfVars.HIVE_AUTHORIZATION_TASK_FACTORY.varname,
            HiveAuthorizationTaskFactoryImpl.class,
            HiveAuthorizationTaskFactory.class);
    String msg = "Unable to create instance of " + authProviderClass.getName() + ": ";
    try {
      Constructor<? extends HiveAuthorizationTaskFactory> constructor =
          authProviderClass.getConstructor(HiveConf.class, Hive.class);
      return constructor.newInstance(conf, db);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (SecurityException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (InstantiationException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (InvocationTargetException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    }
  }

}
