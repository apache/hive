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

import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_CASCADE;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_DATABASECOMMENT;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_IFEXISTS;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_IFNOTEXISTS;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_SHOWDATABASES;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils;
import org.apache.hadoop.hive.ql.exec.FetchTask;
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
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.AlterDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.AlterIndexDesc;
import org.apache.hadoop.hive.ql.plan.AlterIndexDesc.AlterIndexTypes;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes;
import org.apache.hadoop.hive.ql.plan.AlterTableSimpleDesc;
import org.apache.hadoop.hive.ql.plan.CreateDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.CreateIndexDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DescDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DescFunctionDesc;
import org.apache.hadoop.hive.ql.plan.DescTableDesc;
import org.apache.hadoop.hive.ql.plan.DropDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DropIndexDesc;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.GrantDesc;
import org.apache.hadoop.hive.ql.plan.GrantRevokeRoleDDL;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LockTableDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.MsckDesc;
import org.apache.hadoop.hive.ql.plan.PartitionSpec;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.plan.RenamePartitionDesc;
import org.apache.hadoop.hive.ql.plan.RevokeDesc;
import org.apache.hadoop.hive.ql.plan.RoleDDLDesc;
import org.apache.hadoop.hive.ql.plan.ShowColumnsDesc;
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
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.SwitchDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.UnlockTableDesc;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.ql.security.authorization.PrivilegeRegistry;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * DDLSemanticAnalyzer.
 *
 */
public class DDLSemanticAnalyzer extends BaseSemanticAnalyzer {
  private static final Log LOG = LogFactory.getLog(DDLSemanticAnalyzer.class);
  private static final Map<Integer, String> TokenToTypeName = new HashMap<Integer, String>();

  private final Set<String> reservedPartitionValues;
  static {
    TokenToTypeName.put(HiveParser.TOK_BOOLEAN, Constants.BOOLEAN_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_TINYINT, Constants.TINYINT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_SMALLINT, Constants.SMALLINT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_INT, Constants.INT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_BIGINT, Constants.BIGINT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_FLOAT, Constants.FLOAT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DOUBLE, Constants.DOUBLE_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_STRING, Constants.STRING_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_BINARY, Constants.BINARY_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DATE, Constants.DATE_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DATETIME, Constants.DATETIME_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_TIMESTAMP, Constants.TIMESTAMP_TYPE_NAME);
  }

  public static String getTypeName(int token) throws SemanticException {
    // date and datetime types aren't currently supported
    if (token == HiveParser.TOK_DATE || token == HiveParser.TOK_DATETIME) {
      throw new SemanticException(ErrorMsg.UNSUPPORTED_TYPE.getMsg());
    }
    return TokenToTypeName.get(token);
  }

  static class TablePartition {
    String tableName;
    HashMap<String, String> partSpec = null;

    public TablePartition() {
    }

    public TablePartition(ASTNode tblPart) throws SemanticException {
      tableName = unescapeIdentifier(tblPart.getChild(0).getText());
      if (tblPart.getChildCount() > 1) {
        ASTNode part = (ASTNode) tblPart.getChild(1);
        if (part.getToken().getType() == HiveParser.TOK_PARTSPEC) {
          this.partSpec = DDLSemanticAnalyzer.getPartSpec(part);
        }
      }
    }
  }

  public DDLSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
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
  public void analyzeInternal(ASTNode ast) throws SemanticException {

    switch (ast.getToken().getType()) {
    case HiveParser.TOK_ALTERTABLE_PARTITION: {
      ASTNode tablePart = (ASTNode) ast.getChild(0);
      TablePartition tblPart = new TablePartition(tablePart);
      String tableName = tblPart.tableName;
      HashMap<String, String> partSpec = tblPart.partSpec;
      ast = (ASTNode) ast.getChild(1);
      if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_FILEFORMAT) {
        analyzeAlterTableFileFormat(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ALTERPARTS_PROTECTMODE) {
        analyzeAlterTableProtectMode(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_LOCATION) {
        analyzeAlterTableLocation(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ALTERPARTS_MERGEFILES) {
        analyzeAlterTablePartMergeFiles(tablePart, ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SERIALIZER) {
        analyzeAlterTableSerde(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES) {
        analyzeAlterTableSerdeProps(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_RENAMEPART) {
        analyzeAlterTableRenamePart(ast, tableName, partSpec);
      } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTBLPART_SKEWED_LOCATION) {
        analyzeAlterTableSkewedLocation(ast, tableName, partSpec);
      }
      break;
    }
    case HiveParser.TOK_DROPTABLE:
      analyzeDropTable(ast, false);
      break;
    case HiveParser.TOK_CREATEINDEX:
      analyzeCreateIndex(ast);
      break;
    case HiveParser.TOK_DROPINDEX:
      analyzeDropIndex(ast);
      break;
    case HiveParser.TOK_DESCTABLE:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeDescribeTable(ast);
      break;
    case TOK_SHOWDATABASES:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowDatabases(ast);
      break;
    case HiveParser.TOK_SHOWTABLES:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowTables(ast);
      break;
    case HiveParser.TOK_SHOWCOLUMNS:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowColumns(ast);
      break;
    case HiveParser.TOK_SHOW_TABLESTATUS:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowTableStatus(ast);
      break;
    case HiveParser.TOK_SHOW_TBLPROPERTIES:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowTableProperties(ast);
      break;
    case HiveParser.TOK_SHOWFUNCTIONS:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowFunctions(ast);
      break;
    case HiveParser.TOK_SHOWLOCKS:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowLocks(ast);
      break;
    case HiveParser.TOK_DESCFUNCTION:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeDescFunction(ast);
      break;
    case HiveParser.TOK_DESCDATABASE:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeDescDatabase(ast);
      break;
    case HiveParser.TOK_MSCK:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeMetastoreCheck(ast);
      break;
    case HiveParser.TOK_DROPVIEW:
      analyzeDropTable(ast, true);
      break;
    case HiveParser.TOK_ALTERVIEW_PROPERTIES:
      analyzeAlterTableProps(ast, true);
      break;
    case HiveParser.TOK_ALTERVIEW_ADDPARTS:
      // for ALTER VIEW ADD PARTITION, we wrapped the ADD to discriminate
      // view from table; unwrap it now
      analyzeAlterTableAddParts((ASTNode) ast.getChild(0), true);
      break;
    case HiveParser.TOK_ALTERVIEW_DROPPARTS:
      // for ALTER VIEW DROP PARTITION, we wrapped the DROP to discriminate
      // view from table; unwrap it now
      analyzeAlterTableDropParts((ASTNode) ast.getChild(0), true);
      break;
    case HiveParser.TOK_ALTERVIEW_RENAME:
      // for ALTER VIEW RENAME, we wrapped the RENAME to discriminate
      // view from table; unwrap it now
      analyzeAlterTableRename(((ASTNode) ast.getChild(0)), true);
      break;
    case HiveParser.TOK_ALTERTABLE_RENAME:
      analyzeAlterTableRename(ast, false);
      break;
    case HiveParser.TOK_ALTERTABLE_TOUCH:
      analyzeAlterTableTouch(ast);
      break;
    case HiveParser.TOK_ALTERTABLE_ARCHIVE:
      analyzeAlterTableArchive(ast, false);
      break;
    case HiveParser.TOK_ALTERTABLE_UNARCHIVE:
      analyzeAlterTableArchive(ast, true);
      break;
    case HiveParser.TOK_ALTERTABLE_ADDCOLS:
      analyzeAlterTableModifyCols(ast, AlterTableTypes.ADDCOLS);
      break;
    case HiveParser.TOK_ALTERTABLE_REPLACECOLS:
      analyzeAlterTableModifyCols(ast, AlterTableTypes.REPLACECOLS);
      break;
    case HiveParser.TOK_ALTERTABLE_RENAMECOL:
      analyzeAlterTableRenameCol(ast);
      break;
    case HiveParser.TOK_ALTERTABLE_ADDPARTS:
      analyzeAlterTableAddParts(ast, false);
      break;
    case HiveParser.TOK_ALTERTABLE_DROPPARTS:
      analyzeAlterTableDropParts(ast, false);
      break;
    case HiveParser.TOK_ALTERTABLE_PROPERTIES:
      analyzeAlterTableProps(ast, false);
      break;
    case HiveParser.TOK_ALTERTABLE_CLUSTER_SORT:
      analyzeAlterTableClusterSort(ast);
      break;
    case HiveParser.TOK_ALTERINDEX_REBUILD:
      analyzeAlterIndexRebuild(ast);
      break;
    case HiveParser.TOK_ALTERINDEX_PROPERTIES:
      analyzeAlterIndexProps(ast);
      break;
    case HiveParser.TOK_SHOWPARTITIONS:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowPartitions(ast);
      break;
    case HiveParser.TOK_SHOW_CREATETABLE:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowCreateTable(ast);
      break;
    case HiveParser.TOK_SHOWINDEXES:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowIndexes(ast);
      break;
    case HiveParser.TOK_LOCKTABLE:
      analyzeLockTable(ast);
      break;
    case HiveParser.TOK_UNLOCKTABLE:
      analyzeUnlockTable(ast);
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
      analyzeAlterDatabase(ast);
      break;
    case HiveParser.TOK_CREATEROLE:
      analyzeCreateRole(ast);
      break;
    case HiveParser.TOK_DROPROLE:
      analyzeDropRole(ast);
      break;
    case HiveParser.TOK_SHOW_ROLE_GRANT:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowRoleGrant(ast);
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
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowGrant(ast);
      break;
    case HiveParser.TOK_REVOKE:
      analyzeRevoke(ast);
      break;
    case HiveParser.TOK_ALTERTABLE_SKEWED:
      analyzeAltertableSkewedby(ast);
      break;
    default:
      throw new SemanticException("Unsupported command.");
    }
  }

  private void analyzeGrantRevokeRole(boolean grant, ASTNode ast) {
    List<PrincipalDesc> principalDesc = analyzePrincipalListDef(
        (ASTNode) ast.getChild(0));
    List<String> roles = new ArrayList<String>();
    for (int i = 1; i < ast.getChildCount(); i++) {
      roles.add(unescapeIdentifier(ast.getChild(i).getText()));
    }
    String roleOwnerName = "";
    if (SessionState.get() != null
        && SessionState.get().getAuthenticator() != null) {
      roleOwnerName = SessionState.get().getAuthenticator().getUserName();
    }
    GrantRevokeRoleDDL grantRevokeRoleDDL = new GrantRevokeRoleDDL(grant,
        roles, principalDesc, roleOwnerName, PrincipalType.USER, true);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        grantRevokeRoleDDL), conf));
  }

  private void analyzeShowGrant(ASTNode ast) throws SemanticException {
    PrivilegeObjectDesc privHiveObj = null;

    ASTNode principal = (ASTNode) ast.getChild(0);
    PrincipalType type = PrincipalType.USER;
    switch (principal.getType()) {
    case HiveParser.TOK_USER:
      type = PrincipalType.USER;
      break;
    case HiveParser.TOK_GROUP:
      type = PrincipalType.GROUP;
      break;
    case HiveParser.TOK_ROLE:
      type = PrincipalType.ROLE;
      break;
    }
    String principalName = unescapeIdentifier(principal.getChild(0).getText());
    PrincipalDesc principalDesc = new PrincipalDesc(principalName, type);
    List<String> cols = null;
    if (ast.getChildCount() > 1) {
      ASTNode child = (ASTNode) ast.getChild(1);
      if (child.getToken().getType() == HiveParser.TOK_PRIV_OBJECT_COL) {
        privHiveObj = new PrivilegeObjectDesc();
        privHiveObj.setObject(unescapeIdentifier(child.getChild(0).getText()));
        if (child.getChildCount() > 1) {
          for (int i = 1; i < child.getChildCount(); i++) {
            ASTNode grandChild = (ASTNode) child.getChild(i);
            if (grandChild.getToken().getType() == HiveParser.TOK_PARTSPEC) {
              privHiveObj.setPartSpec(DDLSemanticAnalyzer.getPartSpec(grandChild));
            } else if (grandChild.getToken().getType() == HiveParser.TOK_TABCOLNAME) {
              cols = getColumnNames((ASTNode) grandChild);
            } else {
              privHiveObj.setTable(child.getChild(i) != null);
            }
          }
        }
      }
    }

    if (privHiveObj == null && cols != null) {
      throw new SemanticException(
          "For user-level privileges, column sets should be null. columns="
              + cols.toString());
    }

    ShowGrantDesc showGrant = new ShowGrantDesc(ctx.getResFile().toString(),
        principalDesc, privHiveObj, cols);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showGrant), conf));
  }

  private void analyzeGrant(ASTNode ast) throws SemanticException {
    List<PrivilegeDesc> privilegeDesc = analyzePrivilegeListDef(
        (ASTNode) ast.getChild(0));
    List<PrincipalDesc> principalDesc = analyzePrincipalListDef(
        (ASTNode) ast.getChild(1));
    boolean grantOption = false;
    PrivilegeObjectDesc privilegeObj = null;

    if (ast.getChildCount() > 2) {
      for (int i = 2; i < ast.getChildCount(); i++) {
        ASTNode astChild = (ASTNode) ast.getChild(i);
        if (astChild.getType() == HiveParser.TOK_GRANT_WITH_OPTION) {
          grantOption = true;
        } else if (astChild.getType() == HiveParser.TOK_PRIV_OBJECT) {
          privilegeObj = analyzePrivilegeObject(astChild, getOutputs());
        }
      }
    }

    String userName = null;
    if (SessionState.get() != null
        && SessionState.get().getAuthenticator() != null) {
      userName = SessionState.get().getAuthenticator().getUserName();
    }

    GrantDesc grantDesc = new GrantDesc(privilegeObj, privilegeDesc,
        principalDesc, userName, PrincipalType.USER, grantOption);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        grantDesc), conf));
  }

  private void analyzeRevoke(ASTNode ast) throws SemanticException {
    List<PrivilegeDesc> privilegeDesc = analyzePrivilegeListDef(
        (ASTNode) ast.getChild(0));
    List<PrincipalDesc> principalDesc = analyzePrincipalListDef(
        (ASTNode) ast.getChild(1));
    PrivilegeObjectDesc hiveObj = null;
    if (ast.getChildCount() > 2) {
      ASTNode astChild = (ASTNode) ast.getChild(2);
      hiveObj = analyzePrivilegeObject(astChild, getOutputs());
    }

    RevokeDesc revokeDesc = new RevokeDesc(privilegeDesc, principalDesc, hiveObj);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        revokeDesc), conf));
  }

  private PrivilegeObjectDesc analyzePrivilegeObject(ASTNode ast,
      HashSet<WriteEntity> outputs)
      throws SemanticException {
    PrivilegeObjectDesc subject = new PrivilegeObjectDesc();
    subject.setObject(unescapeIdentifier(ast.getChild(0).getText()));
    if (ast.getChildCount() > 1) {
      for (int i = 0; i < ast.getChildCount(); i++) {
        ASTNode astChild = (ASTNode) ast.getChild(i);
        if (astChild.getToken().getType() == HiveParser.TOK_PARTSPEC) {
          subject.setPartSpec(DDLSemanticAnalyzer.getPartSpec(astChild));
        } else {
          subject.setTable(ast.getChild(0) != null);
        }
      }
    }

    try {
      if (subject.getTable()) {
        Table tbl = db.getTable(subject.getObject());
        if (subject.getPartSpec() != null) {
          Partition part = db.getPartition(tbl, subject.getPartSpec(), false);
          outputs.add(new WriteEntity(part));
        } else {
          outputs.add(new WriteEntity(tbl));
        }
      }
    } catch (HiveException e) {
      throw new SemanticException(e);
    }

    return subject;
  }

  private List<PrincipalDesc> analyzePrincipalListDef(ASTNode node) {
    List<PrincipalDesc> principalList = new ArrayList<PrincipalDesc>();

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      PrincipalType type = null;
      switch (child.getType()) {
      case HiveParser.TOK_USER:
        type = PrincipalType.USER;
        break;
      case HiveParser.TOK_GROUP:
        type = PrincipalType.GROUP;
        break;
      case HiveParser.TOK_ROLE:
        type = PrincipalType.ROLE;
        break;
      }
      String principalName = unescapeIdentifier(child.getChild(0).getText());
      PrincipalDesc principalDesc = new PrincipalDesc(principalName, type);
      principalList.add(principalDesc);
    }

    return principalList;
  }

  private List<PrivilegeDesc> analyzePrivilegeListDef(ASTNode node)
      throws SemanticException {
    List<PrivilegeDesc> ret = new ArrayList<PrivilegeDesc>();
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode privilegeDef = (ASTNode) node.getChild(i);
      ASTNode privilegeType = (ASTNode) privilegeDef.getChild(0);
      Privilege privObj = PrivilegeRegistry.getPrivilege(privilegeType.getType());

      if (privObj == null) {
        throw new SemanticException("undefined privilege " + privilegeType.getType());
      }
      List<String> cols = null;
      if (privilegeDef.getChildCount() > 1) {
        cols = getColumnNames((ASTNode) privilegeDef.getChild(1));
      }
      PrivilegeDesc privilegeDesc = new PrivilegeDesc(privObj, cols);
      ret.add(privilegeDesc);
    }
    return ret;
  }

  private void analyzeCreateRole(ASTNode ast) {
    String roleName = unescapeIdentifier(ast.getChild(0).getText());
    RoleDDLDesc createRoleDesc = new RoleDDLDesc(roleName,
        RoleDDLDesc.RoleOperation.CREATE_ROLE);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        createRoleDesc), conf));
  }

  private void analyzeDropRole(ASTNode ast) {
    String roleName = unescapeIdentifier(ast.getChild(0).getText());
    RoleDDLDesc createRoleDesc = new RoleDDLDesc(roleName,
        RoleDDLDesc.RoleOperation.DROP_ROLE);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        createRoleDesc), conf));
  }

  private void analyzeShowRoleGrant(ASTNode ast) {
    ASTNode child = (ASTNode) ast.getChild(0);
    PrincipalType principalType = PrincipalType.USER;
    switch (child.getType()) {
    case HiveParser.TOK_USER:
      principalType = PrincipalType.USER;
      break;
    case HiveParser.TOK_GROUP:
      principalType = PrincipalType.GROUP;
      break;
    case HiveParser.TOK_ROLE:
      principalType = PrincipalType.ROLE;
      break;
    }
    String principalName = unescapeIdentifier(child.getChild(0).getText());
    RoleDDLDesc createRoleDesc = new RoleDDLDesc(principalName, principalType,
        RoleDDLDesc.RoleOperation.SHOW_ROLE_GRANT, null);
    createRoleDesc.setResFile(ctx.getResFile().toString());
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        createRoleDesc), conf));
  }

  private void analyzeAlterDatabase(ASTNode ast) throws SemanticException {

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

    // currently alter database command can only change properties
    AlterDatabaseDesc alterDesc = new AlterDatabaseDesc(dbName, null, null, false);
    alterDesc.setDatabaseProperties(dbProps);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterDesc),
        conf));

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
      case TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      case TOK_DATABASECOMMENT:
        dbComment = unescapeSQLString(childNode.getChild(0).getText());
        break;
      case HiveParser.TOK_DATABASEPROPERTIES:
        dbProps = DDLSemanticAnalyzer.getProps((ASTNode) childNode.getChild(0));
        break;
      case HiveParser.TOK_DATABASELOCATION:
        dbLocation = unescapeSQLString(childNode.getChild(0).getText());
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

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        createDatabaseDesc), conf));
  }

  private void analyzeDropDatabase(ASTNode ast) throws SemanticException {
    String dbName = unescapeIdentifier(ast.getChild(0).getText());
    boolean ifExists = false;
    boolean ifCascade = false;

    if (null != ast.getFirstChildWithType(TOK_IFEXISTS)) {
      ifExists = true;
    }

    if (null != ast.getFirstChildWithType(TOK_CASCADE)) {
      ifCascade = true;
    }

    DropDatabaseDesc dropDatabaseDesc = new DropDatabaseDesc(dbName, ifExists, ifCascade);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), dropDatabaseDesc), conf));
  }

  private void analyzeSwitchDatabase(ASTNode ast) {
    String dbName = unescapeIdentifier(ast.getChild(0).getText());
    SwitchDatabaseDesc switchDatabaseDesc = new SwitchDatabaseDesc(dbName);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        switchDatabaseDesc), conf));
  }



  private void analyzeDropTable(ASTNode ast, boolean expectView)
      throws SemanticException {
    String tableName = getUnescapedName((ASTNode) ast.getChild(0));
    boolean ifExists = (ast.getFirstChildWithType(TOK_IFEXISTS) != null);
    // we want to signal an error if the table/view doesn't exist and we're
    // configured not to fail silently
    boolean throwException =
        !ifExists && !HiveConf.getBoolVar(conf, ConfVars.DROPIGNORESNONEXISTENT);
    try {
      Table tab = db.getTable(db.getCurrentDatabase(), tableName, throwException);
      if (tab != null) {
        inputs.add(new ReadEntity(tab));
        outputs.add(new WriteEntity(tab));
      }
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName));
    }

    DropTableDesc dropTblDesc = new DropTableDesc(
        tableName, expectView, ifExists, true);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        dropTblDesc), conf));
  }

  private void analyzeCreateIndex(ASTNode ast) throws SemanticException {
    String indexName = unescapeIdentifier(ast.getChild(0).getText());
    String typeName = unescapeSQLString(ast.getChild(1).getText());
    String tableName = getUnescapedName((ASTNode) ast.getChild(2));
    List<String> indexedCols = getColumnNames((ASTNode) ast.getChild(3));

    IndexType indexType = HiveIndex.getIndexType(typeName);
    if (indexType != null) {
      typeName = indexType.getHandlerClsName();
    } else {
      try {
        Class.forName(typeName);
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
    StorageFormat storageFormat = new StorageFormat();
    AnalyzeCreateCommonVars shared = new AnalyzeCreateCommonVars();

    for (int idx = 4; idx < ast.getChildCount(); idx++) {
      ASTNode child = (ASTNode) ast.getChild(idx);
      if (storageFormat.fillStorageFormat(child, shared)) {
        continue;
      }
      switch (child.getToken().getType()) {
      case HiveParser.TOK_TABLEROWFORMAT:
        rowFormatParams.analyzeRowFormat(shared, child);
        break;
      case HiveParser.TOK_CREATEINDEX_INDEXTBLNAME:
        ASTNode ch = (ASTNode) child.getChild(0);
        indexTableName = getUnescapedName((ASTNode) ch);
        break;
      case HiveParser.TOK_DEFERRED_REBUILDINDEX:
        deferredRebuild = true;
        break;
      case HiveParser.TOK_TABLELOCATION:
        location = unescapeSQLString(child.getChild(0).getText());
        break;
      case HiveParser.TOK_TABLEPROPERTIES:
        tblProps = DDLSemanticAnalyzer.getProps((ASTNode) child.getChild(0));
        break;
      case HiveParser.TOK_INDEXPROPERTIES:
        idxProps = DDLSemanticAnalyzer.getProps((ASTNode) child.getChild(0));
        break;
      case HiveParser.TOK_TABLESERIALIZER:
        child = (ASTNode) child.getChild(0);
        shared.serde = unescapeSQLString(child.getChild(0).getText());
        if (child.getChildCount() == 2) {
          readProps((ASTNode) (child.getChild(1).getChild(0)),
              shared.serdeProps);
        }
        break;
      case HiveParser.TOK_INDEXCOMMENT:
        child = (ASTNode) child.getChild(0);
        indexComment = unescapeSQLString(child.getText());
      }
    }

    storageFormat.fillDefaultStorageFormat(shared);


    CreateIndexDesc crtIndexDesc = new CreateIndexDesc(tableName, indexName,
        indexedCols, indexTableName, deferredRebuild, storageFormat.inputFormat,
        storageFormat.outputFormat,
        storageFormat.storageHandler, typeName, location, idxProps, tblProps,
        shared.serde, shared.serdeProps, rowFormatParams.collItemDelim,
        rowFormatParams.fieldDelim, rowFormatParams.fieldEscape,
        rowFormatParams.lineDelim, rowFormatParams.mapKeyDelim, indexComment);
    Task<?> createIndex =
        TaskFactory.get(new DDLWork(getInputs(), getOutputs(), crtIndexDesc), conf);
    rootTasks.add(createIndex);
  }

  private void analyzeDropIndex(ASTNode ast) throws SemanticException {
    String indexName = unescapeIdentifier(ast.getChild(0).getText());
    String tableName = getUnescapedName((ASTNode) ast.getChild(1));
    boolean ifExists = (ast.getFirstChildWithType(TOK_IFEXISTS) != null);
    // we want to signal an error if the index doesn't exist and we're
    // configured not to ignore this
    boolean throwException =
        !ifExists && !HiveConf.getBoolVar(conf, ConfVars.DROPIGNORESNONEXISTENT);
    if (throwException) {
      try {
        Index idx = db.getIndex(tableName, indexName);
      } catch (HiveException e) {
        throw new SemanticException(ErrorMsg.INVALID_INDEX.getMsg(indexName));
      }
    }

    DropIndexDesc dropIdxDesc = new DropIndexDesc(indexName, tableName);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        dropIdxDesc), conf));
  }

  private void analyzeAlterIndexRebuild(ASTNode ast) throws SemanticException {
    String baseTableName = unescapeIdentifier(ast.getChild(0).getText());
    String indexName = unescapeIdentifier(ast.getChild(1).getText());
    HashMap<String, String> partSpec = null;
    Tree part = ast.getChild(2);
    if (part != null) {
      partSpec = extractPartitionSpecs(part);
    }
    List<Task<?>> indexBuilder = getIndexBuilderMapRed(baseTableName, indexName, partSpec);
    rootTasks.addAll(indexBuilder);

    // Handle updating index timestamps
    AlterIndexDesc alterIdxDesc = new AlterIndexDesc(AlterIndexTypes.UPDATETIMESTAMP);
    alterIdxDesc.setIndexName(indexName);
    alterIdxDesc.setBaseTableName(baseTableName);
    alterIdxDesc.setDbName(db.getCurrentDatabase());
    alterIdxDesc.setSpec(partSpec);

    Task<?> tsTask = TaskFactory.get(new DDLWork(alterIdxDesc), conf);
    for (Task<?> t : indexBuilder) {
      t.addDependentTask(tsTask);
    }
  }

  private void analyzeAlterIndexProps(ASTNode ast)
      throws SemanticException {

    String baseTableName = getUnescapedName((ASTNode) ast.getChild(0));
    String indexName = unescapeIdentifier(ast.getChild(1).getText());
    HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(2))
        .getChild(0));

    AlterIndexDesc alterIdxDesc =
        new AlterIndexDesc(AlterIndexTypes.ADDPROPS);
    alterIdxDesc.setProps(mapProp);
    alterIdxDesc.setIndexName(indexName);
    alterIdxDesc.setBaseTableName(baseTableName);
    alterIdxDesc.setDbName(db.getCurrentDatabase());

    rootTasks.add(TaskFactory.get(new DDLWork(alterIdxDesc), conf));
  }

  private List<Task<?>> getIndexBuilderMapRed(String baseTableName, String indexName,
      HashMap<String, String> partSpec) throws SemanticException {
    try {
      String dbName = db.getCurrentDatabase();
      Index index = db.getIndex(dbName, baseTableName, indexName);
      Table indexTbl = db.getTable(dbName, index.getIndexTableName());
      String baseTblName = index.getOrigTableName();
      Table baseTbl = db.getTable(dbName, baseTblName);

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

  private void analyzeAlterTableProps(ASTNode ast, boolean expectView)
      throws SemanticException {

    String tableName = getUnescapedName((ASTNode) ast.getChild(0));
    HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(1))
        .getChild(0));
    AlterTableDesc alterTblDesc =
        new AlterTableDesc(AlterTableTypes.ADDPROPS, expectView);
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

    String inputFormat = null;
    String outputFormat = null;
    String storageHandler = null;
    String serde = null;
    ASTNode child = (ASTNode) ast.getChild(0);

    switch (child.getToken().getType()) {
    case HiveParser.TOK_TABLEFILEFORMAT:
      inputFormat = unescapeSQLString(((ASTNode) child.getChild(0)).getToken()
          .getText());
      outputFormat = unescapeSQLString(((ASTNode) child.getChild(1)).getToken()
          .getText());
      try {
        Class.forName(inputFormat);
        Class.forName(outputFormat);
      } catch (ClassNotFoundException e) {
        throw new SemanticException(e);
      }
      break;
    case HiveParser.TOK_STORAGEHANDLER:
      storageHandler =
          unescapeSQLString(((ASTNode) child.getChild(1)).getToken().getText());
      try {
        Class.forName(storageHandler);
      } catch (ClassNotFoundException e) {
        throw new SemanticException(e);
      }
      break;
    case HiveParser.TOK_TBLSEQUENCEFILE:
      inputFormat = SEQUENCEFILE_INPUT;
      outputFormat = SEQUENCEFILE_OUTPUT;
      break;
    case HiveParser.TOK_TBLTEXTFILE:
      inputFormat = TEXTFILE_INPUT;
      outputFormat = TEXTFILE_OUTPUT;
      break;
    case HiveParser.TOK_TBLRCFILE:
      inputFormat = RCFILE_INPUT;
      outputFormat = RCFILE_OUTPUT;
      serde = COLUMNAR_SERDE;
      break;
    case HiveParser.TOK_FILEFORMAT_GENERIC:
      handleGenericFileFormat(child);
      break;
    }

    AlterTableDesc alterTblDesc = new AlterTableDesc(tableName, inputFormat,
        outputFormat, serde, storageHandler, partSpec);

    addInputsOutputsAlterTable(tableName, partSpec, alterTblDesc);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void addInputsOutputsAlterTable(String tableName, HashMap<String, String> partSpec)
      throws SemanticException {
    addInputsOutputsAlterTable(tableName, partSpec, null);
  }

  private void addInputsOutputsAlterTable(String tableName, HashMap<String, String> partSpec,
      AlterTableDesc desc) throws SemanticException {
    Table tab = null;
    try {
      tab = db.getTable(db.getCurrentDatabase(), tableName, true);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName));
    }

    inputs.add(new ReadEntity(tab));

    if ((partSpec == null) || (partSpec.isEmpty())) {
      outputs.add(new WriteEntity(tab));
    }
    else {
      List<Partition> allPartitions = null;
      try {
        if (desc == null || desc.getOp() != AlterTableDesc.AlterTableTypes.ALTERPROTECTMODE) {
          Partition part = db.getPartition(tab, partSpec, false);
          allPartitions = new ArrayList<Partition>(1);
          allPartitions.add(part);
        }
        else {
          allPartitions = db.getPartitions(tab, partSpec);
          if (allPartitions == null || allPartitions.size() == 0) {
            throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partSpec.toString()));
          }
        }
      } catch (HiveException e) {
        throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partSpec.toString()), e);
      }

      if (allPartitions != null) {
        for (Partition part : allPartitions) {
          outputs.add(new WriteEntity(part));
        }
      }
    }

    if (desc != null) {
      validateAlterTableType(tab, desc.getOp(), desc.getExpectView());
    }
  }

  private void analyzeAlterTableLocation(ASTNode ast, String tableName,
      HashMap<String, String> partSpec) throws SemanticException {

    String newLocation = unescapeSQLString(ast.getChild(0).getText());

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

  private void analyzeAlterTablePartMergeFiles(ASTNode tablePartAST, ASTNode ast,
      String tableName, HashMap<String, String> partSpec)
      throws SemanticException {
    AlterTablePartMergeFilesDesc mergeDesc = new AlterTablePartMergeFilesDesc(
        tableName, partSpec);

    List<String> inputDir = new ArrayList<String>();
    Path oldTblPartLoc = null;
    Path newTblPartLoc = null;
    Table tblObj = null;

    try {
      tblObj = db.getTable(tableName);

      List<String> bucketCols = null;
      Class<? extends InputFormat> inputFormatClass = null;
      boolean isArchived = false;
      boolean checkIndex = HiveConf.getBoolVar(conf,
          HiveConf.ConfVars.HIVE_CONCATENATE_CHECK_INDEX);
      if (checkIndex) {
        List<Index> indexes = db.getIndexes(tblObj.getDbName(), tableName,
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
          Partition part = db.getPartition(tblObj, partSpec, false);
          if (part == null) {
            throw new SemanticException("source table " + tableName
                + " is partitioned but partition not found.");
          }
          bucketCols = part.getBucketCols();
          inputFormatClass = part.getInputFormatClass();
          isArchived = ArchiveUtils.isArchived(part);

          Path tabPath = tblObj.getPath();
          Path partPath = part.getPartitionPath();

          // if the table is in a different dfs than the partition,
          // replace the partition's dfs with the table's dfs.
          newTblPartLoc = new Path(tabPath.toUri().getScheme(), tabPath.toUri()
              .getAuthority(), partPath.toUri().getPath());

          oldTblPartLoc = partPath;
        }
      } else {
        inputFormatClass = tblObj.getInputFormatClass();
        bucketCols = tblObj.getBucketCols();

        // input and output are the same
        oldTblPartLoc = tblObj.getPath();
        newTblPartLoc = tblObj.getPath();
      }

      // throw a HiveException for non-rcfile.
      if (!inputFormatClass.equals(RCFileInputFormat.class)) {
        throw new SemanticException(
            "Only RCFileFormat is supportted right now.");
      }

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

      inputDir.add(oldTblPartLoc.toString());

      mergeDesc.setInputDir(inputDir);

      addInputsOutputsAlterTable(tableName, partSpec);
      DDLWork ddlWork = new DDLWork(getInputs(), getOutputs(), mergeDesc);
      ddlWork.setNeedLock(true);
      Task<? extends Serializable> mergeTask = TaskFactory.get(ddlWork, conf);
      TableDesc tblDesc = Utilities.getTableDesc(tblObj);
      String queryTmpdir = ctx.getExternalTmpFileURI(newTblPartLoc.toUri());
      mergeDesc.setOutputDir(queryTmpdir);
      LoadTableDesc ltd = new LoadTableDesc(queryTmpdir, queryTmpdir, tblDesc,
          partSpec == null ? new HashMap<String, String>() : partSpec);
      Task<MoveWork> moveTsk = TaskFactory.get(new MoveWork(null, null, ltd, null, false),
          conf);
      mergeTask.addDependentTask(moveTsk);

      if (conf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
        StatsWork statDesc;
        if (oldTblPartLoc.equals(newTblPartLoc)) {
          // If we're merging to the same location, we can avoid some metastore calls
          tableSpec tablepart = new tableSpec(this.db, conf, tablePartAST);
          statDesc = new StatsWork(tablepart);
        } else {
          statDesc = new StatsWork(ltd);
        }
        statDesc.setNoStatsAggregator(true);
        statDesc.setStatsReliable(conf.getBoolVar(HiveConf.ConfVars.HIVE_STATS_RELIABLE));
        Task<? extends Serializable> statTask = TaskFactory.get(statDesc, conf);
        moveTsk.addDependentTask(statTask);
      }

      rootTasks.add(mergeTask);
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private void analyzeAlterTableClusterSort(ASTNode ast)
      throws SemanticException {
    String tableName = getUnescapedName((ASTNode) ast.getChild(0));
    Table tab = null;

    try {
      tab = db.getTable(db.getCurrentDatabase(), tableName, true);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName));
    }

    inputs.add(new ReadEntity(tab));
    outputs.add(new WriteEntity(tab));

    validateAlterTableType(tab, AlterTableTypes.ADDCLUSTERSORTCOLUMN);

    if (ast.getChildCount() == 1) {
      // This means that we want to turn off bucketing
      AlterTableDesc alterTblDesc = new AlterTableDesc(tableName, -1,
          new ArrayList<String>(), new ArrayList<Order>());
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          alterTblDesc), conf));
    } else {
      ASTNode buckets = (ASTNode) ast.getChild(1);
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

      AlterTableDesc alterTblDesc = new AlterTableDesc(tableName, numBuckets,
          bucketCols, sortCols);
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          alterTblDesc), conf));
    }
  }

  static HashMap<String, String> getProps(ASTNode prop) {
    HashMap<String, String> mapProp = new HashMap<String, String>();
    readProps(prop, mapProp);
    return mapProp;
  }

  /**
   * Get the fully qualified name in the ast. e.g. the ast of the form ^(DOT
   * ^(DOT a b) c) will generate a name of the form a.b.c
   *
   * @param ast
   *          The AST from which the qualified name has to be extracted
   * @return String
   */
  private String getFullyQualifiedName(ASTNode ast) {
    if (ast.getChildCount() == 0) {
      return ast.getText();
    }

    return getFullyQualifiedName((ASTNode) ast.getChild(0)) + "."
        + getFullyQualifiedName((ASTNode) ast.getChild(1));
  }

  /**
   * Create a FetchTask for a given table and thrift ddl schema.
   *
   * @param tablename
   *          tablename
   * @param schema
   *          thrift ddl
   */
  private FetchTask createFetchTask(String schema) {
    Properties prop = new Properties();

    prop.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    prop.setProperty(Constants.SERIALIZATION_NULL_FORMAT, " ");
    String[] colTypes = schema.split("#");
    prop.setProperty("columns", colTypes[0]);
    prop.setProperty("columns.types", colTypes[1]);

    FetchWork fetch = new FetchWork(ctx.getResFile().toString(), new TableDesc(
        LazySimpleSerDe.class, TextInputFormat.class,
        IgnoreKeyTextOutputFormat.class, prop), -1);
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
    Table tab = null;
    try {
      tab = db.getTable(tableName);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName), e);
    }

    if (partSpec != null) {
      Partition part = null;
      try {
        part = db.getPartition(tab, partSpec, false);
      } catch (HiveException e) {
        throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partSpec.toString()), e);
      }

      if (part == null) {
        throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partSpec.toString()));
      }
    }
  }

  private void analyzeDescribeTable(ASTNode ast) throws SemanticException {
    ASTNode tableTypeExpr = (ASTNode) ast.getChild(0);
    String tableName = getFullyQualifiedName((ASTNode) tableTypeExpr
        .getChild(0));

    HashMap<String, String> partSpec = null;
    // get partition metadata if partition specified
    if (tableTypeExpr.getChildCount() == 2) {
      ASTNode partspec = (ASTNode) tableTypeExpr.getChild(1);
      partSpec = getPartSpec(partspec);
    }

    // Handle xpath correctly
    String actualTableName = tableName.substring(0,
        tableName.indexOf('.') == -1 ? tableName.length() : tableName.indexOf('.'));
    validateTable(actualTableName, partSpec);

    DescTableDesc descTblDesc = new DescTableDesc(ctx.getResFile(), tableName, partSpec);
    if (ast.getChildCount() == 2) {
      int descOptions = ast.getChild(1).getType();
      descTblDesc.setFormatted(descOptions == HiveParser.KW_FORMATTED);
      descTblDesc.setExt(descOptions == HiveParser.KW_EXTENDED);
    }
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        descTblDesc), conf));
    setFetchTask(createFetchTask(DescTableDesc.getSchema()));
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
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), descDbDesc), conf));
    setFetchTask(createFetchTask(descDbDesc.getSchema()));
  }

  private static HashMap<String, String> getPartSpec(ASTNode partspec)
      throws SemanticException {
    HashMap<String, String> partSpec = new LinkedHashMap<String, String>();
    for (int i = 0; i < partspec.getChildCount(); ++i) {
      ASTNode partspec_val = (ASTNode) partspec.getChild(i);
      String val = stripQuotes(partspec_val.getChild(1).getText());
      partSpec.put(partspec_val.getChild(0).getText().toLowerCase(), val);
    }
    return partSpec;
  }

  private void analyzeShowPartitions(ASTNode ast) throws SemanticException {
    ShowPartitionsDesc showPartsDesc;
    String tableName = getUnescapedName((ASTNode) ast.getChild(0));
    List<Map<String, String>> partSpecs = getPartitionSpecs(ast);
    // We only can have a single partition spec
    assert (partSpecs.size() <= 1);
    Map<String, String> partSpec = null;
    if (partSpecs.size() > 0) {
      partSpec = partSpecs.get(0);
    }

    validateTable(tableName, null);

    showPartsDesc = new ShowPartitionsDesc(tableName, ctx.getResFile(), partSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showPartsDesc), conf));
    setFetchTask(createFetchTask(showPartsDesc.getSchema()));
  }

  private void analyzeShowCreateTable(ASTNode ast) throws SemanticException {
    ShowCreateTableDesc showCreateTblDesc;
    String tableName = getUnescapedName((ASTNode)ast.getChild(0));
    showCreateTblDesc = new ShowCreateTableDesc(tableName, ctx.getResFile().toString());
    try {
      Table tab = db.getTable(tableName, true);
      if (tab.getTableType() == org.apache.hadoop.hive.metastore.TableType.INDEX_TABLE) {
        throw new SemanticException(ErrorMsg.SHOW_CREATETABLE_INDEX.getMsg(tableName
            + " has table type INDEX_TABLE"));
      }
      inputs.add(new ReadEntity(tab));
    } catch (SemanticException e) {
      throw e;
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName));
    }
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
    String dbName = db.getCurrentDatabase();
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

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showTblsDesc), conf));
    setFetchTask(createFetchTask(showTblsDesc.getSchema()));
  }

  private void analyzeShowColumns(ASTNode ast) throws SemanticException {
    ShowColumnsDesc showColumnsDesc;
    String dbName = null;
    String tableName = null;
    switch (ast.getChildCount()) {
    case 1:
      tableName = getUnescapedName((ASTNode) ast.getChild(0));
      break;
    case 2:
      dbName = getUnescapedName((ASTNode) ast.getChild(0));
      tableName = getUnescapedName((ASTNode) ast.getChild(1));
      break;
    default:
      break;
    }

    try {
      Table tab = null;
      if (dbName == null) {
        tab = db.getTable(tableName, true);
      }
      else {
        tab = db.getTable(dbName, tableName, true);
      }
      inputs.add(new ReadEntity(tab));
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName));
    }

    showColumnsDesc = new ShowColumnsDesc(ctx.getResFile(), dbName, tableName);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showColumnsDesc), conf));
    setFetchTask(createFetchTask(showColumnsDesc.getSchema()));
  }

  private void analyzeShowTableStatus(ASTNode ast) throws SemanticException {
    ShowTableStatusDesc showTblStatusDesc;
    String tableNames = getUnescapedName((ASTNode) ast.getChild(0));
    String dbName = db.getCurrentDatabase();
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
          partSpec = getPartSpec(child);
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
    String tableNames = getUnescapedName((ASTNode) ast.getChild(0));
    String dbName = db.getCurrentDatabase();
    String propertyName = null;
    if (ast.getChildCount() > 1) {
      propertyName = unescapeSQLString(ast.getChild(1).getText());
    }

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
          ASTNode tableTypeExpr = (ASTNode) child;
          tableName = getFullyQualifiedName((ASTNode) tableTypeExpr.getChild(0));
          // get partition metadata if partition specified
          if (tableTypeExpr.getChildCount() == 2) {
            ASTNode partspec = (ASTNode) tableTypeExpr.getChild(1);
            partSpec = getPartSpec(partspec);
          }
        } else if (child.getType() == HiveParser.KW_EXTENDED) {
          isExtended = true;
        }
      }
    }

    ShowLocksDesc showLocksDesc = new ShowLocksDesc(ctx.getResFile(), tableName,
        partSpec, isExtended);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showLocksDesc), conf));
    setFetchTask(createFetchTask(showLocksDesc.getSchema()));

    // Need to initialize the lock manager
    ctx.setNeedLockMgr(true);
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
    List<Map<String, String>> partSpecs = getPartitionSpecs(ast);

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
    List<Map<String, String>> partSpecs = getPartitionSpecs(ast);

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


  private void analyzeAlterTableRename(ASTNode ast, boolean expectView) throws SemanticException {
    String tblName = getUnescapedName((ASTNode) ast.getChild(0));
    AlterTableDesc alterTblDesc = new AlterTableDesc(tblName,
        getUnescapedName((ASTNode) ast.getChild(1)), expectView);

    addInputsOutputsAlterTable(tblName, null, alterTblDesc);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableRenameCol(ASTNode ast) throws SemanticException {
    String tblName = getUnescapedName((ASTNode) ast.getChild(0));
    String newComment = null;
    String newType = null;
    newType = getTypeStringFromAST((ASTNode) ast.getChild(3));
    boolean first = false;
    String flagCol = null;
    ASTNode positionNode = null;
    if (ast.getChildCount() == 6) {
      newComment = unescapeSQLString(ast.getChild(4).getText());
      positionNode = (ASTNode) ast.getChild(5);
    } else if (ast.getChildCount() == 5) {
      if (ast.getChild(4).getType() == HiveParser.StringLiteral) {
        newComment = unescapeSQLString(ast.getChild(4).getText());
      } else {
        positionNode = (ASTNode) ast.getChild(4);
      }
    }

    if (positionNode != null) {
      if (positionNode.getChildCount() == 0) {
        first = true;
      } else {
        flagCol = unescapeIdentifier(positionNode.getChild(0).getText());
      }
    }

    String oldColName = ast.getChild(1).getText();
    String newColName = ast.getChild(2).getText();

    /* Validate the operation of renaming a column name. */
    Table tab = null;
    try {
      tab = db.getTable(tblName);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tblName), e);
    }
    SkewedInfo skewInfo = tab.getTTable().getSd().getSkewedInfo();
    if ((null != skewInfo)
        && (null != skewInfo.getSkewedColNames())
        && skewInfo.getSkewedColNames().contains(oldColName)) {
      throw new SemanticException(oldColName
          + ErrorMsg.ALTER_TABLE_NOT_ALLOWED_RENAME_SKEWED_COLUMN.getMsg());
    }

    AlterTableDesc alterTblDesc = new AlterTableDesc(tblName,
        unescapeIdentifier(oldColName), unescapeIdentifier(newColName),
        newType, newComment, first, flagCol);
    addInputsOutputsAlterTable(tblName, null, alterTblDesc);

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableRenamePart(ASTNode ast, String tblName,
      HashMap<String, String> oldPartSpec) throws SemanticException {
    Map<String, String> newPartSpec = extractPartitionSpecs((ASTNode) ast.getChild(0));
    if (newPartSpec == null) {
      throw new SemanticException("RENAME PARTITION Missing Destination" + ast);
    }
    Table tab = null;
    try {
      tab = db.getTable(db.getCurrentDatabase(), tblName, false);
      if (tab != null) {
        inputs.add(new ReadEntity(tab));
      } else {
        throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tblName));
      }
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tblName));
    }
    validateAlterTableType(tab, AlterTableTypes.RENAMEPARTITION);

    List<Map<String, String>> partSpecs = new ArrayList<Map<String, String>>();
    partSpecs.add(oldPartSpec);
    partSpecs.add(newPartSpec);
    addTablePartsOutputs(tblName, partSpecs);
    RenamePartitionDesc renamePartitionDesc = new RenamePartitionDesc(
        db.getCurrentDatabase(), tblName, oldPartSpec, newPartSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        renamePartitionDesc), conf));
  }

  private void analyzeAlterTableModifyCols(ASTNode ast,
      AlterTableTypes alterType) throws SemanticException {
    String tblName = getUnescapedName((ASTNode) ast.getChild(0));
    List<FieldSchema> newCols = getColumns((ASTNode) ast.getChild(1));
    AlterTableDesc alterTblDesc = new AlterTableDesc(tblName, newCols,
        alterType);

    addInputsOutputsAlterTable(tblName, null, alterTblDesc);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableDropParts(ASTNode ast, boolean expectView)
      throws SemanticException {

    String tblName = getUnescapedName((ASTNode) ast.getChild(0));
    // get table metadata
    List<PartitionSpec> partSpecs = getFullPartitionSpecs(ast);
    Table tab = null;

    try {
      tab = db.getTable(db.getCurrentDatabase(), tblName, false);
      if (tab != null) {
        inputs.add(new ReadEntity(tab));
      }
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tblName));
    }
    validateAlterTableType(tab, AlterTableTypes.DROPPARTITION, expectView);

    // Find out if all partition columns are strings. This is needed for JDO
    boolean stringPartitionColumns = true;
    List<FieldSchema> partCols = tab.getPartCols();

    for (FieldSchema partCol : partCols) {
      if (!partCol.getType().toLowerCase().equals("string")) {
        stringPartitionColumns = false;
        break;
      }
    }

    // Only equality is supported for non-string partition columns
    if (!stringPartitionColumns) {
      for (PartitionSpec partSpec : partSpecs) {
        if (partSpec.isNonEqualityOperator()) {
          throw new SemanticException(
              ErrorMsg.DROP_PARTITION_NON_STRING_PARTCOLS_NONEQUALITY.getMsg());
        }
      }
    }

    if (partSpecs != null) {
      boolean ifExists = (ast.getFirstChildWithType(TOK_IFEXISTS) != null);
      // we want to signal an error if the partition doesn't exist and we're
      // configured not to fail silently
      boolean throwException =
          !ifExists && !HiveConf.getBoolVar(conf, ConfVars.DROPIGNORESNONEXISTENT);
      addTableDropPartsOutputs(tblName, partSpecs, throwException, stringPartitionColumns);
    }

    DropTableDesc dropTblDesc =
        new DropTableDesc(tblName, partSpecs, expectView, stringPartitionColumns);

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        dropTblDesc), conf));
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
  private void analyzeAlterTableAddParts(CommonTree ast, boolean expectView)
      throws SemanticException {

    String tblName = getUnescapedName((ASTNode) ast.getChild(0));
    boolean isView = false;
    Table tab = null;
    try {
      tab = db.getTable(db.getCurrentDatabase(), tblName, false);
      if (tab != null) {
        inputs.add(new ReadEntity(tab));
        isView = tab.isView();
      }
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tblName));
    }
    validateAlterTableType(tab, AlterTableTypes.ADDPARTITION, expectView);

    // partition name to value
    List<Map<String, String>> partSpecs = getPartitionSpecs(ast);
    addTablePartsOutputs(tblName, partSpecs);

    Iterator<Map<String, String>> partIter = partSpecs.iterator();

    String currentLocation = null;
    Map<String, String> currentPart = null;
    boolean ifNotExists = false;
    List<AddPartitionDesc> partitionDescs = new ArrayList<AddPartitionDesc>();

    int numCh = ast.getChildCount();
    for (int num = 1; num < numCh; num++) {
      CommonTree child = (CommonTree) ast.getChild(num);
      switch (child.getToken().getType()) {
      case HiveParser.TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      case HiveParser.TOK_PARTSPEC:
        if (currentPart != null) {
          validatePartitionValues(currentPart);
          AddPartitionDesc addPartitionDesc = new AddPartitionDesc(
              db.getCurrentDatabase(), tblName, currentPart,
              currentLocation, ifNotExists, expectView);
          partitionDescs.add(addPartitionDesc);
        }
        // create new partition, set values
        currentLocation = null;
        currentPart = partIter.next();
        break;
      case HiveParser.TOK_PARTITIONLOCATION:
        // if location specified, set in partition
        currentLocation = unescapeSQLString(child.getChild(0).getText());
        break;
      default:
        throw new SemanticException("Unknown child: " + child);
      }
    }

    // add the last one
    if (currentPart != null) {
      validatePartitionValues(currentPart);
      AddPartitionDesc addPartitionDesc = new AddPartitionDesc(
          db.getCurrentDatabase(), tblName, currentPart,
          currentLocation, ifNotExists, expectView);
      partitionDescs.add(addPartitionDesc);
    }

    for (AddPartitionDesc addPartitionDesc : partitionDescs) {
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          addPartitionDesc), conf));
    }

    if (isView) {
      // Compile internal query to capture underlying table partition
      // dependencies
      StringBuilder cmd = new StringBuilder();
      cmd.append("SELECT * FROM ");
      cmd.append(HiveUtils.unparseIdentifier(tblName));
      cmd.append(" WHERE ");
      boolean firstOr = true;
      for (AddPartitionDesc partitionDesc : partitionDescs) {
        // Perform this check early so that we get a better error message.
        try {
          // Note that isValidSpec throws an exception (it never
          // actually returns false).
          tab.isValidSpec(partitionDesc.getPartSpec());
        } catch (HiveException ex) {
          throw new SemanticException(ex.getMessage(), ex);
        }
        if (firstOr) {
          firstOr = false;
        } else {
          cmd.append(" OR ");
        }
        boolean firstAnd = true;
        cmd.append("(");
        for (Map.Entry<String, String> entry : partitionDesc.getPartSpec().entrySet())
        {
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
      Driver driver = new Driver(conf);
      int rc = driver.compile(cmd.toString());
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
   *           Parsin failed
   */
  private void analyzeAlterTableTouch(CommonTree ast)
      throws SemanticException {

    String tblName = getUnescapedName((ASTNode) ast.getChild(0));
    Table tab;

    try {
      tab = db.getTable(db.getCurrentDatabase(), tblName, false);
      if (tab != null) {
        inputs.add(new ReadEntity(tab));
      }
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tblName));
    }
    validateAlterTableType(tab, AlterTableTypes.TOUCH);

    // partition name to value
    List<Map<String, String>> partSpecs = getPartitionSpecs(ast);

    if (partSpecs.size() == 0) {
      AlterTableSimpleDesc touchDesc = new AlterTableSimpleDesc(
          db.getCurrentDatabase(), tblName, null,
          AlterTableDesc.AlterTableTypes.TOUCH);
      outputs.add(new WriteEntity(tab));
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          touchDesc), conf));
    } else {
      addTablePartsOutputs(tblName, partSpecs);
      for (Map<String, String> partSpec : partSpecs) {
        AlterTableSimpleDesc touchDesc = new AlterTableSimpleDesc(
            db.getCurrentDatabase(), tblName, partSpec,
            AlterTableDesc.AlterTableTypes.TOUCH);
        rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
            touchDesc), conf));
      }
    }
  }

  private void analyzeAlterTableArchive(CommonTree ast, boolean isUnArchive)
      throws SemanticException {

    if (!conf.getBoolVar(HiveConf.ConfVars.HIVEARCHIVEENABLED)) {
      throw new SemanticException(ErrorMsg.ARCHIVE_METHODS_DISABLED.getMsg());

    }
    String tblName = getUnescapedName((ASTNode) ast.getChild(0));
    // partition name to value
    List<Map<String, String>> partSpecs = getPartitionSpecs(ast);

    Table tab = null;
    try {
      tab = db.getTable(db.getCurrentDatabase(), tblName, false);
      if (tab != null) {
        inputs.add(new ReadEntity(tab));
      }
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tblName));
    }
    addTablePartsOutputs(tblName, partSpecs, true);
    validateAlterTableType(tab, AlterTableTypes.ARCHIVE);

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
        db.getCurrentDatabase(), tblName, partSpec,
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
    List<Map<String, String>> specs = getPartitionSpecs(ast);
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
  private List<Map<String, String>> getPartitionSpecs(CommonTree ast)
      throws SemanticException {
    List<Map<String, String>> partSpecs = new ArrayList<Map<String, String>>();
    int childIndex = 0;
    // get partition metadata if partition specified
    for (childIndex = 1; childIndex < ast.getChildCount(); childIndex++) {
      Tree partspec = ast.getChild(childIndex);
      // sanity check
      if (partspec.getType() == HiveParser.TOK_PARTSPEC) {
        Map<String, String> partSpec = new LinkedHashMap<String, String>();
        for (int i = 0; i < partspec.getChildCount(); ++i) {
          CommonTree partspec_val = (CommonTree) partspec.getChild(i);
          String val = stripQuotes(partspec_val.getChild(1).getText());
          partSpec.put(partspec_val.getChild(0).getText().toLowerCase(), val);
        }
        partSpecs.add(partSpec);
      }
    }
    return partSpecs;
  }

  /**
   * Get the partition specs from the tree. This stores the full specification
   * with the comparator operator into the output list.
   *
   * @param ast
   *          Tree to extract partitions from.
   * @return A list of PartitionSpec objects which contain the mapping from
   *         key to operator and value.
   * @throws SemanticException
   */
  private List<PartitionSpec> getFullPartitionSpecs(CommonTree ast)
      throws SemanticException {
    List<PartitionSpec> partSpecList = new ArrayList<PartitionSpec>();

    for (int childIndex = 1; childIndex < ast.getChildCount(); childIndex++) {
      Tree partSpecTree = ast.getChild(childIndex);
      if (partSpecTree.getType() == HiveParser.TOK_PARTSPEC) {
        PartitionSpec partSpec = new PartitionSpec();

        for (int i = 0; i < partSpecTree.getChildCount(); ++i) {
          CommonTree partSpecSingleKey = (CommonTree) partSpecTree.getChild(i);
          assert (partSpecSingleKey.getType() == HiveParser.TOK_PARTVAL);
          String key = partSpecSingleKey.getChild(0).getText().toLowerCase();
          String operator = partSpecSingleKey.getChild(1).getText();
          String val = partSpecSingleKey.getChild(2).getText();
          partSpec.addPredicate(key, operator, val);
        }

        partSpecList.add(partSpec);
      }
    }
    return partSpecList;
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
        if (e.getValue().contains(s)) {
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
  private void addTablePartsOutputs(String tblName, List<Map<String, String>> partSpecs)
      throws SemanticException {
    addTablePartsOutputs(tblName, partSpecs, false, false, null);
  }

  /**
   * Add the table partitions to be modified in the output, so that it is available for the
   * pre-execution hook. If the partition does not exist, no error is thrown.
   */
  private void addTablePartsOutputs(String tblName, List<Map<String, String>> partSpecs,
      boolean allowMany)
      throws SemanticException {
    addTablePartsOutputs(tblName, partSpecs, false, allowMany, null);
  }

  /**
   * Add the table partitions to be modified in the output, so that it is available for the
   * pre-execution hook. If the partition does not exist, throw an error if
   * throwIfNonExistent is true, otherwise ignore it.
   */
  private void addTablePartsOutputs(String tblName, List<Map<String, String>> partSpecs,
      boolean throwIfNonExistent, boolean allowMany, ASTNode ast)
      throws SemanticException {
    Table tab;
    try {
      tab = db.getTable(tblName);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tblName));
    }

    Iterator<Map<String, String>> i;
    int index;
    for (i = partSpecs.iterator(), index = 1; i.hasNext(); ++index) {
      Map<String, String> partSpec = i.next();
      List<Partition> parts = null;
      if (allowMany) {
        try {
          parts = db.getPartitions(tab, partSpec);
        } catch (HiveException e) {
          LOG.error("Got HiveException during obtaining list of partitions");
        }
      } else {
        parts = new ArrayList<Partition>();
        try {
          Partition p = db.getPartition(tab, partSpec, false);
          if (p != null) {
            parts.add(p);
          }
        } catch (HiveException e) {
          LOG.debug("Wrong specification");
        }
      }
      if (parts.isEmpty()) {
        if (throwIfNonExistent) {
          throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(ast.getChild(index)));
        }
      }
      for (Partition p : parts) {
        outputs.add(new WriteEntity(p));
      }
    }
  }

  /**
   * Add the table partitions to be modified in the output, so that it is available for the
   * pre-execution hook. If the partition does not exist, throw an error if
   * throwIfNonExistent is true, otherwise ignore it.
   */
  private void addTableDropPartsOutputs(String tblName, List<PartitionSpec> partSpecs,
      boolean throwIfNonExistent, boolean stringPartitionColumns)
      throws SemanticException {
    Table tab;
    try {
      tab = db.getTable(tblName);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tblName));
    }

    Iterator<PartitionSpec> i;
    int index;
    for (i = partSpecs.iterator(), index = 1; i.hasNext(); ++index) {
      PartitionSpec partSpec = i.next();
      List<Partition> parts = null;
      if (stringPartitionColumns) {
        try {
          parts = db.getPartitionsByFilter(tab, partSpec.toString());
        } catch (Exception e) {
          throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partSpec.toString()), e);
        }
      }
      else {
        try {
          parts = db.getPartitions(tab, partSpec.getPartSpecWithoutOperator());
        } catch (Exception e) {
          throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partSpec.toString()), e);
        }
      }

      if (parts.isEmpty()) {
        if (throwIfNonExistent) {
          throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partSpec.toString()));
        }
      }
      for (Partition p : parts) {
        outputs.add(new WriteEntity(p));
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
  private void analyzeAltertableSkewedby(ASTNode ast) throws SemanticException {
    /**
     * Throw an error if the user tries to use the DDL with
     * hive.internal.ddl.list.bucketing.enable set to false.
     */
    HiveConf hiveConf = SessionState.get().getConf();
    if (!(hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_INTERNAL_DDL_LIST_BUCKETING_ENABLE))) {
      throw new SemanticException(ErrorMsg.HIVE_INTERNAL_DDL_LIST_BUCKETING_DISABLED.getMsg());
    }

    String tableName = getUnescapedName((ASTNode) ast.getChild(0));
    Table tab = null;

    try {
      tab = db.getTable(db.getCurrentDatabase(), tableName, true);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName));
    }
    inputs.add(new ReadEntity(tab));
    outputs.add(new WriteEntity(tab));

    validateAlterTableType(tab, AlterTableTypes.ADDSKEWEDBY);

    if (ast.getChildCount() == 1) {
      /* Convert a skewed table to non-skewed table. */
      AlterTableDesc alterTblDesc = new AlterTableDesc(tableName, true,
          new ArrayList<String>(), new ArrayList<List<String>>());
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          alterTblDesc), conf));
    } else {
      List<String> skewedColNames = new ArrayList<String>();
      List<List<String>> skewedValues = new ArrayList<List<String>>();
      /* skewed column names. */
      ASTNode skewedNode = (ASTNode) ast.getChild(1);
      skewedColNames = analyzeAlterTableSkewedColNames(skewedColNames, skewedNode);
      /* skewed value. */
      Tree vNode = skewedNode.getChild(1);
      if (vNode == null) {
        throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
      } else {
        ASTNode vAstNode = (ASTNode) vNode;
        switch (vAstNode.getToken().getType()) {
        case HiveParser.TOK_TABCOLVALUE:
          for (String str : getColumnValues(vAstNode)) {
            List<String> sList = new ArrayList<String>(Arrays.asList(str));
            skewedValues.add(sList);
          }
          break;
        case HiveParser.TOK_TABCOLVALUE_PAIR:
          List<Node> vLNodes = vAstNode.getChildren();
          for (Node node : vLNodes) {
            if (((ASTNode) node).getToken().getType() != HiveParser.TOK_TABCOLVALUES) {
              throw new SemanticException(
                  ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
            } else {
              Tree leafVNode = ((ASTNode) node).getChild(0);
              if (leafVNode == null) {
                throw new SemanticException(
                    ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
              } else {
                ASTNode lVAstNode = (ASTNode) leafVNode;
                if (lVAstNode.getToken().getType() != HiveParser.TOK_TABCOLVALUE) {
                  throw new SemanticException(
                      ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
                } else {
                  skewedValues.add(new ArrayList<String>(getColumnValues(lVAstNode)));
                }
              }
            }
          }
          break;
        default:
          break;
        }
      }

      AlterTableDesc alterTblDesc = new AlterTableDesc(tableName, false,
          skewedColNames, skewedValues);
      /**
       * Validate information about skewed table
       */
      alterTblDesc.setTable(tab);
      alterTblDesc.validate();
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          alterTblDesc), conf));
    }
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
    if (!(hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_INTERNAL_DDL_LIST_BUCKETING_ENABLE))) {
      throw new SemanticException(ErrorMsg.HIVE_INTERNAL_DDL_LIST_BUCKETING_DISABLED.getMsg());
    }
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
                }
              }
            }
          }
        }
      }
    }
    AlterTableDesc alterTblDesc = new AlterTableDesc(tableName, locations, partSpec);
    addInputsOutputsAlterTable(tableName, partSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
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

}
