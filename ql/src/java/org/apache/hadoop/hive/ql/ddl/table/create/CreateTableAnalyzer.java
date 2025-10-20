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

package org.apache.hadoop.hive.ql.ddl.table.create;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.TransactionalValidationListener;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.misc.sortoder.SortFieldDesc;
import org.apache.hadoop.hive.ql.ddl.misc.sortoder.SortFields;
import org.apache.hadoop.hive.ql.ddl.table.constraint.ConstraintsUtils;
import org.apache.hadoop.hive.ql.ddl.table.convert.AlterTableConvertOperation;
import org.apache.hadoop.hive.ql.ddl.table.create.like.CreateTableLikeDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.skewed.SkewedTableUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.SchemaInferenceUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.PartitionTransform;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.StorageFormat;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.hive.ql.util.NullOrdering;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_DEFAULT_STORAGE_HANDLER;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.DEFAULT_TABLE_TYPE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_CTAS;

@DDLType(types = HiveParser.TOK_CREATETABLE)
public class CreateTableAnalyzer extends CalcitePlanner {

  public CreateTableAnalyzer(QueryState queryState)
      throws SemanticException {
    super(queryState);
  }

  private static final String[] UPDATED_TBL_PROPS =
      {hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES, hive_metastoreConstants.TABLE_BUCKETING_VERSION};

  @Override
  protected ASTNode handlePostCboRewriteContext(PreCboCtx cboCtx, ASTNode newAST)
      throws SemanticException {
    if (cboCtx.getType() == PreCboCtx.Type.CTAS) {
      // CTAS
      init(false);
      setAST(newAST);
      newAST = reAnalyzeCTASAfterCbo(newAST);
    } else {
      newAST = super.handlePostCboRewriteContext(cboCtx, newAST);
    }
    return newAST;
  }

  public ASTNode reAnalyzeCTASAfterCbo(ASTNode newAst)
      throws SemanticException {
    // analyzeCreateTable uses this.ast, but doPhase1 doesn't, so only reset it
    // here.
    newAst = analyzeCreateTable(newAst, getQB(), null);
    if (newAst == null) {
      LOG.error("analyzeCreateTable failed to initialize CTAS after CBO;" + " new ast is " + getAST().dump());
      throw new SemanticException("analyzeCreateTable failed to initialize CTAS after CBO");
    }
    return newAst;
  }

  @Override
  protected boolean genResolvedParseTree(ASTNode ast, PlannerContext plannerCtx)
      throws SemanticException {
    ASTNode child;
    this.ast = ast;
    viewsExpanded = new ArrayList<String>();
    if ((child = analyzeCreateTable(ast, getQB(), plannerCtx)) == null) {
      return false;
    }
    return analyzeAndResolveChildTree(child, plannerCtx);
  }

  /**
   * Checks to see if given partition columns has DEFAULT or CHECK constraints (whether ENABLED or DISABLED)
   *  Or has NOT NULL constraints (only ENABLED)
   * @param partCols partition columns
   * @param defConstraints default constraints
   * @param notNullConstraints not null constraints
   * @param checkConstraints CHECK constraints
   * @return true or false
   */
  private boolean hasConstraints(final List<FieldSchema> partCols, final List<SQLDefaultConstraint> defConstraints,
      final List<SQLNotNullConstraint> notNullConstraints, final List<SQLCheckConstraint> checkConstraints) {
    for (FieldSchema partFS : partCols) {
      for (SQLDefaultConstraint dc : defConstraints) {
        if (dc.getColumn_name().equals(partFS.getName())) {
          return true;
        }
      }
      for (SQLCheckConstraint cc : checkConstraints) {
        if (cc.getColumn_name().equals(partFS.getName())) {
          return true;
        }
      }
      for (SQLNotNullConstraint nc : notNullConstraints) {
        if (nc.getColumn_name().equals(partFS.getName()) && nc.isEnable_cstr()) {
          return true;
        }
      }
    }
    return false;
  }

  private String getSortOrderJson(ASTNode ast) {
    List<SortFieldDesc> sortFieldDescList = new ArrayList<>();
    SortFields sortFields = new SortFields(sortFieldDescList);
    for (int i = 0; i < ast.getChildCount(); i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      SortFieldDesc.SortDirection sortDirection =
          child.getToken().getType() == HiveParser.TOK_TABSORTCOLNAMEDESC ? SortFieldDesc.SortDirection.DESC
              : SortFieldDesc.SortDirection.ASC;
      child = (ASTNode) child.getChild(0);
      String name = unescapeIdentifier(child.getChild(0).getText()).toLowerCase();
      NullOrdering nullOrder = NullOrdering.fromToken(child.getToken().getType());
      sortFieldDescList.add(new SortFieldDesc(name, sortDirection, nullOrder));
    }
    try {
      return JSON_OBJECT_MAPPER.writer().writeValueAsString(sortFields);
    } catch (JsonProcessingException e) {
      LOG.warn("Can not create write order json. ", e);
      return null;
    }
  }

  /**
   * This api is used to determine where to create acid tables are not.
   * if the default table type is set to external, then create transactional table should result in acid tables,
   * else create table should result in external table.
   * */
  private boolean isExternalTableChanged(Map<String, String> tblProp, boolean isTransactional, boolean isExt,
      boolean isTableTypeChanged) {
    if (isTableTypeChanged && tblProp != null && tblProp.getOrDefault(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL,
        "false").equalsIgnoreCase("true") || isTransactional) {
      isExt = false;
    }
    return isExt;
  }

  private static boolean isIcebergTable(Map<String, String> tblProps) {
    return AlterTableConvertOperation.ConversionFormats.ICEBERG.properties().get(META_TABLE_STORAGE)
        .equalsIgnoreCase(tblProps.get(META_TABLE_STORAGE));
  }

  private String getDefaultLocation(String dbName, String tableName, boolean isExt)
      throws SemanticException {
    String tblLocation;
    try {
      Warehouse wh = new Warehouse(conf);
      tblLocation = wh.getDefaultTablePath(db.getDatabase(dbName), tableName, isExt).toUri().getPath();
    } catch (MetaException | HiveException e) {
      throw new SemanticException(e);
    }
    return tblLocation;
  }

  /**
   * Add default properties for table property. If a default parameter exists
   * in the tblProp, the value in tblProp will be kept.
   *
   * @param tblProp
   *          property map
   * @return Modified table property map
   */
  private Map<String, String> validateAndAddDefaultProperties(Map<String, String> tblProp, boolean isExt,
      StorageFormat storageFormat, String qualifiedTableName, List<Order> sortCols, boolean isMaterialization,
      boolean isTemporaryTable, boolean isTransactional, boolean isManaged, String[] qualifiedTabName,
      boolean isTableTypeChanged)
      throws SemanticException {
    Map<String, String> retValue = Optional.ofNullable(tblProp).orElseGet(HashMap::new);

    String paraString = HiveConf.getVar(conf, HiveConf.ConfVars.NEW_TABLE_DEFAULT_PARA);
    if (paraString != null && !paraString.isEmpty()) {
      for (String keyValuePair : paraString.split(",")) {
        String[] keyValue = keyValuePair.split("=", 2);
        if (keyValue.length != 2) {
          continue;
        }
        if (!retValue.containsKey(keyValue[0])) {
          retValue.put(keyValue[0], keyValue[1]);
        }
      }
    }
    if (!retValue.containsKey(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL) && retValue.containsKey(
        hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES)) {
      throw new SemanticException(
          "Cannot specify " + hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES + " without "
              + hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    }
    isExt = isExternalTableChanged(retValue, isTransactional, isExt, isTableTypeChanged);

    if (isExt && HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_EXTERNALTABLE_PURGE_DEFAULT)) {
      if (retValue.get(MetaStoreUtils.EXTERNAL_TABLE_PURGE) == null) {
        retValue.put(MetaStoreUtils.EXTERNAL_TABLE_PURGE, "true");
      }
    }

    boolean makeInsertOnly =
        !isTemporaryTable && HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CREATE_TABLES_AS_INSERT_ONLY);
    boolean makeAcid = !isTemporaryTable && makeAcid();
    // if not specify managed table and create.table.as.external is true
    // ignore makeInsertOnly and makeAcid.
    if (!isManaged && HiveConf.getBoolVar(conf, HiveConf.ConfVars.CREATE_TABLE_AS_EXTERNAL)) {
      makeInsertOnly = false;
      makeAcid = false;
    }
    if ((makeInsertOnly || makeAcid || isTransactional || isManaged) && !isExt && !isMaterialization
        && StringUtils.isBlank(storageFormat.getStorageHandler())
        //don't overwrite user choice if transactional attribute is explicitly set
        && !retValue.containsKey(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL)) {
      if (makeInsertOnly || isTransactional) {
        retValue.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
        retValue.put(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES,
            TransactionalValidationListener.INSERTONLY_TRANSACTIONAL_PROPERTY);
      }
      if (makeAcid || isTransactional || (isManaged && !makeInsertOnly)) {
        retValue = convertToAcidByDefault(storageFormat, qualifiedTableName, sortCols, retValue);
      }
    }
    if (!isExt) {
      addDbAndTabToOutputs(qualifiedTabName, TableType.MANAGED_TABLE, isTemporaryTable, retValue, storageFormat);
    } else {
      addDbAndTabToOutputs(qualifiedTabName, TableType.EXTERNAL_TABLE, isTemporaryTable, retValue, storageFormat);
    }

    if (isIcebergTable(retValue)) {
      SessionStateUtil.addResourceOrThrow(conf, SessionStateUtil.DEFAULT_TABLE_LOCATION,
          getDefaultLocation(qualifiedTabName[0], qualifiedTabName[1], true));
    }
    return retValue;
  }

  /**
   * Update the default table properties with values fetch from the original table properties. The property names are
   * @param source properties of source table, must be not null.
   * @param target properties of target table.
   * @param skipped a list of properties which should be not overwritten. It can be null or empty.
   */
  private void updateDefaultTblProps(Map<String, String> source, Map<String, String> target, List<String> skipped) {
    if (source == null || target == null) {
      return;
    }
    for (String property : UPDATED_TBL_PROPS) {
      if ((skipped == null || !skipped.contains(property)) && source.containsKey(property)) {
        target.put(property, source.get(property));
      }
    }
  }

  /**
   * Analyze the create table command. If it is a regular create-table or
   * create-table-like statements, we create a DDLWork and return true. If it is
   * a create-table-as-select, we get the necessary info such as the SerDe and
   * Storage Format and put it in QB, and return false, indicating the rest of
   * the semantic analyzer need to deal with the select statement with respect
   * to the SerDe and Storage Format.
   */
  ASTNode analyzeCreateTable(ASTNode ast, QB qb, PlannerContext plannerCtx)
      throws SemanticException {
    TableName qualifiedTabName = getQualifiedTableName((ASTNode) ast.getChild(0));
    final String dbDotTab = qualifiedTabName.getNotEmptyDbTable();

    String likeTableName = null;
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> partColNames = new ArrayList<>();
    List<String> bucketCols = new ArrayList<String>();
    List<SQLPrimaryKey> primaryKeys = new ArrayList<SQLPrimaryKey>();
    List<SQLForeignKey> foreignKeys = new ArrayList<SQLForeignKey>();
    List<SQLUniqueConstraint> uniqueConstraints = new ArrayList<>();
    List<SQLNotNullConstraint> notNullConstraints = new ArrayList<>();
    List<SQLDefaultConstraint> defaultConstraints = new ArrayList<>();
    List<ConstraintsUtils.ConstraintInfo> defaultConstraintInfo = new ArrayList<>();
    List<SQLCheckConstraint> checkConstraints = new ArrayList<>();
    List<Order> sortCols = new ArrayList<Order>();
    int numBuckets = -1;
    String comment = null;
    String location = null;
    Map<String, String> tblProps = null;
    boolean ifNotExists = false;
    boolean isExt = false;
    boolean isTemporary = false;
    boolean isManaged = false;
    boolean isMaterialization = false;
    boolean isTransactional = false;
    ASTNode selectStmt = null;
    final int CREATE_TABLE = 0; // regular CREATE TABLE
    final int CTLT = 1; // CREATE TABLE LIKE ... (CTLT)
    final int CTAS = 2; // CREATE TABLE AS SELECT ... (CTAS)
    final int CTT = 3; // CREATE TRANSACTIONAL TABLE
    final int CTLF = 4; // CREATE TABLE LIKE FILE
    int command_type = CREATE_TABLE;
    List<String> skewedColNames = new ArrayList<String>();
    List<List<String>> skewedValues = new ArrayList<List<String>>();
    Map<List<String>, String> listBucketColValuesMapping = new HashMap<List<String>, String>();
    boolean storedAsDirs = false;
    boolean isUserStorageFormat = false;
    boolean partitionTransformSpecExists = false;
    String likeFile = null;
    String likeFileFormat = null;
    String sortOrder = null;
    RowFormatParams rowFormatParams = new RowFormatParams();
    StorageFormat storageFormat = new StorageFormat(conf);

    LOG.info("Creating table " + dbDotTab + " position=" + ast.getCharPositionInLine());
    int numCh = ast.getChildCount();

    // set storage handler if default handler is provided in config
    String defaultStorageHandler = HiveConf.getVar(conf, HIVE_DEFAULT_STORAGE_HANDLER);
    if (defaultStorageHandler != null && !defaultStorageHandler.isEmpty()) {
      LOG.info("Default storage handler class detected in config. Using storage handler class if exists: '{}'",
          defaultStorageHandler);
      storageFormat.setStorageHandler(defaultStorageHandler);
      isUserStorageFormat = true;
    }

    /*
     * Check the 1st-level children and do simple semantic checks: 1) CTLT and
     * CTAS should not coexists. 2) CTLT or CTAS should not coexists with column
     * list (target table schema). 3) CTAS does not support partitioning (for
     * now).
     */
    for (int num = 1; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);
      if (storageFormat.fillStorageFormat(child)) {
        isUserStorageFormat = true;
        continue;
      }
      switch (child.getToken().getType()) {
        case HiveParser.TOK_IFNOTEXISTS:
          ifNotExists = true;
          break;
        case HiveParser.KW_EXTERNAL:
          isExt = true;
          break;
        case HiveParser.KW_MANAGED:
          isManaged = true;
          isTransactional = true;
          break;
        case HiveParser.KW_TEMPORARY:
          isTemporary = true;
          isMaterialization = MATERIALIZATION_MARKER.equals(child.getText());
          break;
        case HiveParser.KW_TRANSACTIONAL:
          isTransactional = true;
          command_type = CTT;
          break;
        case HiveParser.TOK_LIKEFILE:
          if (cols.size() != 0) {
            throw new SemanticException(ErrorMsg.CTLT_COLLST_COEXISTENCE.getMsg());
          }
          likeFileFormat = getUnescapedName((ASTNode) child.getChild(0));
          likeFile = getUnescapedName((ASTNode) child.getChild(1));
          command_type = CTLF;
          break;
        case HiveParser.TOK_LIKETABLE:
          if (child.getChildCount() > 0) {
            likeTableName = getUnescapedName((ASTNode) child.getChild(0));
            if (likeTableName != null) {
              if (command_type == CTAS) {
                throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
              }
              if (cols.size() != 0) {
                throw new SemanticException(ErrorMsg.CTLT_COLLST_COEXISTENCE.getMsg());
              }
            }
            command_type = CTLT;
          }
          break;

        case HiveParser.TOK_QUERY: // CTAS
          if (command_type == CTLT) {
            throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
          }
          if (cols.size() != 0) {
            throw new SemanticException(ErrorMsg.CTAS_COLLST_COEXISTENCE.getMsg());
          }
          if (partCols.size() != 0 || bucketCols.size() != 0) {
            boolean dynPart = HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMIC_PARTITIONING);
            if (dynPart == false) {
              throw new SemanticException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
            } else {
              // TODO: support dynamic partition for CTAS
              throw new SemanticException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
            }
          }
          if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_CTAS_EXTERNAL_TABLES) && isExt) {
            throw new SemanticException(ErrorMsg.CTAS_EXTTBL_COEXISTENCE.getMsg());
          }
          command_type = CTAS;
          if (plannerCtx != null) {
            plannerCtx.setCTASToken(child);
          }
          selectStmt = child;
          break;
        case HiveParser.TOK_TABCOLLIST:
          cols = getColumns(child, true, ctx.getTokenRewriteStream(), primaryKeys, foreignKeys, uniqueConstraints,
              notNullConstraints, defaultConstraintInfo, checkConstraints, conf);
          break;
        case HiveParser.TOK_TABLECOMMENT:
          comment = unescapeSQLString(child.getChild(0).getText());
          break;
        case HiveParser.TOK_TABLEPARTCOLS:
          partCols = getColumns(child, false, ctx.getTokenRewriteStream(), primaryKeys, foreignKeys, uniqueConstraints,
              notNullConstraints, defaultConstraintInfo, checkConstraints, conf);
          if (hasConstraints(partCols, defaultConstraints, notNullConstraints, checkConstraints)) {
            //TODO: these constraints should be supported for partition columns
            throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg(
                "NOT NULL,DEFAULT and CHECK Constraints are not allowed with " + "partition columns. "));
          }
          break;
        case HiveParser.TOK_TABLEPARTCOLSBYSPEC:
          SessionStateUtil.addResourceOrThrow(conf, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC,
              PartitionTransform.getPartitionTransformSpec(child));
          partitionTransformSpecExists = true;
          break;
        case HiveParser.TOK_TABLEPARTCOLNAMES:
          partColNames = getColumnNames(child);
          break;
        case HiveParser.TOK_ALTERTABLE_BUCKETS:
          bucketCols = getColumnNames((ASTNode) child.getChild(0));
          if (child.getChildCount() == 2) {
            numBuckets = Integer.parseInt(child.getChild(1).getText());
          } else {
            sortCols = getColumnNamesOrder((ASTNode) child.getChild(1));
            numBuckets = Integer.parseInt(child.getChild(2).getText());
          }
          break;
        case HiveParser.TOK_WRITE_LOCALLY_ORDERED:
          sortOrder = getSortOrderJson((ASTNode) child.getChild(0));
          break;
        case HiveParser.TOK_TABLEROWFORMAT:
          rowFormatParams.analyzeRowFormat(child);
          break;
        case HiveParser.TOK_TABLELOCATION:
          location = unescapeSQLString(child.getChild(0).getText());
          location = EximUtil.relativeToAbsolutePath(conf, location);
          inputs.add(toReadEntity(location));
          break;
        case HiveParser.TOK_TABLEPROPERTIES:
          tblProps = getProps((ASTNode) child.getChild(0));
          addPropertyReadEntry(tblProps, inputs);
          break;
        case HiveParser.TOK_TABLESERIALIZER:
          child = (ASTNode) child.getChild(0);
          storageFormat.setSerde(unescapeSQLString(child.getChild(0).getText()));
          if (child.getChildCount() == 2) {
            readProps((ASTNode) (child.getChild(1).getChild(0)), storageFormat.getSerdeProps());
          }
          break;
        case HiveParser.TOK_TABLESKEWED:
          /**
           * Throw an error if the user tries to use the DDL with
           * hive.internal.ddl.list.bucketing.enable set to false.
           */
          HiveConf hiveConf = SessionState.get().getConf();

          // skewed column names
          skewedColNames = SkewedTableUtils.analyzeSkewedTableDDLColNames(child);
          // skewed value
          skewedValues = SkewedTableUtils.analyzeDDLSkewedValues(child);
          // stored as directories
          storedAsDirs = analyzeStoredAdDirs(child);

          break;
        default:
          throw new AssertionError("Unknown token: " + child.getToken());
      }
    }

    validateStorageFormat(storageFormat, tblProps, partitionTransformSpecExists);

    if (command_type == CREATE_TABLE || command_type == CTLT || command_type == CTT || command_type == CTLF) {
      queryState.setCommandType(HiveOperation.CREATETABLE);
    } else if (command_type == CTAS) {
      queryState.setCommandType(HiveOperation.CREATETABLE_AS_SELECT);
    } else {
      throw new SemanticException("Unrecognized command.");
    }

    storageFormat.fillDefaultStorageFormat(isExt, false);

    // check for existence of table
    if (ifNotExists) {
      try {
        Table table = getTable(qualifiedTabName, false);
        if (table != null) { // table exists
          return null;
        }
      } catch (HiveException e) {
        // should not occur since second parameter to getTableWithQN is false
        throw new IllegalStateException("Unexpected Exception thrown: " + e.getMessage(), e);
      }
    }

    if (isTemporary) {
      if (location == null) {
        // for temporary tables we set the location to something in the session's scratch dir
        // it has the same life cycle as the tmp table
        try {
          // Generate a unique ID for temp table path.
          // This path will be fixed for the life of the temp table.
          location = SessionState.generateTempTableLocation(conf);
        } catch (MetaException err) {
          throw new SemanticException("Error while generating temp table path:", err);
        }
      }
    }

    // Handle different types of CREATE TABLE command
    // Note: each branch must call addDbAndTabToOutputs after finalizing table properties.
    Database database = getDatabase(qualifiedTabName.getDb());
    boolean isDefaultTableTypeChanged = false;
    if (database.getParameters() != null) {
      String defaultTableType = database.getParameters().getOrDefault(DEFAULT_TABLE_TYPE, null);
      if (defaultTableType != null && defaultTableType.equalsIgnoreCase("external")) {
        isExt = true;
        isDefaultTableTypeChanged = true;
      } else if (defaultTableType != null && defaultTableType.equalsIgnoreCase("acid")) {
        isDefaultTableTypeChanged = true;
        if (isExt) { // create external table on db with default type as acid
          isTransactional = false;
        } else {
          isTransactional = true;
        }
      }
    }
    switch (command_type) {
      case CTLF:
        try {
          if (!SchemaInferenceUtils.doesSupportSchemaInference(conf, likeFileFormat)) {
            throw new SemanticException(ErrorMsg.CTLF_UNSUPPORTED_FORMAT.getErrorCodedMsg(likeFileFormat));
          }
        } catch (HiveException e) {
          throw new SemanticException(e.getMessage(), e);
        }
        // fall through
      case CREATE_TABLE: // REGULAR CREATE TABLE DDL
        if (!CollectionUtils.isEmpty(partColNames)) {
          throw new SemanticException(
              "Partition columns can only declared using their name and types in regular CREATE TABLE statements");
        }
        tblProps =
            validateAndAddDefaultProperties(tblProps, isExt, storageFormat, dbDotTab, sortCols, isMaterialization,
                isTemporary, isTransactional, isManaged,
                new String[]{qualifiedTabName.getDb(), qualifiedTabName.getTable()}, isDefaultTableTypeChanged);
        isExt = isExternalTableChanged(tblProps, isTransactional, isExt, isDefaultTableTypeChanged);
        addDbAndTabToOutputs(new String[]{qualifiedTabName.getDb(), qualifiedTabName.getTable()},
            TableType.MANAGED_TABLE, isTemporary, tblProps, storageFormat);
        if (!Strings.isNullOrEmpty(sortOrder)) {
          tblProps.put("default-sort-order", sortOrder);
        }
        CreateTableDesc crtTblDesc =
            new CreateTableDesc(qualifiedTabName, isExt, isTemporary, cols, partCols, bucketCols, sortCols, numBuckets,
                rowFormatParams.getFieldDelim(), rowFormatParams.getFieldEscape(), rowFormatParams.getCollItemDelim(),
                rowFormatParams.getMapKeyDelim(), rowFormatParams.getLineDelim(), comment,
                storageFormat.getInputFormat(), storageFormat.getOutputFormat(), location, storageFormat.getSerde(),
                storageFormat.getStorageHandler(), storageFormat.getSerdeProps(), tblProps, ifNotExists, skewedColNames,
                skewedValues, primaryKeys, foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints,
                checkConstraints);
        crtTblDesc.setStoredAsSubDirectories(storedAsDirs);
        crtTblDesc.setNullFormat(rowFormatParams.getNullFormat());
        crtTblDesc.setLikeFile(likeFile);
        crtTblDesc.setLikeFileFormat(likeFileFormat);

        crtTblDesc.validate(conf);
        // outputs is empty, which means this create table happens in the current
        // database.
        rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), crtTblDesc)));
        String tblLocation = null;
        if (location != null) {
          tblLocation = location;
        } else {
          tblLocation = getDefaultLocation(qualifiedTabName.getDb(), qualifiedTabName.getTable(), isExt);
        }
        boolean isNativeColumnDefaultSupported = false;
        try {
          HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(conf, storageFormat.getStorageHandler());
          if (storageHandler != null) {
            storageHandler.addResourcesForCreateTable(tblProps, conf);
            isNativeColumnDefaultSupported = storageHandler.supportsDefaultColumnValues(tblProps);
          }
        } catch (HiveException e) {
          throw new RuntimeException(e);
        }

        ConstraintsUtils.constraintInfosToDefaultConstraints(qualifiedTabName, defaultConstraintInfo,
            crtTblDesc.getDefaultConstraints(), isNativeColumnDefaultSupported);
        SessionStateUtil.addResourceOrThrow(conf, META_TABLE_LOCATION, tblLocation);

        if (isExt && ConstraintsUtils.hasEnabledOrValidatedConstraints(notNullConstraints, crtTblDesc.getDefaultConstraints(),
            checkConstraints, isNativeColumnDefaultSupported)) {
          throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg(
              "Constraints are disallowed with External tables. " + "Only RELY is allowed."));
        }

        if (checkConstraints != null && !checkConstraints.isEmpty()) {
          ConstraintsUtils.validateCheckConstraint(cols, checkConstraints, ctx.getConf());
        }
        break;
      case CTT: // CREATE TRANSACTIONAL TABLE
        if (isExt && !isDefaultTableTypeChanged) {
          throw new SemanticException(
              qualifiedTabName.getTable() + " cannot be declared transactional because it's an external table");
        }
        tblProps =
            validateAndAddDefaultProperties(tblProps, isExt, storageFormat, dbDotTab, sortCols, isMaterialization,
                isTemporary, isTransactional, isManaged,
                new String[]{qualifiedTabName.getDb(), qualifiedTabName.getTable()}, isDefaultTableTypeChanged);
        isExt = isExternalTableChanged(tblProps, isTransactional, isExt, isDefaultTableTypeChanged);
        addDbAndTabToOutputs(new String[]{qualifiedTabName.getDb(), qualifiedTabName.getTable()},
            TableType.MANAGED_TABLE, false, tblProps, storageFormat);

        CreateTableDesc crtTranTblDesc =
            new CreateTableDesc(qualifiedTabName, isExt, isTemporary, cols, partCols, bucketCols, sortCols, numBuckets,
                rowFormatParams.getFieldDelim(), rowFormatParams.getFieldEscape(), rowFormatParams.getCollItemDelim(),
                rowFormatParams.getMapKeyDelim(), rowFormatParams.getLineDelim(), comment,
                storageFormat.getInputFormat(), storageFormat.getOutputFormat(), location, storageFormat.getSerde(),
                storageFormat.getStorageHandler(), storageFormat.getSerdeProps(), tblProps, ifNotExists, skewedColNames,
                skewedValues, primaryKeys, foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints,
                checkConstraints);
        crtTranTblDesc.setStoredAsSubDirectories(storedAsDirs);
        crtTranTblDesc.setNullFormat(rowFormatParams.getNullFormat());

        crtTranTblDesc.validate(conf);
        // outputs is empty, which means this create table happens in the current
        // database.
        rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), crtTranTblDesc)));
        break;

      case CTLT: // create table like <tbl_name>

        tblProps =
            validateAndAddDefaultProperties(tblProps, isExt, storageFormat, dbDotTab, sortCols, isMaterialization,
                isTemporary,

                isTransactional, isManaged, new String[]{qualifiedTabName.getDb(), qualifiedTabName.getTable()},
                isDefaultTableTypeChanged);
        tblProps.put(hive_metastoreConstants.TABLE_IS_CTLT, "true");
        isExt = isExternalTableChanged(tblProps, isTransactional, isExt, isDefaultTableTypeChanged);
        addDbAndTabToOutputs(new String[]{qualifiedTabName.getDb(), qualifiedTabName.getTable()},
            TableType.MANAGED_TABLE, isTemporary, tblProps, storageFormat);

        Table likeTable = getTable(likeTableName, false);
        if (likeTable != null) {
          if (isTemporary || isExt || isIcebergTable(tblProps)) {
            updateDefaultTblProps(likeTable.getParameters(), tblProps, new ArrayList<>(
                Arrays.asList(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL,
                    hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES)));
          } else {
            updateDefaultTblProps(likeTable.getParameters(), tblProps, null);
          }
        }
        if (likeTable.getTableType() == TableType.EXTERNAL_TABLE && HiveConf.getBoolVar(conf,
            HiveConf.ConfVars.CREATE_TABLE_AS_EXTERNAL)) {
          isExt = true;
        }
        CreateTableLikeDesc crtTblLikeDesc =
            new CreateTableLikeDesc(dbDotTab, isExt, isTemporary, storageFormat.getInputFormat(),
                storageFormat.getOutputFormat(), location, storageFormat.getSerde(), storageFormat.getSerdeProps(),
                tblProps, ifNotExists, likeTableName, isUserStorageFormat);
        tblLocation = getDefaultLocation(qualifiedTabName.getDb(), qualifiedTabName.getTable(), isExt);
        SessionStateUtil.addResource(conf, META_TABLE_LOCATION, tblLocation);
        rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), crtTblLikeDesc)));
        break;

      case CTAS: // create table as select

        if (isTemporary) {
          if (!ctx.isExplainSkipExecution() && !isMaterialization) {
            SessionState ss = SessionState.get();
            if (ss == null) {
              throw new SemanticException(
                  "No current SessionState, cannot create temporary table " + qualifiedTabName.getNotEmptyDbTable());
            }
            Map<String, Table> tables = SessionHiveMetaStoreClient.getTempTablesForDatabase(qualifiedTabName.getDb(),
                qualifiedTabName.getTable());
            if (tables != null && tables.containsKey(qualifiedTabName.getTable())) {
              throw new SemanticException(
                  "Temporary table " + qualifiedTabName.getNotEmptyDbTable() + " already exists");
            }
          }
        } else {
          // Verify that the table does not already exist
          // dumpTable is only used to check the conflict for non-temporary tables
          try {
            Table dumpTable = db.newTable(dbDotTab);
            if (null != db.getTable(dumpTable.getDbName(), dumpTable.getTableName(), false)
                && !ctx.isExplainSkipExecution()) {
              throw new SemanticException(ErrorMsg.TABLE_ALREADY_EXISTS.getMsg(dbDotTab));
            }
          } catch (HiveException e) {
            throw new SemanticException(e);
          }
        }

        if (location != null && location.length() != 0) {
          Path locPath = new Path(location);
          FileSystem curFs = null;
          FileStatus locStats = null;
          try {
            curFs = locPath.getFileSystem(conf);
            if (curFs != null) {
              locStats = curFs.getFileStatus(locPath);
            }
            if (locStats != null && locStats.isDir()) {
              FileStatus[] lStats = curFs.listStatus(locPath);
              if (lStats != null && lStats.length != 0) {
                // Don't throw an exception if the target location only contains the staging-dirs
                for (FileStatus lStat : lStats) {
                  if (!lStat.getPath().getName().startsWith(HiveConf.getVar(conf, HiveConf.ConfVars.STAGING_DIR))) {
                    throw new SemanticException(ErrorMsg.CTAS_LOCATION_NONEMPTY.getMsg(location));
                  }
                }
              }
            }
          } catch (FileNotFoundException nfe) {
            //we will create the folder if it does not exist.
          } catch (IOException ioE) {
            LOG.debug("Exception when validate folder", ioE);
          }
          tblLocation = location;
        } else {
          tblLocation = getDefaultLocation(qualifiedTabName.getDb(), qualifiedTabName.getTable(), isExt);
        }
        SessionStateUtil.addResource(conf, META_TABLE_LOCATION, tblLocation);
        if (!CollectionUtils.isEmpty(partCols)) {
          throw new SemanticException("Partition columns can only declared using their names in CTAS statements");
        }

        tblProps =
            validateAndAddDefaultProperties(tblProps, isExt, storageFormat, dbDotTab, sortCols, isMaterialization,
                isTemporary, isTransactional, isManaged,
                new String[]{qualifiedTabName.getDb(), qualifiedTabName.getTable()}, isDefaultTableTypeChanged);
        isExt = isExternalTableChanged(tblProps, isTransactional, isExt, isDefaultTableTypeChanged);
        tblProps.put(TABLE_IS_CTAS, "true");
        addDbAndTabToOutputs(new String[]{qualifiedTabName.getDb(), qualifiedTabName.getTable()},
            TableType.MANAGED_TABLE, isTemporary, tblProps, storageFormat);
        tableDesc = new CreateTableDesc(qualifiedTabName, isExt, isTemporary, cols, partColNames, bucketCols, sortCols,
            numBuckets, rowFormatParams.getFieldDelim(), rowFormatParams.getFieldEscape(),
            rowFormatParams.getCollItemDelim(), rowFormatParams.getMapKeyDelim(), rowFormatParams.getLineDelim(),
            comment, storageFormat.getInputFormat(), storageFormat.getOutputFormat(), location,
            storageFormat.getSerde(), storageFormat.getStorageHandler(), storageFormat.getSerdeProps(), tblProps,
            ifNotExists, skewedColNames, skewedValues, true, primaryKeys, foreignKeys, uniqueConstraints,
            notNullConstraints, defaultConstraints, checkConstraints);
        tableDesc.setMaterialization(isMaterialization);
        tableDesc.setStoredAsSubDirectories(storedAsDirs);
        tableDesc.setNullFormat(rowFormatParams.getNullFormat());
        qb.setTableDesc(tableDesc);

        return selectStmt;

      default:
        throw new SemanticException("Unrecognized command.");
    }
    return null;
  }
}
