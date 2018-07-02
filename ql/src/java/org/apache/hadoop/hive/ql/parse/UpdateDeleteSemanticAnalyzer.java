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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.CreateTableLikeDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;
import org.apache.hadoop.hive.ql.plan.ExportWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A subclass of the {@link org.apache.hadoop.hive.ql.parse.SemanticAnalyzer} that just handles
 * update, delete and merge statements.  It works by rewriting the updates and deletes into insert
 * statements (since they are actually inserts) and then doing some patch up to make them work as
 * updates and deletes instead.
 */
public class UpdateDeleteSemanticAnalyzer extends SemanticAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateDeleteSemanticAnalyzer.class);

  private boolean useSuper = false;

  UpdateDeleteSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode tree) throws SemanticException {
    if (useSuper) {
      super.analyzeInternal(tree);
    } else {
      if (!getTxnMgr().supportsAcid()) {
        throw new SemanticException(ErrorMsg.ACID_OP_ON_NONACID_TXNMGR.getMsg());
      }
      switch (tree.getToken().getType()) {
        case HiveParser.TOK_DELETE_FROM:
          analyzeDelete(tree);
          break;
        case HiveParser.TOK_UPDATE_TABLE:
          analyzeUpdate(tree);
          break;
        case HiveParser.TOK_MERGE:
          analyzeMerge(tree);
          break;
        case HiveParser.TOK_EXPORT:
          analyzeAcidExport(tree);
          break;
        default:
          throw new RuntimeException("Asked to parse token " + tree.getName() + " in " +
              "UpdateDeleteSemanticAnalyzer");
      }
      cleanUpMetaColumnAccessControl();

    }
  }
  private boolean updating() {
    return currentOperation == Context.Operation.UPDATE;
  }
  private boolean deleting() {
    return currentOperation == Context.Operation.DELETE;
  }

  /**
   * Exporting an Acid table is more complicated than a flat table.  It may contains delete events,
   * which can only be interpreted properly withing the context of the table/metastore where they
   * were generated.  It may also contain insert events that belong to transactions that aborted
   * where the same constraints apply.
   * In order to make the export artifact free of these constraints, the export does a
   * insert into tmpTable select * from <export table> to filter/apply the events in current
   * context and then export the tmpTable.  This export artifact can now be imported into any
   * table on any cluster (subject to schema checks etc).
   * See {@link #analyzeAcidExport(ASTNode)}
   * @param tree Export statement
   * @return true if exporting an Acid table.
   */
  public static boolean isAcidExport(ASTNode tree) throws SemanticException {
    assert tree != null && tree.getToken() != null &&
        tree.getToken().getType() == HiveParser.TOK_EXPORT;
    Tree tokTab = tree.getChild(0);
    assert tokTab != null && tokTab.getType() == HiveParser.TOK_TAB;
    Table tableHandle = null;
    try {
      tableHandle = getTable((ASTNode) tokTab.getChild(0), Hive.get(), false);
    } catch(HiveException ex) {
      throw new SemanticException(ex);
    }

    //tableHandle can be null if table doesn't exist
    return tableHandle != null && AcidUtils.isFullAcidTable(tableHandle);
  }
  private static String getTmptTableNameForExport(Table exportTable) {
    String tmpTableDb = exportTable.getDbName();
    String tmpTableName = exportTable.getTableName() + "_" +
        UUID.randomUUID().toString().replace('-', '_');
    return Warehouse.getQualifiedName(tmpTableDb, tmpTableName);
  }

  /**
   * See {@link #isAcidExport(ASTNode)}
   * 1. create the temp table T
   * 2. compile 'insert into T select * from acidTable'
   * 3. compile 'export acidTable'  (acidTable will be replaced with T during execution)
   * 4. create task to drop T
   *
   * Using a true temp (session level) table means it should not affect replication and the table
   * is not visible outside the Session that created for security
   */
  private void analyzeAcidExport(ASTNode ast) throws SemanticException {
    assert ast != null && ast.getToken() != null &&
        ast.getToken().getType() == HiveParser.TOK_EXPORT;
    ASTNode tableTree = (ASTNode)ast.getChild(0);
    assert tableTree != null && tableTree.getType() == HiveParser.TOK_TAB;
    ASTNode tokRefOrNameExportTable = (ASTNode) tableTree.getChild(0);
    Table exportTable = getTargetTable(tokRefOrNameExportTable);
    assert AcidUtils.isFullAcidTable(exportTable);

    //need to create the table "manually" rather than creating a task since it has to exist to
    // compile the insert into T...
    String newTableName = getTmptTableNameForExport(exportTable); //this is db.table
    Map<String, String> tblProps = new HashMap<>();
    tblProps.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, Boolean.FALSE.toString());
    CreateTableLikeDesc ctlt = new CreateTableLikeDesc(newTableName,
        false, true, null,
        null, null, null, null,
        tblProps,
        true, //important so we get an exception on name collision
        Warehouse.getQualifiedName(exportTable.getTTable()), false);
    Table newTable;
    try {
      ReadEntity dbForTmpTable = new ReadEntity(db.getDatabase(exportTable.getDbName()));
      inputs.add(dbForTmpTable); //so the plan knows we are 'reading' this db - locks, security...
      DDLTask createTableTask = (DDLTask) TaskFactory.get(
          new DDLWork(new HashSet<>(), new HashSet<>(), ctlt), conf);
      createTableTask.setConf(conf); //above get() doesn't set it
      createTableTask.execute(new DriverContext(new Context(conf)));
      newTable = db.getTable(newTableName);
    } catch(IOException|HiveException ex) {
      throw new SemanticException(ex);
    }

    //now generate insert statement
    //insert into newTableName select * from ts <where partition spec>
    StringBuilder rewrittenQueryStr = generateExportQuery(newTable.getPartCols(),
        tokRefOrNameExportTable, tableTree, newTableName);
    ReparseResult rr = parseRewrittenQuery(rewrittenQueryStr, ctx.getCmd());
    Context rewrittenCtx = rr.rewrittenCtx;
    rewrittenCtx.setIsUpdateDeleteMerge(false); //it's set in parseRewrittenQuery()
    ASTNode rewrittenTree = rr.rewrittenTree;
    try {
      useSuper = true;
      //newTable has to exist at this point to compile
      super.analyze(rewrittenTree, rewrittenCtx);
    } finally {
      useSuper = false;
    }
    //now we have the rootTasks set up for Insert ... Select
    removeStatsTasks(rootTasks);
    //now make an ExportTask from temp table
    /*analyzeExport() creates TableSpec which in turn tries to build
     "public List<Partition> partitions" by looking in the metastore to find Partitions matching
     the partition spec in the Export command.  These of course don't exist yet since we've not
     ran the insert stmt yet!!!!!!!
      */
    Task<ExportWork> exportTask = ExportSemanticAnalyzer.analyzeExport(ast, newTableName,
        db, conf, inputs, outputs);

    AlterTableDesc alterTblDesc = null;
    {
      /**
       * add an alter table task to set transactional props
       * do it after populating temp table so that it's written as non-transactional table but
       * update props before export so that export archive metadata has these props.  This way when
       * IMPORT is done for this archive and target table doesn't exist, it will be created as Acid.
       */
      alterTblDesc = new AlterTableDesc(AlterTableDesc.AlterTableTypes.ADDPROPS);
      HashMap<String, String> mapProps = new HashMap<>();
      mapProps.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, Boolean.TRUE.toString());
      alterTblDesc.setProps(mapProps);
      alterTblDesc.setOldName(newTableName);
    }
    addExportTask(rootTasks, exportTask, TaskFactory.get(
        new DDLWork(getInputs(), getOutputs(), alterTblDesc)));

    {
      /**
       * Now make a task to drop temp table
       * {@link DDLSemanticAnalyzer#analyzeDropTable(ASTNode ast, TableType expectedType)
       */
      ReplicationSpec replicationSpec = new ReplicationSpec();
      DropTableDesc dropTblDesc = new DropTableDesc(newTableName, TableType.MANAGED_TABLE,
          false, true, replicationSpec);
      Task<DDLWork> dropTask =
          TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(), dropTblDesc), conf);
      exportTask.addDependentTask(dropTask);
    }
    markReadEntityForUpdate();
    if(ctx.isExplainPlan()) {
      try {
        //so that "explain" doesn't "leak" tmp tables
        db.dropTable(newTable.getDbName(), newTable.getTableName(), true, true, true);
      } catch(HiveException ex) {
        LOG.warn("Unable to drop " + newTableName + " due to: " + ex.getMessage(), ex);
      }
    }
  }
  /**
   * generate
   * insert into newTableName select * from ts <where partition spec>
   * for EXPORT command
   */
  private StringBuilder generateExportQuery(List<FieldSchema> partCols,
      ASTNode tokRefOrNameExportTable, ASTNode tableTree, String newTableName)
      throws SemanticException {
    StringBuilder rewrittenQueryStr = new StringBuilder("insert into ").append(newTableName);
    addPartitionColsToInsert(partCols, rewrittenQueryStr);
    rewrittenQueryStr.append(" select * from ").append(getFullTableNameForSQL(tokRefOrNameExportTable));
    //builds partition spec so we can build suitable WHERE clause
    TableSpec exportTableSpec = new TableSpec(db, conf, tableTree, false, true);
    if(exportTableSpec.getPartSpec() != null) {
      StringBuilder whereClause = null;
      int partColsIdx = -1; //keep track of corresponding col in partCols
      for(Map.Entry<String, String> ent : exportTableSpec.getPartSpec().entrySet()) {
        partColsIdx++;
        if(ent.getValue() == null) {
          continue; //partial spec
        }
        if(whereClause == null) {
          whereClause = new StringBuilder(" WHERE ");
        }
        if(whereClause.length() > " WHERE ".length()) {
          whereClause.append(" AND ");
        }
        whereClause.append(HiveUtils.unparseIdentifier(ent.getKey(), conf))
            .append(" = ").append(genPartValueString(partCols.get(partColsIdx).getType(), ent.getValue()));
      }
      if(whereClause != null) {
        rewrittenQueryStr.append(whereClause);
      }
    }
    return rewrittenQueryStr;
  }
  /**
   * Makes the exportTask run after all other tasks of the "insert into T ..." are done.
   */
  private void addExportTask(List<Task<?>> rootTasks,
      Task<ExportWork> exportTask, Task<DDLWork> alterTable) {
    for(Task<? extends  Serializable> t : rootTasks) {
      if(t.getNumChild() <= 0) {
        //todo: ConditionalTask#addDependentTask(Task) doesn't do the right thing: HIVE-18978
        t.addDependentTask(alterTable);
        //this is a leaf so add exportTask to follow it
        alterTable.addDependentTask(exportTask);
      } else {
        addExportTask(t.getDependentTasks(), exportTask, alterTable);
      }
    }
  }

  private List<Task<?>> findStatsTasks(
      List<Task<?>> rootTasks, List<Task<?>> statsTasks) {
    for(Task<? extends  Serializable> t : rootTasks) {
      if (t instanceof StatsTask) {
        if(statsTasks == null) {
          statsTasks = new ArrayList<>();
        }
        statsTasks.add(t);
      }
      if(t.getDependentTasks() != null) {
        statsTasks = findStatsTasks(t.getDependentTasks(), statsTasks);
      }
    }
    return statsTasks;
  }

  private void removeStatsTasks(List<Task<?>> rootTasks) {
    List<Task<?>> statsTasks = findStatsTasks(rootTasks, null);
    if(statsTasks == null) {
      return;
    }
    for (Task<?> statsTask : statsTasks) {
      if(statsTask.getParentTasks() == null) {
        continue; //should never happen
      }
      for (Task<?> t : new ArrayList<>(statsTask.getParentTasks())) {
        t.removeDependentTask(statsTask);
      }
    }
  }
  private void analyzeUpdate(ASTNode tree) throws SemanticException {
    currentOperation = Context.Operation.UPDATE;
    reparseAndSuperAnalyze(tree);
  }

  private void analyzeDelete(ASTNode tree) throws SemanticException {
    currentOperation = Context.Operation.DELETE;
    reparseAndSuperAnalyze(tree);
  }

  /**
   * Append list of partition columns to Insert statement, i.e. the 2nd set of partCol1,partCol2
   * INSERT INTO T PARTITION(partCol1,partCol2...) SELECT col1, ... partCol1,partCol2...
   * @param target target table
   */
  private void addPartitionColsToSelect(List<FieldSchema> partCols, StringBuilder rewrittenQueryStr,
                                        ASTNode target) throws SemanticException {
    String targetName = target != null ? getSimpleTableName(target) : null;

    // If the table is partitioned, we need to select the partition columns as well.
    if (partCols != null) {
      for (FieldSchema fschema : partCols) {
        rewrittenQueryStr.append(", ");
        //would be nice if there was a way to determine if quotes are needed
        if(targetName != null) {
          rewrittenQueryStr.append(targetName).append('.');
        }
        rewrittenQueryStr.append(HiveUtils.unparseIdentifier(fschema.getName(), this.conf));
      }
    }
  }
  /**
   * Assert that we are not asked to update a bucketing column or partition column
   * @param colName it's the A in "SET A = B"
   */
  private void checkValidSetClauseTarget(ASTNode colName, Table targetTable) throws SemanticException {
    String columnName = normalizeColName(colName.getText());

    // Make sure this isn't one of the partitioning columns, that's not supported.
    for (FieldSchema fschema : targetTable.getPartCols()) {
      if (fschema.getName().equalsIgnoreCase(columnName)) {
        throw new SemanticException(ErrorMsg.UPDATE_CANNOT_UPDATE_PART_VALUE.getMsg());
      }
    }
    //updating bucket column should move row from one file to another - not supported
    if(targetTable.getBucketCols() != null && targetTable.getBucketCols().contains(columnName)) {
      throw new SemanticException(ErrorMsg.UPDATE_CANNOT_UPDATE_BUCKET_VALUE,columnName);
    }
    boolean foundColumnInTargetTable = false;
    for(FieldSchema col : targetTable.getCols()) {
      if(columnName.equalsIgnoreCase(col.getName())) {
        foundColumnInTargetTable = true;
        break;
      }
    }
    if(!foundColumnInTargetTable) {
      throw new SemanticException(ErrorMsg.INVALID_TARGET_COLUMN_IN_SET_CLAUSE, colName.getText(),
        targetTable.getFullyQualifiedName());
    }
  }
  private ASTNode findLHSofAssignment(ASTNode assignment) {
    assert assignment.getToken().getType() == HiveParser.EQUAL :
      "Expected set assignments to use equals operator but found " + assignment.getName();
    ASTNode tableOrColTok = (ASTNode)assignment.getChildren().get(0);
    assert tableOrColTok.getToken().getType() == HiveParser.TOK_TABLE_OR_COL :
      "Expected left side of assignment to be table or column";
    ASTNode colName = (ASTNode)tableOrColTok.getChildren().get(0);
    assert colName.getToken().getType() == HiveParser.Identifier :
      "Expected column name";
    return colName;
  }
  private Map<String, ASTNode> collectSetColumnsAndExpressions(ASTNode setClause,
                         Set<String> setRCols, Table targetTable) throws SemanticException {
    // An update needs to select all of the columns, as we rewrite the entire row.  Also,
    // we need to figure out which columns we are going to replace.
    assert setClause.getToken().getType() == HiveParser.TOK_SET_COLUMNS_CLAUSE :
      "Expected second child of update token to be set token";

    // Get the children of the set clause, each of which should be a column assignment
    List<? extends Node> assignments = setClause.getChildren();
    // Must be deterministic order map for consistent q-test output across Java versions
    Map<String, ASTNode> setCols = new LinkedHashMap<String, ASTNode>(assignments.size());
    for (Node a : assignments) {
      ASTNode assignment = (ASTNode)a;
      ASTNode colName = findLHSofAssignment(assignment);
      if(setRCols != null) {
        addSetRCols((ASTNode) assignment.getChildren().get(1), setRCols);
      }
      checkValidSetClauseTarget(colName, targetTable);

      String columnName = normalizeColName(colName.getText());
      // This means that in UPDATE T SET x = _something_
      // _something_ can be whatever is supported in SELECT _something_
      setCols.put(columnName, (ASTNode)assignment.getChildren().get(1));
    }
    return setCols;
  }
  /**
   * @return the Metastore representation of the target table
   */
  private Table getTargetTable(ASTNode tabRef) throws SemanticException {
    return getTable(tabRef, db, true);
  }
  /**
   * @param throwException if false, return null if table doesn't exist, else throw
   */
  private static Table getTable(ASTNode tabRef, Hive db, boolean throwException)
      throws SemanticException {
    String[] tableName;
    Table mTable;
    switch (tabRef.getType()) {
      case HiveParser.TOK_TABREF:
        tableName = getQualifiedTableName((ASTNode) tabRef.getChild(0));
        break;
      case HiveParser.TOK_TABNAME:
        tableName = getQualifiedTableName(tabRef);
        break;
      default:
          throw raiseWrongType("TOK_TABREF|TOK_TABNAME", tabRef);
    }
    try {
      mTable = db.getTable(tableName[0], tableName[1], throwException);
    } catch (InvalidTableException e) {
      LOG.error("Failed to find table " + getDotName(tableName) + " got exception "
        + e.getMessage());
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(getDotName(tableName)), e);
    } catch (HiveException e) {
      LOG.error("Failed to find table " + getDotName(tableName) + " got exception "
        + e.getMessage());
      throw new SemanticException(e.getMessage(), e);
    }
    return mTable;
  }
  // Walk through all our inputs and set them to note that this read is part of an update or a
  // delete.
  private void markReadEntityForUpdate() {
    for (ReadEntity input : inputs) {
      if(isWritten(input)) {
        //todo: this is actually not adding anything since LockComponent uses a Trie to "promote" a lock
        //except by accident - when we have a partitioned target table we have a ReadEntity and WriteEntity
        //for the table, so we mark ReadEntity and then delete WriteEntity (replace with Partition entries)
        //so DbTxnManager skips Read lock on the ReadEntity....
        input.setUpdateOrDelete(true);//input.noLockNeeded()?
      }
    }
  }
  /**
   *  For updates, we need to set the column access info so that it contains information on
   *  the columns we are updating.
   *  (But not all the columns of the target table even though the rewritten query writes
   *  all columns of target table since that is an implmentation detail)
   */
  private void setUpAccessControlInfoForUpdate(Table mTable, Map<String, ASTNode> setCols) {
    ColumnAccessInfo cai = new ColumnAccessInfo();
    for (String colName : setCols.keySet()) {
      cai.add(Table.getCompleteName(mTable.getDbName(), mTable.getTableName()), colName);
    }
    setUpdateColumnAccessInfo(cai);
  }
  /**
   * We need to weed ROW__ID out of the input column info, as it doesn't make any sense to
   * require the user to have authorization on that column.
   */
  private void cleanUpMetaColumnAccessControl() {
    //we do this for Update/Delete (incl Merge) because we introduce this column into the query
    //as part of rewrite
    if (columnAccessInfo != null) {
      columnAccessInfo.stripVirtualColumn(VirtualColumn.ROWID);
    }
  }
  /**
   * Parse the newly generated SQL statement to get a new AST
   */
  private ReparseResult parseRewrittenQuery(StringBuilder rewrittenQueryStr, String originalQuery) throws SemanticException {
    // Set dynamic partitioning to nonstrict so that queries do not need any partition
    // references.
    // todo: this may be a perf issue as it prevents the optimizer.. or not
    HiveConf.setVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    // Disable LLAP IO wrapper; doesn't propagate extra ACID columns correctly.
    HiveConf.setBoolVar(conf, ConfVars.LLAP_IO_ROW_WRAPPER_ENABLED, false);
    // Parse the rewritten query string
    Context rewrittenCtx;
    try {
      rewrittenCtx = new Context(conf);
      // We keep track of all the contexts that are created by this query
      // so we can clear them when we finish execution
      ctx.addRewrittenStatementContext(rewrittenCtx);
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.UPDATEDELETE_IO_ERROR.getMsg());
    }
    rewrittenCtx.setExplainConfig(ctx.getExplainConfig());
    rewrittenCtx.setExplainPlan(ctx.isExplainPlan());
    rewrittenCtx.setStatsSource(ctx.getStatsSource());
    rewrittenCtx.setPlanMapper(ctx.getPlanMapper());
    rewrittenCtx.setIsUpdateDeleteMerge(true);
    rewrittenCtx.setCmd(rewrittenQueryStr.toString());

    ASTNode rewrittenTree;
    try {
      LOG.info("Going to reparse <" + originalQuery + "> as \n<" + rewrittenQueryStr.toString() + ">");
      rewrittenTree = ParseUtils.parse(rewrittenQueryStr.toString(), rewrittenCtx);
    } catch (ParseException e) {
      throw new SemanticException(ErrorMsg.UPDATEDELETE_PARSE_ERROR.getMsg(), e);
    }
    return new ReparseResult(rewrittenTree, rewrittenCtx);
  }
  /**
   * Assert it supports Acid write
   */
  private void validateTargetTable(Table mTable) throws SemanticException {
    if (mTable.getTableType() == TableType.VIRTUAL_VIEW ||
      mTable.getTableType() == TableType.MATERIALIZED_VIEW) {
        LOG.error("Table " + mTable.getFullyQualifiedName() + " is a view or materialized view");
        throw new SemanticException(ErrorMsg.UPDATE_DELETE_VIEW.getMsg());
    }
  }
  /**
   * This supports update and delete statements
   */
  private void reparseAndSuperAnalyze(ASTNode tree) throws SemanticException {
    List<? extends Node> children = tree.getChildren();
    // The first child should be the table we are deleting from
    ASTNode tabName = (ASTNode)children.get(0);
    assert tabName.getToken().getType() == HiveParser.TOK_TABNAME :
        "Expected tablename as first child of " + operation() + " but found " + tabName.getName();

    // Rewrite the delete or update into an insert.  Crazy, but it works as deletes and update
    // actually are inserts into the delta file in Hive.  A delete
    // DELETE FROM _tablename_ [WHERE ...]
    // will be rewritten as
    // INSERT INTO TABLE _tablename_ [PARTITION (_partcols_)] SELECT ROW__ID[,
    // _partcols_] from _tablename_ SORT BY ROW__ID
    // An update
    // UPDATE _tablename_ SET x = _expr_ [WHERE...]
    // will be rewritten as
    // INSERT INTO TABLE _tablename_ [PARTITION (_partcols_)] SELECT _all_,
    // _partcols_from _tablename_ SORT BY ROW__ID
    // where _all_ is all the non-partition columns.  The expressions from the set clause will be
    // re-attached later.
    // The where clause will also be re-attached later.
    // The sort by clause is put in there so that records come out in the right order to enable
    // merge on read.

    StringBuilder rewrittenQueryStr = new StringBuilder();
    Table mTable = getTargetTable(tabName);
    validateTargetTable(mTable);

    rewrittenQueryStr.append("insert into table ");
    rewrittenQueryStr.append(getFullTableNameForSQL(tabName));

    addPartitionColsToInsert(mTable.getPartCols(), rewrittenQueryStr);

    rewrittenQueryStr.append(" select ROW__ID");

    Map<Integer, ASTNode> setColExprs = null;
    Map<String, ASTNode> setCols = null;
    // Must be deterministic order set for consistent q-test output across Java versions
    Set<String> setRCols = new LinkedHashSet<String>();
    if (updating()) {
      // We won't write the set
      // expressions in the rewritten query.  We'll patch that up later.
      // The set list from update should be the second child (index 1)
      assert children.size() >= 2 : "Expected update token to have at least two children";
      ASTNode setClause = (ASTNode)children.get(1);
      setCols = collectSetColumnsAndExpressions(setClause, setRCols, mTable);
      setColExprs = new HashMap<>(setClause.getChildCount());

      List<FieldSchema> nonPartCols = mTable.getCols();
      for (int i = 0; i < nonPartCols.size(); i++) {
        rewrittenQueryStr.append(',');
        String name = nonPartCols.get(i).getName();
        ASTNode setCol = setCols.get(name);
        rewrittenQueryStr.append(HiveUtils.unparseIdentifier(name, this.conf));
        if (setCol != null) {
          // This is one of the columns we're setting, record it's position so we can come back
          // later and patch it up.
          // Add one to the index because the select has the ROW__ID as the first column.
          setColExprs.put(i + 1, setCol);
        }
      }
    }

    addPartitionColsToSelect(mTable.getPartCols(), rewrittenQueryStr, null);
    rewrittenQueryStr.append(" from ");
    rewrittenQueryStr.append(getFullTableNameForSQL(tabName));

    ASTNode where = null;
    int whereIndex = deleting() ? 1 : 2;
    if (children.size() > whereIndex) {
      where = (ASTNode)children.get(whereIndex);
      assert where.getToken().getType() == HiveParser.TOK_WHERE :
          "Expected where clause, but found " + where.getName();
    }

    // Add a sort by clause so that the row ids come out in the correct order
    rewrittenQueryStr.append(" sort by ROW__ID ");

    ReparseResult rr = parseRewrittenQuery(rewrittenQueryStr, ctx.getCmd());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = (ASTNode)rewrittenTree.getChildren().get(1);
    assert rewrittenInsert.getToken().getType() == HiveParser.TOK_INSERT :
        "Expected TOK_INSERT as second child of TOK_QUERY but found " + rewrittenInsert.getName();

    if(updating()) {
      rewrittenCtx.setOperation(Context.Operation.UPDATE);
      rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.UPDATE);
    }
    else if(deleting()) {
      rewrittenCtx.setOperation(Context.Operation.DELETE);
      rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.DELETE);
    }

    if (where != null) {
      // The structure of the AST for the rewritten insert statement is:
      // TOK_QUERY -> TOK_FROM
      //          \-> TOK_INSERT -> TOK_INSERT_INTO
      //                        \-> TOK_SELECT
      //                        \-> TOK_SORTBY
      // The following adds the TOK_WHERE and its subtree from the original query as a child of
      // TOK_INSERT, which is where it would have landed if it had been there originally in the
      // string.  We do it this way because it's easy then turning the original AST back into a
      // string and reparsing it.  We have to move the SORT_BY over one,
      // so grab it and then push it to the second slot, and put the where in the first slot
      ASTNode sortBy = (ASTNode)rewrittenInsert.getChildren().get(2);
      assert sortBy.getToken().getType() == HiveParser.TOK_SORTBY :
          "Expected TOK_SORTBY to be first child of TOK_SELECT, but found " + sortBy.getName();
      rewrittenInsert.addChild(sortBy);
      rewrittenInsert.setChild(2, where);
    }

    // Patch up the projection list for updates, putting back the original set expressions.
    if (updating() && setColExprs != null) {
      // Walk through the projection list and replace the column names with the
      // expressions from the original update.  Under the TOK_SELECT (see above) the structure
      // looks like:
      // TOK_SELECT -> TOK_SELEXPR -> expr
      //           \-> TOK_SELEXPR -> expr ...
      ASTNode rewrittenSelect = (ASTNode)rewrittenInsert.getChildren().get(1);
      assert rewrittenSelect.getToken().getType() == HiveParser.TOK_SELECT :
          "Expected TOK_SELECT as second child of TOK_INSERT but found " +
              rewrittenSelect.getName();
      for (Map.Entry<Integer, ASTNode> entry : setColExprs.entrySet()) {
        ASTNode selExpr = (ASTNode)rewrittenSelect.getChildren().get(entry.getKey());
        assert selExpr.getToken().getType() == HiveParser.TOK_SELEXPR :
            "Expected child of TOK_SELECT to be TOK_SELEXPR but was " + selExpr.getName();
        // Now, change it's child
        selExpr.setChild(0, entry.getValue());
      }
    }

    try {
      useSuper = true;
      super.analyze(rewrittenTree, rewrittenCtx);
    } finally {
      useSuper = false;
    }

    updateOutputs(mTable);


    if (updating()) {
      setUpAccessControlInfoForUpdate(mTable, setCols);

      // Add the setRCols to the input list
      for (String colName : setRCols) {
        if(columnAccessInfo != null) {//assuming this means we are not doing Auth
          columnAccessInfo.add(Table.getCompleteName(mTable.getDbName(), mTable.getTableName()),
            colName);
        }
      }
    }
  }
  /**
   * Check that {@code readEntity} is also being written
   */
  private boolean isWritten(Entity readEntity) {
    for(Entity writeEntity : outputs) {
      //make sure to compare them as Entity, i.e. that it's the same table or partition, etc
      if(writeEntity.toString().equalsIgnoreCase(readEntity.toString())) {
        return true;
      }
    }
    return false;
  }
  private String operation() {
    if (currentOperation == Context.Operation.OTHER) {
      throw new IllegalStateException("UpdateDeleteSemanticAnalyzer neither updating nor " +
        "deleting, operation not known.");
    }
    return currentOperation.toString();
  }

  // This method finds any columns on the right side of a set statement (thus rcols) and puts them
  // in a set so we can add them to the list of input cols to check.
  private void addSetRCols(ASTNode node, Set<String> setRCols) {

    // See if this node is a TOK_TABLE_OR_COL.  If so, find the value and put it in the list.  If
    // not, recurse on any children
    if (node.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
      ASTNode colName = (ASTNode)node.getChildren().get(0);
      assert colName.getToken().getType() == HiveParser.Identifier :
          "Expected column name";
      setRCols.add(normalizeColName(colName.getText()));
    } else if (node.getChildren() != null) {
      for (Node n : node.getChildren()) {
        addSetRCols((ASTNode)n, setRCols);
      }
    }
  }

  /**
   * Column names are stored in metastore in lower case, regardless of the CREATE TABLE statement.
   * Unfortunately there is no single place that normalizes the input query.
   * @param colName not null
   */
  private static String normalizeColName(String colName) {
    return colName.toLowerCase();
  }

  private Context.Operation currentOperation = Context.Operation.OTHER;
  private static final String Indent = "  ";

  private IdentifierQuoter quotedIdenfierHelper;

  /**
   * This allows us to take an arbitrary ASTNode and turn it back into SQL that produced it.
   * Since HiveLexer.g is written such that it strips away any ` (back ticks) around
   * quoted identifiers we need to add those back to generated SQL.
   * Additionally, the parser only produces tokens of type Identifier and never
   * QuotedIdentifier (HIVE-6013).  So here we just quote all identifiers.
   * (') around String literals are retained w/o issues
   */
  private static class IdentifierQuoter {
    private final TokenRewriteStream trs;
    private final IdentityHashMap<ASTNode, ASTNode> visitedNodes = new IdentityHashMap<>();
    IdentifierQuoter(TokenRewriteStream trs) {
      this.trs = trs;
      if(trs == null) {
        throw new IllegalArgumentException("Must have a TokenRewriteStream");
      }
    }
    private void visit(ASTNode n) {
      if(n.getType() == HiveParser.Identifier) {
        if(visitedNodes.containsKey(n)) {
          /**
           * Since we are modifying the stream, it's not idempotent.  Ideally, the caller would take
           * care to only quote Identifiers in each subtree once, but this makes it safe
           */
          return;
        }
        visitedNodes.put(n, n);
        trs.insertBefore(n.getToken(), "`");
        trs.insertAfter(n.getToken(), "`");
      }
      if(n.getChildCount() <= 0) {return;}
      for(Node c : n.getChildren()) {
        visit((ASTNode)c);
      }
    }
  }

  /**
   * This allows us to take an arbitrary ASTNode and turn it back into SQL that produced it without
   * needing to understand what it is (except for QuotedIdentifiers)
   *
   */
  private String getMatchedText(ASTNode n) {
    quotedIdenfierHelper.visit(n);
    return ctx.getTokenRewriteStream().toString(n.getTokenStartIndex(),
      n.getTokenStopIndex() + 1).trim();
  }
  /**
   * Here we take a Merge statement AST and generate a semantically equivalent multi-insert
   * statement to execute.  Each Insert leg represents a single WHEN clause.  As much as possible,
   * the new SQL statement is made to look like the input SQL statement so that it's easier to map
   * Query Compiler errors from generated SQL to original one this way.
   * The generated SQL is a complete representation of the original input for the same reason.
   * In many places SemanticAnalyzer throws exceptions that contain (line, position) coordinates.
   * If generated SQL doesn't have everything and is patched up later, these coordinates point to
   * the wrong place.
   *
   * @throws SemanticException
   */
  private void analyzeMerge(ASTNode tree) throws SemanticException {
    currentOperation = Context.Operation.MERGE;
    quotedIdenfierHelper = new IdentifierQuoter(ctx.getTokenRewriteStream());
    /*
     * See org.apache.hadoop.hive.ql.parse.TestMergeStatement for some examples of the merge AST
      For example, given:
      merge into acidTbl using nonAcidPart2 source ON acidTbl.a = source.a2
      WHEN MATCHED THEN UPDATE set b = source.b2
      WHEN NOT MATCHED THEN INSERT VALUES(source.a2, source.b2)

      We get AST like this:
      "(tok_merge " +
        "(tok_tabname acidtbl) (tok_tabref (tok_tabname nonacidpart2) source) " +
        "(= (. (tok_table_or_col acidtbl) a) (. (tok_table_or_col source) a2)) " +
        "(tok_matched " +
        "(tok_update " +
        "(tok_set_columns_clause (= (tok_table_or_col b) (. (tok_table_or_col source) b2))))) " +
        "(tok_not_matched " +
        "tok_insert " +
        "(tok_value_row (. (tok_table_or_col source) a2) (. (tok_table_or_col source) b2))))");

        And need to produce a multi-insert like this to execute:
        FROM acidTbl right outer join nonAcidPart2 ON acidTbl.a = source.a2
        Insert into table acidTbl select nonAcidPart2.a2, nonAcidPart2.b2 where acidTbl.a is null
        INSERT INTO TABLE acidTbl select target.ROW__ID, nonAcidPart2.a2, nonAcidPart2.b2 where nonAcidPart2.a2=acidTbl.a sort by acidTbl.ROW__ID
    */
    /*todo: we need some sort of validation phase over original AST to make things user friendly; for example, if
     original command refers to a column that doesn't exist, this will be caught when processing the rewritten query but
     the errors will point at locations that the user can't map to anything
     - VALUES clause must have the same number of values as target table (including partition cols).  Part cols go last in Select clause of Insert as Select
     todo: do we care to preserve comments in original SQL?
     todo: check if identifiers are propertly escaped/quoted in the generated SQL - it's currently inconsistent
      Look at UnparseTranslator.addIdentifierTranslation() - it does unescape + unparse...
     todo: consider "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET TargetTable.Col1 = SourceTable.Col1 "; what happens when source is empty?  This should be a runtime error - maybe not
      the outer side of ROJ is empty => the join produces 0 rows.  If supporting WHEN NOT MATCHED BY SOURCE, then this should be a runtime error
    */
    ASTNode target = (ASTNode)tree.getChild(0);
    ASTNode source = (ASTNode)tree.getChild(1);
    String targetName = getSimpleTableName(target);
    String sourceName = getSimpleTableName(source);
    ASTNode onClause = (ASTNode) tree.getChild(2);
    String onClauseAsText = getMatchedText(onClause);

    Table targetTable = getTargetTable(target);
    validateTargetTable(targetTable);
    List<ASTNode> whenClauses = findWhenClauses(tree);

    StringBuilder rewrittenQueryStr = new StringBuilder("FROM\n");
    rewrittenQueryStr.append(Indent).append(getFullTableNameForSQL(target));
    if(isAliased(target)) {
      rewrittenQueryStr.append(" ").append(targetName);
    }
    rewrittenQueryStr.append('\n');
    rewrittenQueryStr.append(Indent).append(chooseJoinType(whenClauses)).append("\n");
    if(source.getType() == HiveParser.TOK_SUBQUERY) {
      //this includes the mandatory alias
      rewrittenQueryStr.append(Indent).append(getMatchedText(source));
    }
    else {
      rewrittenQueryStr.append(Indent).append(getFullTableNameForSQL(source));
      if(isAliased(source)) {
        rewrittenQueryStr.append(" ").append(sourceName);
      }
    }
    rewrittenQueryStr.append('\n');
    rewrittenQueryStr.append(Indent).append("ON ").append(onClauseAsText).append('\n');

    /**
     * We allow at most 2 WHEN MATCHED clause, in which case 1 must be Update the other Delete
     * If we have both update and delete, the 1st one (in SQL code) must have "AND <extra predicate>"
     * so that the 2nd can ensure not to process the same rows.
     * Update and Delete may be in any order.  (Insert is always last)
     */
    String extraPredicate = null;
    int numWhenMatchedUpdateClauses = 0, numWhenMatchedDeleteClauses = 0;
    int numInsertClauses = 0;
    for(ASTNode whenClause : whenClauses) {
      switch (getWhenClauseOperation(whenClause).getType()) {
        case HiveParser.TOK_INSERT:
          numInsertClauses++;
          handleInsert(whenClause, rewrittenQueryStr, target, onClause, targetTable, targetName, onClauseAsText);
          break;
        case HiveParser.TOK_UPDATE:
          numWhenMatchedUpdateClauses++;
          String s = handleUpdate(whenClause, rewrittenQueryStr, target, onClauseAsText, targetTable, extraPredicate);
          if(numWhenMatchedUpdateClauses + numWhenMatchedDeleteClauses == 1) {
            extraPredicate = s;//i.e. it's the 1st WHEN MATCHED
          }
          break;
        case HiveParser.TOK_DELETE:
          numWhenMatchedDeleteClauses++;
          String s1 = handleDelete(whenClause, rewrittenQueryStr, target, onClauseAsText, targetTable, extraPredicate);
          if(numWhenMatchedUpdateClauses + numWhenMatchedDeleteClauses == 1) {
            extraPredicate = s1;//i.e. it's the 1st WHEN MATCHED
          }
          break;
        default:
          throw new IllegalStateException("Unexpected WHEN clause type: " + whenClause.getType() +
            addParseInfo(whenClause));
      }
      if(numWhenMatchedDeleteClauses > 1) {
        throw new SemanticException(ErrorMsg.MERGE_TOO_MANY_DELETE, ctx.getCmd());
      }
      if(numWhenMatchedUpdateClauses > 1) {
        throw new SemanticException(ErrorMsg.MERGE_TOO_MANY_UPDATE, ctx.getCmd());
      }
      assert numInsertClauses < 2: "too many Insert clauses";
    }
    if(numWhenMatchedDeleteClauses + numWhenMatchedUpdateClauses == 2 && extraPredicate == null) {
      throw new SemanticException(ErrorMsg.MERGE_PREDIACTE_REQUIRED, ctx.getCmd());
    }
    boolean validating = handleCardinalityViolation(rewrittenQueryStr, target, onClauseAsText,
      targetTable, numWhenMatchedDeleteClauses == 0 && numWhenMatchedUpdateClauses == 0);
    ReparseResult rr = parseRewrittenQuery(rewrittenQueryStr, ctx.getCmd());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;
    rewrittenCtx.setOperation(Context.Operation.MERGE);

    //set dest name mapping on new context; 1st chid is TOK_FROM
    for(int insClauseIdx = 1, whenClauseIdx = 0;
        insClauseIdx < rewrittenTree.getChildCount() - (validating ? 1 : 0/*skip cardinality violation clause*/);
        insClauseIdx++, whenClauseIdx++) {
      //we've added Insert clauses in order or WHEN items in whenClauses
      ASTNode insertClause = (ASTNode) rewrittenTree.getChild(insClauseIdx);
      switch (getWhenClauseOperation(whenClauses.get(whenClauseIdx)).getType()) {
        case HiveParser.TOK_INSERT:
          rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.INSERT);
          break;
        case HiveParser.TOK_UPDATE:
          rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.UPDATE);
          break;
        case HiveParser.TOK_DELETE:
          rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.DELETE);
          break;
        default:
          assert false;
      }
    }
    if(validating) {
      //here means the last branch of the multi-insert is Cardinality Validation
      rewrittenCtx.addDestNamePrefix(rewrittenTree.getChildCount() - 1, Context.DestClausePrefix.INSERT);
    }
    try {
      useSuper = true;
      super.analyze(rewrittenTree, rewrittenCtx);
    } finally {
      useSuper = false;
    }
    updateOutputs(targetTable);
  }

  /**
   * SemanticAnalyzer will generate a WriteEntity for the target table since it doesn't know/check
   * if the read and write are of the same table in "insert ... select ....".  Since DbTxnManager
   * uses Read/WriteEntity objects to decide which locks to acquire, we get more concurrency if we
   * have change the table WriteEntity to a set of partition WriteEntity objects based on
   * ReadEntity objects computed for this table.
   */
  private void updateOutputs(Table targetTable) {
    markReadEntityForUpdate();

    if(targetTable.isPartitioned()) {
      List<ReadEntity> partitionsRead = getRestrictedPartitionSet(targetTable);
      if(!partitionsRead.isEmpty()) {
        //if there is WriteEntity with WriteType=UPDATE/DELETE for target table, replace it with
        //WriteEntity for each partition
        List<WriteEntity> toRemove = new ArrayList<>();
        for(WriteEntity we : outputs) {
          WriteEntity.WriteType wt = we.getWriteType();
          if(isTargetTable(we, targetTable) &&
            (wt == WriteEntity.WriteType.UPDATE || wt == WriteEntity.WriteType.DELETE)) {
            /**
             * The assumption here is that SemanticAnalyzer will will generate ReadEntity for each
             * partition that exists and is matched by the WHERE clause (which may be all of them).
             * Since we don't allow updating the value of a partition column, we know that we always
             * write the same (or fewer) partitions than we read.  Still, the write is a Dynamic
             * Partition write - see HIVE-15032.
             */
            toRemove.add(we);
          }
        }
        outputs.removeAll(toRemove);
        // TODO: why is this like that?
        for(ReadEntity re : partitionsRead) {
          for(WriteEntity original : toRemove) {
            //since we may have both Update and Delete branches, Auth needs to know
            WriteEntity we = new WriteEntity(re.getPartition(), original.getWriteType());
            we.setDynamicPartitionWrite(original.isDynamicPartitionWrite());
            outputs.add(we);
          }
        }
      }
    }
  }
  /**
   * If the optimizer has determined that it only has to read some of the partitions of the
   * target table to satisfy the query, then we know that the write side of update/delete
   * (and update/delete parts of merge)
   * can only write (at most) that set of partitions (since we currently don't allow updating
   * partition (or bucket) columns).  So we want to replace the table level
   * WriteEntity in the outputs with WriteEntity for each of these partitions
   * ToDo: see if this should be moved to SemanticAnalyzer itself since it applies to any
   * insert which does a select against the same table.  Then SemanticAnalyzer would also
   * be able to not use DP for the Insert...
   *
   * Note that the Insert of Merge may be creating new partitions and writing to partitions
   * which were not read  (WHEN NOT MATCHED...).  WriteEntity for that should be created
   * in MoveTask (or some other task after the query is complete)
   */
  private List<ReadEntity> getRestrictedPartitionSet(Table targetTable) {
    List<ReadEntity> partitionsRead = new ArrayList<>();
    for(ReadEntity re : inputs) {
      if(re.isFromTopLevelQuery && re.getType() == Entity.Type.PARTITION && isTargetTable(re, targetTable)) {
        partitionsRead.add(re);
      }
    }
    return partitionsRead;
  }
  /**
   * if there is no WHEN NOT MATCHED THEN INSERT, we don't outer join
   */
  private String chooseJoinType(List<ASTNode> whenClauses) {
    for(ASTNode whenClause : whenClauses) {
      if(getWhenClauseOperation(whenClause).getType() == HiveParser.TOK_INSERT) {
        return "RIGHT OUTER JOIN";
      }
    }
    return "INNER JOIN";
  }
  /**
   * does this Entity belong to target table (partition)
   */
  private boolean isTargetTable(Entity entity, Table targetTable) {
    //todo: https://issues.apache.org/jira/browse/HIVE-15048
    /**
     * is this the right way to compare?  Should it just compare paths?
     * equals() impl looks heavy weight
     */
    return targetTable.equals(entity.getTable());
  }

  /**
   * Per SQL Spec ISO/IEC 9075-2:2011(E) Section 14.2 under "General Rules" Item 6/Subitem a/Subitem 2/Subitem B,
   * an error should be raised if > 1 row of "source" matches the same row in "target".
   * This should not affect the runtime of the query as it's running in parallel with other
   * branches of the multi-insert.  It won't actually write any data to merge_tmp_table since the
   * cardinality_violation() UDF throws an error whenever it's called killing the query
   * @return true if another Insert clause was added
   */
  private boolean handleCardinalityViolation(StringBuilder rewrittenQueryStr, ASTNode target,
                                             String onClauseAsString, Table targetTable,
                                             boolean onlyHaveWhenNotMatchedClause)
              throws SemanticException {
    if(!conf.getBoolVar(HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK)) {
      LOG.info("Merge statement cardinality violation check is disabled: " +
        HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK.varname);
      return false;
    }
    if(onlyHaveWhenNotMatchedClause) {
      //if no update or delete in Merge, there is no need to to do cardinality check
      return false;
    }
    //this is a tmp table and thus Session scoped and acid requires SQL statement to be serial in a
    // given session, i.e. the name can be fixed across all invocations
    String tableName = "merge_tmp_table";
    rewrittenQueryStr.append("\nINSERT INTO ").append(tableName)
      .append("\n  SELECT cardinality_violation(")
      .append(getSimpleTableName(target)).append(".ROW__ID");
      addPartitionColsToSelect(targetTable.getPartCols(), rewrittenQueryStr, target);

      rewrittenQueryStr.append(")\n WHERE ").append(onClauseAsString)
      .append(" GROUP BY ").append(getSimpleTableName(target)).append(".ROW__ID");

      addPartitionColsToSelect(targetTable.getPartCols(), rewrittenQueryStr, target);

      rewrittenQueryStr.append(" HAVING count(*) > 1");
    //say table T has partition p, we are generating
    //select cardinality_violation(ROW_ID, p) WHERE ... GROUP BY ROW__ID, p
    //the Group By args are passed to cardinality_violation to add the violating value to the error msg
    try {
      if (null == db.getTable(tableName, false)) {
        StorageFormat format = new StorageFormat(conf);
        format.processStorageFormat("TextFile");
        Table table = db.newTable(tableName);
        table.setSerializationLib(format.getSerde());
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new FieldSchema("val", "int", null));
        table.setFields(fields);
        table.setDataLocation(Warehouse.getDnsPath(new Path(SessionState.get().getTempTableSpace(),
          tableName), conf));
        table.getTTable().setTemporary(true);
        table.setStoredAsSubDirectories(false);
        table.setInputFormatClass(format.getInputFormat());
        table.setOutputFormatClass(format.getOutputFormat());
        db.createTable(table, true);
      }
    }
    catch(HiveException|MetaException e) {
      throw new SemanticException(e.getMessage(), e);
    }
    return true;
  }
  /**
   * @param onClauseAsString - because there is no clone() and we need to use in multiple places
   * @param deleteExtraPredicate - see notes at caller
   */
  private String handleUpdate(ASTNode whenMatchedUpdateClause, StringBuilder rewrittenQueryStr,
                              ASTNode target, String onClauseAsString, Table targetTable,
                              String deleteExtraPredicate) throws SemanticException {
    assert whenMatchedUpdateClause.getType() == HiveParser.TOK_MATCHED;
    assert getWhenClauseOperation(whenMatchedUpdateClause).getType() == HiveParser.TOK_UPDATE;
    String targetName = getSimpleTableName(target);
    rewrittenQueryStr.append("INSERT INTO ").append(getFullTableNameForSQL(target));
    addPartitionColsToInsert(targetTable.getPartCols(), rewrittenQueryStr);
    rewrittenQueryStr.append("    -- update clause\n select ").append(targetName).append(".ROW__ID");

    ASTNode setClause = (ASTNode)getWhenClauseOperation(whenMatchedUpdateClause).getChild(0);
    //columns being updated -> update expressions; "setRCols" (last param) is null because we use actual expressions
    //before reparsing, i.e. they are known to SemanticAnalyzer logic
    Map<String, ASTNode> setColsExprs = collectSetColumnsAndExpressions(setClause, null, targetTable);
    //if target table has cols c1,c2,c3 and p1 partition col and we had "SET c2 = 5, c1 = current_date()" we want to end up with
    //insert into target (p1) select current_date(), 5, c3, p1 where ....
    //since we take the RHS of set exactly as it was in Input, we don't need to deal with quoting/escaping column/table names
    List<FieldSchema> nonPartCols = targetTable.getCols();
    for(FieldSchema fs : nonPartCols) {
      rewrittenQueryStr.append(", ");
      String name = fs.getName();
      if (setColsExprs.containsKey(name)) {
        String rhsExp = getMatchedText(setColsExprs.get(name));
        //"set a=5, b=8" - rhsExp picks up the next char (e.g. ',') from the token stream
        switch (rhsExp.charAt(rhsExp.length() - 1)) {
          case ',':
          case '\n':
            rhsExp = rhsExp.substring(0, rhsExp.length() - 1);
        }
        rewrittenQueryStr.append(rhsExp);
      }
      else {
        rewrittenQueryStr.append(getSimpleTableName(target)).append(".").append(HiveUtils.unparseIdentifier(name, this.conf));
      }
    }
    addPartitionColsToSelect(targetTable.getPartCols(), rewrittenQueryStr, target);
    rewrittenQueryStr.append("\n   WHERE ").append(onClauseAsString);
    String extraPredicate = getWhenClausePredicate(whenMatchedUpdateClause);
    if(extraPredicate != null) {
      //we have WHEN MATCHED AND <boolean expr> THEN DELETE
      rewrittenQueryStr.append(" AND ").append(extraPredicate);
    }
    if(deleteExtraPredicate != null) {
      rewrittenQueryStr.append(" AND NOT(").append(deleteExtraPredicate).append(")");
    }
    rewrittenQueryStr.append("\n sort by ");
    rewrittenQueryStr.append(targetName).append(".ROW__ID \n");

    setUpAccessControlInfoForUpdate(targetTable, setColsExprs);
    //we don't deal with columns on RHS of SET expression since the whole expr is part of the
    //rewritten SQL statement and is thus handled by SemanticAnalzyer.  Nor do we have to
    //figure which cols on RHS are from source and which from target

    return extraPredicate;
  }
  /**
   * @param onClauseAsString - because there is no clone() and we need to use in multiple places
   * @param updateExtraPredicate - see notes at caller
   */
  private String handleDelete(ASTNode whenMatchedDeleteClause, StringBuilder rewrittenQueryStr, ASTNode target,
                            String onClauseAsString, Table targetTable, String updateExtraPredicate) throws SemanticException {
    assert whenMatchedDeleteClause.getType() == HiveParser.TOK_MATCHED;
    assert getWhenClauseOperation(whenMatchedDeleteClause).getType() == HiveParser.TOK_DELETE;
    List<FieldSchema> partCols = targetTable.getPartCols();
    String targetName = getSimpleTableName(target);
    rewrittenQueryStr.append("INSERT INTO ").append(getFullTableNameForSQL(target));
    addPartitionColsToInsert(partCols, rewrittenQueryStr);

    rewrittenQueryStr.append("    -- delete clause\n select ").append(targetName).append(".ROW__ID ");
    addPartitionColsToSelect(partCols, rewrittenQueryStr, target);
    rewrittenQueryStr.append("\n   WHERE ").append(onClauseAsString);
    String extraPredicate = getWhenClausePredicate(whenMatchedDeleteClause);
    if(extraPredicate != null) {
      //we have WHEN MATCHED AND <boolean expr> THEN DELETE
      rewrittenQueryStr.append(" AND ").append(extraPredicate);
    }
    if(updateExtraPredicate != null) {
      rewrittenQueryStr.append(" AND NOT(").append(updateExtraPredicate).append(")");
    }
    rewrittenQueryStr.append("\n sort by ");
    rewrittenQueryStr.append(targetName).append(".ROW__ID \n");
    return extraPredicate;
  }
  private static String addParseInfo(ASTNode n) {
    return " at " + ErrorMsg.renderPosition(n);
  }

  /**
   * Returns the table name to use in the generated query preserving original quotes/escapes if any
   * @see #getFullTableNameForSQL(ASTNode)
   */
  private String getSimpleTableName(ASTNode n) throws SemanticException {
    return HiveUtils.unparseIdentifier(getSimpleTableNameBase(n), this.conf);
  }
  private String getSimpleTableNameBase(ASTNode n) throws SemanticException {
    switch (n.getType()) {
      case HiveParser.TOK_TABREF:
        int aliasIndex = findTabRefIdxs(n)[0];
        if (aliasIndex != 0) {
          return n.getChild(aliasIndex).getText();//the alias
        }
        return getSimpleTableNameBase((ASTNode) n.getChild(0));
        case HiveParser.TOK_TABNAME:
        if(n.getChildCount() == 2) {
          //db.table -> return table
          return n.getChild(1).getText();
        }
        return n.getChild(0).getText();
      case HiveParser.TOK_SUBQUERY:
        return n.getChild(1).getText();//the alias
      default:
        throw raiseWrongType("TOK_TABREF|TOK_TABNAME|TOK_SUBQUERY", n);
    }
  }

  private static final class ReparseResult {
    private final ASTNode rewrittenTree;
    private final Context rewrittenCtx;
    ReparseResult(ASTNode n, Context c) {
      rewrittenTree = n;
      rewrittenCtx = c;
    }
  }

  private boolean isAliased(ASTNode n) {
    switch (n.getType()) {
      case HiveParser.TOK_TABREF:
        return findTabRefIdxs(n)[0] != 0;
      case HiveParser.TOK_TABNAME:
        return false;
      case HiveParser.TOK_SUBQUERY:
        assert n.getChildCount() > 1 : "Expected Derived Table to be aliased";
        return true;
      default:
        throw raiseWrongType("TOK_TABREF|TOK_TABNAME", n);
    }
  }
  /**
   * Collect WHEN clauses from Merge statement AST
   */
  private List<ASTNode> findWhenClauses(ASTNode tree) throws SemanticException {
    assert tree.getType() == HiveParser.TOK_MERGE;
    List<ASTNode> whenClauses = new ArrayList<>();
    for(int idx = 3; idx < tree.getChildCount(); idx++) {
      ASTNode whenClause = (ASTNode)tree.getChild(idx);
      assert whenClause.getType() == HiveParser.TOK_MATCHED ||
        whenClause.getType() == HiveParser.TOK_NOT_MATCHED :
        "Unexpected node type found: " + whenClause.getType() + addParseInfo(whenClause);
      whenClauses.add(whenClause);
    }
    if(whenClauses.size() <= 0) {
      //Futureproofing: the parser will actually not allow this
      throw new SemanticException("Must have at least 1 WHEN clause in MERGE statement");
    }
    return whenClauses;
  }
  private ASTNode getWhenClauseOperation(ASTNode whenClause) {
    if(!(whenClause.getType() == HiveParser.TOK_MATCHED || whenClause.getType() == HiveParser.TOK_NOT_MATCHED)) {
      throw  raiseWrongType("Expected TOK_MATCHED|TOK_NOT_MATCHED", whenClause);
    }
    return (ASTNode) whenClause.getChild(0);
  }
  /**
   * returns the <boolean predicate> as in WHEN MATCHED AND <boolean predicate> THEN...
   * @return may be null
   */
  private String getWhenClausePredicate(ASTNode whenClause) {
    if(!(whenClause.getType() == HiveParser.TOK_MATCHED || whenClause.getType() == HiveParser.TOK_NOT_MATCHED)) {
      throw  raiseWrongType("Expected TOK_MATCHED|TOK_NOT_MATCHED", whenClause);
    }
    if(whenClause.getChildCount() == 2) {
      return getMatchedText((ASTNode)whenClause.getChild(1));
    }
    return null;
  }
  /**
   * Generates the Insert leg of the multi-insert SQL to represent WHEN NOT MATCHED THEN INSERT clause
   * @param targetTableNameInSourceQuery - simple name/alias
   * @throws SemanticException
   */
  private void handleInsert(ASTNode whenNotMatchedClause, StringBuilder rewrittenQueryStr, ASTNode target,
                            ASTNode onClause, Table targetTable,
                            String targetTableNameInSourceQuery, String onClauseAsString) throws SemanticException {
    assert whenNotMatchedClause.getType() == HiveParser.TOK_NOT_MATCHED;
    assert getWhenClauseOperation(whenNotMatchedClause).getType() == HiveParser.TOK_INSERT;
    List<FieldSchema> partCols = targetTable.getPartCols();
    String valuesClause = getMatchedText((ASTNode)getWhenClauseOperation(whenNotMatchedClause).getChild(0));
    valuesClause = valuesClause.substring(1, valuesClause.length() - 1);//strip '(' and ')'
    valuesClause = SemanticAnalyzer.replaceDefaultKeywordForMerge(valuesClause, targetTable);

    rewrittenQueryStr.append("INSERT INTO ").append(getFullTableNameForSQL(target));
    addPartitionColsToInsert(partCols, rewrittenQueryStr);

    OnClauseAnalyzer oca = new OnClauseAnalyzer(onClause, targetTable, targetTableNameInSourceQuery,
      conf, onClauseAsString);
    oca.analyze();
    rewrittenQueryStr.append("    -- insert clause\n  select ")
      .append(valuesClause).append("\n   WHERE ").append(oca.getPredicate());
    String extraPredicate = getWhenClausePredicate(whenNotMatchedClause);
    if(extraPredicate != null) {
      //we have WHEN NOT MATCHED AND <boolean expr> THEN INSERT
      rewrittenQueryStr.append(" AND ")
        .append(getMatchedText(((ASTNode)whenNotMatchedClause.getChild(1)))).append('\n');
    }
  }
  /**
   * Suppose the input Merge statement has ON target.a = source.b and c = d.  Assume, that 'c' is from
   * target table and 'd' is from source expression.  In order to properly
   * generate the Insert for WHEN NOT MATCHED THEN INSERT, we need to make sure that the Where
   * clause of this Insert contains "target.a is null and target.c is null"  This ensures that this
   * Insert leg does not receive any rows that are processed by Insert corresponding to
   * WHEN MATCHED THEN ... clauses.  (Implicit in this is a mini resolver that figures out if an
   * unqualified column is part of the target table.  We can get away with this simple logic because
   * we know that target is always a table (as opposed to some derived table).
   * The job of this class is to generate this predicate.
   *
   * Note that is this predicate cannot simply be NOT(on-clause-expr).  IF on-clause-expr evaluates
   * to Unknown, it will be treated as False in the WHEN MATCHED Inserts but NOT(Unknown) = Unknown,
   * and so it will be False for WHEN NOT MATCHED Insert...
   */
  private static final class OnClauseAnalyzer {
    private final ASTNode onClause;
    private final Map<String, List<String>> table2column = new HashMap<>();
    private final List<String> unresolvedColumns = new ArrayList<>();
    private final List<FieldSchema> allTargetTableColumns = new ArrayList<>();
    private final Set<String> tableNamesFound = new HashSet<>();
    private final String targetTableNameInSourceQuery;
    private final HiveConf conf;
    private final String onClauseAsString;
    /**
     * @param targetTableNameInSourceQuery alias or simple name
     */
    OnClauseAnalyzer(ASTNode onClause, Table targetTable, String targetTableNameInSourceQuery,
                     HiveConf conf, String onClauseAsString) {
      this.onClause = onClause;
      allTargetTableColumns.addAll(targetTable.getCols());
      allTargetTableColumns.addAll(targetTable.getPartCols());
      this.targetTableNameInSourceQuery = unescapeIdentifier(targetTableNameInSourceQuery);
      this.conf = conf;
      this.onClauseAsString = onClauseAsString;
    }
    /**
     * finds all columns and groups by table ref (if there is one)
     */
    private void visit(ASTNode n) {
      if(n.getType() == HiveParser.TOK_TABLE_OR_COL) {
        ASTNode parent = (ASTNode) n.getParent();
        if(parent != null && parent.getType() == HiveParser.DOT) {
          //the ref must be a table, so look for column name as right child of DOT
          if(parent.getParent() != null && parent.getParent().getType() == HiveParser.DOT) {
            //I don't think this can happen... but just in case
            throw new IllegalArgumentException("Found unexpected db.table.col reference in " + onClauseAsString);
          }
          addColumn2Table(n.getChild(0).getText(), parent.getChild(1).getText());
        }
        else {
          //must be just a column name
          unresolvedColumns.add(n.getChild(0).getText());
        }
      }
      if(n.getChildCount() == 0) {
        return;
      }
      for(Node child : n.getChildren()) {
        visit((ASTNode)child);
      }
    }
    private void analyze() {
      visit(onClause);
      if(tableNamesFound.size() > 2) {
        throw new IllegalArgumentException("Found > 2 table refs in ON clause.  Found " +
          tableNamesFound + " in " + onClauseAsString);
      }
      handleUnresolvedColumns();
      if(tableNamesFound.size() > 2) {
        throw new IllegalArgumentException("Found > 2 table refs in ON clause (incl unresolved).  " +
          "Found " + tableNamesFound + " in " + onClauseAsString);
      }
    }
    /**
     * Find those that belong to target table
     */
    private void handleUnresolvedColumns() {
      if(unresolvedColumns.isEmpty()) { return; }
      for(String c : unresolvedColumns) {
        for(FieldSchema fs : allTargetTableColumns) {
          if(c.equalsIgnoreCase(fs.getName())) {
            //c belongs to target table; strictly speaking there maybe an ambiguous ref but
            //this will be caught later when multi-insert is parsed
            addColumn2Table(targetTableNameInSourceQuery.toLowerCase(), c);
            break;
          }
        }
      }
    }
    private void addColumn2Table(String tableName, String columnName) {
      tableName = tableName.toLowerCase();//normalize name for mapping
      tableNamesFound.add(tableName);
      List<String> cols = table2column.get(tableName);
      if(cols == null) {
        cols = new ArrayList<>();
        table2column.put(tableName, cols);
      }
      //we want to preserve 'columnName' as it was in original input query so that rewrite
      //looks as much as possible like original query
      cols.add(columnName);
    }
    /**
     * Now generate the predicate for Where clause
     */
    private String getPredicate() {
      //normilize table name for mapping
      List<String> targetCols = table2column.get(targetTableNameInSourceQuery.toLowerCase());
      if(targetCols == null) {
        /*e.g. ON source.t=1
        * this is not strictly speaking invlaid but it does ensure that all columns from target
        * table are all NULL for every row.  This would make any WHEN MATCHED clause invalid since
        * we don't have a ROW__ID.  The WHEN NOT MATCHED could be meaningful but it's just data from
        * source satisfying source.t=1...  not worth the effort to support this*/
        throw new IllegalArgumentException(ErrorMsg.INVALID_TABLE_IN_ON_CLAUSE_OF_MERGE
          .format(targetTableNameInSourceQuery, onClauseAsString));
      }
      StringBuilder sb = new StringBuilder();
      for(String col : targetCols) {
        if(sb.length() > 0) {
          sb.append(" AND ");
        }
        //but preserve table name in SQL
        sb.append(HiveUtils.unparseIdentifier(targetTableNameInSourceQuery, conf)).append(".").append(HiveUtils.unparseIdentifier(col, conf)).append(" IS NULL");
      }
      return sb.toString();
    }
  }
}
