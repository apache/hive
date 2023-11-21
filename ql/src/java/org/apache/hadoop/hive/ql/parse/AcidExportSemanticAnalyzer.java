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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLTask;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.create.like.CreateTableLikeDesc;
import org.apache.hadoop.hive.ql.ddl.table.drop.DropTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.properties.AlterTableSetPropertiesDesc;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseUtils.ReparseResult;
import org.apache.hadoop.hive.ql.plan.ExportWork;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * A subclass of the {@link org.apache.hadoop.hive.ql.parse.SemanticAnalyzer} that just handles
 * acid export statements. It works by rewriting the acid export into insert statements into a temporary table,
 * and then export it from there.
 */
public class AcidExportSemanticAnalyzer extends RewriteSemanticAnalyzer<Object> {
  AcidExportSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState, null);
  }

  @Override
  protected ASTNode getTargetTableNode(ASTNode tree) {
    ASTNode tableTree = (ASTNode)tree.getChild(0);
    assert tableTree != null && tableTree.getType() == HiveParser.TOK_TAB;
    return (ASTNode) tableTree.getChild(0);
  }

  protected void analyze(ASTNode tree, Table table, ASTNode tableNameNode) throws SemanticException {
    if (tree.getToken().getType() != HiveParser.TOK_EXPORT) {
      throw new RuntimeException("Asked to parse token " + tree.getName() + " in " +
          "AcidExportSemanticAnalyzer");
    }
    analyzeAcidExport(tree, table, tableNameNode);
  }

  /**
   * Exporting an Acid table is more complicated than a flat table.  It may contains delete events,
   * which can only be interpreted properly withing the context of the table/metastore where they
   * were generated.  It may also contain insert events that belong to transactions that aborted
   * where the same constraints apply.
   * In order to make the export artifact free of these constraints, the export does a
   * insert into tmpTable select * from &lt;export table&gt; to filter/apply the events in current
   * context and then export the tmpTable.  This export artifact can now be imported into any
   * table on any cluster (subject to schema checks etc).
   * See {@link #analyzeAcidExport(ASTNode, Table , ASTNode)}
   * @param tree Export statement
   * @return true if exporting an Acid table.
   */
  public static boolean isAcidExport(ASTNode tree) throws SemanticException {
    assert tree != null && tree.getToken() != null && tree.getToken().getType() == HiveParser.TOK_EXPORT;
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
    String tmpTableName = exportTable.getTableName() + "_" + UUID.randomUUID().toString().replace('-', '_');
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
  private void analyzeAcidExport(ASTNode ast, Table exportTable, ASTNode tokRefOrNameExportTable) throws SemanticException {
    assert ast != null && ast.getToken() != null && ast.getToken().getType() == HiveParser.TOK_EXPORT;

    if (exportTable != null && (exportTable.isView() || exportTable.isMaterializedView())) {
      throw new SemanticException("Views and Materialized Views can not be exported.");
    }
    assert AcidUtils.isFullAcidTable(exportTable);

    //need to create the table "manually" rather than creating a task since it has to exist to
    // compile the insert into T...
    final String newTableName = getTmptTableNameForExport(exportTable); //this is db.table
    final TableName newTableNameRef = HiveTableName.of(newTableName);
    Map<String, String> tblProps = new HashMap<>();
    tblProps.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, Boolean.FALSE.toString());
    String location;

    // for temporary tables we set the location to something in the session's scratch dir
    // it has the same life cycle as the tmp table
    try {
      // Generate a unique ID for temp table path.
      // This path will be fixed for the life of the temp table.
      Path path = new Path(SessionState.getTempTableSpace(conf), UUID.randomUUID().toString());
      path = Warehouse.getDnsPath(path, conf);
      location = path.toString();
    } catch (MetaException err) {
      throw new SemanticException("Error while generating temp table path:", err);
    }

    CreateTableLikeDesc ctlt = new CreateTableLikeDesc(newTableName,
        false, true, null,
        null, location, null, null,
        tblProps,
        true, //important so we get an exception on name collision
        Warehouse.getQualifiedName(exportTable.getTTable()), false);
    Table newTable;
    try {
      ReadEntity dbForTmpTable = new ReadEntity(db.getDatabase(exportTable.getDbName()));
      inputs.add(dbForTmpTable); //so the plan knows we are 'reading' this db - locks, security...
      DDLTask createTableTask = (DDLTask) TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(), ctlt), conf);
      createTableTask.setConf(conf); //above get() doesn't set it
      Context context = new Context(conf);
      createTableTask.initialize(null, null, new TaskQueue(context), context);
      createTableTask.execute();
      newTable = db.getTable(newTableName);
    } catch(HiveException ex) {
      throw new SemanticException(ex);
    }

    //now generate insert statement
    //insert into newTableName select * from ts <where partition spec>
    StringBuilder rewrittenQueryStr = generateExportQuery(
            newTable.getPartCols(), tokRefOrNameExportTable, (ASTNode) tokRefOrNameExportTable.parent, newTableName);
    ReparseResult rr = ParseUtils.parseRewrittenQuery(ctx, rewrittenQueryStr);
    Context rewrittenCtx = rr.rewrittenCtx;
    rewrittenCtx.setIsUpdateDeleteMerge(false); //it's set in parseRewrittenQuery()
    ASTNode rewrittenTree = rr.rewrittenTree;
    //newTable has to exist at this point to compile
    analyzeRewrittenTree(rewrittenTree, rewrittenCtx);
    //now we have the rootTasks set up for Insert ... Select
    removeStatsTasks(rootTasks);
    //now make an ExportTask from temp table
    /*analyzeExport() creates TableSpec which in turn tries to build
     "public List<Partition> partitions" by looking in the metastore to find Partitions matching
     the partition spec in the Export command.  These of course don't exist yet since we've not
     ran the insert stmt yet!!!!!!!
      */
    Task<ExportWork> exportTask = ExportSemanticAnalyzer.analyzeExport(ast, newTableName, db, conf, inputs, outputs);

    // Add an alter table task to set transactional props
    // do it after populating temp table so that it's written as non-transactional table but
    // update props before export so that export archive metadata has these props.  This way when
    // IMPORT is done for this archive and target table doesn't exist, it will be created as Acid.
    Map<String, String> mapProps = new HashMap<>();
    mapProps.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, Boolean.TRUE.toString());
    AlterTableSetPropertiesDesc alterTblDesc = new AlterTableSetPropertiesDesc(newTableNameRef, null, null, false,
        mapProps, false, false, null);
    addExportTask(rootTasks, exportTask, TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblDesc)));

    // Now make a task to drop temp table
    // {@link DropTableAnalyzer#analyzeInternal(ASTNode ast)
    ReplicationSpec replicationSpec = new ReplicationSpec();
    DropTableDesc dropTblDesc = new DropTableDesc(newTableName, false, true, replicationSpec);
    Task<DDLWork> dropTask = TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(), dropTblDesc), conf);
    exportTask.addDependentTask(dropTask);
    markReadEntityForUpdate();
    if (ctx.isExplainPlan()) {
      try {
        //so that "explain" doesn't "leak" tmp tables
        // TODO: catalog
        db.dropTable(newTable.getDbName(), newTable.getTableName(), true, true, true);
      } catch(HiveException ex) {
        LOG.warn("Unable to drop " + newTableName + " due to: " + ex.getMessage(), ex);
      }
    }
  }

  /**
   * Generate
   * insert into newTableName select * from ts <where partition spec>
   * for EXPORT command.
   */
  private StringBuilder generateExportQuery(List<FieldSchema> partCols, ASTNode tokRefOrNameExportTable,
      ASTNode tableTree, String newTableName) throws SemanticException {
    StringBuilder rewrittenQueryStr = new StringBuilder("insert into ").append(newTableName);
    addPartitionColsToInsert(partCols, rewrittenQueryStr);
    rewrittenQueryStr.append(" select * from ").append(getFullTableNameForSQL(tokRefOrNameExportTable));
    //builds partition spec so we can build suitable WHERE clause
    TableSpec exportTableSpec = new TableSpec(db, conf, tableTree, false, true);
    if (exportTableSpec.getPartSpec() != null) {
      StringBuilder whereClause = null;
      int partColsIdx = -1; //keep track of corresponding col in partCols
      for (Map.Entry<String, String> ent : exportTableSpec.getPartSpec().entrySet()) {
        partColsIdx++;
        if (ent.getValue() == null) {
          continue; //partial spec
        }
        if (whereClause == null) {
          whereClause = new StringBuilder(" WHERE ");
        }
        if (whereClause.length() > " WHERE ".length()) {
          whereClause.append(" AND ");
        }
        whereClause.append(HiveUtils.unparseIdentifier(ent.getKey(), conf))
            .append(" = ").append(genPartValueString(partCols.get(partColsIdx).getType(), ent.getValue()));
      }
      if (whereClause != null) {
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
    for (Task<? extends  Serializable> t : rootTasks) {
      if (t.getNumChild() <= 0) {
        //todo: ConditionalTask#addDependentTask(Task) doesn't do the right thing: HIVE-18978
        t.addDependentTask(alterTable);
        //this is a leaf so add exportTask to follow it
        alterTable.addDependentTask(exportTask);
      } else {
        addExportTask(t.getDependentTasks(), exportTask, alterTable);
      }
    }
  }

  private void removeStatsTasks(List<Task<?>> rootTasks) {
    List<Task<?>> statsTasks = findStatsTasks(rootTasks, null);
    if (statsTasks == null) {
      return;
    }
    for (Task<?> statsTask : statsTasks) {
      if (statsTask.getParentTasks() == null) {
        continue; //should never happen
      }
      for (Task<?> t : new ArrayList<>(statsTask.getParentTasks())) {
        t.removeDependentTask(statsTask);
      }
    }
  }

  private List<Task<?>> findStatsTasks(
      List<Task<?>> rootTasks, List<Task<?>> statsTasks) {
    for (Task<? extends  Serializable> t : rootTasks) {
      if (t instanceof StatsTask) {
        if (statsTasks == null) {
          statsTasks = new ArrayList<>();
        }
        statsTasks.add(t);
      }
      if (t.getDependentTasks() != null) {
        statsTasks = findStatsTasks(t.getDependentTasks(), statsTasks);
      }
    }
    return statsTasks;
  }
}
