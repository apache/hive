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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A subclass of the {@link org.apache.hadoop.hive.ql.parse.SemanticAnalyzer} that just handles
 * update, delete and merge statements.  It works by rewriting the updates and deletes into insert
 * statements (since they are actually inserts) and then doing some patch up to make them work as
 * updates and deletes instead.
 */
public abstract class RewriteSemanticAnalyzer extends CalcitePlanner {
  protected static final Logger LOG = LoggerFactory.getLogger(RewriteSemanticAnalyzer.class);

  protected boolean useSuper = false;

  RewriteSemanticAnalyzer(QueryState queryState) throws SemanticException {
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
      analyze(tree);
      cleanUpMetaColumnAccessControl();
    }
  }

  protected abstract void analyze(ASTNode tree) throws SemanticException;

  /**
   * Append list of partition columns to Insert statement, i.e. the 2nd set of partCol1,partCol2
   * INSERT INTO T PARTITION(partCol1,partCol2...) SELECT col1, ... partCol1,partCol2...
   * @param target target table
   */
  protected void addPartitionColsToSelect(List<FieldSchema> partCols, StringBuilder rewrittenQueryStr,
                                        ASTNode target) throws SemanticException {
    String targetName = target != null ? getSimpleTableName(target) : null;

    // If the table is partitioned, we need to select the partition columns as well.
    if (partCols != null) {
      for (FieldSchema fschema : partCols) {
        rewrittenQueryStr.append(", ");
        //would be nice if there was a way to determine if quotes are needed
        if (targetName != null) {
          rewrittenQueryStr.append(targetName).append('.');
        }
        rewrittenQueryStr.append(HiveUtils.unparseIdentifier(fschema.getName(), this.conf));
      }
    }
  }

  /**
   * Assert that we are not asked to update a bucketing column or partition column.
   * @param colName it's the A in "SET A = B"
   */
  protected void checkValidSetClauseTarget(ASTNode colName, Table targetTable) throws SemanticException {
    String columnName = normalizeColName(colName.getText());

    // Make sure this isn't one of the partitioning columns, that's not supported.
    for (FieldSchema fschema : targetTable.getPartCols()) {
      if (fschema.getName().equalsIgnoreCase(columnName)) {
        throw new SemanticException(ErrorMsg.UPDATE_CANNOT_UPDATE_PART_VALUE.getMsg());
      }
    }
    //updating bucket column should move row from one file to another - not supported
    if (targetTable.getBucketCols() != null && targetTable.getBucketCols().contains(columnName)) {
      throw new SemanticException(ErrorMsg.UPDATE_CANNOT_UPDATE_BUCKET_VALUE, columnName);
    }
    boolean foundColumnInTargetTable = false;
    for (FieldSchema col : targetTable.getCols()) {
      if (columnName.equalsIgnoreCase(col.getName())) {
        foundColumnInTargetTable = true;
        break;
      }
    }
    if (!foundColumnInTargetTable) {
      throw new SemanticException(ErrorMsg.INVALID_TARGET_COLUMN_IN_SET_CLAUSE, colName.getText(),
        targetTable.getFullyQualifiedName());
    }
  }

  protected ASTNode findLHSofAssignment(ASTNode assignment) {
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

  protected Map<String, ASTNode> collectSetColumnsAndExpressions(ASTNode setClause,
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
      if (setRCols != null) {
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
  protected Table getTargetTable(ASTNode tabRef) throws SemanticException {
    return getTable(tabRef, db, true);
  }

  /**
   * @param throwException if false, return null if table doesn't exist, else throw
   */
  protected static Table getTable(ASTNode tabRef, Hive db, boolean throwException) throws SemanticException {
    TableName tableName;
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

    Table mTable;
    try {
      mTable = db.getTable(tableName.getDb(), tableName.getTable(), throwException);
    } catch (InvalidTableException e) {
      LOG.error("Failed to find table " + tableName.getNotEmptyDbTable() + " got exception " + e.getMessage());
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName.getNotEmptyDbTable()), e);
    } catch (HiveException e) {
      LOG.error("Failed to find table " + tableName.getNotEmptyDbTable() + " got exception " + e.getMessage());
      throw new SemanticException(e.getMessage(), e);
    }
    return mTable;
  }

  /**
   *  Walk through all our inputs and set them to note that this read is part of an update or a delete.
   */
  protected void markReadEntityForUpdate() {
    for (ReadEntity input : inputs) {
      if (isWritten(input)) {
        //TODO: this is actually not adding anything since LockComponent uses a Trie to "promote" a lock
        //except by accident - when we have a partitioned target table we have a ReadEntity and WriteEntity
        //for the table, so we mark ReadEntity and then delete WriteEntity (replace with Partition entries)
        //so DbTxnManager skips Read lock on the ReadEntity....
        input.setUpdateOrDelete(true); //input.noLockNeeded()?
      }
    }
  }

  /**
   *  For updates, we need to set the column access info so that it contains information on
   *  the columns we are updating.
   *  (But not all the columns of the target table even though the rewritten query writes
   *  all columns of target table since that is an implementation detail).
   */
  protected void setUpAccessControlInfoForUpdate(Table mTable, Map<String, ASTNode> setCols) {
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
   * Parse the newly generated SQL statement to get a new AST.
   */
  protected ReparseResult parseRewrittenQuery(StringBuilder rewrittenQueryStr, String originalQuery)
      throws SemanticException {
    // Set dynamic partitioning to nonstrict so that queries do not need any partition
    // references.
    // TODO: this may be a perf issue as it prevents the optimizer.. or not
    HiveConf.setVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    // Disable LLAP IO wrapper; doesn't propagate extra ACID columns correctly.
    HiveConf.setBoolVar(conf, ConfVars.LLAP_IO_ROW_WRAPPER_ENABLED, false);
    // Parse the rewritten query string
    Context rewrittenCtx;
    rewrittenCtx = new Context(conf);
    rewrittenCtx.setHDFSCleanup(true);
    // We keep track of all the contexts that are created by this query
    // so we can clear them when we finish execution
    ctx.addSubContext(rewrittenCtx);
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
   * Assert it supports Acid write.
   */
  protected void validateTargetTable(Table mTable) throws SemanticException {
    if (mTable.getTableType() == TableType.VIRTUAL_VIEW || mTable.getTableType() == TableType.MATERIALIZED_VIEW) {
      LOG.error("Table " + mTable.getFullyQualifiedName() + " is a view or materialized view");
      throw new SemanticException(ErrorMsg.UPDATE_DELETE_VIEW.getMsg());
    }
  }

  /**
   * Check that {@code readEntity} is also being written.
   */
  private boolean isWritten(Entity readEntity) {
    for (Entity writeEntity : outputs) {
      //make sure to compare them as Entity, i.e. that it's the same table or partition, etc
      if (writeEntity.toString().equalsIgnoreCase(readEntity.toString())) {
        return true;
      }
    }
    return false;
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

  /**
   * SemanticAnalyzer will generate a WriteEntity for the target table since it doesn't know/check
   * if the read and write are of the same table in "insert ... select ....".  Since DbTxnManager
   * uses Read/WriteEntity objects to decide which locks to acquire, we get more concurrency if we
   * have change the table WriteEntity to a set of partition WriteEntity objects based on
   * ReadEntity objects computed for this table.
   */
  protected void updateOutputs(Table targetTable) {
    markReadEntityForUpdate();

    if (targetTable.isPartitioned()) {
      List<ReadEntity> partitionsRead = getRestrictedPartitionSet(targetTable);
      if (!partitionsRead.isEmpty()) {
        // if there is WriteEntity with WriteType=UPDATE/DELETE for target table, replace it with
        // WriteEntity for each partition
        List<WriteEntity> toRemove = new ArrayList<>();
        for (WriteEntity we : outputs) {
          WriteEntity.WriteType wt = we.getWriteType();
          if (isTargetTable(we, targetTable) &&
              (wt == WriteEntity.WriteType.UPDATE || wt == WriteEntity.WriteType.DELETE)) {
            // The assumption here is that SemanticAnalyzer will will generate ReadEntity for each
            // partition that exists and is matched by the WHERE clause (which may be all of them).
            // Since we don't allow updating the value of a partition column, we know that we always
            // write the same (or fewer) partitions than we read.  Still, the write is a Dynamic
            // Partition write - see HIVE-15032.
            toRemove.add(we);
          }
        }
        outputs.removeAll(toRemove);
        // TODO: why is this like that?
        for (ReadEntity re : partitionsRead) {
          for (WriteEntity original : toRemove) {
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
   * in MoveTask (or some other task after the query is complete).
   */
  private List<ReadEntity> getRestrictedPartitionSet(Table targetTable) {
    List<ReadEntity> partitionsRead = new ArrayList<>();
    for (ReadEntity re : inputs) {
      if (re.isFromTopLevelQuery && re.getType() == Entity.Type.PARTITION && isTargetTable(re, targetTable)) {
        partitionsRead.add(re);
      }
    }
    return partitionsRead;
  }

  /**
   * Does this Entity belong to target table (partition).
   */
  private boolean isTargetTable(Entity entity, Table targetTable) {
    //todo: https://issues.apache.org/jira/browse/HIVE-15048
    // Since any DDL now advances the write id, we should ignore the write Id,
    // while comparing two tables
    return targetTable.equalsWithIgnoreWriteId(entity.getTable());
  }

  /**
   * Returns the table name to use in the generated query preserving original quotes/escapes if any.
   * @see #getFullTableNameForSQL(ASTNode)
   */
  protected String getSimpleTableName(ASTNode n) throws SemanticException {
    return HiveUtils.unparseIdentifier(getSimpleTableNameBase(n), this.conf);
  }

  protected String getSimpleTableNameBase(ASTNode n) throws SemanticException {
    switch (n.getType()) {
    case HiveParser.TOK_TABREF:
      int aliasIndex = findTabRefIdxs(n)[0];
      if (aliasIndex != 0) {
        return n.getChild(aliasIndex).getText(); //the alias
      }
      return getSimpleTableNameBase((ASTNode) n.getChild(0));
    case HiveParser.TOK_TABNAME:
      if (n.getChildCount() == 2) {
        //db.table -> return table
        return n.getChild(1).getText();
      }
      return n.getChild(0).getText();
    case HiveParser.TOK_SUBQUERY:
      return n.getChild(1).getText(); //the alias
    default:
      throw raiseWrongType("TOK_TABREF|TOK_TABNAME|TOK_SUBQUERY", n);
    }
  }

  protected static final class ReparseResult {
    final ASTNode rewrittenTree;
    final Context rewrittenCtx;
    ReparseResult(ASTNode n, Context c) {
      rewrittenTree = n;
      rewrittenCtx = c;
    }
  }
}
