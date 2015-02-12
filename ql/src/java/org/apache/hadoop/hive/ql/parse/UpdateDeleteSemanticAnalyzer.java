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

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.session.SessionState;


/**
 * A subclass of the {@link org.apache.hadoop.hive.ql.parse.SemanticAnalyzer} that just handles
 * update and delete statements.  It works by rewriting the updates and deletes into insert
 * statements (since they are actually inserts) and then doing some patch up to make them work as
 * updates and deletes instead.
 */
public class UpdateDeleteSemanticAnalyzer extends SemanticAnalyzer {

  boolean useSuper = false;

  public UpdateDeleteSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  @Override
  public void analyzeInternal(ASTNode tree) throws SemanticException {
    if (useSuper) {
      super.analyzeInternal(tree);
    } else {

      if (!SessionState.get().getTxnMgr().supportsAcid()) {
        throw new SemanticException(ErrorMsg.ACID_OP_ON_NONACID_TXNMGR.getMsg());
      }
      switch (tree.getToken().getType()) {
        case HiveParser.TOK_DELETE_FROM:
          analyzeDelete(tree);
          return;

        case HiveParser.TOK_UPDATE_TABLE:
          analyzeUpdate(tree);
          return;

        default:
          throw new RuntimeException("Asked to parse token " + tree.getName() + " in " +
              "UpdateDeleteSemanticAnalyzer");
      }
    }
  }

  @Override
  protected boolean updating() {
    return ctx.getAcidOperation() == AcidUtils.Operation.UPDATE;
  }

  @Override
  protected boolean deleting() {
    return ctx.getAcidOperation() == AcidUtils.Operation.DELETE;
  }

  private void analyzeUpdate(ASTNode tree) throws SemanticException {
    ctx.setAcidOperation(AcidUtils.Operation.UPDATE);
    reparseAndSuperAnalyze(tree);
  }

  private void analyzeDelete(ASTNode tree) throws SemanticException {
    ctx.setAcidOperation(AcidUtils.Operation.DELETE);
    reparseAndSuperAnalyze(tree);
  }

  private void reparseAndSuperAnalyze(ASTNode tree) throws SemanticException {
    List<? extends Node> children = tree.getChildren();
    // The first child should be the table we are deleting from
    ASTNode tabName = (ASTNode)children.get(0);
    assert tabName.getToken().getType() == HiveParser.TOK_TABNAME :
        "Expected tablename as first child of " + operation() + " but found " + tabName.getName();
    String[] tableName = getQualifiedTableName(tabName);

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
    Table mTable;
    try {
      mTable = db.getTable(tableName[0], tableName[1]);
    } catch (InvalidTableException e) {
      LOG.error("Failed to find table " + getDotName(tableName) + " got exception "
          + e.getMessage());
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(getDotName(tableName)), e);
    } catch (HiveException e) {
      LOG.error("Failed to find table " + getDotName(tableName) + " got exception "
          + e.getMessage());
      throw new SemanticException(e.getMessage(), e);
    }

    List<FieldSchema> partCols = mTable.getPartCols();

    rewrittenQueryStr.append("insert into table ");
    rewrittenQueryStr.append(getDotName(tableName));

    // If the table is partitioned we have to put the partition() clause in
    if (partCols != null && partCols.size() > 0) {
      rewrittenQueryStr.append(" partition (");
      boolean first = true;
      for (FieldSchema fschema : partCols) {
        if (first) first = false;
        else rewrittenQueryStr.append(", ");
        rewrittenQueryStr.append(fschema.getName());
      }
      rewrittenQueryStr.append(")");
    }

    rewrittenQueryStr.append(" select ROW__ID");
    Map<Integer, ASTNode> setColExprs = null;
    Map<String, ASTNode> setCols = null;
    // Must be deterministic order set for consistent q-test output across Java versions
    Set<String> setRCols = new LinkedHashSet<String>();
    if (updating()) {
      // An update needs to select all of the columns, as we rewrite the entire row.  Also,
      // we need to figure out which columns we are going to replace.  We won't write the set
      // expressions in the rewritten query.  We'll patch that up later.
      // The set list from update should be the second child (index 1)
      assert children.size() >= 2 : "Expected update token to have at least two children";
      ASTNode setClause = (ASTNode)children.get(1);
      assert setClause.getToken().getType() == HiveParser.TOK_SET_COLUMNS_CLAUSE :
          "Expected second child of update token to be set token";

      // Get the children of the set clause, each of which should be a column assignment
      List<? extends Node> assignments = setClause.getChildren();
      // Must be deterministic order map for consistent q-test output across Java versions
      setCols = new LinkedHashMap<String, ASTNode>(assignments.size());
      setColExprs = new HashMap<Integer, ASTNode>(assignments.size());
      for (Node a : assignments) {
        ASTNode assignment = (ASTNode)a;
        assert assignment.getToken().getType() == HiveParser.EQUAL :
            "Expected set assignments to use equals operator but found " + assignment.getName();
        ASTNode tableOrColTok = (ASTNode)assignment.getChildren().get(0);
        assert tableOrColTok.getToken().getType() == HiveParser.TOK_TABLE_OR_COL :
            "Expected left side of assignment to be table or column";
        ASTNode colName = (ASTNode)tableOrColTok.getChildren().get(0);
        assert colName.getToken().getType() == HiveParser.Identifier :
            "Expected column name";

        addSetRCols((ASTNode) assignment.getChildren().get(1), setRCols);

        String columnName = colName.getText();

        // Make sure this isn't one of the partitioning columns, that's not supported.
        if (partCols != null) {
          for (FieldSchema fschema : partCols) {
            if (fschema.getName().equalsIgnoreCase(columnName)) {
              throw new SemanticException(ErrorMsg.UPDATE_CANNOT_UPDATE_PART_VALUE.getMsg());
            }
          }
        }

        // This means that in UPDATE T SET x = _something_
        // _something_ can be whatever is supported in SELECT _something_
        setCols.put(columnName, (ASTNode)assignment.getChildren().get(1));
      }

      List<FieldSchema> nonPartCols = mTable.getCols();
      for (int i = 0; i < nonPartCols.size(); i++) {
        rewrittenQueryStr.append(',');
        String name = nonPartCols.get(i).getName();
        ASTNode setCol = setCols.get(name);
        rewrittenQueryStr.append(name);
        if (setCol != null) {
          // This is one of the columns we're setting, record it's position so we can come back
          // later and patch it up.
          // Add one to the index because the select has the ROW__ID as the first column.
          setColExprs.put(i + 1, setCol);
        }
      }
    }

    // If the table is partitioned, we need to select the partition columns as well.
    if (partCols != null) {
      for (FieldSchema fschema : partCols) {
        rewrittenQueryStr.append(", ");
        rewrittenQueryStr.append(fschema.getName());
      }
    }
    rewrittenQueryStr.append(" from ");
    rewrittenQueryStr.append(getDotName(tableName));

    ASTNode where = null;
    int whereIndex = deleting() ? 1 : 2;
    if (children.size() > whereIndex) {
      where = (ASTNode)children.get(whereIndex);
      assert where.getToken().getType() == HiveParser.TOK_WHERE :
          "Expected where clause, but found " + where.getName();
    }

    // Add a sort by clause so that the row ids come out in the correct order
    rewrittenQueryStr.append(" sort by ROW__ID ");

    // Parse the rewritten query string
    Context rewrittenCtx;
    try {
      // Set dynamic partitioning to nonstrict so that queries do not need any partition
      // references.
      HiveConf.setVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
      rewrittenCtx = new Context(conf);
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.UPDATEDELETE_IO_ERROR.getMsg());
    }
    rewrittenCtx.setCmd(rewrittenQueryStr.toString());
    rewrittenCtx.setAcidOperation(ctx.getAcidOperation());

    ParseDriver pd = new ParseDriver();
    ASTNode rewrittenTree;
    try {
      LOG.info("Going to reparse " + operation() + " as <" + rewrittenQueryStr.toString() + ">");
      rewrittenTree = pd.parse(rewrittenQueryStr.toString(), rewrittenCtx);
      rewrittenTree = ParseUtils.findRootNonNullToken(rewrittenTree);

    } catch (ParseException e) {
      throw new SemanticException(ErrorMsg.UPDATEDELETE_PARSE_ERROR.getMsg(), e);
    }

    ASTNode rewrittenInsert = (ASTNode)rewrittenTree.getChildren().get(1);
    assert rewrittenInsert.getToken().getType() == HiveParser.TOK_INSERT :
        "Expected TOK_INSERT as second child of TOK_QUERY but found " + rewrittenInsert.getName();

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

    // Walk through all our inputs and set them to note that this read is part of an update or a
    // delete.
    for (ReadEntity input : inputs) {
      input.setUpdateOrDelete(true);
    }

    if (inputIsPartitioned(inputs)) {
      // In order to avoid locking the entire write table we need to replace the single WriteEntity
      // with a WriteEntity for each partition
      outputs.clear();
      for (ReadEntity input : inputs) {
        if (input.getTyp() == Entity.Type.PARTITION) {
          WriteEntity.WriteType writeType = deleting() ? WriteEntity.WriteType.DELETE :
              WriteEntity.WriteType.UPDATE;
          outputs.add(new WriteEntity(input.getPartition(), writeType));
        }
      }
    } else {
      // We still need to patch up the WriteEntities as they will have an insert type.  Change
      // them to the appropriate type for our operation.
      for (WriteEntity output : outputs) {
        output.setWriteType(deleting() ? WriteEntity.WriteType.DELETE :
            WriteEntity.WriteType.UPDATE);
      }
    }

    // For updates, we need to set the column access info so that it contains information on
    // the columns we are updating.
    if (updating()) {
      ColumnAccessInfo cai = new ColumnAccessInfo();
      for (String colName : setCols.keySet()) {
        cai.add(Table.getCompleteName(mTable.getDbName(), mTable.getTableName()), colName);
      }
      setUpdateColumnAccessInfo(cai);

      // Add the setRCols to the input list
      for (String colName : setRCols) {
        if(columnAccessInfo != null) {//assuming this means we are not doing Auth
          columnAccessInfo.add(Table.getCompleteName(mTable.getDbName(), mTable.getTableName()),
            colName);
        }
      }
    }

    // We need to weed ROW__ID out of the input column info, as it doesn't make any sense to
    // require the user to have authorization on that column.
    if (columnAccessInfo != null) {
      columnAccessInfo.stripVirtualColumn(VirtualColumn.ROWID);
    }
  }

  private String operation() {
    if (updating()) return "update";
    else if (deleting()) return "delete";
    else throw new IllegalStateException("UpdateDeleteSemanticAnalyzer neither updating nor " +
          "deleting, operation not known.");
  }

  private boolean inputIsPartitioned(Set<ReadEntity> inputs) {
    // We cannot simply look at the first entry, as in the case where the input is partitioned
    // there will be a table entry as well.  So look for at least one partition entry.
    for (ReadEntity re : inputs) {
      if (re.getTyp() == Entity.Type.PARTITION) {
        return true;
      }
    }
    return false;
  }

  // This method find any columns on the right side of a set statement (thus rcols) and puts them
  // in a set so we can add them to the list of input cols to check.
  private void addSetRCols(ASTNode node, Set<String> setRCols) {

    // See if this node is a TOK_TABLE_OR_COL.  If so, find the value and put it in the list.  If
    // not, recurse on any children
    if (node.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
      ASTNode colName = (ASTNode)node.getChildren().get(0);
      assert colName.getToken().getType() == HiveParser.Identifier :
          "Expected column name";
      setRCols.add(colName.getText());
    } else if (node.getChildren() != null) {
      for (Node n : node.getChildren()) {
        addSetRCols((ASTNode)n, setRCols);
      }
    }
  }
}
