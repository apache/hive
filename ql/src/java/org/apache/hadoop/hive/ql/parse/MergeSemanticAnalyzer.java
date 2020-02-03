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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;


/**
 * A subclass of the {@link org.apache.hadoop.hive.ql.parse.SemanticAnalyzer} that just handles
 * merge statements. It works by rewriting the updates and deletes into insert statements (since
 * they are actually inserts) and then doing some patch up to make them work as merges instead.
 */
public class MergeSemanticAnalyzer extends RewriteSemanticAnalyzer {
  MergeSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyze(ASTNode tree) throws SemanticException {
    if (tree.getToken().getType() != HiveParser.TOK_MERGE) {
      throw new RuntimeException("Asked to parse token " + tree.getName() + " in " +
          "MergeSemanticAnalyzer");
    }
    analyzeMerge(tree);
  }

  private static final String INDENT = "  ";

  private IdentifierQuoter quotedIdentifierHelper;

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
      if (trs == null) {
        throw new IllegalArgumentException("Must have a TokenRewriteStream");
      }
    }

    private void visit(ASTNode n) {
      if (n.getType() == HiveParser.Identifier) {
        if (visitedNodes.containsKey(n)) {
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
      if (n.getChildCount() <= 0) {
        return;
      }
      for (Node c : n.getChildren()) {
        visit((ASTNode)c);
      }
    }
  }

  /**
   * This allows us to take an arbitrary ASTNode and turn it back into SQL that produced it without
   * needing to understand what it is (except for QuotedIdentifiers).
   */
  private String getMatchedText(ASTNode n) {
    quotedIdentifierHelper.visit(n);
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
    quotedIdentifierHelper = new IdentifierQuoter(ctx.getTokenRewriteStream());
    /*
     * See org.apache.hadoop.hive.ql.parse.TestMergeStatement for some examples of the merge AST
      For example, given:
      MERGE INTO acidTbl USING nonAcidPart2 source ON acidTbl.a = source.a2
      WHEN MATCHED THEN UPDATE SET b = source.b2
      WHEN NOT MATCHED THEN INSERT VALUES (source.a2, source.b2)

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
        FROM acidTbl RIGHT OUTER JOIN nonAcidPart2 ON acidTbl.a = source.a2
        INSERT INTO TABLE acidTbl SELECT nonAcidPart2.a2, nonAcidPart2.b2 WHERE acidTbl.a IS null
        INSERT INTO TABLE acidTbl SELECT target.ROW__ID, nonAcidPart2.a2, nonAcidPart2.b2
        WHERE nonAcidPart2.a2=acidTbl.a SORT BY acidTbl.ROW__ID
    */
    /*todo: we need some sort of validation phase over original AST to make things user friendly; for example, if
     original command refers to a column that doesn't exist, this will be caught when processing the rewritten query but
     the errors will point at locations that the user can't map to anything
     - VALUES clause must have the same number of values as target table (including partition cols).  Part cols go last
     in Select clause of Insert as Select
     todo: do we care to preserve comments in original SQL?
     todo: check if identifiers are properly escaped/quoted in the generated SQL - it's currently inconsistent
      Look at UnparseTranslator.addIdentifierTranslation() - it does unescape + unparse...
     todo: consider "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET TargetTable.Col1 = SourceTable.Col1 "; what happens when
     source is empty?  This should be a runtime error - maybe not the outer side of ROJ is empty => the join produces 0
     rows. If supporting WHEN NOT MATCHED BY SOURCE, then this should be a runtime error
    */
    ASTNode target = (ASTNode)tree.getChild(0);
    ASTNode source = (ASTNode)tree.getChild(1);
    String targetName = getSimpleTableName(target);
    String sourceName = getSimpleTableName(source);
    ASTNode onClause = (ASTNode) tree.getChild(2);
    String onClauseAsText = getMatchedText(onClause);

    int whenClauseBegins = 3;
    boolean hasHint = false;
    // query hint
    ASTNode qHint = (ASTNode) tree.getChild(3);
    if (qHint.getType() == HiveParser.QUERY_HINT) {
      hasHint = true;
      whenClauseBegins++;
    }
    Table targetTable = getTargetTable(target);
    validateTargetTable(targetTable);
    List<ASTNode> whenClauses = findWhenClauses(tree, whenClauseBegins);

    StringBuilder rewrittenQueryStr = new StringBuilder("FROM\n");

    rewrittenQueryStr.append(INDENT).append(getFullTableNameForSQL(target));
    if (isAliased(target)) {
      rewrittenQueryStr.append(" ").append(targetName);
    }
    rewrittenQueryStr.append('\n');
    rewrittenQueryStr.append(INDENT).append(chooseJoinType(whenClauses)).append("\n");
    if (source.getType() == HiveParser.TOK_SUBQUERY) {
      //this includes the mandatory alias
      rewrittenQueryStr.append(INDENT).append(getMatchedText(source));
    } else {
      rewrittenQueryStr.append(INDENT).append(getFullTableNameForSQL(source));
      if (isAliased(source)) {
        rewrittenQueryStr.append(" ").append(sourceName);
      }
    }
    rewrittenQueryStr.append('\n');
    rewrittenQueryStr.append(INDENT).append("ON ").append(onClauseAsText).append('\n');

    // Add the hint if any
    String hintStr = null;
    if (hasHint) {
      hintStr = " /*+ " + qHint.getText() + " */ ";
    }
    final boolean splitUpdateEarly =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.MERGE_SPLIT_UPDATE);
    /**
     * We allow at most 2 WHEN MATCHED clause, in which case 1 must be Update the other Delete
     * If we have both update and delete, the 1st one (in SQL code) must have "AND <extra predicate>"
     * so that the 2nd can ensure not to process the same rows.
     * Update and Delete may be in any order.  (Insert is always last)
     */
    String extraPredicate = null;
    int numWhenMatchedUpdateClauses = 0, numWhenMatchedDeleteClauses = 0;
    int numInsertClauses = 0;
    boolean hintProcessed = false;
    for (ASTNode whenClause : whenClauses) {
      switch (getWhenClauseOperation(whenClause).getType()) {
      case HiveParser.TOK_INSERT:
        numInsertClauses++;
        handleInsert(whenClause, rewrittenQueryStr, target, onClause,
            targetTable, targetName, onClauseAsText, hintProcessed ? null : hintStr);
        hintProcessed = true;
        break;
      case HiveParser.TOK_UPDATE:
        numWhenMatchedUpdateClauses++;
        String s = handleUpdate(whenClause, rewrittenQueryStr, target,
            onClauseAsText, targetTable, extraPredicate, hintProcessed ? null : hintStr,
            splitUpdateEarly);
        hintProcessed = true;
        if (numWhenMatchedUpdateClauses + numWhenMatchedDeleteClauses == 1) {
          extraPredicate = s; //i.e. it's the 1st WHEN MATCHED
        }
        break;
      case HiveParser.TOK_DELETE:
        numWhenMatchedDeleteClauses++;
        String s1 = handleDelete(whenClause, rewrittenQueryStr, target,
            onClauseAsText, targetTable, extraPredicate, hintProcessed ? null : hintStr, false);
        hintProcessed = true;
        if (numWhenMatchedUpdateClauses + numWhenMatchedDeleteClauses == 1) {
          extraPredicate = s1; //i.e. it's the 1st WHEN MATCHED
        }
        break;
      default:
        throw new IllegalStateException("Unexpected WHEN clause type: " + whenClause.getType() +
            addParseInfo(whenClause));
      }
      if (numWhenMatchedDeleteClauses > 1) {
        throw new SemanticException(ErrorMsg.MERGE_TOO_MANY_DELETE, ctx.getCmd());
      }
      if (numWhenMatchedUpdateClauses > 1) {
        throw new SemanticException(ErrorMsg.MERGE_TOO_MANY_UPDATE, ctx.getCmd());
      }
      assert numInsertClauses < 2: "too many Insert clauses";
    }
    if (numWhenMatchedDeleteClauses + numWhenMatchedUpdateClauses == 2 && extraPredicate == null) {
      throw new SemanticException(ErrorMsg.MERGE_PREDIACTE_REQUIRED, ctx.getCmd());
    }

    boolean validating = handleCardinalityViolation(rewrittenQueryStr, target, onClauseAsText, targetTable,
        numWhenMatchedDeleteClauses == 0 && numWhenMatchedUpdateClauses == 0);
    ReparseResult rr = parseRewrittenQuery(rewrittenQueryStr, ctx.getCmd());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;
    rewrittenCtx.setOperation(Context.Operation.MERGE);

    //set dest name mapping on new context; 1st child is TOK_FROM
    for (int insClauseIdx = 1, whenClauseIdx = 0;
        insClauseIdx < rewrittenTree.getChildCount() - (validating ? 1 : 0/*skip cardinality violation clause*/);
        insClauseIdx++, whenClauseIdx++) {
      //we've added Insert clauses in order or WHEN items in whenClauses
      switch (getWhenClauseOperation(whenClauses.get(whenClauseIdx)).getType()) {
      case HiveParser.TOK_INSERT:
        rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.INSERT);
        break;
      case HiveParser.TOK_UPDATE:
        if(!splitUpdateEarly) {
          rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.UPDATE);
        } else {
        /* With 2 branches for the update, the 1st branch is the INSERT part
          and the next one is the DELETE.  WriteSet tracking treats 2 concurrent DELETES
          as in conflict so Lost Update is still prevented since the delete event lands in the
          partition/bucket where the original version of the row was so any concurrent update/delete
          of the same row will land in the same partition/bucket.

          If the insert part lands in a different partition, it should not conflict with another
          Update of that partition since that update by definition cannot be of the same row.
          If we ever enforce unique constraints we may have to have I+I be in conflict*/
          rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.INSERT);
          rewrittenCtx.addDestNamePrefix(++insClauseIdx, Context.DestClausePrefix.DELETE);
        }
        break;
      case HiveParser.TOK_DELETE:
        rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.DELETE);
        break;
      default:
        assert false;
      }
    }
    if (validating) {
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
   * If there is no WHEN NOT MATCHED THEN INSERT, we don't outer join.
   */
  private String chooseJoinType(List<ASTNode> whenClauses) {
    for (ASTNode whenClause : whenClauses) {
      if (getWhenClauseOperation(whenClause).getType() == HiveParser.TOK_INSERT) {
        return "RIGHT OUTER JOIN";
      }
    }
    return "INNER JOIN";
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
      String onClauseAsString, Table targetTable, boolean onlyHaveWhenNotMatchedClause)
              throws SemanticException {
    if (!conf.getBoolVar(HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK)) {
      LOG.info("Merge statement cardinality violation check is disabled: " +
          HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK.varname);
      return false;
    }
    if (onlyHaveWhenNotMatchedClause) {
      //if no update or delete in Merge, there is no need to to do cardinality check
      return false;
    }
    //this is a tmp table and thus Session scoped and acid requires SQL statement to be serial in a
    // given session, i.e. the name can be fixed across all invocations
    String tableName = "merge_tmp_table";
    rewrittenQueryStr.append("INSERT INTO ").append(tableName)
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
    } catch(HiveException|MetaException e) {
      throw new SemanticException(e.getMessage(), e);
    }
    return true;
  }

  /**
   * @param onClauseAsString - because there is no clone() and we need to use in multiple places
   * @param deleteExtraPredicate - see notes at caller
   */
  private String handleUpdate(ASTNode whenMatchedUpdateClause, StringBuilder rewrittenQueryStr, ASTNode target,
      String onClauseAsString, Table targetTable, String deleteExtraPredicate, String hintStr, boolean splitUpdateEarly)
      throws SemanticException {
    assert whenMatchedUpdateClause.getType() == HiveParser.TOK_MATCHED;
    assert getWhenClauseOperation(whenMatchedUpdateClause).getType() == HiveParser.TOK_UPDATE;
    String targetName = getSimpleTableName(target);
    rewrittenQueryStr.append("INSERT INTO ").append(getFullTableNameForSQL(target));
    addPartitionColsToInsert(targetTable.getPartCols(), rewrittenQueryStr);
    rewrittenQueryStr.append("    -- update clause").append(splitUpdateEarly ? "(insert part)": "")
        .append("\n SELECT ");
    if (hintStr != null) {
      rewrittenQueryStr.append(hintStr);
    }
    if(!splitUpdateEarly) {
      rewrittenQueryStr.append(targetName).append(".ROW__ID, ");
    }

    ASTNode setClause = (ASTNode)getWhenClauseOperation(whenMatchedUpdateClause).getChild(0);
    //columns being updated -> update expressions; "setRCols" (last param) is null because we use actual expressions
    //before re-parsing, i.e. they are known to SemanticAnalyzer logic
    Map<String, ASTNode> setColsExprs = collectSetColumnsAndExpressions(setClause, null, targetTable);
    //if target table has cols c1,c2,c3 and p1 partition col and we had "SET c2 = 5, c1 = current_date()" we want to end
    //up with
    //insert into target (p1) select current_date(), 5, c3, p1 where ....
    //since we take the RHS of set exactly as it was in Input, we don't need to deal with quoting/escaping column/table
    //names
    List<FieldSchema> nonPartCols = targetTable.getCols();
    for(int i = 0; i < nonPartCols.size(); i++) {
      FieldSchema fs = nonPartCols.get(i);
      if(i > 0) {
        rewrittenQueryStr.append(", ");
      }
      String name = fs.getName();
      if (setColsExprs.containsKey(name)) {
        String rhsExp = getMatchedText(setColsExprs.get(name));
        //"set a=5, b=8" - rhsExp picks up the next char (e.g. ',') from the token stream
        switch (rhsExp.charAt(rhsExp.length() - 1)) {
        case ',':
        case '\n':
          rhsExp = rhsExp.substring(0, rhsExp.length() - 1);
          break;
        default:
          //do nothing
        }
        rewrittenQueryStr.append(rhsExp);
      } else {
        rewrittenQueryStr.append(getSimpleTableName(target))
          .append(".")
          .append(HiveUtils.unparseIdentifier(name, this.conf));
      }
    }
    addPartitionColsToSelect(targetTable.getPartCols(), rewrittenQueryStr, target);
    rewrittenQueryStr.append("\n   WHERE ").append(onClauseAsString);
    String extraPredicate = getWhenClausePredicate(whenMatchedUpdateClause);
    if (extraPredicate != null) {
      //we have WHEN MATCHED AND <boolean expr> THEN DELETE
      rewrittenQueryStr.append(" AND ").append(extraPredicate);
    }
    if (deleteExtraPredicate != null) {
      rewrittenQueryStr.append(" AND NOT(").append(deleteExtraPredicate).append(")");
    }
    if(!splitUpdateEarly) {
      rewrittenQueryStr.append("\n SORT BY ");
      rewrittenQueryStr.append(targetName).append(".ROW__ID ");
    }
    rewrittenQueryStr.append("\n");

    setUpAccessControlInfoForUpdate(targetTable, setColsExprs);
    //we don't deal with columns on RHS of SET expression since the whole expr is part of the
    //rewritten SQL statement and is thus handled by SemanticAnalyzer.  Nor do we have to
    //figure which cols on RHS are from source and which from target

    if(splitUpdateEarly) {
      /**
       * this is part of the WHEN MATCHED UPDATE, so we ignore any 'extra predicate' generated
       * by this call to handleDelete()
       */
      handleDelete(whenMatchedUpdateClause, rewrittenQueryStr, target, onClauseAsString,
          targetTable, deleteExtraPredicate, hintStr, true);
    }

    return extraPredicate;
  }

  /**
   * @param onClauseAsString - because there is no clone() and we need to use in multiple places
   * @param updateExtraPredicate - see notes at caller
   */
  private String handleDelete(ASTNode whenMatchedDeleteClause, StringBuilder rewrittenQueryStr,
      ASTNode target, String onClauseAsString, Table targetTable, String updateExtraPredicate,
      String hintStr, boolean splitUpdateEarly) throws SemanticException {
    assert whenMatchedDeleteClause.getType() == HiveParser.TOK_MATCHED;
    assert (splitUpdateEarly &&
        getWhenClauseOperation(whenMatchedDeleteClause).getType() == HiveParser.TOK_UPDATE) ||
        getWhenClauseOperation(whenMatchedDeleteClause).getType() == HiveParser.TOK_DELETE;
    List<FieldSchema> partCols = targetTable.getPartCols();
    String targetName = getSimpleTableName(target);
    rewrittenQueryStr.append("INSERT INTO ").append(getFullTableNameForSQL(target));
    addPartitionColsToInsert(partCols, rewrittenQueryStr);

    if(splitUpdateEarly) {
      rewrittenQueryStr.append("    -- update clause (delete part)\n SELECT ");
    } else {
      rewrittenQueryStr.append("    -- delete clause\n SELECT ");
    }
    if (hintStr != null) {
      rewrittenQueryStr.append(hintStr);
    }
    rewrittenQueryStr.append(targetName).append(".ROW__ID ");
    addPartitionColsToSelect(partCols, rewrittenQueryStr, target);
    rewrittenQueryStr.append("\n   WHERE ").append(onClauseAsString);
    String extraPredicate = getWhenClausePredicate(whenMatchedDeleteClause);
    if (extraPredicate != null) {
      //we have WHEN MATCHED AND <boolean expr> THEN DELETE
      rewrittenQueryStr.append(" AND ").append(extraPredicate);
    }
    if (updateExtraPredicate != null) {
      rewrittenQueryStr.append(" AND NOT(").append(updateExtraPredicate).append(")");
    }
    rewrittenQueryStr.append("\n SORT BY ");
    rewrittenQueryStr.append(targetName).append(".ROW__ID \n");
    return extraPredicate;
  }

  private static String addParseInfo(ASTNode n) {
    return " at " + ASTErrorUtils.renderPosition(n);
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
   * Collect WHEN clauses from Merge statement AST.
   */
  private List<ASTNode> findWhenClauses(ASTNode tree, int start) throws SemanticException {
    assert tree.getType() == HiveParser.TOK_MERGE;
    List<ASTNode> whenClauses = new ArrayList<>();
    for (int idx = start; idx < tree.getChildCount(); idx++) {
      ASTNode whenClause = (ASTNode)tree.getChild(idx);
      assert whenClause.getType() == HiveParser.TOK_MATCHED ||
        whenClause.getType() == HiveParser.TOK_NOT_MATCHED :
        "Unexpected node type found: " + whenClause.getType() + addParseInfo(whenClause);
      whenClauses.add(whenClause);
    }
    if (whenClauses.size() <= 0) {
      //Futureproofing: the parser will actually not allow this
      throw new SemanticException("Must have at least 1 WHEN clause in MERGE statement");
    }
    return whenClauses;
  }

  private ASTNode getWhenClauseOperation(ASTNode whenClause) {
    if (!(whenClause.getType() == HiveParser.TOK_MATCHED || whenClause.getType() == HiveParser.TOK_NOT_MATCHED)) {
      throw  raiseWrongType("Expected TOK_MATCHED|TOK_NOT_MATCHED", whenClause);
    }
    return (ASTNode) whenClause.getChild(0);
  }

  /**
   * Returns the <boolean predicate> as in WHEN MATCHED AND <boolean predicate> THEN...
   * @return may be null
   */
  private String getWhenClausePredicate(ASTNode whenClause) {
    if (!(whenClause.getType() == HiveParser.TOK_MATCHED || whenClause.getType() == HiveParser.TOK_NOT_MATCHED)) {
      throw raiseWrongType("Expected TOK_MATCHED|TOK_NOT_MATCHED", whenClause);
    }
    if (whenClause.getChildCount() == 2) {
      return getMatchedText((ASTNode)whenClause.getChild(1));
    }
    return null;
  }

  /**
   * Generates the Insert leg of the multi-insert SQL to represent WHEN NOT MATCHED THEN INSERT clause.
   * @param targetTableNameInSourceQuery - simple name/alias
   * @throws SemanticException
   */
  private void handleInsert(ASTNode whenNotMatchedClause, StringBuilder rewrittenQueryStr, ASTNode target,
      ASTNode onClause, Table targetTable, String targetTableNameInSourceQuery, String onClauseAsString,
      String hintStr) throws SemanticException {
    ASTNode whenClauseOperation = getWhenClauseOperation(whenNotMatchedClause);
    assert whenNotMatchedClause.getType() == HiveParser.TOK_NOT_MATCHED;
    assert whenClauseOperation.getType() == HiveParser.TOK_INSERT;

    // identify the node that contains the values to insert and the optional column list node
    ArrayList<Node> children = whenClauseOperation.getChildren();
    ASTNode valuesNode =
        (ASTNode)children.stream().filter(n -> ((ASTNode)n).getType() == HiveParser.TOK_FUNCTION).findFirst().get();
    ASTNode columnListNode =
        (ASTNode)children.stream().filter(n -> ((ASTNode)n).getType() == HiveParser.TOK_TABCOLNAME).findFirst()
        .orElse(null);

    // if column list is specified, then it has to have the same number of elements as the values
    // valuesNode has a child for struct, the rest are the columns
    if (columnListNode != null && columnListNode.getChildCount() != (valuesNode.getChildCount() - 1)) {
      throw new SemanticException(String.format("Column schema must have the same length as values (%d vs %d)",
          columnListNode.getChildCount(), valuesNode.getChildCount() - 1));
    }

    rewrittenQueryStr.append("INSERT INTO ").append(getFullTableNameForSQL(target));
    if (columnListNode != null) {
      rewrittenQueryStr.append(' ').append(getMatchedText(columnListNode));
    }
    addPartitionColsToInsert(targetTable.getPartCols(), rewrittenQueryStr);

    rewrittenQueryStr.append("    -- insert clause\n  SELECT ");
    if (hintStr != null) {
      rewrittenQueryStr.append(hintStr);
    }

    OnClauseAnalyzer oca = new OnClauseAnalyzer(onClause, targetTable, targetTableNameInSourceQuery,
        conf, onClauseAsString);
    oca.analyze();

    String valuesClause = getMatchedText(valuesNode);
    valuesClause = valuesClause.substring(1, valuesClause.length() - 1); //strip '(' and ')'
    valuesClause = replaceDefaultKeywordForMerge(valuesClause, targetTable, columnListNode);
    rewrittenQueryStr.append(valuesClause).append("\n   WHERE ").append(oca.getPredicate());

    String extraPredicate = getWhenClausePredicate(whenNotMatchedClause);
    if (extraPredicate != null) {
      //we have WHEN NOT MATCHED AND <boolean expr> THEN INSERT
      rewrittenQueryStr.append(" AND ")
        .append(getMatchedText(((ASTNode)whenNotMatchedClause.getChild(1))));
    }
    rewrittenQueryStr.append('\n');
  }

  private String replaceDefaultKeywordForMerge(String valueClause, Table table, ASTNode columnListNode)
      throws SemanticException {
    if (!valueClause.toLowerCase().contains("`default`")) {
      return valueClause;
    }

    Map<String, String> colNameToDefaultConstraint = getColNameToDefaultValueMap(table);
    String[] values = valueClause.trim().split(",");
    String[] replacedValues = new String[values.length];

    // the list of the column names may be set in the query
    String[] columnNames = columnListNode == null ?
      table.getAllCols().stream().map(f -> f.getName()).toArray(size -> new String[size]) :
      columnListNode.getChildren().stream().map(n -> ((ASTNode)n).toString()).toArray(size -> new String[size]);

    for (int i = 0; i < values.length; i++) {
      if (values[i].trim().toLowerCase().equals("`default`")) {
        replacedValues[i] = MapUtils.getString(colNameToDefaultConstraint, columnNames[i], "null");
      } else {
        replacedValues[i] = values[i];
      }
    }
    return StringUtils.join(replacedValues, ',');
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
     * Finds all columns and groups by table ref (if there is one).
     */
    private void visit(ASTNode n) {
      if (n.getType() == HiveParser.TOK_TABLE_OR_COL) {
        ASTNode parent = (ASTNode) n.getParent();
        if (parent != null && parent.getType() == HiveParser.DOT) {
          //the ref must be a table, so look for column name as right child of DOT
          if (parent.getParent() != null && parent.getParent().getType() == HiveParser.DOT) {
            //I don't think this can happen... but just in case
            throw new IllegalArgumentException("Found unexpected db.table.col reference in " + onClauseAsString);
          }
          addColumn2Table(n.getChild(0).getText(), parent.getChild(1).getText());
        } else {
          //must be just a column name
          unresolvedColumns.add(n.getChild(0).getText());
        }
      }
      if (n.getChildCount() == 0) {
        return;
      }
      for (Node child : n.getChildren()) {
        visit((ASTNode)child);
      }
    }

    private void analyze() {
      visit(onClause);
      if (tableNamesFound.size() > 2) {
        throw new IllegalArgumentException("Found > 2 table refs in ON clause.  Found " +
          tableNamesFound + " in " + onClauseAsString);
      }
      handleUnresolvedColumns();
      if (tableNamesFound.size() > 2) {
        throw new IllegalArgumentException("Found > 2 table refs in ON clause (incl unresolved).  " +
          "Found " + tableNamesFound + " in " + onClauseAsString);
      }
    }

    /**
     * Find those that belong to target table.
     */
    private void handleUnresolvedColumns() {
      if (unresolvedColumns.isEmpty()) {
        return;
      }
      for (String c : unresolvedColumns) {
        for (FieldSchema fs : allTargetTableColumns) {
          if (c.equalsIgnoreCase(fs.getName())) {
            //c belongs to target table; strictly speaking there maybe an ambiguous ref but
            //this will be caught later when multi-insert is parsed
            addColumn2Table(targetTableNameInSourceQuery.toLowerCase(), c);
            break;
          }
        }
      }
    }

    private void addColumn2Table(String tableName, String columnName) {
      tableName = tableName.toLowerCase(); //normalize name for mapping
      tableNamesFound.add(tableName);
      List<String> cols = table2column.get(tableName);
      if (cols == null) {
        cols = new ArrayList<>();
        table2column.put(tableName, cols);
      }
      //we want to preserve 'columnName' as it was in original input query so that rewrite
      //looks as much as possible like original query
      cols.add(columnName);
    }

    /**
     * Now generate the predicate for Where clause.
     */
    private String getPredicate() {
      //normilize table name for mapping
      List<String> targetCols = table2column.get(targetTableNameInSourceQuery.toLowerCase());
      if (targetCols == null) {
        /*e.g. ON source.t=1
        * this is not strictly speaking invalid but it does ensure that all columns from target
        * table are all NULL for every row.  This would make any WHEN MATCHED clause invalid since
        * we don't have a ROW__ID.  The WHEN NOT MATCHED could be meaningful but it's just data from
        * source satisfying source.t=1...  not worth the effort to support this*/
        throw new IllegalArgumentException(ErrorMsg.INVALID_TABLE_IN_ON_CLAUSE_OF_MERGE
          .format(targetTableNameInSourceQuery, onClauseAsString));
      }
      StringBuilder sb = new StringBuilder();
      for (String col : targetCols) {
        if (sb.length() > 0) {
          sb.append(" AND ");
        }
        //but preserve table name in SQL
        sb.append(HiveUtils.unparseIdentifier(targetTableNameInSourceQuery, conf))
          .append(".")
          .append(HiveUtils.unparseIdentifier(col, conf))
          .append(" IS NULL");
      }
      return sb.toString();
    }
  }
}
