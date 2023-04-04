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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.io.AcidUtils;
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
  private int numWhenMatchedUpdateClauses;
  private int numWhenMatchedDeleteClauses;

  MergeSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected ASTNode getTargetTableNode(ASTNode tree) {
    return (ASTNode)tree.getChild(0);
  }

  @Override
  public void analyze(ASTNode tree, Table targetTable, ASTNode tableNameNode) throws SemanticException {
    boolean nonNativeAcid = AcidUtils.isNonNativeAcidTable(targetTable, true);
    if (nonNativeAcid) {
      throw new SemanticException(ErrorMsg.NON_NATIVE_ACID_UPDATE.getErrorCodedMsg());
    }
    analyzeMerge(tree, targetTable, tableNameNode);
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
  protected void analyzeMerge(ASTNode tree, Table targetTable, ASTNode targetNameNode)
          throws SemanticException {
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
    if (tree.getToken().getType() != HiveParser.TOK_MERGE) {
      throw new RuntimeException("Asked to parse token " + tree.getName() + " in " +
              "MergeSemanticAnalyzer");
    }

    ctx.setOperation(Context.Operation.MERGE);
    ASTNode source = (ASTNode)tree.getChild(1);
    String targetName = getSimpleTableName(targetNameNode);
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
    List<ASTNode> whenClauses = findWhenClauses(tree, whenClauseBegins);

    StringBuilder rewrittenQueryStr = createRewrittenQueryStrBuilder();
    rewrittenQueryStr.append("(SELECT ");
    String subQueryAlias = isAliased(targetNameNode) ? targetName : targetTable.getTTable().getTableName();
    ColumnAppender columnAppender = getColumnAppender(subQueryAlias, StringUtils.EMPTY);
    columnAppender.appendAcidSelectColumns(rewrittenQueryStr, Context.Operation.MERGE);

    rewrittenQueryStr.deleteCharAt(rewrittenQueryStr.length() - 1); // remove last ','
    addColsToSelect(targetTable.getCols(), rewrittenQueryStr);
    addColsToSelect(targetTable.getPartCols(), rewrittenQueryStr);
    rewrittenQueryStr.append(" FROM ").append(getFullTableNameForSQL(targetNameNode)).append(") ");
    rewrittenQueryStr.append(subQueryAlias);
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
    /**
     * We allow at most 2 WHEN MATCHED clause, in which case 1 must be Update the other Delete
     * If we have both update and delete, the 1st one (in SQL code) must have "AND <extra predicate>"
     * so that the 2nd can ensure not to process the same rows.
     * Update and Delete may be in any order.  (Insert is always last)
     */
    String extraPredicate = null;
    int numInsertClauses = 0;
    numWhenMatchedUpdateClauses = 0;
    numWhenMatchedDeleteClauses = 0;
    boolean hintProcessed = false;
    for (ASTNode whenClause : whenClauses) {
      switch (getWhenClauseOperation(whenClause).getType()) {
      case HiveParser.TOK_INSERT:
        numInsertClauses++;
        handleInsert(whenClause, rewrittenQueryStr, targetNameNode, onClause,
            targetTable, targetName, onClauseAsText, hintProcessed ? null : hintStr);
        hintProcessed = true;
        break;
      case HiveParser.TOK_UPDATE:
        numWhenMatchedUpdateClauses++;
        String s = handleUpdate(whenClause, rewrittenQueryStr, targetNameNode,
            onClauseAsText, targetTable, extraPredicate, hintProcessed ? null : hintStr, columnAppender);
        hintProcessed = true;
        if (numWhenMatchedUpdateClauses + numWhenMatchedDeleteClauses == 1) {
          extraPredicate = s; //i.e. it's the 1st WHEN MATCHED
        }
        break;
      case HiveParser.TOK_DELETE:
        numWhenMatchedDeleteClauses++;
        String s1 = handleDelete(whenClause, rewrittenQueryStr,
            onClauseAsText, extraPredicate, hintProcessed ? null : hintStr, columnAppender);
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

    boolean validating = handleCardinalityViolation(rewrittenQueryStr, targetNameNode, onClauseAsText, targetTable,
        numWhenMatchedDeleteClauses == 0 && numWhenMatchedUpdateClauses == 0, columnAppender);
    ReparseResult rr = parseRewrittenQuery(rewrittenQueryStr, ctx.getCmd());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;
    rewrittenCtx.setOperation(Context.Operation.MERGE);

    //set dest name mapping on new context; 1st child is TOK_FROM
    int insClauseIdx = 1;
    for (int whenClauseIdx = 0;
        insClauseIdx < rewrittenTree.getChildCount() - (validating ? 1 : 0/*skip cardinality violation clause*/);
        whenClauseIdx++) {
      //we've added Insert clauses in order or WHEN items in whenClauses
      switch (getWhenClauseOperation(whenClauses.get(whenClauseIdx)).getType()) {
      case HiveParser.TOK_INSERT:
        rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.INSERT);
        ++insClauseIdx;
        break;
      case HiveParser.TOK_UPDATE:
        insClauseIdx += addDestNamePrefixOfUpdate(insClauseIdx, rewrittenCtx);
        break;
      case HiveParser.TOK_DELETE:
        rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.DELETE);
        ++insClauseIdx;
        break;
      default:
        assert false;
      }
    }
    if (validating) {
      //here means the last branch of the multi-insert is Cardinality Validation
      rewrittenCtx.addDestNamePrefix(rewrittenTree.getChildCount() - 1, Context.DestClausePrefix.INSERT);
    }

    analyzeRewrittenTree(rewrittenTree, rewrittenCtx);
    updateOutputs(targetTable);
  }

  /**
   * This sets the destination name prefix for update clause.
   * @param insClauseIdx index of insert clause in the rewritten multi-insert represents the merge update clause.
   * @param rewrittenCtx the {@link Context} stores the prefixes
   * @return the number of prefixes set.
   */
  protected int addDestNamePrefixOfUpdate(int insClauseIdx, Context rewrittenCtx) {
    rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.UPDATE);
    return 1;
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
      String onClauseAsString, Table targetTable, boolean onlyHaveWhenNotMatchedClause, ColumnAppender columnAppender)
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
    List<String> sortKeys = columnAppender.getSortKeys();
    rewrittenQueryStr.append("INSERT INTO ").append(tableName)
      .append("\n  SELECT cardinality_violation(")
      .append(StringUtils.join(sortKeys, ","));
    addColsToSelect(targetTable.getPartCols(), rewrittenQueryStr, target);

    rewrittenQueryStr.append(")\n WHERE ").append(onClauseAsString)
      .append(" GROUP BY ").append(StringUtils.join(sortKeys, ","));

    addColsToSelect(targetTable.getPartCols(), rewrittenQueryStr, target);

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
                              String onClauseAsString, Table targetTable, String deleteExtraPredicate, String hintStr,
                              ColumnAppender columnAppender)
      throws SemanticException {
    assert whenMatchedUpdateClause.getType() == HiveParser.TOK_MATCHED;
    assert getWhenClauseOperation(whenMatchedUpdateClause).getType() == HiveParser.TOK_UPDATE;
    String targetName = getSimpleTableName(target);
    List<String> values = new ArrayList<>(targetTable.getCols().size());

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
    Map<String, String> colNameToDefaultConstraint = getColNameToDefaultValueMap(targetTable);
    for (FieldSchema fs : nonPartCols) {
      String name = fs.getName();
      if (setColsExprs.containsKey(name)) {
        ASTNode setColExpr = setColsExprs.get(name);
        if (setColExpr.getType() == HiveParser.TOK_TABLE_OR_COL &&
                setColExpr.getChildCount() == 1 && setColExpr.getChild(0).getType() == HiveParser.TOK_DEFAULT_VALUE) {
          UnparseTranslator defaultValueTranslator = new UnparseTranslator(conf);
          defaultValueTranslator.enable();
          defaultValueTranslator.addDefaultValueTranslation(
                  setColsExprs.get(name), colNameToDefaultConstraint.get(name));
          defaultValueTranslator.applyTranslations(ctx.getTokenRewriteStream());
        }

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

        values.add(rhsExp);
      } else {
        values.add(targetName + "." + HiveUtils.unparseIdentifier(name, this.conf));
      }
    }
    addPartitionColsAsValues(targetTable.getPartCols(), targetName, values);

    String extraPredicate = handleUpdate(whenMatchedUpdateClause, rewrittenQueryStr, onClauseAsString,
            deleteExtraPredicate, hintStr, columnAppender, targetName, values);

    setUpAccessControlInfoForUpdate(targetTable, setColsExprs);
    return extraPredicate;
  }

  protected String handleUpdate(ASTNode whenMatchedUpdateClause, StringBuilder rewrittenQueryStr,
                                String onClauseAsString, String deleteExtraPredicate, String hintStr,
                                ColumnAppender columnAppender, String targetName, List<String> values) {
    values.add(0, targetName + ".ROW__ID");

    rewrittenQueryStr.append("    -- update clause").append("\n");
    appendInsertBranch(rewrittenQueryStr, hintStr, values);

    String extraPredicate = addWhereClauseOfUpdate(
            rewrittenQueryStr, onClauseAsString, whenMatchedUpdateClause, deleteExtraPredicate);

    appendSortBy(rewrittenQueryStr, Collections.singletonList(targetName + ".ROW__ID "));
    rewrittenQueryStr.append("\n");

    return extraPredicate;
  }

  protected String addWhereClauseOfUpdate(StringBuilder rewrittenQueryStr, String onClauseAsString,
                                          ASTNode whenMatchedUpdateClause, String deleteExtraPredicate) {
    rewrittenQueryStr.append(INDENT).append("WHERE ").append(onClauseAsString);
    String extraPredicate = getWhenClausePredicate(whenMatchedUpdateClause);
    if (extraPredicate != null) {
      //we have WHEN MATCHED AND <boolean expr> THEN DELETE
      rewrittenQueryStr.append(" AND ").append(extraPredicate);
    }
    if (deleteExtraPredicate != null) {
      rewrittenQueryStr.append(" AND NOT(").append(deleteExtraPredicate).append(")");
    }

    return extraPredicate;
  }

  /**
   * @param onClauseAsString - because there is no clone() and we need to use in multiple places
   * @param updateExtraPredicate - see notes at caller
   */
  protected String handleDelete(ASTNode whenMatchedDeleteClause, StringBuilder rewrittenQueryStr,
      String onClauseAsString, String updateExtraPredicate,
      String hintStr, ColumnAppender columnAppender) {
    assert whenMatchedDeleteClause.getType() == HiveParser.TOK_MATCHED;

    List<String> deleteValues = columnAppender.getDeleteValues(Context.Operation.DELETE);
    appendInsertBranch(rewrittenQueryStr, hintStr, deleteValues);

    rewrittenQueryStr.append(INDENT).append("WHERE ").append(onClauseAsString);
    String extraPredicate = getWhenClausePredicate(whenMatchedDeleteClause);
    if (extraPredicate != null) {
      //we have WHEN MATCHED AND <boolean expr> THEN DELETE
      rewrittenQueryStr.append(" AND ").append(extraPredicate);
    }
    if (updateExtraPredicate != null) {
      rewrittenQueryStr.append(" AND NOT(").append(updateExtraPredicate).append(")");
    }
    List<String> sortKeys = columnAppender.getSortKeys();
    rewrittenQueryStr.append("\n").append(INDENT);
    appendSortBy(rewrittenQueryStr, sortKeys);
    return extraPredicate;
  }

  private static String addParseInfo(ASTNode n) {
    return " at " + ASTErrorUtils.renderPosition(n);
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

  protected ASTNode getWhenClauseOperation(ASTNode whenClause) {
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
    List<Node> children = whenClauseOperation.getChildren();
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

    rewrittenQueryStr.append("    -- insert clause\n  SELECT ");
    if (hintStr != null) {
      rewrittenQueryStr.append(hintStr);
    }

    OnClauseAnalyzer oca = new OnClauseAnalyzer(onClause, targetTable, targetTableNameInSourceQuery,
        conf, onClauseAsString);
    oca.analyze();

    UnparseTranslator defaultValuesTranslator = new UnparseTranslator(conf);
    defaultValuesTranslator.enable();
    List<String> targetSchema = processTableColumnNames(columnListNode, targetTable.getFullyQualifiedName());
    collectDefaultValues(valuesNode, targetTable, targetSchema, defaultValuesTranslator);
    defaultValuesTranslator.applyTranslations(ctx.getTokenRewriteStream());
    String valuesClause = getMatchedText(valuesNode);
    valuesClause = valuesClause.substring(1, valuesClause.length() - 1); //strip '(' and ')'
    rewrittenQueryStr.append(valuesClause).append("\n   WHERE ").append(oca.getPredicate());

    String extraPredicate = getWhenClausePredicate(whenNotMatchedClause);
    if (extraPredicate != null) {
      //we have WHEN NOT MATCHED AND <boolean expr> THEN INSERT
      rewrittenQueryStr.append(" AND ")
        .append(getMatchedText(((ASTNode)whenNotMatchedClause.getChild(1))));
    }
    rewrittenQueryStr.append('\n');
  }

  private void collectDefaultValues(
          ASTNode valueClause, Table targetTable, List<String> targetSchema, UnparseTranslator unparseTranslator)
          throws SemanticException {
    List<String> defaultConstraints = getDefaultConstraints(targetTable, targetSchema);
    for (int j = 0; j < defaultConstraints.size(); j++) {
      unparseTranslator.addDefaultValueTranslation((ASTNode) valueClause.getChild(j + 1), defaultConstraints.get(j));
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

  @Override
  protected boolean allowOutputMultipleTimes() {
    return conf.getBoolVar(HiveConf.ConfVars.SPLIT_UPDATE) || conf.getBoolVar(HiveConf.ConfVars.MERGE_SPLIT_UPDATE);
  }

  @Override
  protected boolean enableColumnStatsCollecting() {
    return numWhenMatchedUpdateClauses == 0 && numWhenMatchedDeleteClauses == 0;
  }
}
