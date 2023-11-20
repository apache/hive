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

package org.apache.hadoop.hive.ql.parse.rewrite;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.COWWithClauseBuilder;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlGenerator;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory.TARGET_PREFIX;

public class CopyOnWriteMergeRewriter extends MergeRewriter {

  public CopyOnWriteMergeRewriter(Hive db, HiveConf conf, SqlGeneratorFactory sqlGeneratorFactory) {
    super(db, conf, sqlGeneratorFactory);
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context ctx, MergeStatement mergeStatement) throws SemanticException {

    setOperation(ctx);
    MultiInsertSqlGenerator sqlGenerator = sqlGeneratorFactory.createSqlGenerator();
    handleSource(mergeStatement, sqlGenerator);

    sqlGenerator.append('\n');
    sqlGenerator.append("INSERT INTO ").appendTargetTableName();
    sqlGenerator.append('\n');
    
    List<MergeStatement.WhenClause> whenClauses = Lists.newArrayList(mergeStatement.getWhenClauses());
    
    Optional<String> extraPredicate = whenClauses.stream()
      .filter(whenClause -> !(whenClause instanceof MergeStatement.InsertClause))
      .map(MergeStatement.WhenClause::getExtraPredicate)
      .map(Strings::nullToEmpty)
      .reduce((p1, p2) -> isNotBlank(p2) ? p1 + " OR " + p2 : p2);

    whenClauses.removeIf(whenClause -> whenClause instanceof MergeStatement.DeleteClause);
    extraPredicate.ifPresent(p -> whenClauses.add(new MergeStatement.DeleteClause(p, null)));

    MergeStatement.MergeSqlGenerator mergeSqlGenerator = createMergeSqlGenerator(mergeStatement, sqlGenerator);

    for (MergeStatement.WhenClause whenClause : whenClauses) {
      whenClause.toSql(mergeSqlGenerator);
    }
    
    // TODO: handleCardinalityViolation;
    
    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(ctx, sqlGenerator.toString());
    Context rewrittenCtx = rr.rewrittenCtx;
    setOperation(rewrittenCtx);

    //set dest name mapping on new context; 1st child is TOK_FROM
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.MERGE);
    return rr;
  }

  @Override
  protected CopyOnWriteMergeWhenClauseSqlGenerator createMergeSqlGenerator(
      MergeStatement mergeStatement, MultiInsertSqlGenerator sqlGenerator) {
    return new CopyOnWriteMergeWhenClauseSqlGenerator(conf, sqlGenerator, mergeStatement);
  }
  
  private void handleSource(MergeStatement mergeStatement, MultiInsertSqlGenerator sqlGenerator) {
    boolean hasWhenNotMatchedInsertClause = mergeStatement.hasWhenNotMatchedInsertClause();
    
    String sourceName = mergeStatement.getSourceName();
    String sourceAlias = mergeStatement.getSourceAlias();
    
    String targetAlias = mergeStatement.getTargetAlias();
    String onClauseAsString = replaceColumnRefsWithTargetPrefix(targetAlias, mergeStatement.getOnClauseAsText());

    sqlGenerator.newCteExpr();
    
    sqlGenerator.append(sourceName + " AS ( SELECT * FROM\n");
    sqlGenerator.append("(SELECT ");
    sqlGenerator.appendAcidSelectColumns(Context.Operation.MERGE);
    sqlGenerator.appendAllColsOfTargetTable(TARGET_PREFIX);
    sqlGenerator.append(" FROM ").appendTargetTableName().append(") ");
    sqlGenerator.append(targetAlias);
    sqlGenerator.append('\n');
    sqlGenerator.indent().append(hasWhenNotMatchedInsertClause ? "FULL OUTER JOIN" : "LEFT OUTER JOIN").append("\n");
    sqlGenerator.indent().append(sourceAlias);
    sqlGenerator.append('\n');
    sqlGenerator.indent().append("ON ").append(onClauseAsString);
    sqlGenerator.append('\n');
    sqlGenerator.append(")");
    
    sqlGenerator.addCteExpr();
  }

  private static String replaceColumnRefsWithTargetPrefix(String columnRef, String strValue) {
    return strValue.replaceAll(columnRef + "\\.(`?)", "$1" + TARGET_PREFIX);
  }

  static class CopyOnWriteMergeWhenClauseSqlGenerator extends MergeRewriter.MergeWhenClauseSqlGenerator {

    private final COWWithClauseBuilder cowWithClauseBuilder;

    CopyOnWriteMergeWhenClauseSqlGenerator(
      HiveConf conf, MultiInsertSqlGenerator sqlGenerator, MergeStatement mergeStatement) {
      super(conf, sqlGenerator, mergeStatement);
      this.cowWithClauseBuilder = new COWWithClauseBuilder();
    }

    @Override
    public void appendWhenNotMatchedInsertClause(MergeStatement.InsertClause insertClause) {
      String targetAlias = mergeStatement.getTargetAlias();
      
      if (mergeStatement.getWhenClauses().size() > 1) {
        sqlGenerator.append("union all\n");
      }
      sqlGenerator.append("    -- insert clause\n").append("SELECT ");
      
      if (isNotBlank(hintStr)) {
        sqlGenerator.append(hintStr);
        hintStr = null;
      }
      List<String> values = sqlGenerator.getDeleteValues(Context.Operation.MERGE);
      values.add(insertClause.getValuesClause());
      
      sqlGenerator.append(StringUtils.join(values, ","));
      sqlGenerator.append("\nFROM " + mergeStatement.getSourceName());
      sqlGenerator.append("\n   WHERE ");
      
      StringBuilder whereClause = new StringBuilder(insertClause.getPredicate());
      
      if (insertClause.getExtraPredicate() != null) {
        //we have WHEN NOT MATCHED AND <boolean expr> THEN INSERT
        whereClause.append(" AND ").append(insertClause.getExtraPredicate());
      }
      sqlGenerator.append(
          replaceColumnRefsWithTargetPrefix(targetAlias, whereClause.toString()));
      sqlGenerator.append('\n');
    }

    @Override
    public void appendWhenMatchedUpdateClause(MergeStatement.UpdateClause updateClause) {
      Table targetTable = mergeStatement.getTargetTable();
      String targetAlias = mergeStatement.getTargetAlias();
      String onClauseAsString = mergeStatement.getOnClauseAsText();

      UnaryOperator<String> columnRefsFunc = value -> replaceColumnRefsWithTargetPrefix(targetAlias, value);
      sqlGenerator.append("    -- update clause (insert part)\n").append("SELECT ");

      if (isNotBlank(hintStr)) {
        sqlGenerator.append(hintStr);
        hintStr = null;
      }
      List<String> values = new ArrayList<>(targetTable.getCols().size() + targetTable.getPartCols().size());
      values.addAll(sqlGenerator.getDeleteValues(Context.Operation.MERGE));
      addValues(targetTable, targetAlias, updateClause.getNewValuesMap(), values);
      
      sqlGenerator.append(columnRefsFunc.apply(StringUtils.join(values, ",")));
      sqlGenerator.append("\nFROM " + mergeStatement.getSourceName());

      addWhereClauseOfUpdate(
          onClauseAsString, updateClause.getExtraPredicate(), updateClause.getDeleteExtraPredicate(), sqlGenerator,
          columnRefsFunc);
      sqlGenerator.append("\n");
    }
    
    @Override
    protected String getRhsExpValue(String newValue, String alias) {
        return String.format("%s AS %s", newValue, alias);
    }

    @Override
    protected void handleWhenMatchedDelete(String onClauseAsString, String extraPredicate, String updateExtraPredicate,
                                         String hintStr, MultiInsertSqlGenerator sqlGenerator) {
      String targetAlias = mergeStatement.getTargetAlias();
      String sourceName = mergeStatement.getSourceName();
      String onClausePredicate = mergeStatement.getOnClausePredicate();

      UnaryOperator<String> columnRefsFunc = value -> replaceColumnRefsWithTargetPrefix(targetAlias, value);
      List<String> deleteValues = sqlGenerator.getDeleteValues(Context.Operation.DELETE);

      List<MergeStatement.WhenClause> whenClauses = mergeStatement.getWhenClauses();
      if (whenClauses.size() > 1 || whenClauses.get(0) instanceof MergeStatement.UpdateClause) {
        sqlGenerator.append("union all\n");
      }
      sqlGenerator.append("    -- delete clause\n").append("SELECT ");

      if (isNotBlank(hintStr)) {
        sqlGenerator.append(hintStr);
      }
      sqlGenerator.append(StringUtils.join(deleteValues, ","));
      sqlGenerator.append("\nFROM " + sourceName);
      sqlGenerator.indent().append("WHERE ");

      StringBuilder whereClause = new StringBuilder(onClauseAsString);
      if (isNotBlank(extraPredicate)) {
        //we have WHEN MATCHED AND <boolean expr> THEN DELETE
        whereClause.append(" AND ").append(extraPredicate);
      }
      String whereClauseStr = columnRefsFunc.apply(whereClause.toString());
      String filePathCol = HiveUtils.unparseIdentifier(TARGET_PREFIX + VirtualColumn.FILE_PATH.getName(), conf);

      sqlGenerator.append("\n").indent();
      sqlGenerator.append("NOT(").append(whereClauseStr.replace("=","<=>"));
      
      if (isNotBlank(onClausePredicate)) {
        sqlGenerator.append(" OR ");
        sqlGenerator.append(columnRefsFunc.apply(mergeStatement.getOnClausePredicate()));
      }
      sqlGenerator.append(")\n").indent();
      // Add the file path filter that matches the delete condition.
      sqlGenerator.append("AND ").append(filePathCol);
      sqlGenerator.append(" IN ( select ").append(filePathCol).append(" from t )");
      sqlGenerator.append("\nunion all");
      sqlGenerator.append("\nselect * from t");

      cowWithClauseBuilder.appendWith(sqlGenerator, sourceName, filePathCol, whereClauseStr, false);
    }
  }
}
