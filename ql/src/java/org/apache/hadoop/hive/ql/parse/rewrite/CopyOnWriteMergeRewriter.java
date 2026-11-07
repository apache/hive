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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.parse.rewrite;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.COWWithClauseBuilder;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlGenerator;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.PartitionSetValuesClause;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SetValuesClauseBase;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.hadoop.hive.ql.metadata.RowLineageUtils.addRowLineageValuesForAppendWhenNotMatchedClause;
import static org.apache.hadoop.hive.ql.metadata.RowLineageUtils.addSourceForRowLineageCopyOnWriteMerge;
import static org.apache.hadoop.hive.ql.metadata.RowLineageUtils.addValuesForRowLineageForCopyOnMerge;
import static org.apache.hadoop.hive.ql.metadata.RowLineageUtils.shouldAddRowLineageColumnsForMerge;
import static org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory.TARGET_PREFIX;

public class CopyOnWriteMergeRewriter extends MergeRewriter {
  private static final String SOURCE_PRESENCE_COLUMN = "hive_cow_source_row_marker";

  public CopyOnWriteMergeRewriter(Hive db, HiveConf conf, SqlGeneratorFactory sqlGeneratorFactory) {
    super(db, conf, sqlGeneratorFactory);
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context ctx, MergeStatement mergeStatement) throws SemanticException {

    setOperation(ctx);
    MultiInsertSqlGenerator sqlGenerator = sqlGeneratorFactory.createSqlGenerator();
    isRowLineageSupported = shouldAddRowLineageColumnsForMerge(mergeStatement, conf);
    handleSource(mergeStatement, sqlGenerator);

    sqlGenerator.append('\n');
    sqlGenerator.append("INSERT INTO ").appendTargetTableName();
    sqlGenerator.append('\n');
    
    List<MergeStatement.WhenClause> whenClauses = Lists.newArrayList(mergeStatement.getWhenClauses());

    List<MergeStatement.WhenClause> matchedClauses = whenClauses.stream()
        .filter(whenClause -> !(whenClause instanceof MergeStatement.InsertClause))
        .toList();
    String combinedExtraPredicate = null;
    if (!matchedClauses.isEmpty()
        && matchedClauses.stream().noneMatch(whenClause -> !isNotBlank(whenClause.getExtraPredicate()))) {
      combinedExtraPredicate = matchedClauses.stream()
          .map(MergeStatement.WhenClause::getExtraPredicate)
          .map(predicate -> parenthesizePredicate(predicate))
          .collect(Collectors.joining(" OR "));
    }

    whenClauses.removeIf(whenClause -> whenClause instanceof MergeStatement.DeleteClause);
    if (!matchedClauses.isEmpty()) {
      whenClauses.add(new MergeStatement.DeleteClause(combinedExtraPredicate, null));
    }

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
    return new CopyOnWriteMergeWhenClauseSqlGenerator(conf, sqlGenerator, mergeStatement,
        new PartitionSetValuesClause(conf, (newValue, alias) -> String.format("%s AS %s", newValue, alias)));
  }
  
  private void handleSource(MergeStatement mergeStatement, MultiInsertSqlGenerator sqlGenerator) {
    boolean hasWhenNotMatchedInsertClause = mergeStatement.hasWhenNotMatchedInsertClause();
    
    String sourceName = mergeStatement.getSourceName();
    String sourceAlias = mergeStatement.getSourceAlias();
    
    String targetAlias = mergeStatement.getTargetAlias();
    String onClauseAsString = replaceColumnRefsWithTargetPrefix(targetAlias, mergeStatement.getOnClauseAsText());
    String sourcePresenceCol = sourcePresenceColumn(mergeStatement, conf);

    sqlGenerator.newCteExpr();
    
    sqlGenerator.append(sourceName + " AS ( SELECT * FROM\n");
    sqlGenerator.append("(SELECT ");
    sqlGenerator.appendAcidSelectColumns(Context.Operation.MERGE);
    sqlGenerator.appendAllColsOfTargetTable(TARGET_PREFIX);
    addSourceForRowLineageCopyOnWriteMerge(isRowLineageSupported, sqlGenerator, conf);
    sqlGenerator.append(" FROM ").appendTargetTableName().append(") ");
    sqlGenerator.append(targetAlias);
    sqlGenerator.append('\n');
    sqlGenerator.indent().append(hasWhenNotMatchedInsertClause ? "FULL OUTER JOIN" : "LEFT OUTER JOIN").append("\n");
    sqlGenerator.indent().append("(SELECT ").append(sourceName).append(".*, TRUE AS ")
        .append(sourcePresenceCol)
        .append(" FROM ").append(sourceAlias).append("\n) ").append(sourceName);
    sqlGenerator.append('\n');
    sqlGenerator.indent().append("ON ").append(onClauseAsString);
    sqlGenerator.append('\n');
    sqlGenerator.append(")");
    
    sqlGenerator.addCteExpr();
  }

  private static String parenthesizePredicate(String predicate) {
    return "(\n" + predicate + "\n)";
  }

  private static String sourcePresenceColumn(MergeStatement mergeStatement, HiveConf conf) {
    Set<String> reservedColumns = new HashSet<>();
    mergeStatement.getTargetTable().getAllCols().forEach(column ->
        reservedColumns.add(column.getName().toLowerCase(Locale.ROOT)));

    StringBuilder sqlText = new StringBuilder(mergeStatement.getOnClauseAsText())
        .append(' ').append(mergeStatement.getSourceAlias());
    mergeStatement.getWhenClauses().forEach(whenClause ->
        sqlText.append(' ').append(whenClause.getExtraPredicate()));

    String candidate = SOURCE_PRESENCE_COLUMN;
    int suffix = 0;
    while (reservedColumns.contains(candidate.toLowerCase(Locale.ROOT))
        || sqlText.toString().toLowerCase(Locale.ROOT).contains(candidate.toLowerCase(Locale.ROOT))) {
      candidate = SOURCE_PRESENCE_COLUMN + "_" + ++suffix;
    }
    return HiveUtils.unparseIdentifier(candidate, conf);
  }

  private static String replaceColumnRefsWithTargetPrefix(String columnRef, String strValue) {
    return strValue.replaceAll(columnRef + "\\.(`?)", "$1" + TARGET_PREFIX);
  }

  static class CopyOnWriteMergeWhenClauseSqlGenerator extends MergeRewriter.MergeWhenClauseSqlGenerator {

    private final COWWithClauseBuilder cowWithClauseBuilder;
    private int subQueryCount = 0;

    CopyOnWriteMergeWhenClauseSqlGenerator(
        HiveConf conf, MultiInsertSqlGenerator sqlGenerator,
        MergeStatement mergeStatement, SetValuesClauseBase setvaluesClause) {
      super(conf, sqlGenerator, mergeStatement,
          shouldAddRowLineageColumnsForMerge(mergeStatement, conf), setvaluesClause);
      this.cowWithClauseBuilder = new COWWithClauseBuilder();
    }

    @Override
    public void appendWhenNotMatchedInsertClause(MergeStatement.InsertClause insertClause) {
      String targetAlias = mergeStatement.getTargetAlias();
      
      if (++subQueryCount > 1) {
        sqlGenerator.append("union all\n");
      }
      sqlGenerator.append("    -- insert clause\n").append("SELECT ");
      
      if (isNotBlank(hintStr)) {
        sqlGenerator.append(hintStr);
        hintStr = null;
      }
      List<String> values = sqlGenerator.getDeleteValues(Context.Operation.MERGE);
      
      if (insertClause.getColumnList() != null) {
        List<String> columnNames = insertClause.getColumnList();
        List<String> columnValues = insertClause.getValuesClause();
        
        Map<String, String> columnMap = IntStream.range(0, columnNames.size()).boxed().collect(
            Collectors.toMap(columnNames::get, columnValues::get));
        for (FieldSchema col : mergeStatement.getTargetTable().getAllCols()) {
          values.add(columnMap.getOrDefault(col.getName(), "null"));
        }
      } else {
        values.addAll(insertClause.getValuesClause());
      }
      sqlGenerator.append(
          StringUtils.join(addRowLineageValuesForAppendWhenNotMatchedClause(isRowLineageSupported, values), ","));
      sqlGenerator.append("\nFROM " + mergeStatement.getSourceName());
      sqlGenerator.append("\n   WHERE ");
      
      String filePathCol = HiveUtils.unparseIdentifier(TARGET_PREFIX + VirtualColumn.FILE_PATH.getName(), conf);
      String sourcePresenceCol = sourcePresenceColumn(mergeStatement, conf);
      StringBuilder whereClause = new StringBuilder(filePathCol).append(" IS NULL AND ")
          .append(sourcePresenceCol).append(" IS NOT NULL");

      if (insertClause.getExtraPredicate() != null) {
        //we have WHEN NOT MATCHED AND <boolean expr> THEN INSERT
        whereClause.append(" AND ").append(parenthesizePredicate(insertClause.getExtraPredicate()));
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
      String filePathCol = HiveUtils.unparseIdentifier(TARGET_PREFIX + VirtualColumn.FILE_PATH.getName(), conf);
      String sourcePresenceCol = sourcePresenceColumn(mergeStatement, conf);
      String matchedPredicate = parenthesizePredicate(onClauseAsString) + " AND " + filePathCol
          + " IS NOT NULL AND " + sourcePresenceCol + " IS NOT NULL";

      UnaryOperator<String> columnRefsFunc = value -> replaceColumnRefsWithTargetPrefix(targetAlias, value);
      sqlGenerator.append("    -- update clause (insert part)\n").append("SELECT ");
      ++subQueryCount;
      
      if (isNotBlank(hintStr)) {
        sqlGenerator.append(hintStr);
        hintStr = null;
      }
      List<String> values = new ArrayList<>(targetTable.getAllCols().size());
      values.addAll(sqlGenerator.getDeleteValues(Context.Operation.MERGE));
      setValuesClause.addValues(targetTable, targetAlias, updateClause.getNewValuesMap(), values);
      addValuesForRowLineageForCopyOnMerge(isRowLineageSupported, values,
          "NULL AS " + HiveUtils.unparseIdentifier(VirtualColumn.LAST_UPDATED_SEQUENCE_NUMBER.getName()), conf);
      sqlGenerator.append(columnRefsFunc.apply(StringUtils.join(values, ",")));
      sqlGenerator.append("\nFROM " + mergeStatement.getSourceName());

      addWhereClauseOfUpdate(
          matchedPredicate, updateClause.getExtraPredicate(), updateClause.getDeleteExtraPredicate(), sqlGenerator,
          columnRefsFunc);
      sqlGenerator.append("\n");
    }

    @Override
    protected void handleWhenMatchedDelete(String onClauseAsString, String extraPredicate, String updateExtraPredicate,
                                         String hintStr, MultiInsertSqlGenerator sqlGenerator) {
      String targetAlias = mergeStatement.getTargetAlias();
      String sourceName = mergeStatement.getSourceName();

      UnaryOperator<String> columnRefsFunc = value -> replaceColumnRefsWithTargetPrefix(targetAlias, value);
      List<String> deleteValues = sqlGenerator.getDeleteValues(Context.Operation.DELETE);
      addValuesForRowLineageForCopyOnMerge(isRowLineageSupported, deleteValues,
          HiveUtils.unparseIdentifier(TARGET_PREFIX + VirtualColumn.LAST_UPDATED_SEQUENCE_NUMBER.getName()), conf);

      if (++subQueryCount > 1) {
        sqlGenerator.append("union all\n");
      }
      sqlGenerator.append("    -- delete clause\n").append("SELECT ");

      if (isNotBlank(hintStr)) {
        sqlGenerator.append(hintStr);
      }
      sqlGenerator.append(StringUtils.join(deleteValues, ","));
      sqlGenerator.append("\nFROM " + sourceName);
      sqlGenerator.indent().append("WHERE ");

      String filePathCol = HiveUtils.unparseIdentifier(TARGET_PREFIX + VirtualColumn.FILE_PATH.getName(), conf);
      String sourcePresenceCol = sourcePresenceColumn(mergeStatement, conf);
      StringBuilder whereClause = new StringBuilder(parenthesizePredicate(onClauseAsString))
          .append(" AND ").append(filePathCol).append(" IS NOT NULL AND ")
          .append(sourcePresenceCol).append(" IS NOT NULL");
      if (isNotBlank(extraPredicate)) {
        //we have WHEN MATCHED AND <boolean expr> THEN DELETE
        whereClause.append(" AND ").append(parenthesizePredicate(extraPredicate));
      }
      String whereClauseStr = columnRefsFunc.apply(whereClause.toString());
      cowWithClauseBuilder.appendWith(sqlGenerator, sourceName, filePathCol, whereClauseStr, false,
          isRowLineageSupported, TARGET_PREFIX);

      sqlGenerator.append("\n").indent();
      sqlGenerator.append("( NOT(%s) OR (%s) IS NULL )".replace("%s", whereClauseStr));
      
      sqlGenerator.append("\n").indent();
      // Add the file path filter that matches the delete condition.
      sqlGenerator.append("AND ").append(filePathCol);
      sqlGenerator.append(" IN ( select ").append(filePathCol).append(" from ")
          .append(cowWithClauseBuilder.getMarkerCteName()).append(" )");
      sqlGenerator.append("\nunion all");
      sqlGenerator.append("\nselect * from ").append(cowWithClauseBuilder.getMarkerCteName());

    }
  }
}
