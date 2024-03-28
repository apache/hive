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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.StorageFormat;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlGenerator;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.hadoop.hive.serde.serdeConstants;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class MergeRewriter implements Rewriter<MergeStatement>, MergeStatement.DestClausePrefixSetter {

  private final Hive db;
  protected final HiveConf conf;
  protected final SqlGeneratorFactory sqlGeneratorFactory;

  public MergeRewriter(Hive db, HiveConf conf, SqlGeneratorFactory sqlGeneratorFactory) {
    this.db = db;
    this.conf = conf;
    this.sqlGeneratorFactory = sqlGeneratorFactory;
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context ctx, MergeStatement mergeStatement) throws SemanticException {

    setOperation(ctx);
    MultiInsertSqlGenerator sqlGenerator = sqlGeneratorFactory.createSqlGenerator();
    handleSource(mergeStatement.hasWhenNotMatchedInsertClause(), mergeStatement.getSourceAlias(),
        mergeStatement.getOnClauseAsText(), sqlGenerator);

    MergeStatement.MergeSqlGenerator mergeSqlGenerator = createMergeSqlGenerator(mergeStatement, sqlGenerator);
    for (MergeStatement.WhenClause whenClause : mergeStatement.getWhenClauses()) {
      whenClause.toSql(mergeSqlGenerator);
    }

    boolean validateCardinalityViolation = mergeStatement.shouldValidateCardinalityViolation(conf);
    if (validateCardinalityViolation) {
      handleCardinalityViolation(mergeStatement.getTargetAlias(), mergeStatement.getOnClauseAsText(), sqlGenerator);
    }

    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(ctx, sqlGenerator.toString());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;
    setOperation(rewrittenCtx);

    //set dest name mapping on new context; 1st child is TOK_FROM
    int insClauseIdx = 1;
    for (MergeStatement.WhenClause whenClause : mergeStatement.getWhenClauses()) {
      insClauseIdx += whenClause.addDestNamePrefixOfInsert(this, insClauseIdx, rewrittenCtx);
    }

    if (validateCardinalityViolation) {
      //here means the last branch of the multi-insert is Cardinality Validation
      rewrittenCtx.addDestNamePrefix(rewrittenTree.getChildCount() - 1, Context.DestClausePrefix.INSERT);
    }

    return rr;
  }

  protected MergeWhenClauseSqlGenerator createMergeSqlGenerator(
      MergeStatement mergeStatement, MultiInsertSqlGenerator sqlGenerator) {
    return new MergeWhenClauseSqlGenerator(conf, sqlGenerator, mergeStatement);
  }

  private void handleSource(boolean hasWhenNotMatchedClause, String sourceAlias, String onClauseAsText,
                            MultiInsertSqlGenerator sqlGenerator) {
    sqlGenerator.append("FROM\n");
    sqlGenerator.append("(SELECT ");
    sqlGenerator.appendAcidSelectColumns(Context.Operation.MERGE);
    sqlGenerator.appendAllColsOfTargetTable();
    sqlGenerator.append(" FROM ").appendTargetTableName().append(") ");
    sqlGenerator.appendSubQueryAlias();
    sqlGenerator.append('\n');
    sqlGenerator.indent().append(hasWhenNotMatchedClause ? "RIGHT OUTER JOIN" : "INNER JOIN").append("\n");
    sqlGenerator.indent().append(sourceAlias);
    sqlGenerator.append('\n');
    sqlGenerator.indent().append("ON ").append(onClauseAsText).append('\n');
  }

  private void handleCardinalityViolation(
      String targetAlias, String onClauseAsString, MultiInsertSqlGenerator sqlGenerator)
      throws SemanticException {
    //this is a tmp table and thus Session scoped and acid requires SQL statement to be serial in a
    // given session, i.e. the name can be fixed across all invocations
    String tableName = "merge_tmp_table";
    List<String> sortKeys = sqlGenerator.getSortKeys();
    sqlGenerator.append("INSERT INTO ").append(tableName)
        .append("\n  SELECT cardinality_violation(")
        .append(StringUtils.join(sortKeys, ","));
    sqlGenerator.appendPartColsOfTargetTableWithComma(targetAlias);

    sqlGenerator.append(")\n WHERE ").append(onClauseAsString)
        .append(" GROUP BY ").append(StringUtils.join(sortKeys, ","));

    sqlGenerator.appendPartColsOfTargetTableWithComma(targetAlias);

    sqlGenerator.append(" HAVING count(*) > 1");
    //say table T has partition p, we are generating
    //select cardinality_violation(ROW_ID, p) WHERE ... GROUP BY ROW__ID, p
    //the Group By args are passed to cardinality_violation to add the violating value to the error msg
    try {
      if (null == db.getTable(tableName, false)) {
        StorageFormat format = new StorageFormat(conf);
        format.processStorageFormat("TextFile");
        Table table = db.newTable(tableName);
        table.setSerializationLib(format.getSerde());
        List<FieldSchema> fields = new ArrayList<>();
        fields.add(new FieldSchema("val", serdeConstants.INT_TYPE_NAME, null));
        table.setFields(fields);
        table.setDataLocation(Warehouse.getDnsPath(new Path(SessionState.get().getTempTableSpace(),
            tableName), conf));
        table.getTTable().setTemporary(true);
        table.setStoredAsSubDirectories(false);
        table.setInputFormatClass(format.getInputFormat());
        table.setOutputFormatClass(format.getOutputFormat());
        db.createTable(table, true);
      }
    } catch (HiveException | MetaException e) {
      throw new SemanticException(e.getMessage(), e);
    }
  }

  protected void setOperation(Context context) {
    context.setOperation(Context.Operation.MERGE);
  }

  protected static class MergeWhenClauseSqlGenerator implements MergeStatement.MergeSqlGenerator {

    protected final HiveConf conf;
    protected final MultiInsertSqlGenerator sqlGenerator;
    protected final MergeStatement mergeStatement;
    protected String hintStr;

    MergeWhenClauseSqlGenerator(HiveConf conf, MultiInsertSqlGenerator sqlGenerator, MergeStatement mergeStatement) {
      this.conf = conf;
      this.sqlGenerator = sqlGenerator;
      this.mergeStatement = mergeStatement;
      this.hintStr = mergeStatement.getHintStr();
    }

    @Override
    public void appendWhenNotMatchedInsertClause(MergeStatement.InsertClause insertClause) {
      sqlGenerator.append("INSERT INTO ").append(mergeStatement.getTargetName());
      if (insertClause.getColumnListText() != null) {
        sqlGenerator.append(' ').append(insertClause.getColumnListText());
      }

      sqlGenerator.append("    -- insert clause\n  SELECT ");
      if (isNotBlank(hintStr)) {
        sqlGenerator.append(hintStr);
        hintStr = null;
      }

      sqlGenerator.append(insertClause.getValuesClause()).append("\n   WHERE ").append(insertClause.getPredicate());

      if (insertClause.getExtraPredicate() != null) {
        //we have WHEN NOT MATCHED AND <boolean expr> THEN INSERT
        sqlGenerator.append(" AND ").append(insertClause.getExtraPredicate());
      }
      sqlGenerator.append('\n');
    }


    @Override
    public void appendWhenMatchedUpdateClause(MergeStatement.UpdateClause updateClause) {
      Table targetTable = mergeStatement.getTargetTable();
      String targetAlias = mergeStatement.getTargetAlias();
      String onClauseAsString = mergeStatement.getOnClauseAsText();

      sqlGenerator.append("    -- update clause").append("\n");
      List<String> valuesAndAcidSortKeys = new ArrayList<>(
          targetTable.getCols().size() + targetTable.getPartCols().size() + 1);
      valuesAndAcidSortKeys.addAll(sqlGenerator.getSortKeys());
      addValues(targetTable, targetAlias, updateClause.getNewValuesMap(), valuesAndAcidSortKeys);
      sqlGenerator.appendInsertBranch(hintStr, valuesAndAcidSortKeys);
      hintStr = null;

      addWhereClauseOfUpdate(
          onClauseAsString, updateClause.getExtraPredicate(), updateClause.getDeleteExtraPredicate(), sqlGenerator);

      sqlGenerator.appendSortBy(sqlGenerator.getSortKeys());
    }

    protected void addValues(Table targetTable, String targetAlias, Map<String, String> newValues,
                             List<String> values) {
      UnaryOperator<String> formatter = name -> String.format("%s.%s", targetAlias, 
          HiveUtils.unparseIdentifier(name, conf));
      
      for (FieldSchema fieldSchema : targetTable.getCols()) {
        if (newValues.containsKey(fieldSchema.getName())) {
          String rhsExp = newValues.get(fieldSchema.getName());
          values.add(getRhsExpValue(rhsExp, formatter.apply(fieldSchema.getName())));
        } else {
          values.add(formatter.apply(fieldSchema.getName()));
        }
      }
      
      targetTable.getPartCols().forEach(fieldSchema -> values.add(
          formatter.apply(fieldSchema.getName())));
    }
    
    protected String getRhsExpValue(String newValue, String alias) {
      return newValue;
    }

    protected void addWhereClauseOfUpdate(String onClauseAsString, String extraPredicate, String deleteExtraPredicate,
                                          MultiInsertSqlGenerator sqlGenerator) {
      addWhereClauseOfUpdate(onClauseAsString, extraPredicate, deleteExtraPredicate, sqlGenerator, UnaryOperator.identity());
    }
    
    protected void addWhereClauseOfUpdate(String onClauseAsString, String extraPredicate, String deleteExtraPredicate,
                                          MultiInsertSqlGenerator sqlGenerator, UnaryOperator<String> columnRefsFunc) {
      StringBuilder whereClause = new StringBuilder(onClauseAsString);
      if (extraPredicate != null) {
        //we have WHEN MATCHED AND <boolean expr> THEN DELETE
        whereClause.append(" AND ").append(extraPredicate);
      }
      if (deleteExtraPredicate != null) {
        whereClause.append(" AND NOT(").append(deleteExtraPredicate).append(")");
      }
      sqlGenerator.indent().append("WHERE ");
      sqlGenerator.append(columnRefsFunc.apply(whereClause.toString()));
    }

    @Override
    public void appendWhenMatchedDeleteClause(MergeStatement.DeleteClause deleteClause) {
      handleWhenMatchedDelete(mergeStatement.getOnClauseAsText(),
          deleteClause.getExtraPredicate(), deleteClause.getUpdateExtraPredicate(), hintStr, sqlGenerator);
      hintStr = null;
    }

    protected void handleWhenMatchedDelete(String onClauseAsString, String extraPredicate, String updateExtraPredicate,
                                         String hintStr, MultiInsertSqlGenerator sqlGenerator) {
      sqlGenerator.appendDeleteBranch(hintStr);

      sqlGenerator.indent().append("WHERE ").append(onClauseAsString);
      if (extraPredicate != null) {
        //we have WHEN MATCHED AND <boolean expr> THEN DELETE
        sqlGenerator.append(" AND ").append(extraPredicate);
      }
      if (updateExtraPredicate != null) {
        sqlGenerator.append(" AND NOT(").append(updateExtraPredicate).append(")");
      }
      sqlGenerator.append("\n").indent();
      sqlGenerator.appendSortKeys();
    }
  }
}
