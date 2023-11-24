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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MergeStatement {
  protected static final Logger LOG = LoggerFactory.getLogger(MergeStatement.class);

  public static MergeStatementBuilder withTarget(Table targetTable, String targetName, String targetAlias) {
    return new MergeStatementBuilder(targetTable, targetName, targetAlias);
  }

  public static class MergeStatementBuilder {
    private final Table targetTable;
    private final String targetName;
    private final String targetAlias;
    private String sourceName;
    private String sourceAlias;
    private String onClausePredicate;
    private String onClauseAsText;
    private String hintStr;
    private final List<WhenClause> whenClauses;

    private MergeStatementBuilder(Table targetTable, String targetName, String targetAlias) {
      this.targetTable = targetTable;
      this.targetName = targetName;
      this.targetAlias = targetAlias;
      whenClauses = new ArrayList<>(3);
    }

    public MergeStatementBuilder sourceName(String sourceName) {
      this.sourceName = sourceName;
      return this;
    }
    public MergeStatementBuilder sourceAlias(String sourceAlias) {
      this.sourceAlias = sourceAlias;
      return this;
    }
    public MergeStatementBuilder onClausePredicate(String onClausePredicate) {
      this.onClausePredicate = onClausePredicate;
      return this;
    }
    public MergeStatementBuilder onClauseAsText(String onClauseAsText) {
      this.onClauseAsText = onClauseAsText;
      return this;
    }
    public MergeStatementBuilder hintStr(String hintStr) {
      this.hintStr = hintStr;
      return this;
    }

    public MergeStatementBuilder addWhenClause(WhenClause whenClause) {
      whenClauses.add(whenClause);
      return this;
    }

    public MergeStatement build() {
      return new MergeStatement(targetTable, targetName, targetAlias, sourceName, sourceAlias,
          onClausePredicate, onClauseAsText, hintStr,
          Collections.unmodifiableList(whenClauses));
    }
  }

  private final Table targetTable;
  private final String targetName;
  private final String targetAlias;
  private final String sourceName;
  private final String sourceAlias;
  private final String onClausePredicate;
  private final String onClauseAsText;
  private final String hintStr;
  private final List<WhenClause> whenClauses;

  private MergeStatement(Table targetTable, String targetName, String targetAlias, String sourceName, String sourceAlias,
                         String onClausePredicate, String onClauseAsText, String hintStr, List<WhenClause> whenClauses) {
    this.targetTable = targetTable;
    this.targetName = targetName;
    this.targetAlias = targetAlias;
    this.sourceName = sourceName;
    this.sourceAlias = sourceAlias;
    this.onClausePredicate = onClausePredicate;
    this.onClauseAsText = onClauseAsText;
    this.hintStr = hintStr;
    this.whenClauses = whenClauses;
  }

  public Table getTargetTable() {
    return targetTable;
  }

  public String getTargetName() {
    return targetName;
  }

  public String getTargetAlias() {
    return targetAlias;
  }

  public String getSourceName() {
    return sourceName;
  }

  public String getSourceAlias() {
    return sourceAlias;
  }

  public String getOnClausePredicate() {
    return onClausePredicate;
  }
  
  public String getOnClauseAsText() {
    return onClauseAsText;
  }

  public String getHintStr() {
    return hintStr;
  }

  public List<WhenClause> getWhenClauses() {
    return whenClauses;
  }

  public boolean hasWhenNotMatchedInsertClause() {
    for (WhenClause whenClause : whenClauses) {
      if (whenClause instanceof InsertClause) {
        return true;
      }
    }

    return false;
  }

  /**
   * Per SQL Spec ISO/IEC 9075-2:2011(E) Section 14.2 under "General Rules" Item 6/Subitem a/Subitem 2/Subitem B,
   * an error should be raised if &gt; 1 row of "source" matches the same row in "target".
   * This should not affect the runtime of the query as it's running in parallel with other
   * branches of the multi-insert.  It won't actually write any data to merge_tmp_table since the
   * cardinality_violation() UDF throws an error whenever it's called killing the query
   * @return true if another Insert clause was added
   */
  public boolean shouldValidateCardinalityViolation(HiveConf conf) {
    if (!conf.getBoolVar(HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK)) {
      LOG.info("Merge statement cardinality violation check is disabled: {}",
          HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK.varname);
      return false;
    }
    //if no update or delete in Merge, there is no need to do cardinality check
    boolean onlyHaveWhenNotMatchedClause = whenClauses.size() == 1 && whenClauses.get(0) instanceof InsertClause;
    return !onlyHaveWhenNotMatchedClause;
  }

  protected abstract static class WhenClause {
    private final String extraPredicate;

    public WhenClause(String extraPredicate) {
      this.extraPredicate = extraPredicate;
    }

    public String getExtraPredicate() {
      return extraPredicate;
    }

    public abstract void toSql(MergeSqlGenerator sqlGenerator);
    public abstract int addDestNamePrefixOfInsert(
        DestClausePrefixSetter destClausePrefixSetter, int pos, Context context);
  }

  public static class InsertClause extends WhenClause {
    private final String columnListText;
    private final String valuesClause;
    private final String predicate;


    public InsertClause(String columnListText, String valuesClause, String predicate, String extraPredicate) {
      super(extraPredicate);
      this.predicate = predicate;
      this.columnListText = columnListText;
      this.valuesClause = valuesClause;
    }

    public String getColumnListText() {
      return columnListText;
    }

    public String getValuesClause() {
      return valuesClause;
    }

    public String getPredicate() {
      return predicate;
    }

    @Override
    public void toSql(MergeSqlGenerator sqlGenerator) {
      sqlGenerator.appendWhenNotMatchedInsertClause(this);
    }

    @Override
    public int addDestNamePrefixOfInsert(DestClausePrefixSetter setter, int pos, Context context) {
      return setter.addDestNamePrefixOfInsert(pos, context);
    }
  }

  public static class UpdateClause extends WhenClause {
    private final Map<String, String> newValuesMap;
    private final String deleteExtraPredicate;

    public UpdateClause(String extraPredicate, String deleteExtraPredicate, Map<String, String> newValuesMap) {
      super(extraPredicate);
      this.newValuesMap = newValuesMap;
      this.deleteExtraPredicate = deleteExtraPredicate;
    }

    public Map<String, String> getNewValuesMap() {
      return newValuesMap;
    }

    public String getDeleteExtraPredicate() {
      return deleteExtraPredicate;
    }

    @Override
    public void toSql(MergeSqlGenerator sqlGenerator) {
      sqlGenerator.appendWhenMatchedUpdateClause(this);
    }

    @Override
    public int addDestNamePrefixOfInsert(DestClausePrefixSetter setter, int pos, Context context) {
      return setter.addDestNamePrefixOfUpdate(pos, context);
    }
  }

  public static class DeleteClause extends WhenClause {
    private final String updateExtraPredicate;

    public DeleteClause(String extraPredicate, String updateExtraPredicate) {
      super(extraPredicate);
      this.updateExtraPredicate = updateExtraPredicate;
    }

    public String getUpdateExtraPredicate() {
      return updateExtraPredicate;
    }

    @Override
    public void toSql(MergeSqlGenerator sqlGenerator) {
      sqlGenerator.appendWhenMatchedDeleteClause(this);
    }

    @Override
    public int addDestNamePrefixOfInsert(DestClausePrefixSetter setter, int pos, Context context) {
      return setter.addDestNamePrefixOfDelete(pos, context);
    }
  }

  public interface MergeSqlGenerator {
    void appendWhenNotMatchedInsertClause(InsertClause insertClause);
    void appendWhenMatchedUpdateClause(UpdateClause updateClause);
    void appendWhenMatchedDeleteClause(DeleteClause deleteClause);
  }

  public interface DestClausePrefixSetter {
    default int addDestNamePrefixOfInsert(int pos, Context context) {
      context.addDestNamePrefix(pos, Context.DestClausePrefix.INSERT);
      return 1;
    }

    default int addDestNamePrefixOfUpdate(int pos, Context context) {
      context.addDestNamePrefix(pos, Context.DestClausePrefix.UPDATE);
      return 1;
    }

    default int addDestNamePrefixOfDelete(int pos, Context context) {
      context.addDestNamePrefix(pos, Context.DestClausePrefix.DELETE);
      return 1;
    }
  }
}
