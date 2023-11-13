package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private String onClauseAsText;
    private String hintStr;
    private InsertClause insertClause;
    private UpdateClause updateClause;
    private DeleteClause deleteClause;

    private MergeStatementBuilder(Table targetTable, String targetName, String targetAlias) {
      this.targetTable = targetTable;
      this.targetName = targetName;
      this.targetAlias = targetAlias;
    }

    public MergeStatementBuilder sourceName(String sourceName) {
      this.sourceName = sourceName;
      return this;
    }
    public MergeStatementBuilder sourceAlias(String sourceAlias) {
      this.sourceAlias = sourceAlias;
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
    public MergeStatementBuilder insertClause(InsertClause insertClause) {
      this.insertClause = insertClause;
      return this;
    }

    public MergeStatementBuilder updateClause(UpdateClause updateClause) {
      this.updateClause = updateClause;
      return this;
    }

    public MergeStatementBuilder deleteClause(DeleteClause deleteClause) {
      this.deleteClause = deleteClause;
      return this;
    }

    public MergeStatement build() {
      return new MergeStatement(targetTable, targetName, targetAlias, sourceName, sourceAlias, onClauseAsText, hintStr,
          insertClause, updateClause, deleteClause);
    }
  }

  private final Table targetTable;
  private final String targetName;
  private final String targetAlias;
  private final String sourceName;
  private final String sourceAlias;
  private final String onClauseAsText;
  private final String hintStr;
  private final InsertClause insertClause;
  private final UpdateClause updateClause;
  private final DeleteClause deleteClause;

  private MergeStatement(Table targetTable, String targetName, String targetAlias, String sourceName, String sourceAlias,
                         String onClauseAsText, String hintStr, InsertClause insertClause, UpdateClause updateClause,
                         DeleteClause deleteClause) {
    this.targetTable = targetTable;
    this.targetName = targetName;
    this.targetAlias = targetAlias;
    this.sourceName = sourceName;
    this.sourceAlias = sourceAlias;
    this.onClauseAsText = onClauseAsText;
    this.hintStr = hintStr;
    this.insertClause = insertClause;
    this.updateClause = updateClause;
    this.deleteClause = deleteClause;
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

  public String getOnClauseAsText() {
    return onClauseAsText;
  }

  public String getHintStr() {
    return hintStr;
  }

  public InsertClause getInsertClause() {
    return insertClause;
  }

  public UpdateClause getUpdateClause() {
    return updateClause;
  }

  public DeleteClause getDeleteClause() {
    return deleteClause;
  }

  /**
   * Per SQL Spec ISO/IEC 9075-2:2011(E) Section 14.2 under "General Rules" Item 6/Subitem a/Subitem 2/Subitem B,
   * an error should be raised if > 1 row of "source" matches the same row in "target".
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
    boolean onlyHaveWhenNotMatchedClause = updateClause == null && deleteClause == null;
    return !onlyHaveWhenNotMatchedClause;
  }

  public static class WhenClause {
    private final String extraPredicate;

    public WhenClause(String extraPredicate) {
      this.extraPredicate = extraPredicate;
    }

    public String getExtraPredicate() {
      return extraPredicate;
    }
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
  }
}
