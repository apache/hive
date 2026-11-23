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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlGenerator;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.PartitionSetValuesClause;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SetValuesClause;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SetValuesClauseBase;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.ql.metadata.RowLineageUtils.addRowLineageColumnsForWhenMatchedUpdateClause;

public class SplitMergeRewriter extends MergeRewriter {

  public SplitMergeRewriter(Hive db, HiveConf conf, SqlGeneratorFactory sqlGeneratorFactory) {
    super(db, conf, sqlGeneratorFactory);
  }

  @Override
  protected MergeWhenClauseSqlGenerator createMergeSqlGenerator(
      MergeStatement mergeStatement, MultiInsertSqlGenerator sqlGenerator) {
    return new SplitMergeWhenClauseSqlGenerator(conf, sqlGenerator, mergeStatement, isRowLineageSupported,
        mergeStatement.getTargetTable().hasNonNativePartitionSupport() ?
            new PartitionSetValuesClause(conf) : new SetValuesClause(conf));
  }

  static class SplitMergeWhenClauseSqlGenerator extends MergeWhenClauseSqlGenerator {
    private final boolean isRowLineageSupported;
    // the deletes of the WHEN MATCHED clauses in clause order, appended as a single delete branch by build()
    private final List<DeleteBranch> deleteBranches = new ArrayList<>(2);

    SplitMergeWhenClauseSqlGenerator(
        HiveConf conf, MultiInsertSqlGenerator sqlGenerator, MergeStatement mergeStatement,
        boolean isRowLineageSupported, SetValuesClauseBase setValuesClause) {
      super(conf, sqlGenerator, mergeStatement, isRowLineageSupported, setValuesClause);
      this.isRowLineageSupported = isRowLineageSupported;
    }

    @Override
    public void appendWhenMatchedUpdateClause(MergeStatement.UpdateClause updateClause) {
      Table targetTable = mergeStatement.getTargetTable();
      String targetAlias = mergeStatement.getTargetAlias();
      String onClauseAsString = mergeStatement.getOnClauseAsText();

      addBranch(new DeleteOfUpdateBranch());
      sqlGenerator.append("    -- update clause (insert part)\n");
      List<String> values = new ArrayList<>(targetTable.getAllCols().size());
      setValuesClause.addValues(targetTable, targetAlias, updateClause.getNewValuesMap(), values);
      addRowLineageColumnsForWhenMatchedUpdateClause(isRowLineageSupported, values, targetAlias, conf);
      sqlGenerator.appendInsertBranch(hintStr, values);
      hintStr = null;

      addWhereClauseOfUpdate(
          onClauseAsString, updateClause.getExtraPredicate(), updateClause.getDeleteExtraPredicate(), sqlGenerator);

      sqlGenerator.append("\n");

      deleteBranches.add(new DeleteBranch("update clause (delete part)",
          updateClause.getExtraPredicate(), updateClause.getDeleteExtraPredicate()));
    }

    @Override
    public void appendWhenMatchedDeleteClause(MergeStatement.DeleteClause deleteClause) {
      deleteBranches.add(new DeleteBranch("delete clause",
          deleteClause.getExtraPredicate(), deleteClause.getUpdateExtraPredicate()));
    }

    @Override
    public void build() {
      if (deleteBranches.isEmpty()) {
        return;
      }
      addBranch(new Branch(Context.DestClausePrefix.DELETE));
      sqlGenerator.append(deleteBranches.stream().map(DeleteBranch::comment)
          .collect(Collectors.joining(" and ", "    -- ", "\n")));
      String onClauseAsString = mergeStatement.getOnClauseAsText();
      if (deleteBranches.size() == 1) {
        DeleteBranch deleteBranch = deleteBranches.getFirst();
        handleWhenMatchedDelete(onClauseAsString, deleteBranch.extraPredicate(),
            deleteBranch.precedingClausePredicate(), hintStr, sqlGenerator);
      } else {
        // The update and delete clauses never match the same row, so a single delete branch deletes the same rows, into
        // one delete delta or set of delete files. A target that needs all the deletes of a data file in one writer
        // (DVs) requires it.
        String deletePredicate = deleteBranches.stream()
            .map(deleteBranch -> "(" + deleteBranch.clausePredicate() + ")")
            .collect(Collectors.joining(" OR ", "(", ")"));
        handleWhenMatchedDelete(onClauseAsString, deletePredicate, null, hintStr, sqlGenerator);
      }
      hintStr = null;
    }

    private record DeleteBranch(String comment, String extraPredicate, String precedingClausePredicate) {
      // the matched rows the WHEN MATCHED clause handles: its own predicate, minus the rows of the clause before it
      String clausePredicate() {
        String predicate = extraPredicate == null ? "true" : "(" + extraPredicate + ")";
        return precedingClausePredicate == null ? predicate : predicate + " AND NOT(" + precedingClausePredicate + ")";
      }
    }
  }

  @Override
  public void setOperation(Context context) {
    context.setOperation(Context.Operation.MERGE, true);
  }

  /**
   * The half of a split update whose rows are not counted: the delete branch counts the updated rows, so the insert
   * branch of the update does not.
   */
  private static class DeleteOfUpdateBranch extends Branch {
    DeleteOfUpdateBranch() {
      super(Context.DestClausePrefix.INSERT);
    }

    @Override
    protected void addDestNamePrefix(int pos, Context context) {
      context.addDeleteOfUpdateDestNamePrefix(pos, prefix);
    }
  }
}
