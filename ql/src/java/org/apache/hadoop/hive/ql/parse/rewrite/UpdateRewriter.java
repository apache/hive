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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlGenerator;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SetClausePatcher;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.WhereClausePatcher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpdateRewriter implements Rewriter<UpdateStatement> {

  public static final Context.Operation OPERATION = Context.Operation.UPDATE;

  protected final HiveConf conf;
  protected final SqlGeneratorFactory sqlGeneratorFactory;
  private final WhereClausePatcher whereClausePatcher;
  private final SetClausePatcher setClausePatcher;

  public UpdateRewriter(HiveConf conf, SqlGeneratorFactory sqlGeneratorFactory) {
    this.conf = conf;
    this.sqlGeneratorFactory = sqlGeneratorFactory;
    this.whereClausePatcher = new WhereClausePatcher();
    this.setClausePatcher = new SetClausePatcher();
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context context, UpdateStatement updateBlock)
      throws SemanticException {

    MultiInsertSqlGenerator sqlGenerator = sqlGeneratorFactory.createSqlGenerator();

    sqlGenerator.append("insert into table ");
    sqlGenerator.appendTargetTableName();
    sqlGenerator.appendPartitionColsOfTarget();

    int columnOffset = sqlGenerator.getDeleteValues(OPERATION).size();
    sqlGenerator.append(" select ");
    sqlGenerator.appendAcidSelectColumns(OPERATION);
    sqlGenerator.removeLastChar();

    Map<Integer, ASTNode> setColExprs = new HashMap<>(updateBlock.getSetCols().size());
    List<FieldSchema> nonPartCols = updateBlock.getTargetTable().getCols();
    for (int i = 0; i < nonPartCols.size(); i++) {
      sqlGenerator.append(",");
      String name = nonPartCols.get(i).getName();
      ASTNode setCol = updateBlock.getSetCols().get(name);
      sqlGenerator.append(HiveUtils.unparseIdentifier(name, this.conf));
      if (setCol != null) {
        // This is one of the columns we're setting, record it's position so we can come back
        // later and patch it up.
        // Add one to the index because the select has the ROW__ID as the first column.
        setColExprs.put(columnOffset + i, setCol);
      }
    }

    sqlGenerator.append(" from ");
    sqlGenerator.appendTargetTableName();

    // Add a sort by clause so that the row ids come out in the correct order
    sqlGenerator.appendSortBy(sqlGenerator.getSortKeys());

    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(context, sqlGenerator.toString());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = (ASTNode) rewrittenTree.getChildren().get(1);
    rewrittenCtx.setOperation(OPERATION);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.UPDATE);

    if (updateBlock.getWhereTree() != null) {
      whereClausePatcher.patch(rewrittenInsert, updateBlock.getWhereTree());
    }

    setClausePatcher.patchProjectionForUpdate(rewrittenInsert, setColExprs);

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    return rr;
  }
}
