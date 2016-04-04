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

import java.util.List;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main purpose for this class is for authorization. More specifically, row
 * filtering and column masking are done through this class. We first call
 * create function to create the corresponding strings for row filtering and
 * column masking. We then replace the TAB_REF with the strings.
 */
public class TableMask {

  protected final Logger LOG = LoggerFactory.getLogger(TableMask.class);
  HiveAuthorizer authorizer;
  private UnparseTranslator translator;
  private boolean enable;
  private boolean needsRewrite;

  public TableMask(SemanticAnalyzer analyzer, HiveConf conf) throws SemanticException {
    try {
      authorizer = SessionState.get().getAuthorizerV2();
      if (authorizer != null && needTransform()) {
        enable = true;
        translator = new UnparseTranslator(conf);
        translator.enable();
      }
    } catch (Exception e) {
      LOG.warn("Failed to initialize masking policy");
      throw new SemanticException(e);
    }
  }

  private String createRowMask(String db, String name) throws SemanticException {
    return authorizer.getRowFilterExpression(db, name);
  }

  private String createExpressions(String db, String tbl, String colName) throws SemanticException {
    return authorizer.getCellValueTransformer(db, tbl, colName);
  }

  public boolean isEnabled() throws SemanticException {
    return enable;
  }

  public boolean needTransform() throws SemanticException {
    return authorizer.needTransform();
  }

  public boolean needTransform(String database, String table) throws SemanticException {
    return authorizer.needTransform(database, table);
  }

  public String create(Table table, String additionalTabInfo, String alias) throws SemanticException {
    String db = table.getDbName();
    String tbl = table.getTableName();
    StringBuilder sb = new StringBuilder();
    sb.append("(SELECT ");
    List<FieldSchema> cols = table.getAllCols();
    boolean firstOne = true;
    for (FieldSchema fs : cols) {
      if (!firstOne) {
        sb.append(", ");
      } else {
        firstOne = false;
      }
      String colName = fs.getName();
      String expr = createExpressions(db, tbl, colName);
      if (expr == null) {
        sb.append(colName);
      } else {
        sb.append(expr + " AS " + colName);
      }
    }
    sb.append(" FROM " + tbl);
    sb.append(" " + additionalTabInfo);
    String filter = createRowMask(db, tbl);
    if (filter != null) {
      sb.append(" WHERE " + filter);
    }
    sb.append(")" + alias);
    LOG.debug("TableMask creates `" + sb.toString() + "`");
    return sb.toString();
  }

  void addTableMasking(ASTNode node, String replacementText) throws SemanticException {
	  translator.addTranslation(node, replacementText);
  }

  void applyTableMasking(TokenRewriteStream tokenRewriteStream) throws SemanticException {
	  translator.applyTranslations(tokenRewriteStream);
  }

  public boolean needsRewrite() {
    return needsRewrite;
  }

  public void setNeedsRewrite(boolean needsRewrite) {
    this.needsRewrite = needsRewrite;
  }

}
