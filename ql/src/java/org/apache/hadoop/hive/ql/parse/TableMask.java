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

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
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
  private HiveAuthzContext queryContext;

  public TableMask(SemanticAnalyzer analyzer, HiveConf conf) throws SemanticException {
    try {
      authorizer = SessionState.get().getAuthorizerV2();
      String cmdString = analyzer.ctx.getCmd();
      SessionState ss = SessionState.get();
      HiveAuthzContext.Builder ctxBuilder = new HiveAuthzContext.Builder();
      ctxBuilder.setCommandString(cmdString);
      ctxBuilder.setUserIpAddress(ss.getUserIpAddress());
      ctxBuilder.setForwardedAddresses(ss.getForwardedAddresses());
      queryContext = ctxBuilder.build();
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

  public List<HivePrivilegeObject> applyRowFilterAndColumnMasking(List<HivePrivilegeObject> privObjs)
      throws SemanticException {
    return authorizer.applyRowFilterAndColumnMasking(queryContext, privObjs);
  }

  public boolean isEnabled() throws SemanticException {
    return enable;
  }

  public boolean needTransform() throws SemanticException {
    return authorizer.needTransform();
  }

  public String create(HivePrivilegeObject privObject, MaskAndFilterInfo maskAndFilterInfo)
      throws SemanticException {
    StringBuilder sb = new StringBuilder();
    sb.append("(SELECT ");
    boolean firstOne = true;
    List<String> exprs = privObject.getCellValueTransformers();
    if (exprs != null) {
      if (exprs.size() != privObject.getColumns().size()) {
        throw new SemanticException("Expect " + privObject.getColumns().size() + " columns in "
            + privObject.getObjectName() + ", but only find " + exprs.size());
      }
      List<String> colTypes = maskAndFilterInfo.colTypes;
      for (int index = 0; index < exprs.size(); index++) {
        String expr = exprs.get(index);
        if (expr == null) {
          throw new SemanticException("Expect string type CellValueTransformer in "
              + privObject.getObjectName() + ", but only find null");
        }
        if (!firstOne) {
          sb.append(", ");
        } else {
          firstOne = false;
        }
        String colName = privObject.getColumns().get(index);
        if (!expr.equals(colName)) {
          // CAST(expr AS COLTYPE) AS COLNAME
          sb.append("CAST(" + expr + " AS " + colTypes.get(index) + ") AS " + colName);
        } else {
          sb.append(expr);
        }
      }
    } else {
      for (int index = 0; index < privObject.getColumns().size(); index++) {
        String expr = privObject.getColumns().get(index);
        if (!firstOne) {
          sb.append(", ");
        } else {
          firstOne = false;
        }
        sb.append(expr);
      }
    }
    sb.append(" FROM " + privObject.getObjectName());
    sb.append(" " + maskAndFilterInfo.additionalTabInfo);
    String filter = privObject.getRowFilterExpression();
    if (filter != null) {
      sb.append(" WHERE " + filter);
    }
    sb.append(")" + maskAndFilterInfo.alias);
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
