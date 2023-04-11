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
package org.apache.hadoop.hive.ql.parse.relnodegen;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.ASTErrorUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.QBSubQueryParseInfo;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.UnparseTranslator;
import org.apache.hadoop.hive.ql.parse.type.RexNodeTypeCheck;
import org.apache.hadoop.hive.ql.parse.type.TypeCheckCtx;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRexExprList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RexNodeGenerator {
  private final UnparseTranslator unparseTranslator;
  private final HiveConf conf;

  public RexNodeGenerator(UnparseTranslator unparseTranslator, HiveConf conf) {
    this.unparseTranslator = unparseTranslator;
    this.conf = conf;
  }

  public RexNode genRexNode(ASTNode expr, RowResolver input,
      RowResolver outerRR, Map<ASTNode, QBSubQueryParseInfo> subqueryToRelNode,
      boolean useCaching, RexBuilder rexBuilder) throws SemanticException {
    TypeCheckCtx tcCtx = new TypeCheckCtx(input, rexBuilder, useCaching, false);
    tcCtx.setOuterRR(outerRR);
    tcCtx.setSubqueryToRelNode(subqueryToRelNode);
    return genRexNode(expr, input, tcCtx);
  }

  /**
   * Generates a Calcite {@link RexNode} for the expression with TypeCheckCtx.
   */
  public RexNode genRexNode(ASTNode expr, RowResolver input, RexBuilder rexBuilder)
      throws SemanticException {
    // Since the user didn't supply a customized type-checking context,
    // use default settings.
    return genRexNode(expr, input, true, false, rexBuilder);
  }

  public RexNode genRexNode(ASTNode expr, RowResolver input, boolean useCaching,
      boolean foldExpr, RexBuilder rexBuilder) throws SemanticException {
    TypeCheckCtx tcCtx = new TypeCheckCtx(input, rexBuilder, useCaching, foldExpr);
    return genRexNode(expr, input, tcCtx);
  }

  /**
   * Generates a Calcite {@link RexNode} for the expression and children of it
   * with default TypeCheckCtx.
   */
  public Map<ASTNode, RexNode> genAllRexNode(ASTNode expr, RowResolver input, RexBuilder rexBuilder)
      throws SemanticException {
    TypeCheckCtx tcCtx = new TypeCheckCtx(input, rexBuilder);
    return genAllRexNode(expr, input, tcCtx);
  }

  /**
   * Returns a Calcite {@link RexNode} for the expression.
   * If it is evaluated already in previous operator, it can be retrieved from cache.
   */
  public RexNode genRexNode(ASTNode expr, RowResolver input,
      TypeCheckCtx tcCtx) throws SemanticException {
    RexNode cached = null;
    if (tcCtx.isUseCaching()) {
      cached = getRexNodeCached(expr, input, tcCtx);
    }
    if (cached == null) {
      Map<ASTNode, RexNode> allExprs = genAllRexNode(expr, input, tcCtx);
      return allExprs.get(expr);
    }
    return cached;
  }

  /**
   * Find RexNode for the expression cached in the RowResolver. Returns null if not exists.
   */
  private RexNode getRexNodeCached(ASTNode node, RowResolver input,
      TypeCheckCtx tcCtx) throws SemanticException {
    ColumnInfo colInfo = input.getExpression(node);
    if (colInfo != null) {
      ASTNode source = input.getExpressionSource(node);
      if (source != null) {
        unparseTranslator.addCopyTranslation(node, source);
      }
      return RexNodeTypeCheck.toExprNode(colInfo, input, 0, tcCtx.getRexBuilder());
    }
    return null;
  }

  /**
   * Generates all of the Calcite {@link RexNode}s for the expression and children of it
   * passed in the arguments. This function uses the row resolver and the metadata information
   * that are passed as arguments to resolve the column names to internal names.
   *
   * @param expr
   *          The expression
   * @param input
   *          The row resolver
   * @param tcCtx
   *          Customized type-checking context
   * @return expression to exprNodeDesc mapping
   * @throws SemanticException Failed to evaluate expression
   */
  public Map<ASTNode, RexNode> genAllRexNode(ASTNode expr, RowResolver input,
      TypeCheckCtx tcCtx) throws SemanticException {
    // Create the walker and  the rules dispatcher.
    tcCtx.setUnparseTranslator(unparseTranslator);

    Map<ASTNode, RexNode> nodeOutputs =
        RexNodeTypeCheck.genExprNode(expr, tcCtx);
    RexNode desc = nodeOutputs.get(expr);
    if (desc == null) {
      String tableOrCol = BaseSemanticAnalyzer.unescapeIdentifier(expr
          .getChild(0).getText());
      ColumnInfo colInfo = input.get(null, tableOrCol);
      String errMsg;
      if (colInfo == null && input.getIsExprResolver()){
        errMsg = ASTErrorUtils.getMsg(
            ErrorMsg.NON_KEY_EXPR_IN_GROUPBY.getMsg(), expr);
      } else {
        errMsg = tcCtx.getError();
      }
      throw new SemanticException(Optional.ofNullable(errMsg).orElse("Error in parsing "));
    }
    if (desc instanceof HiveRexExprList) {
      throw new SemanticException("TOK_ALLCOLREF is not supported in current context");
    }

    if (!unparseTranslator.isEnabled()) {
      // Not creating a view, so no need to track view expansions.
      return nodeOutputs;
    }

    List<ASTNode> fieldDescList = new ArrayList<>();

    for (Map.Entry<ASTNode, RexNode> entry : nodeOutputs.entrySet()) {
      if (!(entry.getValue() instanceof RexInputRef)) {
        // we need to translate the RexFieldAccess too, e.g., identifiers in
        // struct<>.
        if (entry.getValue() instanceof RexFieldAccess) {
          fieldDescList.add(entry.getKey());
        }
        continue;
      }
      ASTNode node = entry.getKey();
      RexInputRef columnDesc = (RexInputRef) entry.getValue();
      int index = columnDesc.getIndex();
      String[] tmp;
      if (index < input.getColumnInfos().size()) {
        ColumnInfo columnInfo = input.getColumnInfos().get(index);
        if (columnInfo.getTabAlias() == null
            || columnInfo.getTabAlias().length() == 0) {
          // These aren't real column refs; instead, they are special
          // internal expressions used in the representation of aggregation.
          continue;
        }
        tmp = input.reverseLookup(columnInfo.getInternalName());
      } else {
        // in subquery case, tmp may be from outside.
        ColumnInfo columnInfo = tcCtx.getOuterRR().getColumnInfos().get(
            index - input.getColumnInfos().size());
        if (columnInfo.getTabAlias() == null
            || columnInfo.getTabAlias().length() == 0) {
          continue;
        }
        tmp = tcCtx.getOuterRR().reverseLookup(columnInfo.getInternalName());
      }
      StringBuilder replacementText = new StringBuilder();
      replacementText.append(HiveUtils.unparseIdentifier(tmp[0], conf));
      replacementText.append(".");
      replacementText.append(HiveUtils.unparseIdentifier(tmp[1], conf));
      unparseTranslator.addTranslation(node, replacementText.toString());
    }

    for (ASTNode node : fieldDescList) {
      Map<ASTNode, String> map = SemanticAnalyzer.translateFieldDesc(node, conf);
      for (Map.Entry<ASTNode, String> entry : map.entrySet()) {
        unparseTranslator.addTranslation(entry.getKey(), entry.getValue().toLowerCase());
      }
    }

    return nodeOutputs;
  }
}
