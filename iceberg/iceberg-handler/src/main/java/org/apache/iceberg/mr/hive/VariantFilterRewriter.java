/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.util.List;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTryVariantGet;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFVariantGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class VariantFilterRewriter {
  private static final Logger LOG = LoggerFactory.getLogger(VariantFilterRewriter.class);

  private VariantFilterRewriter() {
  }

  static RewriteResult rewriteForShredding(ExprNodeGenericFuncDesc predicate) {
    if (predicate == null) {
      return RewriteResult.none();
    }

    ExprNodeGenericFuncDesc cloned = (ExprNodeGenericFuncDesc) predicate.clone();
    ExprNodeDesc rewrittenRoot = rewriteNode(cloned);
    if (rewrittenRoot instanceof ExprNodeGenericFuncDesc) {
      return RewriteResult.of((ExprNodeGenericFuncDesc) rewrittenRoot);
    }

    // If rewrites ended up replacing the root, fall back to the original predicate clone to avoid
    // changing the expected root type.
    return RewriteResult.of(cloned);
  }

  private static ExprNodeDesc rewriteNode(ExprNodeDesc node) {
    if (node == null) {
      return null;
    }

    if (node instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc) node;
      List<ExprNodeDesc> children = funcDesc.getChildren();
      if (children != null) {
        for (int i = 0; i < children.size(); i++) {
          ExprNodeDesc rewrittenChild = rewriteNode(children.get(i));
          children.set(i, rewrittenChild);
        }
      }

      if (isVariantGet(funcDesc)) {
        ExprNodeDesc replacement = rewriteVariantFunction(funcDesc);
        if (replacement != null) {
          return replacement;
        }
      }
    }

    return node;
  }

  private static boolean isVariantGet(ExprNodeGenericFuncDesc funcDesc) {
    GenericUDF udf = funcDesc.getGenericUDF();
    return udf instanceof GenericUDFVariantGet || udf instanceof GenericUDFTryVariantGet;
  }

  private static ExprNodeDesc rewriteVariantFunction(ExprNodeGenericFuncDesc funcDesc) {
    List<ExprNodeDesc> args = funcDesc.getChildren();
    if (args == null || args.size() < 2) {
      return null;
    }

    ExprNodeDesc variantColumn = args.get(0);
    ExprNodeDesc jsonPathDesc = args.get(1);

    if (!(variantColumn instanceof ExprNodeColumnDesc) ||
        !(jsonPathDesc instanceof ExprNodeConstantDesc)) {
      return null;
    }

    Object literal = ((ExprNodeConstantDesc) jsonPathDesc).getValue();
    if (!(literal instanceof String)) {
      return null;
    }

    String fieldName = extractTopLevelField((String) literal);
    if (fieldName == null) {
      return null;
    }

    ExprNodeColumnDesc variantColumnDesc = (ExprNodeColumnDesc) variantColumn;
    String shreddedColumn = variantColumnDesc.getColumn() + ".typed_value." + fieldName;
    LOG.debug("Rewriting variant predicate to use shredded column {}", shreddedColumn);
    return new ExprNodeColumnDesc(
        funcDesc.getTypeInfo(),
        shreddedColumn,
        variantColumnDesc.getTabAlias(),
        variantColumnDesc.getIsPartitionColOrVirtualCol());
  }

  private static String extractTopLevelField(String jsonPath) {
    if (jsonPath == null) {
      return null;
    }

    String trimmed = jsonPath.trim();
    if (!trimmed.startsWith("$.") || trimmed.length() <= 2) {
      return null;
    }

    String remaining = trimmed.substring(2);
    // Only allow top-level field names (no nested objects or arrays)
    if (remaining.contains(".") || remaining.contains("[") || remaining.contains("]")) {
      return null;
    }

    return remaining;
  }

  private static boolean containsShreddedReference(ExprNodeDesc node) {
    if (node == null) {
      return false;
    }

    if (node instanceof ExprNodeColumnDesc) {
      String column = ((ExprNodeColumnDesc) node).getColumn();
      return column != null && column.contains(".typed_value.");
    }

    List<ExprNodeDesc> children = node.getChildren();
    if (children != null) {
      for (ExprNodeDesc child : children) {
        if (containsShreddedReference(child)) {
          return true;
        }
      }
    }
    return false;
  }

  static final class RewriteResult {
    private final ExprNodeGenericFuncDesc expression;
    private final boolean rewritten;

    private RewriteResult(ExprNodeGenericFuncDesc expression, boolean rewritten) {
      this.expression = expression;
      this.rewritten = rewritten;
    }

    private static final RewriteResult NONE = new RewriteResult(null, false);

    static RewriteResult none() {
      return NONE;
    }

    static RewriteResult of(ExprNodeGenericFuncDesc expression) {
      if (expression == null) {
        return none();
      }
      return new RewriteResult(expression, containsShreddedReference(expression));
    }

    ExprNodeGenericFuncDesc expression() {
      return expression;
    }

    boolean hasVariantRewrite() {
      return rewritten;
    }
  }
}
