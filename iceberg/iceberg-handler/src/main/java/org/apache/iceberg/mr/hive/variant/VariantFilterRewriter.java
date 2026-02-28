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

package org.apache.iceberg.mr.hive.variant;

import java.util.List;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFVariantGet;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.BoundExtract;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundExtract;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VariantFilterRewriter {
  private static final Logger LOG = LoggerFactory.getLogger(VariantFilterRewriter.class);

  private VariantFilterRewriter() {
  }

  /**
   * Strips {@code extract(...)} predicates and shredded {@code typed_value} column references from an
   * Iceberg expression by replacing them with {@code alwaysTrue()}.
   *
   * <p>Used to make expressions safe for Iceberg's Evaluator, which cannot handle variant predicates.
   * Hive applies the full predicate after reading, so correctness is preserved.
   */
  public static Expression stripVariantExtractPredicates(Expression expr) {
    return strip(expr);
  }

  private static Expression strip(Expression expr) {
    if (expr == null) {
      return null;
    }

    if (expr == Expressions.alwaysTrue() || expr == Expressions.alwaysFalse()) {
      return expr;
    }

    Expression predicateResult = stripPredicate(expr);
    if (predicateResult != null) {
      return predicateResult;
    }

    Expression logicalResult = stripLogical(expr);
    if (logicalResult != null) {
      return logicalResult;
    }

    // Unknown expression type: do not attempt to rewrite.
    return expr;
  }

  private static Expression stripPredicate(Expression expr) {
    if (expr instanceof UnboundPredicate<?> unbound) {
      if (unbound.term() instanceof UnboundExtract) {
        return Expressions.alwaysTrue();
      }
      if (unbound.term() instanceof NamedReference<?> ref &&
          ref.name() != null &&
          ref.name().contains(VariantPathUtil.TYPED_VALUE_SEGMENT)) {
        return Expressions.alwaysTrue();
      }
      return expr;
    }

    if (expr instanceof BoundPredicate<?> bound) {
      if (bound.term() instanceof BoundExtract) {
        return Expressions.alwaysTrue();
      }
      if (bound.term() instanceof BoundReference<?> ref &&
          ref.name() != null &&
          ref.name().contains(VariantPathUtil.TYPED_VALUE_SEGMENT)) {
        return Expressions.alwaysTrue();
      }
      return expr;
    }

    return null;
  }

  private static Expression stripLogical(Expression expr) {
    if (expr instanceof Not not) {
      Expression child = strip(not.child());
      // alwaysTrue() means "unknown" after stripping, NOT(unknown) is still unknown
      if (child == Expressions.alwaysTrue()) {
        return Expressions.alwaysTrue();
      }
      return Expressions.not(child);
    }

    if (expr instanceof And and) {
      return Expressions.and(strip(and.left()), strip(and.right()));
    }

    if (expr instanceof Or or) {
      return Expressions.or(strip(or.left()), strip(or.right()));
    }

    return null;
  }

  public static ExprNodeGenericFuncDesc rewriteForShredding(ExprNodeGenericFuncDesc predicate) {
    if (predicate == null) {
      return null;
    }

    ExprNodeGenericFuncDesc cloned = (ExprNodeGenericFuncDesc) predicate.clone();
    ExprNodeDesc rewrittenRoot = rewriteNode(cloned);
    if (rewrittenRoot instanceof ExprNodeGenericFuncDesc) {
      return (ExprNodeGenericFuncDesc) rewrittenRoot;
    }

    // If rewrites ended up replacing the root, fall back to the original predicate clone to avoid
    // changing the expected root type.
    return cloned;
  }

  private static ExprNodeDesc rewriteNode(ExprNodeDesc node) {
    if (node == null) {
      return null;
    }

    if (node instanceof ExprNodeGenericFuncDesc funcDesc) {
      List<ExprNodeDesc> children = funcDesc.getChildren();
      if (children != null) {
        children.replaceAll(VariantFilterRewriter::rewriteNode);
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
    return funcDesc.getGenericUDF() instanceof GenericUDFVariantGet;
  }

  private static ExprNodeDesc rewriteVariantFunction(ExprNodeGenericFuncDesc funcDesc) {
    List<ExprNodeDesc> args = funcDesc.getChildren();
    if (args == null || args.size() < 2) {
      return null;
    }

    ExprNodeDesc variantColumn = args.get(0);
    ExprNodeDesc jsonPathDesc = args.get(1);

    if (!(variantColumn instanceof ExprNodeColumnDesc variantColumnDesc) ||
        !(jsonPathDesc instanceof ExprNodeConstantDesc)) {
      return null;
    }

    Object literal = ((ExprNodeConstantDesc) jsonPathDesc).getValue();
    if (!(literal instanceof String)) {
      return null;
    }

    List<String> fieldPath = VariantPathUtil.parseSimpleObjectPath((String) literal);
    if (fieldPath == null || fieldPath.isEmpty()) {
      return null;
    }

    String shreddedColumn = VariantPathUtil.shreddedColumnPath(variantColumnDesc.getColumn(), fieldPath);
    LOG.debug("Rewriting variant predicate to use shredded column {}", shreddedColumn);

    return new ExprNodeColumnDesc(
        funcDesc.getTypeInfo(),
        shreddedColumn,
        variantColumnDesc.getTabAlias(),
        variantColumnDesc.getIsPartitionColOrVirtualCol());
  }
}
