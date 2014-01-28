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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.util.ReflectionUtils;

public class ExprNodeDescUtils {

  public static int indexOf(ExprNodeDesc origin, List<ExprNodeDesc> sources) {
    for (int i = 0; i < sources.size(); i++) {
      if (origin.isSame(sources.get(i))) {
        return i;
      }
    }
    return -1;
  }

  // traversing origin, find ExprNodeDesc in sources and replaces it with ExprNodeDesc
  // in targets having same index.
  // return null if failed to find
  public static ExprNodeDesc replace(ExprNodeDesc origin,
      List<ExprNodeDesc> sources, List<ExprNodeDesc> targets) {
    int index = indexOf(origin, sources);
    if (index >= 0) {
      return targets.get(index);
    }
    // encountered column or field which cannot be found in sources
    if (origin instanceof ExprNodeColumnDesc || origin instanceof ExprNodeFieldDesc) {
      return null;
    }
    // for ExprNodeGenericFuncDesc, it should be deterministic and stateless
    if (origin instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) origin;
      if (!FunctionRegistry.isDeterministic(func.getGenericUDF())
          || FunctionRegistry.isStateful(func.getGenericUDF())) {
        return null;
      }
      List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
      for (int i = 0; i < origin.getChildren().size(); i++) {
        ExprNodeDesc child = replace(origin.getChildren().get(i), sources, targets);
        if (child == null) {
          return null;
        }
        children.add(child);
      }
      // duplicate function with possibily replaced children
      ExprNodeGenericFuncDesc clone = (ExprNodeGenericFuncDesc) func.clone();
      clone.setChildren(children);
      return clone;
    }
    // constant or null, just return it
    return origin;
  }

  /**
   * return true if predicate is already included in source
    */
  public static boolean containsPredicate(ExprNodeDesc source, ExprNodeDesc predicate) {
    if (source.isSame(predicate)) {
      return true;
    }
    if (FunctionRegistry.isOpAnd(source)) {
      if (containsPredicate(source.getChildren().get(0), predicate) ||
          containsPredicate(source.getChildren().get(1), predicate)) {
        return true;
      }
    }
    return false;
  }

  /**
   * bind two predicates by AND op
   */
  public static ExprNodeGenericFuncDesc mergePredicates(ExprNodeDesc prev, ExprNodeDesc next) {
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(2);
    children.add(prev);
    children.add(next);
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        FunctionRegistry.getGenericUDFForAnd(), children);
  }

  /**
   * bind n predicates by AND op
   */
  public static ExprNodeDesc mergePredicates(List<ExprNodeDesc> exprs) {
    ExprNodeDesc prev = null;
    for (ExprNodeDesc expr : exprs) {
      if (prev == null) {
        prev = expr;
        continue;
      }
      prev = mergePredicates(prev, expr);
    }
    return prev;
  }

  /**
   * split predicates by AND op
   */
  public static List<ExprNodeDesc> split(ExprNodeDesc current) {
    return split(current, new ArrayList<ExprNodeDesc>());
  }

  /**
   * split predicates by AND op
   */
  public static List<ExprNodeDesc> split(ExprNodeDesc current, List<ExprNodeDesc> splitted) {
    if (FunctionRegistry.isOpAnd(current)) {
      for (ExprNodeDesc child : current.getChildren()) {
        split(child, splitted);
      }
      return splitted;
    }
    if (indexOf(current, splitted) < 0) {
      splitted.add(current);
    }
    return splitted;
  }

  /**
   * Recommend name for the expression
   */
  public static String recommendInputName(ExprNodeDesc desc) {
    if (desc instanceof ExprNodeColumnDesc) {
      return ((ExprNodeColumnDesc)desc).getColumn();
    }
    List<ExprNodeDesc> children = desc.getChildren();
    if (FunctionRegistry.isOpPreserveInputName(desc) && !children.isEmpty() &&
      children.get(0) instanceof ExprNodeColumnDesc) {
      return ((ExprNodeColumnDesc)children.get(0)).getColumn();
    }
    return null;
  }

  /**
   * Return false if the expression has any non determinitic function
   */
  public static boolean isDeterministic(ExprNodeDesc desc) {
    if (desc instanceof ExprNodeGenericFuncDesc) {
      if (!FunctionRegistry.isDeterministic(((ExprNodeGenericFuncDesc)desc).getGenericUDF())) {
        return false;
      }
    }
    if (desc.getChildren() != null) {
      for (ExprNodeDesc child : desc.getChildren()) {
        if (!isDeterministic(child)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Convert expressions in current operator to those in terminal operator, which
   * is an ancestor of current or null (back to top operator).
   */
  public static ArrayList<ExprNodeDesc> backtrack(List<ExprNodeDesc> sources,
      Operator<?> current, Operator<?> terminal) throws SemanticException {
    ArrayList<ExprNodeDesc> result = new ArrayList<ExprNodeDesc>();
    for (ExprNodeDesc expr : sources) {
      result.add(backtrack(expr, current, terminal));
    }
    return result;
  }

  public static ExprNodeDesc backtrack(ExprNodeDesc source, Operator<?> current,
      Operator<?> terminal) throws SemanticException {
    Operator<?> parent = getSingleParent(current, terminal);
    if (parent == null) {
      return source;
    }
    if (source instanceof ExprNodeGenericFuncDesc) {
      // all children expression should be resolved
      ExprNodeGenericFuncDesc function = (ExprNodeGenericFuncDesc) source.clone();
      function.setChildren(backtrack(function.getChildren(), current, terminal));
      return function;
    }
    if (source instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc column = (ExprNodeColumnDesc) source;
      return backtrack(column, parent, terminal);
    }
    if (source instanceof ExprNodeFieldDesc) {
      // field expression should be resolved
      ExprNodeFieldDesc field = (ExprNodeFieldDesc) source.clone();
      field.setDesc(backtrack(field.getDesc(), current, terminal));
      return field;
    }
    // constant or null expr, just return
    return source;
  }

  // Resolve column expression to input expression by using expression mapping in current operator
  private static ExprNodeDesc backtrack(ExprNodeColumnDesc column, Operator<?> current,
      Operator<?> terminal) throws SemanticException {
    Map<String, ExprNodeDesc> mapping = current.getColumnExprMap();
    if (mapping == null || !mapping.containsKey(column.getColumn())) {
      return backtrack((ExprNodeDesc)column, current, terminal);
    }
    ExprNodeDesc mapped = mapping.get(column.getColumn());
    return backtrack(mapped, current, terminal);
  }

  public static Operator<?> getSingleParent(Operator<?> current, Operator<?> terminal)
      throws SemanticException {
    if (current == terminal) {
      return null;
    }
    List<Operator<?>> parents = current.getParentOperators();
    if (parents == null || parents.isEmpty()) {
      if (terminal != null) {
        throw new SemanticException("Failed to meet terminal operator");
      }
      return null;
    }
    if (parents.size() == 1) {
      return parents.get(0);
    }
    if (terminal != null && parents.contains(terminal)) {
      return terminal;
    }
    throw new SemanticException("Met multiple parent operators");
  }

  public static ExprNodeDesc[] extractComparePair(ExprNodeDesc expr1, ExprNodeDesc expr2) {
    expr1 = extractConstant(expr1);
    expr2 = extractConstant(expr2);
    if (expr1 instanceof ExprNodeColumnDesc && expr2 instanceof ExprNodeConstantDesc) {
      return new ExprNodeDesc[] {expr1, expr2};
    }
    if (expr1 instanceof ExprNodeConstantDesc && expr2 instanceof ExprNodeColumnDesc) {
      return new ExprNodeDesc[] {expr2, expr1, null}; // add null as a marker (inverted order)
    }
    // todo: constant op constant
    return null;
  }

  // from IndexPredicateAnalyzer
  private static ExprNodeDesc extractConstant(ExprNodeDesc expr) {
    if (!(expr instanceof ExprNodeGenericFuncDesc)) {
      return expr;
    }
    ExprNodeConstantDesc folded = foldConstant(((ExprNodeGenericFuncDesc) expr));
    return folded == null ? expr : folded;
  }

  private static ExprNodeConstantDesc foldConstant(ExprNodeGenericFuncDesc func) {
    GenericUDF udf = func.getGenericUDF();
    if (!FunctionRegistry.isDeterministic(udf) || FunctionRegistry.isStateful(udf)) {
      return null;
    }
    try {
      // If the UDF depends on any external resources, we can't fold because the
      // resources may not be available at compile time.
      if (udf instanceof GenericUDFBridge) {
        UDF internal = ReflectionUtils.newInstance(((GenericUDFBridge) udf).getUdfClass(), null);
        if (internal.getRequiredFiles() != null || internal.getRequiredJars() != null) {
          return null;
        }
      } else {
        if (udf.getRequiredFiles() != null || udf.getRequiredJars() != null) {
          return null;
        }
      }

      if (func.getChildren() != null) {
        for (ExprNodeDesc child : func.getChildren()) {
          if (child instanceof ExprNodeConstantDesc) {
            continue;
          }
          if (child instanceof ExprNodeGenericFuncDesc) {
            if (foldConstant((ExprNodeGenericFuncDesc) child) != null) {
              continue;
            }
          }
          return null;
        }
      }
      ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(func);
      ObjectInspector output = evaluator.initialize(null);

      Object constant = evaluator.evaluate(null);
      Object java = ObjectInspectorUtils.copyToStandardJavaObject(constant, output);

      return new ExprNodeConstantDesc(java);
    } catch (Exception e) {
      return null;
    }
  }
}
