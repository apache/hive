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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagateProcFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.Lists;

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
      if (!FunctionRegistry.isConsistentWithinQuery(func.getGenericUDF())) {
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
      // duplicate function with possibly replaced children
      ExprNodeGenericFuncDesc clone = (ExprNodeGenericFuncDesc) func.clone();
      clone.setChildren(children);
      return clone;
    }
    // constant or null, just return it
    return origin;
  }

  private static boolean isDefaultPartition(ExprNodeDesc origin, String defaultPartitionName) {
    if (origin instanceof ExprNodeConstantDesc && ((ExprNodeConstantDesc)origin).getValue() != null &&
        ((ExprNodeConstantDesc)origin).getValue().equals(defaultPartitionName)) {
      return true;
    } else {
      return false;
    }
  }

  public static void replaceEqualDefaultPartition(ExprNodeDesc origin,
      String defaultPartitionName) throws SemanticException {
    ExprNodeColumnDesc column = null;
    ExprNodeConstantDesc defaultPartition = null;
    if (origin instanceof ExprNodeGenericFuncDesc
        && (((ExprNodeGenericFuncDesc) origin)
            .getGenericUDF() instanceof GenericUDFOPEqual
            || ((ExprNodeGenericFuncDesc) origin)
                .getGenericUDF() instanceof GenericUDFOPNotEqual)) {
      if (isDefaultPartition(origin.getChildren().get(0),
          defaultPartitionName)) {
        defaultPartition = (ExprNodeConstantDesc) origin.getChildren().get(0);
        column = (ExprNodeColumnDesc) origin.getChildren().get(1);
      } else if (isDefaultPartition(origin.getChildren().get(1),
          defaultPartitionName)) {
        column = (ExprNodeColumnDesc) origin.getChildren().get(0);
        defaultPartition = (ExprNodeConstantDesc) origin.getChildren().get(1);
      }
    }
    // Found
    if (column != null) {
      origin.getChildren().remove(defaultPartition);
      String fnName;
      if (((ExprNodeGenericFuncDesc) origin)
          .getGenericUDF() instanceof GenericUDFOPEqual) {
        fnName = "isnull";
      } else {
        fnName = "isnotnull";
      }
      ((ExprNodeGenericFuncDesc) origin).setGenericUDF(
          FunctionRegistry.getFunctionInfo(fnName).getGenericUDF());
    } else {
      if (origin.getChildren() != null) {
        for (ExprNodeDesc child : origin.getChildren()) {
          replaceEqualDefaultPartition(child, defaultPartitionName);
        }
      }
    }
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
    final List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(2);
    if (FunctionRegistry.isOpAnd(prev)) {
      children.addAll(prev.getChildren());
    } else {
      children.add(prev);
    }
    if (FunctionRegistry.isOpAnd(next)) {
      children.addAll(next.getChildren());
    } else {
      children.add(next);
    }
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
   * Return false if the expression has any non deterministic function
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

  public static ArrayList<ExprNodeDesc> clone(List<ExprNodeDesc> sources) {
    ArrayList<ExprNodeDesc> result = new ArrayList<ExprNodeDesc>();
    for (ExprNodeDesc expr : sources) {
      result.add(expr.clone());
    }
    return result;
  }

  /**
   * Convert expressions in current operator to those in terminal operator, which
   * is an ancestor of current or null (back to top operator).
   * Possibly contain null values for non-traceable exprs
   */
  public static ArrayList<ExprNodeDesc> backtrack(List<ExprNodeDesc> sources,
      Operator<?> current, Operator<?> terminal) throws SemanticException {
    return backtrack(sources, current, terminal, false);
  }

  public static ArrayList<ExprNodeDesc> backtrack(List<ExprNodeDesc> sources,
      Operator<?> current, Operator<?> terminal, boolean foldExpr) throws SemanticException {
    ArrayList<ExprNodeDesc> result = new ArrayList<ExprNodeDesc>();
    for (ExprNodeDesc expr : sources) {
      result.add(backtrack(expr, current, terminal, foldExpr));
    }
    return result;
  }

  public static ExprNodeDesc backtrack(ExprNodeDesc source, Operator<?> current,
      Operator<?> terminal) throws SemanticException {
    return backtrack(source, current, terminal, false);
  }

  public static ExprNodeDesc backtrack(ExprNodeDesc source, Operator<?> current,
      Operator<?> terminal, boolean foldExpr) throws SemanticException {
    Operator<?> parent = getSingleParent(current, terminal);
    if (parent == null) {
      return source;
    }
    if (!foldExpr && isConstant(source)) {
      //constant, just return
      return source;
    }
    if (source instanceof ExprNodeGenericFuncDesc) {
      // all children expression should be resolved
      ExprNodeGenericFuncDesc function = (ExprNodeGenericFuncDesc) source.clone();
      List<ExprNodeDesc> children = backtrack(function.getChildren(), current, terminal, foldExpr);
      for (ExprNodeDesc child : children) {
        if (child == null) {
          // Could not resolve all of the function children, fail
          return null;
        }
      }
      function.setChildren(children);
      if (foldExpr) {
        // fold after replacing, if possible
        ExprNodeDesc foldedFunction = ConstantPropagateProcFactory.foldExpr(function);
        if (foldedFunction != null) {
          return foldedFunction;
        }
      }
      return function;
    }
    if (source instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc column = (ExprNodeColumnDesc) source;
      return backtrack(column, parent, terminal);
    }
    if (source instanceof ExprNodeFieldDesc) {
      // field expression should be resolved
      ExprNodeFieldDesc field = (ExprNodeFieldDesc) source.clone();
      ExprNodeDesc fieldDesc = backtrack(field.getDesc(), current, terminal, foldExpr);
      if (fieldDesc == null) {
        return null;
      }
      field.setDesc(fieldDesc);
      return field;
    }
    // just return
    return source;
  }

  // Resolve column expression to input expression by using expression mapping in current operator
  private static ExprNodeDesc backtrack(ExprNodeColumnDesc column, Operator<?> current,
      Operator<?> terminal) throws SemanticException {
    Map<String, ExprNodeDesc> mapping = current.getColumnExprMap();
    if (mapping == null) {
      return backtrack((ExprNodeDesc)column, current, terminal);
    }
    ExprNodeDesc mapped = mapping.get(column.getColumn());
    return mapped == null ? null : backtrack(mapped, current, terminal);
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

  public static List<ExprNodeDesc> resolveJoinKeysAsRSColumns(List<ExprNodeDesc> sourceList,
      Operator<?> reduceSinkOp) {
    ArrayList<ExprNodeDesc> result = new ArrayList<ExprNodeDesc>(sourceList.size());
    for (ExprNodeDesc source : sourceList) {
      ExprNodeDesc newExpr = resolveJoinKeysAsRSColumns(source, reduceSinkOp);
      if (newExpr == null) {
        return null;
      }
      result.add(newExpr);
    }
    return result;
  }

  /**
   * Join keys are expressions based on the select operator. Resolve the expressions so they
   * are based on the ReduceSink operator
   *   SEL -> RS -> JOIN
   * @param source
   * @param reduceSinkOp
   * @return
   */
  public static ExprNodeDesc resolveJoinKeysAsRSColumns(ExprNodeDesc source, Operator<?> reduceSinkOp) {
    // Assuming this is only being done for join keys. As a result we shouldn't have to recursively
    // check any nested child expressions, because the result of the expression should exist as an
    // output column of the ReduceSink operator
    if (source == null) {
      return null;
    }

    // columnExprMap has the reverse of what we need - a mapping of the internal column names
    // to the ExprNodeDesc from the previous operation.
    // Find the key/value where the ExprNodeDesc value matches the column we are searching for.
    // The key portion of the entry will be the internal column name for the join key expression.
    for (Map.Entry<String, ExprNodeDesc> mapEntry : reduceSinkOp.getColumnExprMap().entrySet()) {
      if (mapEntry.getValue().isSame(source)) {
        String columnInternalName = mapEntry.getKey();
        if (source instanceof ExprNodeColumnDesc) {
          // The join key is a table column. Create the ExprNodeDesc based on this column.
          ColumnInfo columnInfo = reduceSinkOp.getSchema().getColumnInfo(columnInternalName);
          return new ExprNodeColumnDesc(columnInfo);
        } else {
          // Join key expression is likely some expression involving functions/operators, so there
          // is no actual table column for this. But the ReduceSink operator should still have an
          // output column corresponding to this expression, using the columnInternalName.
          // TODO: does tableAlias matter for this kind of expression?
          return new ExprNodeColumnDesc(source.getTypeInfo(), columnInternalName, "", false);
        }
      }
    }

    return null;  // Couldn't find reference to expression
  }

  public static ExprNodeDesc[] extractComparePair(ExprNodeDesc expr1, ExprNodeDesc expr2) {
    expr1 = extractConstant(expr1);
    expr2 = extractConstant(expr2);
    if (expr1 instanceof ExprNodeColumnDesc && expr2 instanceof ExprNodeConstantDesc) {
      return new ExprNodeDesc[] {expr1, expr2};
    }
    if (expr1 instanceof ExprNodeConstantDesc && expr2 instanceof ExprNodeColumnDesc) {
      return new ExprNodeDesc[] {expr1, expr2};
    }
    // handles cases where the query has a predicate "column-name=constant"
    if (expr1 instanceof ExprNodeFieldDesc && expr2 instanceof ExprNodeConstantDesc) {
      ExprNodeColumnDesc columnDesc = extractColumn(expr1);
      return columnDesc != null ? new ExprNodeDesc[] {columnDesc, expr2, expr1} : null;
    }
    // handles cases where the query has a predicate "constant=column-name"
    if (expr1 instanceof ExprNodeConstantDesc && expr2 instanceof ExprNodeFieldDesc) {
      ExprNodeColumnDesc columnDesc = extractColumn(expr2);
      return columnDesc != null ? new ExprNodeDesc[] {expr1, columnDesc, expr2} : null;
    }
    // todo: constant op constant
    return null;
  }

  /**
   * Extract fields from the given {@link ExprNodeFieldDesc node descriptor}
   * */
  public static String[] extractFields(ExprNodeFieldDesc expr) {
    return extractFields(expr, new ArrayList<String>()).toArray(new String[0]);
  }

  /*
   * Recursively extract fields from ExprNodeDesc. Deeply nested structs can have multiple levels of
   * fields in them
   */
  private static List<String> extractFields(ExprNodeDesc expr, List<String> fields) {
    if (expr instanceof ExprNodeFieldDesc) {
      ExprNodeFieldDesc field = (ExprNodeFieldDesc)expr;
      fields.add(field.getFieldName());
      return extractFields(field.getDesc(), fields);
    }
    if (expr instanceof ExprNodeColumnDesc) {
      return fields;
    }
    throw new IllegalStateException(
        "Unexpected exception while extracting fields from ExprNodeDesc");
  }

  /*
   * Extract column from the given ExprNodeDesc
   */
  private static ExprNodeColumnDesc extractColumn(ExprNodeDesc expr) {
    if (expr instanceof ExprNodeColumnDesc) {
      return (ExprNodeColumnDesc)expr;
    }
    if (expr instanceof ExprNodeFieldDesc) {
      return extractColumn(((ExprNodeFieldDesc)expr).getDesc());
    }
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
    if (!FunctionRegistry.isConsistentWithinQuery(udf)) {
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

	public static void getExprNodeColumnDesc(List<ExprNodeDesc> exprDescList,
			Map<Integer, ExprNodeDesc> hashCodeTocolumnDescMap) {
		for (ExprNodeDesc exprNodeDesc : exprDescList) {
			getExprNodeColumnDesc(exprNodeDesc, hashCodeTocolumnDescMap);
		}
	}

	/**
	 * Get Map of ExprNodeColumnDesc HashCode to ExprNodeColumnDesc.
	 * 
	 * @param exprDesc
	 * @param hashCodeToColumnDescMap
	 *            Assumption: If two ExprNodeColumnDesc have same hash code then
	 *            they are logically referring to same projection
	 */
	public static void getExprNodeColumnDesc(ExprNodeDesc exprDesc,
			Map<Integer, ExprNodeDesc> hashCodeToColumnDescMap) {
		if (exprDesc instanceof ExprNodeColumnDesc) {
			hashCodeToColumnDescMap.put(exprDesc.hashCode(), exprDesc);
		} else if (exprDesc instanceof ExprNodeColumnListDesc) {
			for (ExprNodeDesc child : exprDesc.getChildren()) {
				getExprNodeColumnDesc(child, hashCodeToColumnDescMap);
			}
		} else if (exprDesc instanceof ExprNodeGenericFuncDesc) {
			for (ExprNodeDesc child : exprDesc.getChildren()) {
				getExprNodeColumnDesc(child, hashCodeToColumnDescMap);
			}
		} else if (exprDesc instanceof ExprNodeFieldDesc) {
			getExprNodeColumnDesc(((ExprNodeFieldDesc) exprDesc).getDesc(),
					hashCodeToColumnDescMap);
		} else if( exprDesc instanceof  ExprNodeSubQueryDesc) {
		    getExprNodeColumnDesc(((ExprNodeSubQueryDesc) exprDesc).getSubQueryLhs(),
                    hashCodeToColumnDescMap);
        }

	}

  public static boolean isConstant(ExprNodeDesc value) {
    if (value instanceof ExprNodeConstantDesc) {
      return true;
    }
    if (value instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) value;
      if (!FunctionRegistry.isConsistentWithinQuery(func.getGenericUDF())) {
        return false;
      }
      for (ExprNodeDesc child : func.getChildren()) {
        if (!isConstant(child)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  public static boolean isAllConstants(List<ExprNodeDesc> value) {
    for (ExprNodeDesc expr : value) {
      if (!(expr instanceof ExprNodeConstantDesc)) {
        return false;
      }
    }
    return true;
  }

  public static boolean isNullConstant(ExprNodeDesc value) {
    if ((value instanceof ExprNodeConstantDesc)
        && ((ExprNodeConstantDesc) value).getValue() == null) {
      return true;
    }
    return false;
  }

  public static PrimitiveTypeInfo deriveMinArgumentCast(
      ExprNodeDesc childExpr, TypeInfo targetType) {
    assert targetType instanceof PrimitiveTypeInfo : "Not a primitive type" + targetType;
    PrimitiveTypeInfo pti = (PrimitiveTypeInfo)targetType;
    // We only do the minimum cast for decimals. Other types are assumed safe; fix if needed.
    // We also don't do anything for non-primitive children (maybe we should assert).
    if ((pti.getPrimitiveCategory() != PrimitiveCategory.DECIMAL)
        || (!(childExpr.getTypeInfo() instanceof PrimitiveTypeInfo))) return pti;
    PrimitiveTypeInfo childTi = (PrimitiveTypeInfo)childExpr.getTypeInfo();
    // If the child is also decimal, no cast is needed (we hope - can target type be narrower?).
    return HiveDecimalUtils.getDecimalTypeForPrimitiveCategory(childTi);
  }

  /**
   * Build ExprNodeColumnDesc for the projections in the input operator from
   * sartpos to endpos(both included). Operator must have an associated
   * colExprMap.
   * 
   * @param inputOp
   *          Input Hive Operator
   * @param startPos
   *          starting position in the input operator schema; must be >=0 and <=
   *          endPos
   * @param endPos
   *          end position in the input operator schema; must be >=0.
   * @return List of ExprNodeDesc
   */
  public static ArrayList<ExprNodeDesc> genExprNodeDesc(Operator inputOp, int startPos, int endPos,
      boolean addEmptyTabAlias, boolean setColToNonVirtual) {
    ArrayList<ExprNodeDesc> exprColLst = new ArrayList<ExprNodeDesc>();
    List<ColumnInfo> colInfoLst = inputOp.getSchema().getSignature();

    String tabAlias;
    boolean vc;
    ColumnInfo ci;
    for (int i = startPos; i <= endPos; i++) {
      ci = colInfoLst.get(i);
      tabAlias = ci.getTabAlias();
      if (addEmptyTabAlias) {
        tabAlias = "";
      }
      vc = ci.getIsVirtualCol();
      if (setColToNonVirtual) {
        vc = false;
      }
      exprColLst.add(new ExprNodeColumnDesc(ci.getType(), ci.getInternalName(), tabAlias, vc));
    }

    return exprColLst;
  }  

  public static List<ExprNodeDesc> flattenExprList(List<ExprNodeDesc> sourceList) {
    ArrayList<ExprNodeDesc> result = new ArrayList<ExprNodeDesc>(sourceList.size());
    for (ExprNodeDesc source : sourceList) {
      result.add(flattenExpr(source));
    }
    return result;
  }

  /**
   * A normal reduce operator's rowObjectInspector looks like a struct containing
   *  nested key/value structs that contain the column values:
   *  { key: { reducesinkkey0:int }, value: { _col0:int, _col1:int, .. } }
   *
   * While the rowObjectInspector looks the same for vectorized queries during
   * compilation time, within the tasks at query execution the rowObjectInspector
   * has changed to a flatter structure without nested key/value structs:
   *  { 'key.reducesinkkey0':int, 'value._col0':int, 'value._col1':int, .. }
   *
   * Trying to fetch 'key.reducesinkkey0' by name from the list of flattened
   * ObjectInspectors does not work because the '.' gets interpreted as a field member,
   * even though it is a flattened list of column values.
   * This workaround converts the column name referenced in the ExprNodeDesc
   * from a nested field name (key.reducesinkkey0) to key_reducesinkkey0,
   * simply by replacing '.' with '_'.
   * @param source
   * @return
   */
  public static ExprNodeDesc flattenExpr(ExprNodeDesc source) {
    if (source instanceof ExprNodeGenericFuncDesc) {
      // all children expression should be resolved
      ExprNodeGenericFuncDesc function = (ExprNodeGenericFuncDesc) source.clone();
      List<ExprNodeDesc> newChildren = flattenExprList(function.getChildren());
      for (ExprNodeDesc newChild : newChildren) {
        if (newChild == null) {
          // Could not resolve all of the function children, fail
          return null;
        }
      }
      function.setChildren(newChildren);
      return function;
    }
    if (source instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc column = (ExprNodeColumnDesc) source;
      // Create a new ColumnInfo, replacing STRUCT.COLUMN with STRUCT_COLUMN
      String newColumn = column.getColumn().replace('.', '_');
      return new ExprNodeColumnDesc(source.getTypeInfo(), newColumn, column.getTabAlias(), false);
    }
    if (source instanceof ExprNodeFieldDesc) {
      // field expression should be resolved
      ExprNodeFieldDesc field = (ExprNodeFieldDesc) source.clone();
      ExprNodeDesc fieldDesc = flattenExpr(field.getDesc());
      if (fieldDesc == null) {
        return null;
      }
      field.setDesc(fieldDesc);
      return field;
    }
    // constant or null expr, just return
    return source;
  }

  public static String extractColName(ExprNodeDesc root) {
    if (root instanceof ExprNodeColumnDesc) {
      return ((ExprNodeColumnDesc) root).getColumn();
    } else {
      if (root.getChildren() == null) {
        return null;
      }

      String column = null;
      for (ExprNodeDesc d: root.getChildren()) {
        String candidate = extractColName(d);
        if (column != null && candidate != null) {
          return null;
        } else if (candidate != null) {
          column = candidate;
        }
      }
      return column;
    }
  }

  public static ExprNodeColumnDesc getColumnExpr(ExprNodeDesc expr) {
    while (FunctionRegistry.isOpCast(expr)) {
      expr = expr.getChildren().get(0);
    }
    return (expr instanceof ExprNodeColumnDesc) ? (ExprNodeColumnDesc)expr : null;
  }

  // Find the constant origin of a certain column if it is originated from a constant
  // Otherwise, it returns the expression that originated the column
  public static ExprNodeDesc findConstantExprOrigin(String dpCol, Operator<? extends OperatorDesc> op) {
    ExprNodeDesc expr = op.getColumnExprMap().get(dpCol);
    ExprNodeDesc foldedExpr;
    // If it is a function, we try to fold it
    if (expr instanceof ExprNodeGenericFuncDesc) {
      foldedExpr = ConstantPropagateProcFactory.foldExpr((ExprNodeGenericFuncDesc)expr);
      if (foldedExpr == null) {
        foldedExpr = expr;
      }
    } else {
      foldedExpr = expr;
    }
    // If it is a column reference, we will try to resolve it
    if (foldedExpr instanceof ExprNodeColumnDesc) {
      Operator<? extends OperatorDesc> originOp = null;
      for(Operator<? extends OperatorDesc> parentOp : op.getParentOperators()) {
        if (parentOp.getColumnExprMap() != null) {
          originOp = parentOp;
          break;
        }
      }
      if (originOp != null) {
        return findConstantExprOrigin(((ExprNodeColumnDesc)foldedExpr).getColumn(), originOp);
      }
    }
    // Otherwise, we return the expression
    return foldedExpr;
  }

  /**
   * Checks whether the keys of a parent operator are a prefix of the keys of a
   * child operator.
   * @param childKeys keys of the child operator
   * @param parentKeys keys of the parent operator
   * @param childOp child operator
   * @param parentOp parent operator
   * @return true if the keys are a prefix, false otherwise
   * @throws SemanticException
   */
  public static boolean checkPrefixKeys(List<ExprNodeDesc> childKeys, List<ExprNodeDesc> parentKeys,
      Operator<? extends OperatorDesc> childOp, Operator<? extends OperatorDesc> parentOp)
          throws SemanticException {
    return checkPrefixKeys(childKeys, parentKeys, childOp, parentOp, false);
  }

  /**
   * Checks whether the keys of a child operator are a prefix of the keys of a
   * parent operator.
   * @param childKeys keys of the child operator
   * @param parentKeys keys of the parent operator
   * @param childOp child operator
   * @param parentOp parent operator
   * @return true if the keys are a prefix, false otherwise
   * @throws SemanticException
   */
  public static boolean checkPrefixKeysUpstream(List<ExprNodeDesc> childKeys, List<ExprNodeDesc> parentKeys,
      Operator<? extends OperatorDesc> childOp, Operator<? extends OperatorDesc> parentOp)
          throws SemanticException {
    return checkPrefixKeys(childKeys, parentKeys, childOp, parentOp, true);
  }

  private static boolean checkPrefixKeys(List<ExprNodeDesc> childKeys, List<ExprNodeDesc> parentKeys,
      Operator<? extends OperatorDesc> childOp, Operator<? extends OperatorDesc> parentOp,
      boolean upstream) throws SemanticException {
    if (childKeys == null || childKeys.isEmpty()) {
      if (parentKeys != null && !parentKeys.isEmpty()) {
        return false;
      }
      return true;
    }
    if (parentKeys == null || parentKeys.isEmpty()) {
      return false;
    }
    int size;
    if (upstream) {
      if (childKeys.size() > parentKeys.size()) {
        return false;
      }
      size = childKeys.size();
    } else {
      if (parentKeys.size() > childKeys.size()) {
        return false;
      }
      size = parentKeys.size();
    }
    for (int i = 0; i < size; i++) {
      ExprNodeDesc expr = ExprNodeDescUtils.backtrack(childKeys.get(i), childOp, parentOp);
      if (expr == null) {
        // cKey is not present in parent
        return false;
      }
      if (!expr.isSame(parentKeys.get(i))) {
        return false;
      }
    }
    return true;
  }

  public static class ColumnOrigin {
    public ExprNodeColumnDesc col;
    public Operator<?> op;

    public ColumnOrigin(ExprNodeColumnDesc col, Operator<?> op) {
      super();
      this.col = col;
      this.op = op;
    }
  }

  private static ExprNodeDesc findParentExpr(ExprNodeColumnDesc col, Operator<?> op) {
    ExprNodeDesc parentExpr = col;
    Map<String, ExprNodeDesc> mapping = op.getColumnExprMap();
    if (mapping != null) {
      parentExpr = mapping.get(col.getColumn());
      if (parentExpr == null && op instanceof ReduceSinkOperator) {
        return col;
      }
    }
    return parentExpr;
  }

  public static ColumnOrigin findColumnOrigin(ExprNodeDesc expr, Operator<?> op) {
    if (expr == null || op == null) {
      // bad input
      return null;
    }

    ExprNodeColumnDesc col = ExprNodeDescUtils.getColumnExpr(expr);
    if (col == null) {
      // not a column
      return null;
    }

    Operator<?> parentOp = null;
    int numParents = op.getNumParent();
    if (numParents == 0) {
      return new ColumnOrigin(col, op);
    }

    ExprNodeDesc parentExpr = findParentExpr(col, op);
    if (parentExpr == null) {
      // couldn't find proper parent column expr
      return null;
    }

    if (numParents == 1) {
      parentOp = op.getParentOperators().get(0);
    } else {
      // Multiple parents - find the right one based on the table alias in the parentExpr
      ExprNodeColumnDesc parentCol = ExprNodeDescUtils.getColumnExpr(parentExpr);
      if (parentCol != null) {
        for (Operator<?> currParent : op.getParentOperators()) {
          RowSchema schema = currParent.getSchema();
          if (schema == null) {
            // Happens in case of TezDummyStoreOperator
            return null;
          }
          if (schema.getTableNames().contains(parentCol.getTabAlias())) {
            parentOp = currParent;
            break;
          }
        }
      }
    }

    if (parentOp == null) {
      return null;
    }

    return findColumnOrigin(parentExpr, parentOp);
  }

  // Null-safe isSame
  public static boolean isSame(ExprNodeDesc desc1, ExprNodeDesc desc2) {
    return (desc1 == desc2) || (desc1 != null && desc1.isSame(desc2));
  }

  // Null-safe isSame for lists of ExprNodeDesc
  public static boolean isSame(List<ExprNodeDesc> first, List<ExprNodeDesc> second) {
    if (first == second) {
      return true;
    }
    if (first == null || second == null || first.size() != second.size()) {
      return false;
    }
    for (int i = 0; i < first.size(); i++) {
      if (!first.get(i).isSame(second.get(i))) {
        return false;
      }
    }
    return true;
  }
}
