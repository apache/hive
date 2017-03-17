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
package org.apache.hadoop.hive.ql.optimizer.listbucketingpruner;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;


/**
 * Utility for list bucketing prune.
 *
 */
public final class ListBucketingPrunerUtils {

  /* Default list bucketing directory name. internal use only not for client. */
  public static final String HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME =
      "HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME";
  /* Default list bucketing directory key. internal use only not for client. */
  public static final String HIVE_LIST_BUCKETING_DEFAULT_KEY = "HIVE_DEFAULT_LIST_BUCKETING_KEY";

  /**
   * Decide if pruner skips the skewed directory
   * Input: if the skewed value matches the expression tree
   * Ouput: if pruner should skip the directory represented by the skewed value
   * If match result is unknown(null) or true, pruner doesn't skip the directory
   * If match result is false, pruner skips the dir.
   * @param bool
   *          if the skewed value matches the expression tree
   * @return
   */
  public static boolean skipSkewedDirectory(Boolean bool) {
    if (bool == null) {
      return false;
    }
    return !bool.booleanValue();
  }

  /**
   * or 2 Boolean operands in the context of pruning match
   *
   * Operand one|Operand another | or result
   * unknown | T | T
   * unknown | F | unknown
   * unknown | unknown | unknown
   * T | T | T
   * T | F | T
   * T | unknown | unknown
   * F | T | T
   * F | F | F
   * F | unknown | unknown
   */
  public static Boolean orBoolOperand(Boolean o, Boolean a) {
    // pick up unknown case
    if (o == null) {
      if ((a == null) || !a) {
        return null;
      } else {
        return a;
      }
    } else if (a == null) {
      return null;
    }

    return (o || a);
  }

  /**
   * And 2 Boolean operands in the context of pruning match
   *
   * Operand one|Operand another | And result
   * unknown | T | unknown
   * unknown | F | F
   * unknown | unknown | unknown
   * T | T | T
   * T | F | F
   * T | unknown | unknown
   * F | T | F
   * F | F | F
   * F | unknown | F
   * @param o
   *          one operand
   * @param a
   *          another operand
   * @return result
   */
  public static Boolean andBoolOperand(Boolean o, Boolean a) {
    // pick up unknown case and let and operator handle the rest
    if (o == null) {
      if ((a == null) || a) {
        return null;
      } else {
        return a;
      }
    } else if (a == null) {
      return o ? null : Boolean.FALSE;
    }

    return (o && a);
  }

  /**
   * Not a Boolean operand in the context of pruning match
   *
   * Operand | Not
   * T | F
   * F | T
   * unknown | unknown
   * @param input
   *          match result
   * @return
   */
  public static Boolean notBoolOperand(Boolean input) {
    if (input == null) {
      return null;
    }
    return input ? Boolean.FALSE : Boolean.TRUE;
  }

  /**
   * 1. Walk through the tree to decide value
   * 1.1 true means the element matches the expression tree
   * 1.2 false means the element doesn't match the expression tree
   * 1.3 unknown means not sure if the element matches the expression tree
   *
   * Example:
   * skewed column: C1, C2
   * cell: (1,a) , (1,b) , (1,c) , (1,other), (2,a), (2,b) , (2,c), (2,other), (other,a), (other,b),
   * (other,c), (other,other)
   *
   * * Expression Tree : ((c1=1) and (c2=a)) or ( (c1=3) or (c2=b))
   *
   * or
   * / \
   * and or
   * / \ / \
   * c1=1 c2=a c1=3 c2=b
   * @throws SemanticException
   *
   */
  static Boolean evaluateExprOnCell(List<String> skewedCols, List<String> cell,
      ExprNodeDesc pruner, List<List<String>> uniqSkewedValues) throws SemanticException {
    return recursiveExpr(pruner, skewedCols, cell, uniqSkewedValues);
  }

  /**
   * Walk through expression tree recursively to evaluate.
   *
   *
   * @param node
   * @param skewedCols
   * @param cell
   * @return
   * @throws SemanticException
   */
  private static Boolean recursiveExpr(final ExprNodeDesc node, final List<String> skewedCols,
      final List<String> cell, final List<List<String>> uniqSkewedValues)
      throws SemanticException {
    if (isUnknownState(node)) {
      return null;
    }

    if (node instanceof ExprNodeGenericFuncDesc) {
      if (((ExprNodeGenericFuncDesc) node).getGenericUDF() instanceof GenericUDFOPEqual) {
        return evaluateEqualNd(node, skewedCols, cell, uniqSkewedValues);
      } else if (FunctionRegistry.isOpAnd(node)) {
        return evaluateAndNode(node, skewedCols, cell, uniqSkewedValues);
      } else if (FunctionRegistry.isOpOr(node)) {
        return evaluateOrNode(node, skewedCols, cell, uniqSkewedValues);
      } else if (FunctionRegistry.isOpNot(node)) {
        return evaluateNotNode(node, skewedCols, cell, uniqSkewedValues);
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  /**
   * Evaluate equal node.
   *
   *
   * @param node
   * @param skewedCols
   * @param cell
   * @param uniqSkewedValues
   * @return
   * @throws SemanticException
   */
  private static Boolean evaluateEqualNd(final ExprNodeDesc node, final List<String> skewedCols,
      final List<String> cell, final List<List<String>> uniqSkewedValues) throws SemanticException {
    Boolean result = null;
    List<ExprNodeDesc> children = ((ExprNodeGenericFuncDesc) node).getChildren();
    assert ((children != null) && (children.size() == 2)) : "GenericUDFOPEqual should have 2 " +
        "ExprNodeDesc. Node name : " + node.getName();
    ExprNodeDesc left = children.get(0);
    ExprNodeDesc right = children.get(1);
    assert (left instanceof ExprNodeColumnDesc && right instanceof ExprNodeConstantDesc) :
      "GenericUDFOPEqual should have 2 children: "
        + " the first is ExprNodeColumnDesc and the second is ExprNodeConstantDesc. "
        + "But this one, the first one is " + left.getName() + " and the second is "
        + right.getName();
    result = startComparisonInEqualNode(skewedCols, cell, uniqSkewedValues, result, left, right);
    return result;
  }

  /**
   * Comparison in equal node
   *
   * @param skewedCols
   * @param cell
   * @param uniqSkewedValues
   * @param result
   * @param left
   * @param right
   * @return
   * @throws SemanticException
   */
  private static Boolean startComparisonInEqualNode(final List<String> skewedCols,
      final List<String> cell, final List<List<String>> uniqSkewedValues, Boolean result,
      ExprNodeDesc left, ExprNodeDesc right) throws SemanticException {
    String columnNameInFilter = ((ExprNodeColumnDesc) left).getColumn();
    String constantValueInFilter = ((ExprNodeConstantDesc) right).getValue().toString();
    assert (skewedCols.contains(columnNameInFilter)) : "List bucketing pruner has a column name "
        + columnNameInFilter
        + " which is not found in the partition's skewed column list";
    int index = skewedCols.indexOf(columnNameInFilter);
    assert (index < cell.size()) : "GenericUDFOPEqual has a ExprNodeColumnDesc ("
        + columnNameInFilter + ") which is " + index + "th" + "skewed column. "
        + " But it can't find the matching part in cell." + " Because the cell size is "
        + cell.size();
    String cellValueInPosition = cell.get(index);
    assert (index < uniqSkewedValues.size()) : "GenericUDFOPEqual has a ExprNodeColumnDesc ("
        + columnNameInFilter + ") which is " + index + "th" + "skewed column. "
        + " But it can't find the matching part in uniq skewed value list."
        + " Because the cell size is "
        + uniqSkewedValues.size();
    List<String> uniqSkewedValuesInPosition = uniqSkewedValues.get(index);
    result = coreComparisonInEqualNode(constantValueInFilter, cellValueInPosition,
        uniqSkewedValuesInPosition);
    return result;
  }

  /**
   * Compare
   * @param constantValueInFilter
   * @param cellValueInPosition
   * @param uniqSkewedValuesInPosition
   * @return
   */
  private static Boolean coreComparisonInEqualNode(String constantValueInFilter,
      String cellValueInPosition, List<String> uniqSkewedValuesInPosition) {
   Boolean result;
    // Compare cell value with constant value in filter
    // 1 if they match and cell value isn't other, return true
    // 2 if they don't match but cell is other and value in filter is not skewed value,
    //   return unknown. why not true? true is not enough. since not true is false,
    //   but not unknown is unknown.
    //   For example, skewed column C, skewed value 1, 2. clause: where not ( c =3)
    //   cell is other, evaluate (not(c=3)).
    //   other to (c=3), if ture. not(c=3) will be false. but it is wrong skip default dir
    //   but, if unknown. not(c=3) will be unknown. we will choose default dir.
    // 3 all others, return false
    if (cellValueInPosition.equals(constantValueInFilter)
        && !cellValueInPosition.equals(ListBucketingPrunerUtils.HIVE_LIST_BUCKETING_DEFAULT_KEY)) {
      result = Boolean.TRUE;
    } else if (cellValueInPosition.equals(ListBucketingPrunerUtils.HIVE_LIST_BUCKETING_DEFAULT_KEY)
        && !uniqSkewedValuesInPosition.contains(constantValueInFilter)) {
      result = null;
    } else {
      result = Boolean.FALSE;
    }
    return result;
  }

  private static Boolean evaluateNotNode(final ExprNodeDesc node, final List<String> skewedCols,
      final List<String> cell, final List<List<String>> uniqSkewedValues) throws SemanticException {
    List<ExprNodeDesc> children = ((ExprNodeGenericFuncDesc) node).getChildren();
    if ((children == null) || (children.size() != 1)) {
      throw new SemanticException("GenericUDFOPNot should have 1 ExprNodeDesc. Node name : "
          + node.getName());
    }
    ExprNodeDesc child = children.get(0);
    return notBoolOperand(recursiveExpr(child, skewedCols, cell, uniqSkewedValues));
  }

  private static Boolean evaluateOrNode(final ExprNodeDesc node, final List<String> skewedCols,
      final List<String> cell, final List<List<String>> uniqSkewedValues) throws SemanticException {
    List<ExprNodeDesc> children = ((ExprNodeGenericFuncDesc) node).getChildren();
    if ((children == null) || (children.size() != 2)) {
      throw new SemanticException("GenericUDFOPOr should have 2 ExprNodeDesc. Node name : "
          + node.getName());
    }
    ExprNodeDesc left = children.get(0);
    ExprNodeDesc right = children.get(1);
    return orBoolOperand(recursiveExpr(left, skewedCols, cell, uniqSkewedValues),
        recursiveExpr(right, skewedCols, cell, uniqSkewedValues));
  }

  private static Boolean evaluateAndNode(final ExprNodeDesc node, final List<String> skewedCols,
      final List<String> cell, final List<List<String>> uniqSkewedValues) throws SemanticException {
    List<ExprNodeDesc> children = ((ExprNodeGenericFuncDesc) node).getChildren();
    if ((children == null) || (children.size() != 2)) {
      throw new SemanticException("GenericUDFOPAnd should have 2 ExprNodeDesc. Node name : "
          + node.getName());
    }
    ExprNodeDesc left = children.get(0);
    ExprNodeDesc right = children.get(1);
    return andBoolOperand(recursiveExpr(left, skewedCols, cell, uniqSkewedValues),
        recursiveExpr(right, skewedCols, cell, uniqSkewedValues));
  }

  /**
   * Check if the node is unknown
   *
   *
   * unknown is marked in {@link #transform(ParseContext)} <blockquote>
   *
   * <pre>
   * newcd = new ExprNodeConstantDesc(cd.getTypeInfo(), null)
   * </pre>
   *
   * like
   *
   * 1. non-skewed column
   *
   * 2. non and/or/not ...
   *
   *
   * @param descNd
   * @return
   */
  static boolean isUnknownState(ExprNodeDesc descNd) {
    boolean unknown = false;
    if ((descNd == null)
        || (descNd instanceof ExprNodeConstantDesc
        && ((ExprNodeConstantDesc) descNd).getValue() == null)) {
      unknown = true;
    }
    return unknown;
  }

  /**
   * check if the partition is list bucketing
   *
   * @param part
   * @return
   */
  public static boolean isListBucketingPart(Partition part) {
    return (part.getSkewedColNames() != null) && (part.getSkewedColNames().size() > 0)
        && (part.getSkewedColValues() != null) && (part.getSkewedColValues().size() > 0)
        && (part.getSkewedColValueLocationMaps() != null)
        && (part.getSkewedColValueLocationMaps().size() > 0);
  }

}
