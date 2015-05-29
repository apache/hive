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

package org.apache.hadoop.hive.ql.io.sarg;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.parquet.FilterPredicateLeafBuilder;
import org.apache.hadoop.hive.ql.io.parquet.LeafFilterFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualNS;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import parquet.filter2.predicate.FilterApi;
import parquet.filter2.predicate.FilterPredicate;

/**
 * The implementation of SearchArguments.
 */
final class SearchArgumentImpl implements SearchArgument {
  public static final Log LOG = LogFactory.getLog(SearchArgumentImpl.class);

  static final class PredicateLeafImpl implements PredicateLeaf {
    private final Operator operator;
    private final Type type;
    private final String columnName;
    private final Object literal;
    private final List<Object> literalList;

    PredicateLeafImpl() {
      operator = null;
      type = null;
      columnName = null;
      literal = null;
      literalList = null;
    }

    PredicateLeafImpl(Operator operator,
                      Type type,
                      String columnName,
                      Object literal,
                      List<Object> literalList) {
      this.operator = operator;
      this.type = type;
      this.columnName = columnName;
      this.literal = literal;
      this.literalList = literalList;
    }

    @Override
    public Operator getOperator() {
      return operator;
    }

    @Override
    public Type getType(){
      return type;
    }

    @Override
    public String getColumnName() {
      return columnName;
    }

    @Override
    public Object getLiteral() {
      // To get around a kryo 2.22 bug while deserialize a Timestamp into Date
      // (https://github.com/EsotericSoftware/kryo/issues/88)
      // When we see a Date, convert back into Timestamp
      if (literal instanceof java.util.Date) {
        return new Timestamp(((java.util.Date)literal).getTime());
      }
      return literal;
    }

    @Override
    public List<Object> getLiteralList() {
      return literalList;
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append('(');
      buffer.append(operator);
      buffer.append(' ');
      buffer.append(columnName);
      if (literal != null) {
        buffer.append(' ');
        buffer.append(literal);
      } else if (literalList != null) {
        for(Object lit: literalList) {
          buffer.append(' ');
          buffer.append(lit.toString());
        }
      }
      buffer.append(')');
      return buffer.toString();
    }

    private static boolean isEqual(Object left, Object right) {
      if (left == right) {
        return true;
      } else if (left == null || right == null) {
        return false;
      } else {
        return left.equals(right);
      }
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || other.getClass() != getClass()) {
        return false;
      } else if (other == this) {
        return true;
      } else {
        PredicateLeafImpl o = (PredicateLeafImpl) other;
        return operator == o.operator &&
            type == o.type &&
            columnName.equals(o.columnName) &&
            isEqual(literal, o.literal) &&
            isEqual(literalList, o.literalList);
      }
    }

    @Override
    public int hashCode() {
      return operator.hashCode() +
             type.hashCode() * 17 +
             columnName.hashCode() * 3 * 17+
             (literal == null ? 0 : literal.hashCode()) * 101 * 3 * 17 +
             (literalList == null ? 0 : literalList.hashCode()) *
                 103 * 101 * 3 * 17;
    }
  }

  static class ExpressionTree {
    static enum Operator {OR, AND, NOT, LEAF, CONSTANT}
    private final Operator operator;
    private final List<ExpressionTree> children;
    private final int leaf;
    private final TruthValue constant;

    ExpressionTree() {
      operator = null;
      children = null;
      leaf = 0;
      constant = null;
    }

    ExpressionTree(Operator op, ExpressionTree... kids) {
      operator = op;
      children = new ArrayList<ExpressionTree>();
      leaf = -1;
      this.constant = null;
      Collections.addAll(children, kids);
    }

    ExpressionTree(int leaf) {
      operator = Operator.LEAF;
      children = null;
      this.leaf = leaf;
      this.constant = null;
    }

    ExpressionTree(TruthValue constant) {
      operator = Operator.CONSTANT;
      children = null;
      this.leaf = -1;
      this.constant = constant;
    }

    ExpressionTree(ExpressionTree other) {
      this.operator = other.operator;
      if (other.children == null) {
        this.children = null;
      } else {
        this.children = new ArrayList<ExpressionTree>();
        for(ExpressionTree child: other.children) {
          children.add(new ExpressionTree(child));
        }
      }
      this.leaf = other.leaf;
      this.constant = other.constant;
    }

    TruthValue evaluate(TruthValue[] leaves) {
      TruthValue result = null;
      switch (operator) {
        case OR:
          for(ExpressionTree child: children) {
            result = child.evaluate(leaves).or(result);
          }
          return result;
        case AND:
          for(ExpressionTree child: children) {
            result = child.evaluate(leaves).and(result);
          }
          return result;
        case NOT:
          return children.get(0).evaluate(leaves).not();
        case LEAF:
          return leaves[leaf];
        case CONSTANT:
          return constant;
        default:
          throw new IllegalStateException("Unknown operator: " + operator);
      }
    }

    FilterPredicate translate(List<PredicateLeaf> leafs){
      FilterPredicate p = null;
      switch (operator) {
        case OR:
          for(ExpressionTree child: children) {
            if (p == null) {
              p = child.translate(leafs);
            } else {
              FilterPredicate right = child.translate(leafs);
              // constant means no filter, ignore it when it is null
              if(right != null){
                p = FilterApi.or(p, right);
              }
            }
          }
          return p;
        case AND:
          for(ExpressionTree child: children) {
            if (p == null) {
              p = child.translate(leafs);
            } else {
              FilterPredicate right = child.translate(leafs);
              // constant means no filter, ignore it when it is null
              if(right != null){
                p = FilterApi.and(p, right);
              }
            }
          }
          return p;
        case NOT:
          FilterPredicate op = children.get(0).translate(leafs);
          if (op != null) {
            return FilterApi.not(op);
          } else {
            return null;
          }
        case LEAF:
          return buildFilterPredicateFromPredicateLeaf(leafs.get(leaf));
        case CONSTANT:
          return null;// no filter will be executed for constant
        default:
          throw new IllegalStateException("Unknown operator: " + operator);
      }
    }

    private FilterPredicate buildFilterPredicateFromPredicateLeaf(PredicateLeaf leaf) {
      LeafFilterFactory leafFilterFactory = new LeafFilterFactory();
      FilterPredicateLeafBuilder builder;
      try {
        builder = leafFilterFactory
          .getLeafFilterBuilderByType(leaf.getType());
        if (builder == null) {
          return null;
        }
        if (isMultiLiteralsOperator(leaf.getOperator())) {
          return builder.buildPredicate(leaf.getOperator(),
              leaf.getLiteralList(),
              leaf.getColumnName());
        } else {
          return builder
            .buildPredict(leaf.getOperator(),
              leaf.getLiteral(),
              leaf.getColumnName());
        }
      } catch (Exception e) {
        LOG.error("fail to build predicate filter leaf with errors" + e, e);
        return null;
      }
    }

    private boolean isMultiLiteralsOperator(PredicateLeaf.Operator op) {
      return (op == PredicateLeaf.Operator.IN) || (op == PredicateLeaf.Operator.BETWEEN);
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      switch (operator) {
        case OR:
          buffer.append("(or");
          for(ExpressionTree child: children) {
            buffer.append(' ');
            buffer.append(child.toString());
          }
          buffer.append(')');
          break;
        case AND:
          buffer.append("(and");
          for(ExpressionTree child: children) {
            buffer.append(' ');
            buffer.append(child.toString());
          }
          buffer.append(')');
          break;
        case NOT:
          buffer.append("(not ");
          buffer.append(children.get(0));
          buffer.append(')');
          break;
        case LEAF:
          buffer.append("leaf-");
          buffer.append(leaf);
          break;
        case CONSTANT:
          buffer.append(constant);
          break;
      }
      return buffer.toString();
    }

    Operator getOperator() {
      return operator;
    }

    List<ExpressionTree> getChildren() {
      return children;
    }
  }

  static class ExpressionBuilder {
    // max threshold for CNF conversion. having >8 elements in andList will be converted to maybe
    private static final int CNF_COMBINATIONS_THRESHOLD = 256;
    private final List<PredicateLeaf> leaves = new ArrayList<PredicateLeaf>();

    /**
     * Get the type of the given expression node.
     * @param expr the expression to get the type of
     * @return int, string, or float or null if we don't know the type
     */
    private static PredicateLeaf.Type getType(ExprNodeDesc expr) {
      TypeInfo type = expr.getTypeInfo();
      if (type.getCategory() == ObjectInspector.Category.PRIMITIVE) {
        switch (((PrimitiveTypeInfo) type).getPrimitiveCategory()) {
          case BYTE:
          case SHORT:
          case INT:
            return PredicateLeaf.Type.INTEGER;
          case LONG:
            return PredicateLeaf.Type.LONG;
          case CHAR:
          case VARCHAR:
          case STRING:
            return PredicateLeaf.Type.STRING;
          case FLOAT:
          case DOUBLE:
            return PredicateLeaf.Type.FLOAT;
          case DATE:
            return PredicateLeaf.Type.DATE;
          case TIMESTAMP:
            return PredicateLeaf.Type.TIMESTAMP;
          case DECIMAL:
            return PredicateLeaf.Type.DECIMAL;
          case BOOLEAN:
        	return PredicateLeaf.Type.BOOLEAN;
          default:
        }
      }
      return null;
    }

    /**
     * Get the column name referenced in the expression. It must be at the top
     * level of this expression and there must be exactly one column.
     * @param expr the expression to look in
     * @param variable the slot the variable is expected in
     * @return the column name or null if there isn't exactly one column
     */
    private static String getColumnName(ExprNodeGenericFuncDesc expr,
                                        int variable) {
      List<ExprNodeDesc> children = expr.getChildren();
      if (variable < 0 || variable >= children.size()) {
        return null;
      }
      ExprNodeDesc child = children.get(variable);
      if (child instanceof ExprNodeColumnDesc) {
        return ((ExprNodeColumnDesc) child).getColumn();
      }
      return null;
    }

    private static Object boxLiteral(ExprNodeConstantDesc lit) {
      switch (getType(lit)) {
        case INTEGER:
          return ((Number) lit.getValue()).intValue();
        case LONG:
          return ((Number) lit.getValue()).longValue();
        case STRING:
          return StringUtils.stripEnd(lit.getValue().toString(), null);
        case FLOAT:
          return Double.parseDouble(lit.getValue().toString());
        case DATE:
        case TIMESTAMP:
        case DECIMAL:
        case BOOLEAN:
          return lit;
        default:
          throw new IllegalArgumentException("Unknown literal " + getType(lit));
      }
    }

    private static Object getLiteral(ExprNodeGenericFuncDesc expr) {
      Object result = null;
      List<ExprNodeDesc> children = expr.getChildren();
      if (children.size() != 2) {
        return null;
      }
      for(ExprNodeDesc child: children) {
        if (child instanceof ExprNodeConstantDesc) {
          if (result != null) {
            return null;
          }
          result = boxLiteral((ExprNodeConstantDesc) child);
        }
      }
      return result;
    }

    private static List<Object> getLiteralList(ExprNodeGenericFuncDesc expr,
                                               int start) {
      List<Object> result = new ArrayList<Object>();
      List<ExprNodeDesc> children = expr.getChildren();
      // ignore the first child, since it is the variable
      for(ExprNodeDesc child: children.subList(start, children.size())) {
        if (child instanceof ExprNodeConstantDesc) {
          result.add(boxLiteral((ExprNodeConstantDesc) child));
        } else {
          // if we get some non-literals, we need to punt
          return null;
        }
      }
      return result;
    }

    private ExpressionTree createLeaf(PredicateLeaf.Operator operator,
                                      ExprNodeGenericFuncDesc expression,
                                      List<PredicateLeaf> leafCache,
                                      int variable) {
      String columnName = getColumnName(expression, variable);
      if (columnName == null) {
        return new ExpressionTree(TruthValue.YES_NO_NULL);
      }
      PredicateLeaf.Type type = getType(expression.getChildren().get(variable));
      if (type == null) {
        return new ExpressionTree(TruthValue.YES_NO_NULL);
      }

      Object literal = null;
      List<Object> literalList = null;
      switch (operator) {
        case IS_NULL:
          break;
        case IN:
        case BETWEEN:
          literalList = getLiteralList(expression, variable + 1);
          if (literalList == null) {
            return new ExpressionTree(TruthValue.YES_NO_NULL);
          }
          break;
        default:
          literal = getLiteral(expression);
          if (literal == null) {
            return new ExpressionTree(TruthValue.YES_NO_NULL);
          }
          break;
      }
      // if the variable was on the right, we need to swap things around
      boolean needSwap = false;
      if (variable != 0) {
        if (operator == PredicateLeaf.Operator.LESS_THAN) {
          needSwap = true;
          operator = PredicateLeaf.Operator.LESS_THAN_EQUALS;
        } else if (operator == PredicateLeaf.Operator.LESS_THAN_EQUALS) {
          needSwap = true;
          operator = PredicateLeaf.Operator.LESS_THAN;
        }
      }
      leafCache.add(new PredicateLeafImpl(operator, type, columnName,
          literal, literalList));
      ExpressionTree result = new ExpressionTree(leafCache.size() - 1);
      if (needSwap) {
        result = negate(result);
      }
      return result;
    }

    /**
     * Find the variable in the expression.
     * @param expr the expression to look in
     * @return the index of the variable or -1 if there is not exactly one
     *   variable.
     */
    private int findVariable(ExprNodeDesc expr) {
      int result = -1;
      List<ExprNodeDesc> children = expr.getChildren();
      for(int i = 0; i < children.size(); ++i) {
        ExprNodeDesc child = children.get(i);
        if (child instanceof ExprNodeColumnDesc) {
          // if we already found a variable, this isn't a sarg
          if (result != -1) {
            return -1;
          } else {
            result = i;
          }
        }
      }
      return result;
    }

    /**
     * Create a leaf expression when we aren't sure where the variable is
     * located.
     * @param operator the operator type that was found
     * @param expression the expression to check
     * @param leafCache the list of leaves
     * @return if the expression is a sarg, return it, otherwise null
     */
    private ExpressionTree createLeaf(PredicateLeaf.Operator operator,
                                      ExprNodeGenericFuncDesc expression,
                                      List<PredicateLeaf> leafCache) {
      return createLeaf(operator, expression, leafCache,
          findVariable(expression));
    }

    private ExpressionTree negate(ExpressionTree expr) {
      ExpressionTree result = new ExpressionTree(ExpressionTree.Operator.NOT);
      result.children.add(expr);
      return result;
    }

    private void addChildren(ExpressionTree result,
                             ExprNodeGenericFuncDesc node,
                             List<PredicateLeaf> leafCache) {
      for(ExprNodeDesc child: node.getChildren()) {
        result.children.add(parse(child, leafCache));
      }
    }

    /**
     * Do the recursive parse of the Hive ExprNodeDesc into our ExpressionTree.
     * @param expression the Hive ExprNodeDesc
     * @return the non-normalized ExpressionTree
     */
    private ExpressionTree parse(ExprNodeDesc expression,
                                 List<PredicateLeaf> leafCache) {
      // if we don't know the expression, just assume maybe
      if (expression.getClass() != ExprNodeGenericFuncDesc.class) {
        return new ExpressionTree(TruthValue.YES_NO_NULL);
      }
      // get the kind of expression
      ExprNodeGenericFuncDesc expr = (ExprNodeGenericFuncDesc) expression;
      Class<?> op = expr.getGenericUDF().getClass();
      ExpressionTree result;

      // handle the logical operators
      if (op == GenericUDFOPOr.class) {
        result = new ExpressionTree(ExpressionTree.Operator.OR);
        addChildren(result, expr, leafCache);
      } else if (op == GenericUDFOPAnd.class) {
        result = new ExpressionTree(ExpressionTree.Operator.AND);
        addChildren(result, expr, leafCache);
      } else if (op == GenericUDFOPNot.class) {
        result = new ExpressionTree(ExpressionTree.Operator.NOT);
        addChildren(result, expr, leafCache);
      } else if (op == GenericUDFOPEqual.class) {
        result = createLeaf(PredicateLeaf.Operator.EQUALS, expr, leafCache);
      } else if (op == GenericUDFOPNotEqual.class) {
        result = negate(createLeaf(PredicateLeaf.Operator.EQUALS, expr,
            leafCache));
      } else if (op == GenericUDFOPEqualNS.class) {
        result = createLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS, expr,
            leafCache);
      } else if (op == GenericUDFOPGreaterThan.class) {
        result = negate(createLeaf(PredicateLeaf.Operator.LESS_THAN_EQUALS,
            expr, leafCache));
      } else if (op == GenericUDFOPEqualOrGreaterThan.class) {
        result = negate(createLeaf(PredicateLeaf.Operator.LESS_THAN, expr,
            leafCache));
      } else if (op == GenericUDFOPLessThan.class) {
        result = createLeaf(PredicateLeaf.Operator.LESS_THAN, expr, leafCache);
      } else if (op == GenericUDFOPEqualOrLessThan.class) {
        result = createLeaf(PredicateLeaf.Operator.LESS_THAN_EQUALS, expr,
            leafCache);
      } else if (op == GenericUDFIn.class) {
        result = createLeaf(PredicateLeaf.Operator.IN, expr, leafCache, 0);
      } else if (op == GenericUDFBetween.class) {
        result = createLeaf(PredicateLeaf.Operator.BETWEEN, expr, leafCache,
            1);
      } else if (op == GenericUDFOPNull.class) {
        result = createLeaf(PredicateLeaf.Operator.IS_NULL, expr, leafCache,
            0);
      } else if (op == GenericUDFOPNotNull.class) {
        result = negate(createLeaf(PredicateLeaf.Operator.IS_NULL, expr,
            leafCache, 0));

      // otherwise, we didn't understand it, so mark it maybe
      } else {
        result = new ExpressionTree(TruthValue.YES_NO_NULL);
      }
      return result;
    }

    /**
     * Push the negations all the way to just before the leaves. Also remove
     * double negatives.
     * @param root the expression to normalize
     * @return the normalized expression, which may share some or all of the
     * nodes of the original expression.
     */
    static ExpressionTree pushDownNot(ExpressionTree root) {
      if (root.operator == ExpressionTree.Operator.NOT) {
        ExpressionTree child = root.children.get(0);
        switch (child.operator) {
          case NOT:
            return pushDownNot(child.children.get(0));
          case CONSTANT:
            return  new ExpressionTree(child.constant.not());
          case AND:
            root = new ExpressionTree(ExpressionTree.Operator.OR);
            for(ExpressionTree kid: child.children) {
              root.children.add(pushDownNot(new
                  ExpressionTree(ExpressionTree.Operator.NOT, kid)));
            }
            break;
          case OR:
            root = new ExpressionTree(ExpressionTree.Operator.AND);
            for(ExpressionTree kid: child.children) {
              root.children.add(pushDownNot(new ExpressionTree
                  (ExpressionTree.Operator.NOT, kid)));
            }
            break;
          // for leaf, we don't do anything
          default:
            break;
        }
      } else if (root.children != null) {
        // iterate through children and push down not for each one
        for(int i=0; i < root.children.size(); ++i) {
          root.children.set(i, pushDownNot(root.children.get(i)));
        }
      }
      return root;
    }

    /**
     * Remove MAYBE values from the expression. If they are in an AND operator,
     * they are dropped. If they are in an OR operator, they kill their parent.
     * This assumes that pushDownNot has already been called.
     * @param expr The expression to clean up
     * @return The cleaned up expression
     */
    static ExpressionTree foldMaybe(ExpressionTree expr) {
      if (expr.children != null) {
        for(int i=0; i < expr.children.size(); ++i) {
          ExpressionTree child = foldMaybe(expr.children.get(i));
          if (child.constant == TruthValue.YES_NO_NULL) {
            switch (expr.operator) {
              case AND:
                expr.children.remove(i);
                i -= 1;
                break;
              case OR:
                // a maybe will kill the or condition
                return child;
              default:
                throw new IllegalStateException("Got a maybe as child of " +
                  expr);
            }
          } else {
            expr.children.set(i, child);
          }
        }
        if (expr.children.isEmpty()) {
          return new ExpressionTree(TruthValue.YES_NO_NULL);
        }
      }
      return expr;
    }

    /**
     * Generate all combinations of items on the andList. For each item on the
     * andList, it generates all combinations of one child from each and
     * expression. Thus, (and a b) (and c d) will be expanded to: (or a c)
     * (or a d) (or b c) (or b d). If there are items on the nonAndList, they
     * are added to each or expression.
     * @param result a list to put the results onto
     * @param andList a list of and expressions
     * @param nonAndList a list of non-and expressions
     */
    private static void generateAllCombinations(List<ExpressionTree> result,
                                                List<ExpressionTree> andList,
                                                List<ExpressionTree> nonAndList
                                               ) {
      List<ExpressionTree> kids = andList.get(0).children;
      if (result.isEmpty()) {
        for(ExpressionTree kid: kids) {
          ExpressionTree or = new ExpressionTree(ExpressionTree.Operator.OR);
          result.add(or);
          for(ExpressionTree node: nonAndList) {
            or.children.add(new ExpressionTree(node));
          }
          or.children.add(kid);
        }
      } else {
        List<ExpressionTree> work = new ArrayList<ExpressionTree>(result);
        result.clear();
        for(ExpressionTree kid: kids) {
          for(ExpressionTree or: work) {
            ExpressionTree copy = new ExpressionTree(or);
            copy.children.add(kid);
            result.add(copy);
          }
        }
      }
      if (andList.size() > 1) {
        generateAllCombinations(result, andList.subList(1, andList.size()),
            nonAndList);
      }
    }

    /**
     * Convert an expression so that the top level operator is AND with OR
     * operators under it. This routine assumes that all of the NOT operators
     * have been pushed to the leaves via pushdDownNot.
     * @param root the expression
     * @return the normalized expression
     */
    static ExpressionTree convertToCNF(ExpressionTree root) {
      if (root.children != null) {
        // convert all of the children to CNF
        int size = root.children.size();
        for(int i=0; i < size; ++i) {
          root.children.set(i, convertToCNF(root.children.get(i)));
        }
        if (root.operator == ExpressionTree.Operator.OR) {
          // a list of leaves that weren't under AND expressions
          List<ExpressionTree> nonAndList = new ArrayList<ExpressionTree>();
          // a list of AND expressions that we need to distribute
          List<ExpressionTree> andList = new ArrayList<ExpressionTree>();
          for(ExpressionTree child: root.children) {
            if (child.operator == ExpressionTree.Operator.AND) {
              andList.add(child);
            } else if (child.operator == ExpressionTree.Operator.OR) {
              // pull apart the kids of the OR expression
              for(ExpressionTree grandkid: child.children) {
                nonAndList.add(grandkid);
              }
            } else {
              nonAndList.add(child);
            }
          }
          if (!andList.isEmpty()) {
            if (checkCombinationsThreshold(andList)) {
              root = new ExpressionTree(ExpressionTree.Operator.AND);
              generateAllCombinations(root.children, andList, nonAndList);
            } else {
              root = new ExpressionTree(TruthValue.YES_NO_NULL);
            }
          }
        }
      }
      return root;
    }

    private static boolean checkCombinationsThreshold(List<ExpressionTree> andList) {
      int numComb = 1;
      for (ExpressionTree tree : andList) {
        numComb *= tree.children.size();
        if (numComb > CNF_COMBINATIONS_THRESHOLD) {
          return false;
        }
      }
      return true;
    }

    /**
     * Converts multi-level ands and ors into single level ones.
     * @param root the expression to flatten
     * @return the flattened expression, which will always be root with
     *   potentially modified children.
     */
    static ExpressionTree flatten(ExpressionTree root) {
      if (root.children != null) {
        // iterate through the index, so that if we add more children,
        // they don't get re-visited
        for(int i=0; i < root.children.size(); ++i) {
          ExpressionTree child = flatten(root.children.get(i));
          // do we need to flatten?
          if (child.operator == root.operator &&
              child.operator != ExpressionTree.Operator.NOT) {
            boolean first = true;
            for(ExpressionTree grandkid: child.children) {
              // for the first grandkid replace the original parent
              if (first) {
                first = false;
                root.children.set(i, grandkid);
              } else {
                root.children.add(++i, grandkid);
              }
            }
          } else {
            root.children.set(i, child);
          }
        }
        // if we have a singleton AND or OR, just return the child
        if ((root.operator == ExpressionTree.Operator.OR ||
             root.operator == ExpressionTree.Operator.AND) &&
            root.children.size() == 1) {
          return root.children.get(0);
        }
      }
      return root;
    }

    /**
     * Iterates through the expression, finding all of the leaves. It creates
     * the leaves list with each unique leaf that is found in the expression.
     * The expression is updated with the new leaf ids for each leaf.
     * @param expr the expression to find the leaves in
     * @param leafCache the list of all of the leaves
     * @param lookup a map that is used to uniquify the leaves
     * @return The potentially modified expression
     */
    private ExpressionTree buildLeafList(ExpressionTree expr,
                                         List<PredicateLeaf> leafCache,
                                         Map<PredicateLeaf,
                                             ExpressionTree> lookup) {
      if (expr.children != null) {
        for(int i=0; i < expr.children.size(); ++i) {
          expr.children.set(i, buildLeafList(expr.children.get(i), leafCache,
              lookup));
        }
      } else if (expr.operator == ExpressionTree.Operator.LEAF) {
        PredicateLeaf leaf = leafCache.get(expr.leaf);
        ExpressionTree val = lookup.get(leaf);
        if (val == null) {
          val = new ExpressionTree(leaves.size());
          lookup.put(leaf, val);
          leaves.add(leaf);
        }
        return val;
      }
      return expr;
    }

    /**
     * Builds the expression and leaf list from the original predicate.
     * @param expression the expression to translate
     * @return The normalized expression.
     */
    ExpressionTree expression(ExprNodeGenericFuncDesc expression) {
      List<PredicateLeaf> leafCache = new ArrayList<PredicateLeaf>();
      ExpressionTree expr = parse(expression, leafCache);
      return expression(expr, leafCache);
    }

    /**
     * Builds the expression and optimized leaf list from a non-normalized
     * expression. Sets the leaves field with the unique leaves.
     * @param expr non-normalized expression
     * @param leaves non-unique leaves
     * @return the normalized expression
     */
    ExpressionTree expression(ExpressionTree expr,
                              List<PredicateLeaf> leaves) {
      expr = pushDownNot(expr);
      expr = foldMaybe(expr);
      expr = flatten(expr);
      expr = convertToCNF(expr);
      expr = flatten(expr);
      expr =  buildLeafList(expr, leaves,
          new HashMap<PredicateLeaf, ExpressionTree>());
      return expr;
    }

    List<PredicateLeaf> getLeaves() {
      return leaves;
    }
  }

  private final List<PredicateLeaf> leaves;
  private final ExpressionTree expression;

  SearchArgumentImpl(ExprNodeGenericFuncDesc expr) {
    if (expr == null) {
      leaves = new ArrayList<PredicateLeaf>();
      expression = null;
    } else {
      ExpressionBuilder builder = new ExpressionBuilder();
      expression = builder.expression(expr);
      leaves = builder.getLeaves();
    }
  }

  SearchArgumentImpl() {
    leaves = null;
    expression = null;
  }

  SearchArgumentImpl(ExpressionTree expression, List<PredicateLeaf> leaves) {
    this.expression = expression;
    this.leaves = leaves;
  }

  @Override
  public List<PredicateLeaf> getLeaves() {
    return leaves;
  }

  @Override
  public TruthValue evaluate(TruthValue[] leaves) {
    return expression == null ? TruthValue.YES : expression.evaluate(leaves);
  }

  ExpressionTree getExpression() {
    return expression;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    for(int i=0; i < leaves.size(); ++i) {
      buffer.append("leaf-");
      buffer.append(i);
      buffer.append(" = ");
      buffer.append(leaves.get(i).toString());
      buffer.append('\n');
    }
    buffer.append("expr = ");
    buffer.append(expression);
    return buffer.toString();
  }

  public String toKryo() {
    Output out = new Output(4 * 1024, 10 * 1024 * 1024);
    new Kryo().writeObject(out, this);
    out.close();
    return Base64.encodeBase64String(out.toBytes());
  }

  static SearchArgument fromKryo(String value) {
    Input input = new Input(Base64.decodeBase64(value));
    return new Kryo().readObject(input, SearchArgumentImpl.class);
  }

  @Override
  public FilterPredicate toFilterPredicate() {
    return expression.translate(leaves);
  }

  private static class BuilderImpl implements Builder {
    private final Deque<ExpressionTree> currentTree =
        new ArrayDeque<ExpressionTree>();
    private final List<PredicateLeaf> leaves = new ArrayList<PredicateLeaf>();
    private ExpressionTree root = null;

    @Override
    public Builder startOr() {
      ExpressionTree node = new ExpressionTree(ExpressionTree.Operator.OR);
      if (currentTree.size() != 0) {
        ExpressionTree parent = currentTree.getFirst();
        parent.children.add(node);
      }
      currentTree.addFirst(node);
      return this;
    }

    @Override
    public Builder startAnd() {
      ExpressionTree node = new ExpressionTree(ExpressionTree.Operator.AND);
      if (currentTree.size() != 0) {
        ExpressionTree parent = currentTree.getFirst();
        parent.children.add(node);
      }
      currentTree.addFirst(node);
      return this;
    }

    @Override
    public Builder startNot() {
      ExpressionTree node = new ExpressionTree(ExpressionTree.Operator.NOT);
      if (currentTree.size() != 0) {
        ExpressionTree parent = currentTree.getFirst();
        parent.children.add(node);
      }
      currentTree.addFirst(node);
      return this;
    }

    @Override
    public Builder end() {
      root = currentTree.removeFirst();
      if (root.children.size() == 0) {
        throw new IllegalArgumentException("Can't create expression " + root +
            " with no children.");
      }
      if (root.operator == ExpressionTree.Operator.NOT &&
          root.children.size() != 1) {
        throw new IllegalArgumentException("Can't create not expression " +
            root + " with more than 1 child.");
      }
      return this;
    }

    private static Object boxLiteral(Object literal) {
      if (literal instanceof String ||
          literal instanceof Long ||
          literal instanceof Double ||
          literal instanceof DateWritable ||
          literal instanceof Timestamp ||
          literal instanceof HiveDecimal ||
          literal instanceof BigDecimal ||
          literal instanceof Boolean) {
        return literal;
      } else if (literal instanceof HiveChar ||
          literal instanceof HiveVarchar) {
        return StringUtils.stripEnd(literal.toString(), null);
      } else if (literal instanceof Byte ||
          literal instanceof Short ||
          literal instanceof Integer) {
        return ((Number) literal).longValue();
      } else if (literal instanceof Float) {
        // to avoid change in precision when upcasting float to double
        // we convert the literal to string and parse it as double. (HIVE-8460)
        return Double.parseDouble(literal.toString());
      } else {
        throw new IllegalArgumentException("Unknown type for literal " +
            literal);
      }
    }

    private static PredicateLeaf.Type getType(Object literal) {
      if (literal instanceof Byte ||
          literal instanceof Short ||
          literal instanceof Integer) {
        return PredicateLeaf.Type.INTEGER;
      } else if(literal instanceof Long){
        return PredicateLeaf.Type.LONG;
      }else if (literal instanceof HiveChar ||
          literal instanceof HiveVarchar ||
          literal instanceof String) {
        return PredicateLeaf.Type.STRING;
      } else if (literal instanceof Float ||
          literal instanceof Double) {
        return PredicateLeaf.Type.FLOAT;
      } else if (literal instanceof DateWritable) {
        return PredicateLeaf.Type.DATE;
      } else if (literal instanceof Timestamp) {
        return PredicateLeaf.Type.TIMESTAMP;
      }else if (literal instanceof HiveDecimal ||
          literal instanceof BigDecimal) {
        return PredicateLeaf.Type.DECIMAL;
      } else if (literal instanceof Boolean) {
        return PredicateLeaf.Type.BOOLEAN;
      }
      throw new IllegalArgumentException("Unknown type for literal " + literal);
    }

    @Override
    public Builder lessThan(String column, Object literal) {
      ExpressionTree parent = currentTree.getFirst();
      Object box = boxLiteral(literal);
      PredicateLeaf leaf =
          new PredicateLeafImpl(PredicateLeaf.Operator.LESS_THAN,
              getType(box), column, box, null);
      leaves.add(leaf);
      parent.children.add(new ExpressionTree(leaves.size() - 1));
      return this;
    }

    @Override
    public Builder lessThanEquals(String column, Object literal) {
      ExpressionTree parent = currentTree.getFirst();
      Object box = boxLiteral(literal);
      PredicateLeaf leaf =
          new PredicateLeafImpl(PredicateLeaf.Operator.LESS_THAN_EQUALS,
              getType(box), column, box, null);
      leaves.add(leaf);
      parent.children.add(new ExpressionTree(leaves.size() - 1));
      return this;
    }

    @Override
    public Builder equals(String column, Object literal) {
      ExpressionTree parent = currentTree.getFirst();
      Object box = boxLiteral(literal);
      PredicateLeaf leaf =
          new PredicateLeafImpl(PredicateLeaf.Operator.EQUALS,
              getType(box), column, box, null);
      leaves.add(leaf);
      parent.children.add(new ExpressionTree(leaves.size() - 1));
      return this;
    }

    @Override
    public Builder nullSafeEquals(String column, Object literal) {
      ExpressionTree parent = currentTree.getFirst();
      Object box = boxLiteral(literal);
      PredicateLeaf leaf =
          new PredicateLeafImpl(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
              getType(box), column, box, null);
      leaves.add(leaf);
      parent.children.add(new ExpressionTree(leaves.size() - 1));
      return this;
    }

    @Override
    public Builder in(String column, Object... literal) {
      ExpressionTree parent = currentTree.getFirst();
      if (literal.length == 0) {
        throw new IllegalArgumentException("Can't create in expression with "
            + "no arguments");
      }
      List<Object> argList = new ArrayList<Object>();
      for(Object lit: literal){
        argList.add(boxLiteral(lit));
      }

      PredicateLeaf leaf =
          new PredicateLeafImpl(PredicateLeaf.Operator.IN,
              getType(argList.get(0)), column, null, argList);
      leaves.add(leaf);
      parent.children.add(new ExpressionTree(leaves.size() - 1));
      return this;
    }

    @Override
    public Builder isNull(String column) {
      ExpressionTree parent = currentTree.getFirst();
      PredicateLeaf leaf =
          new PredicateLeafImpl(PredicateLeaf.Operator.IS_NULL,
              PredicateLeaf.Type.STRING, column, null, null);
      leaves.add(leaf);
      parent.children.add(new ExpressionTree(leaves.size() - 1));
      return this;
    }

    @Override
    public Builder between(String column, Object lower, Object upper) {
      ExpressionTree parent = currentTree.getFirst();
      List<Object> argList = new ArrayList<Object>();
      argList.add(boxLiteral(lower));
      argList.add(boxLiteral(upper));
      PredicateLeaf leaf =
          new PredicateLeafImpl(PredicateLeaf.Operator.BETWEEN,
              getType(argList.get(0)), column, null, argList);
      leaves.add(leaf);
      parent.children.add(new ExpressionTree(leaves.size() - 1));
      return this;
    }

    @Override
    public SearchArgument build() {
      if (currentTree.size() != 0) {
        throw new IllegalArgumentException("Failed to end " +
            currentTree.size() + " operations.");
      }
      ExpressionBuilder internal = new ExpressionBuilder();
      ExpressionTree normalized = internal.expression(root, leaves);
      return new SearchArgumentImpl(normalized, internal.getLeaves());
    }
  }

  public static Builder newBuilder() {
    return new BuilderImpl();
  }
}
