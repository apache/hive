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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.sarg.LiteralDelegate;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicValueDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
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
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

public class ConvertAstToSearchArg {
  private static final Logger LOG = LoggerFactory.getLogger(ConvertAstToSearchArg.class);
  private final SearchArgument.Builder builder;
  private final Configuration conf;

  /**
   * Builds the expression and leaf list from the original predicate.
   * @param expression the expression to translate.
   */
  ConvertAstToSearchArg(Configuration conf, ExprNodeGenericFuncDesc expression) {
    this.conf = conf;
    builder = SearchArgumentFactory.newBuilder(conf);
    parse(expression);
  }

  /**
   * Build the search argument from the expression.
   * @return the search argument
   */
  public SearchArgument buildSearchArgument() {
    return builder.build();
  }

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

  private static Object boxLiteral(ExprNodeConstantDesc constantDesc,
                                   PredicateLeaf.Type type) {
    Object lit = constantDesc.getValue();
    if (lit == null) {
      return null;
    }
    switch (type) {
      case LONG:
        if (lit instanceof HiveDecimal) {
          HiveDecimal dec = (HiveDecimal) lit;
          if (!dec.isLong()) {
            throw new ArithmeticException("Overflow");
          }
          return dec.longValue();
        }
        return ((Number) lit).longValue();
      case STRING:
        if (lit instanceof HiveChar) {
          return ((HiveChar) lit).getPaddedValue();
        } else if (lit instanceof String) {
          return lit;
        } else {
          return lit.toString();
        }
      case FLOAT:
        if (lit instanceof HiveDecimal) {
          // HiveDecimal -> Float -> Number -> Double
          return ((Number)((HiveDecimal) lit).floatValue()).doubleValue();
        } else {
          return ((Number) lit).doubleValue();
        }
      case TIMESTAMP:
        return Timestamp.valueOf(lit.toString());
      case DATE:
        return Date.valueOf(lit.toString());
      case DECIMAL:
        return new HiveDecimalWritable(lit.toString());
      case BOOLEAN:
        return lit;
      default:
        throw new IllegalArgumentException("Unknown literal " + type);
    }
  }

  /**
   * Find the child that is the literal.
   * @param expr the parent node to check
   * @param type the type of the expression
   * @return the literal boxed if found or null
   */
  private static Object findLiteral(Configuration conf, ExprNodeGenericFuncDesc expr,
                                    PredicateLeaf.Type type) {
    List<ExprNodeDesc> children = expr.getChildren();
    if (children.size() != 2) {
      return null;
    }
    Object result = null;
    for(ExprNodeDesc child: children) {
      Object currentResult = getLiteral(conf, child, type);
      if (currentResult != null) {
        // Both children in the expression should not be literal
        if (result != null) {
          return null;
        }
        result = currentResult;
      }
    }
    return result;
  }

  private static Object getLiteral(Configuration conf, ExprNodeDesc child, PredicateLeaf.Type type) {
    if (child instanceof ExprNodeConstantDesc) {
      return boxLiteral((ExprNodeConstantDesc) child, type);
    } else if (child instanceof ExprNodeDynamicValueDesc) {
      LiteralDelegate value = ((ExprNodeDynamicValueDesc) child).getDynamicValue();
      value.setConf(conf);
      return value;
    }
    return null;
  }

  /**
   * Return the boxed literal at the given position
   * @param expr the parent node
   * @param type the type of the expression
   * @param position the child position to check
   * @return the boxed literal if found otherwise null
   */
  private static Object getLiteral(Configuration conf, ExprNodeGenericFuncDesc expr,
                                   PredicateLeaf.Type type,
                                   int position) {
    List<ExprNodeDesc> children = expr.getChildren();
    ExprNodeDesc child = children.get(position);
    return getLiteral(conf, child, type);
  }

  private static Object[] getLiteralList(ExprNodeGenericFuncDesc expr,
                                         PredicateLeaf.Type type,
                                         int start) {
    List<ExprNodeDesc> children = expr.getChildren();
    Object[] result = new Object[children.size() - start];

    // ignore the first child, since it is the variable
    int posn = 0;
    for(ExprNodeDesc child: children.subList(start, children.size())) {
      if (child instanceof ExprNodeConstantDesc) {
        result[posn++] = boxLiteral((ExprNodeConstantDesc) child, type);
      } else {
        // if we get some non-literals, we need to punt
        return null;
      }
    }
    return result;
  }

  private void createLeaf(PredicateLeaf.Operator operator,
                          ExprNodeGenericFuncDesc expression,
                          int variable) {
    String columnName = getColumnName(expression, variable);
    if (columnName == null) {
      builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
      return;
    }
    PredicateLeaf.Type type = getType(expression.getChildren().get(variable));
    if (type == null) {
      builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
      return;
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
    if (needSwap) {
      builder.startNot();
    }

    try {
      switch (operator) {
        case IS_NULL:
          builder.isNull(columnName, type);
          break;
        case EQUALS:
          builder.equals(columnName, type, findLiteral(conf, expression, type));
          break;
        case NULL_SAFE_EQUALS:
          builder.nullSafeEquals(columnName, type, findLiteral(conf, expression, type));
          break;
        case LESS_THAN:
          builder.lessThan(columnName, type, findLiteral(conf, expression, type));
          break;
        case LESS_THAN_EQUALS:
          builder.lessThanEquals(columnName, type, findLiteral(conf, expression, type));
          break;
        case IN:
          builder.in(columnName, type,
              getLiteralList(expression, type, variable + 1));
          break;
        case BETWEEN:
          builder.between(columnName, type,
              getLiteral(conf, expression, type, variable + 1),
              getLiteral(conf, expression, type, variable + 2));
          break;
      }
    } catch (Exception e) {
      LOG.warn("Exception thrown during SARG creation. Returning YES_NO_NULL." +
          " Exception: " + e.getMessage());
      builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
    }

    if (needSwap) {
      builder.end();
    }
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
   */
  private void createLeaf(PredicateLeaf.Operator operator,
                          ExprNodeGenericFuncDesc expression) {
    createLeaf(operator, expression, findVariable(expression));
  }

  private void addChildren(ExprNodeGenericFuncDesc node) {
    for(ExprNodeDesc child: node.getChildren()) {
      parse(child);
    }
  }

  /**
   * Do the recursive parse of the Hive ExprNodeDesc into our ExpressionTree.
   * @param expression the Hive ExprNodeDesc
   */
  private void parse(ExprNodeDesc expression) {
    // Most of the stuff we can handle are generic function descriptions, so
    // handle the special cases.
    if (expression.getClass() != ExprNodeGenericFuncDesc.class) {

      // if it is a reference to a boolean column, covert it to a truth test.
      if (expression instanceof ExprNodeColumnDesc) {
        ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) expression;
        if (columnDesc.getTypeString().equals("boolean")) {
          builder.equals(columnDesc.getColumn(), PredicateLeaf.Type.BOOLEAN,
              true);
          return;
        }
      }

      // otherwise, we don't know what to do so make it a maybe
      builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
      return;
    }

    // get the kind of expression
    ExprNodeGenericFuncDesc expr = (ExprNodeGenericFuncDesc) expression;
    Class<?> op = expr.getGenericUDF().getClass();

    // handle the logical operators
    if (op == GenericUDFOPOr.class) {
      builder.startOr();
      addChildren(expr);
      builder.end();
    } else if (op == GenericUDFOPAnd.class) {
      builder.startAnd();
      addChildren(expr);
      builder.end();
    } else if (op == GenericUDFOPNot.class) {
      builder.startNot();
      addChildren(expr);
      builder.end();
    } else if (op == GenericUDFOPEqual.class) {
      createLeaf(PredicateLeaf.Operator.EQUALS, expr);
    } else if (op == GenericUDFOPNotEqual.class) {
      builder.startNot();
      createLeaf(PredicateLeaf.Operator.EQUALS, expr);
      builder.end();
    } else if (op == GenericUDFOPEqualNS.class) {
      createLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS, expr);
    } else if (op == GenericUDFOPGreaterThan.class) {
      builder.startNot();
      createLeaf(PredicateLeaf.Operator.LESS_THAN_EQUALS, expr);
      builder.end();
    } else if (op == GenericUDFOPEqualOrGreaterThan.class) {
      builder.startNot();
      createLeaf(PredicateLeaf.Operator.LESS_THAN, expr);
      builder.end();
    } else if (op == GenericUDFOPLessThan.class) {
      createLeaf(PredicateLeaf.Operator.LESS_THAN, expr);
    } else if (op == GenericUDFOPEqualOrLessThan.class) {
      createLeaf(PredicateLeaf.Operator.LESS_THAN_EQUALS, expr);
    } else if (op == GenericUDFIn.class) {
      createLeaf(PredicateLeaf.Operator.IN, expr, 0);
    } else if (op == GenericUDFBetween.class) {
      createLeaf(PredicateLeaf.Operator.BETWEEN, expr, 1);
    } else if (op == GenericUDFOPNull.class) {
      createLeaf(PredicateLeaf.Operator.IS_NULL, expr, 0);
    } else if (op == GenericUDFOPNotNull.class) {
      builder.startNot();
      createLeaf(PredicateLeaf.Operator.IS_NULL, expr, 0);
      builder.end();

      // otherwise, we didn't understand it, so mark it maybe
    } else {
      builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
    }
  }


  public static final String SARG_PUSHDOWN = "sarg.pushdown";

  public static SearchArgument create(Configuration conf, ExprNodeGenericFuncDesc expression) {
    return new ConvertAstToSearchArg(conf, expression).buildSearchArgument();
  }


  private final static ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
    protected Kryo initialValue() { return new Kryo(); }
  };

  public static SearchArgument create(String kryo) {
    return create(Base64.decodeBase64(kryo));
  }

  public static SearchArgument create(byte[] kryoBytes) {
    return kryo.get().readObject(new Input(kryoBytes), SearchArgumentImpl.class);
  }

  public static SearchArgument createFromConf(Configuration conf) {
    String sargString;
    if ((sargString = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR)) != null) {
      return create(conf, SerializationUtilities.deserializeExpression(sargString));
    } else if ((sargString = conf.get(SARG_PUSHDOWN)) != null) {
      return create(sargString);
    }
    return null;
  }

  public static boolean canCreateFromConf(Configuration conf) {
    return conf.get(TableScanDesc.FILTER_EXPR_CONF_STR) != null || conf.get(SARG_PUSHDOWN) != null;
  }

}
