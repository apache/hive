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

package org.apache.hadoop.hive.ql.parse.type;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.StrictChecks;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.lib.CostLessRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.SubqueryExpressionWalker;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSubquerySemanticException;
import org.apache.hadoop.hive.ql.parse.ASTErrorUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.SubqueryType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;

/**
 * The Factory for creating typecheck processors. The typecheck processors are
 * used to processes the syntax trees for expressions and convert them into
 * expression Node Descriptor trees. They also introduce the correct conversion
 * functions to do proper implicit conversion.
 *
 * At instantiation time, a expression factory needs to be provided to this class.
 */
public class TypeCheckProcFactory<T> {

  static final Logger LOG = LoggerFactory.getLogger(
      TypeCheckProcFactory.class.getName());

  static final HashMap<Integer, String> SPECIAL_UNARY_OPERATOR_TEXT_MAP;
  static final HashMap<Integer, String> CONVERSION_FUNCTION_TEXT_MAP;
  static final HashSet<Integer> WINDOWING_TOKENS;
  private static final Object ALIAS_PLACEHOLDER = new Object();

  static {
    SPECIAL_UNARY_OPERATOR_TEXT_MAP = new HashMap<>();
    SPECIAL_UNARY_OPERATOR_TEXT_MAP.put(HiveParser.PLUS, "positive");
    SPECIAL_UNARY_OPERATOR_TEXT_MAP.put(HiveParser.MINUS, "negative");
    CONVERSION_FUNCTION_TEXT_MAP = new HashMap<Integer, String>();
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_BOOLEAN,
        serdeConstants.BOOLEAN_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_TINYINT,
        serdeConstants.TINYINT_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_SMALLINT,
        serdeConstants.SMALLINT_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_INT,
        serdeConstants.INT_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_BIGINT,
        serdeConstants.BIGINT_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_FLOAT,
        serdeConstants.FLOAT_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_DOUBLE,
        serdeConstants.DOUBLE_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_STRING,
        serdeConstants.STRING_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_CHAR,
        serdeConstants.CHAR_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_VARCHAR,
        serdeConstants.VARCHAR_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_BINARY,
        serdeConstants.BINARY_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_DATE,
        serdeConstants.DATE_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_TIMESTAMP,
        serdeConstants.TIMESTAMP_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_TIMESTAMPLOCALTZ,
        serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_INTERVAL_YEAR_MONTH,
        serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_INTERVAL_DAY_TIME,
        serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_DECIMAL,
        serdeConstants.DECIMAL_TYPE_NAME);
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_MAP, "toMap");
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_LIST, "toArray");
    CONVERSION_FUNCTION_TEXT_MAP.put(HiveParser.TOK_STRUCT, "toStruct");

    WINDOWING_TOKENS = new HashSet<Integer>();
    WINDOWING_TOKENS.add(HiveParser.KW_OVER);
    WINDOWING_TOKENS.add(HiveParser.TOK_PARTITIONINGSPEC);
    WINDOWING_TOKENS.add(HiveParser.TOK_DISTRIBUTEBY);
    WINDOWING_TOKENS.add(HiveParser.TOK_SORTBY);
    WINDOWING_TOKENS.add(HiveParser.TOK_CLUSTERBY);
    WINDOWING_TOKENS.add(HiveParser.TOK_WINDOWSPEC);
    WINDOWING_TOKENS.add(HiveParser.TOK_WINDOWRANGE);
    WINDOWING_TOKENS.add(HiveParser.TOK_WINDOWVALUES);
    WINDOWING_TOKENS.add(HiveParser.KW_UNBOUNDED);
    WINDOWING_TOKENS.add(HiveParser.KW_PRECEDING);
    WINDOWING_TOKENS.add(HiveParser.KW_FOLLOWING);
    WINDOWING_TOKENS.add(HiveParser.KW_CURRENT);
    WINDOWING_TOKENS.add(HiveParser.TOK_TABSORTCOLNAMEASC);
    WINDOWING_TOKENS.add(HiveParser.TOK_TABSORTCOLNAMEDESC);
    WINDOWING_TOKENS.add(HiveParser.TOK_NULLS_FIRST);
    WINDOWING_TOKENS.add(HiveParser.TOK_NULLS_LAST);
  }

  /**
   * Factory that will be used to create the different expressions.
   */
  protected final ExprFactory<T> exprFactory;

  protected TypeCheckProcFactory(ExprFactory<T> exprFactory) {
    this.exprFactory = exprFactory;
  }

  protected Map<ASTNode, T> genExprNode(ASTNode expr, TypeCheckCtx tcCtx) throws SemanticException {
    // Create the walker, the rules dispatcher and the context.
    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree

    SetMultimap<Integer, SemanticNodeProcessor> astNodeToProcessor = HashMultimap.create();
    astNodeToProcessor.put(HiveParser.TOK_NULL, getNullExprProcessor());
    astNodeToProcessor.put(HiveParser.TOK_PARAMETER, getDynamicParameterProcessor());
    astNodeToProcessor.put(HiveParser.Number, getNumExprProcessor());
    astNodeToProcessor.put(HiveParser.IntegralLiteral, getNumExprProcessor());
    astNodeToProcessor.put(HiveParser.NumberLiteral, getNumExprProcessor());

    astNodeToProcessor.put(HiveParser.Identifier, getStrExprProcessor());
    astNodeToProcessor.put(HiveParser.StringLiteral, getStrExprProcessor());
    astNodeToProcessor.put(HiveParser.TOK_CHARSETLITERAL, getStrExprProcessor());
    astNodeToProcessor.put(HiveParser.TOK_STRINGLITERALSEQUENCE, getStrExprProcessor());
    astNodeToProcessor.put(HiveParser.KW_IF, getStrExprProcessor());
    astNodeToProcessor.put(HiveParser.KW_CASE, getStrExprProcessor());
    astNodeToProcessor.put(HiveParser.KW_WHEN, getStrExprProcessor());
    astNodeToProcessor.put(HiveParser.KW_IN, getStrExprProcessor());
    astNodeToProcessor.put(HiveParser.KW_ARRAY, getStrExprProcessor());
    astNodeToProcessor.put(HiveParser.KW_MAP, getStrExprProcessor());
    astNodeToProcessor.put(HiveParser.KW_STRUCT, getStrExprProcessor());
    astNodeToProcessor.put(HiveParser.KW_EXISTS, getStrExprProcessor());
    astNodeToProcessor.put(HiveParser.TOK_SUBQUERY_OP_NOTIN, getStrExprProcessor());

    astNodeToProcessor.put(HiveParser.KW_TRUE, getBoolExprProcessor());
    astNodeToProcessor.put(HiveParser.KW_FALSE, getBoolExprProcessor());

    astNodeToProcessor.put(HiveParser.TOK_DATELITERAL, getDateTimeExprProcessor());
    astNodeToProcessor.put(HiveParser.TOK_TIMESTAMPLITERAL, getDateTimeExprProcessor());
    astNodeToProcessor.put(HiveParser.TOK_TIMESTAMPLOCALTZLITERAL, getDateTimeExprProcessor());

    astNodeToProcessor.put(HiveParser.TOK_INTERVAL_YEAR_MONTH_LITERAL, getIntervalExprProcessor());
    astNodeToProcessor.put(HiveParser.TOK_INTERVAL_DAY_TIME_LITERAL, getIntervalExprProcessor());
    astNodeToProcessor.put(HiveParser.TOK_INTERVAL_YEAR_LITERAL, getIntervalExprProcessor());
    astNodeToProcessor.put(HiveParser.TOK_INTERVAL_MONTH_LITERAL, getIntervalExprProcessor());
    astNodeToProcessor.put(HiveParser.TOK_INTERVAL_DAY_LITERAL, getIntervalExprProcessor());
    astNodeToProcessor.put(HiveParser.TOK_INTERVAL_HOUR_LITERAL, getIntervalExprProcessor());
    astNodeToProcessor.put(HiveParser.TOK_INTERVAL_MINUTE_LITERAL, getIntervalExprProcessor());
    astNodeToProcessor.put(HiveParser.TOK_INTERVAL_SECOND_LITERAL, getIntervalExprProcessor());

    astNodeToProcessor.put(HiveParser.TOK_TABLE_OR_COL, getColumnExprProcessor());

    astNodeToProcessor.put(HiveParser.TOK_SUBQUERY_EXPR, getSubQueryExprProcessor());
    astNodeToProcessor.put(HiveParser.TOK_ALIAS, getValueAliasProcessor());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    SemanticDispatcher disp = new CostLessRuleDispatcher(getDefaultExprProcessor(),
        astNodeToProcessor, tcCtx);
    SemanticGraphWalker ogw = new SubqueryExpressionWalker(disp);

    // Create a list of top nodes
    ArrayList<Node> topNodes = Lists.<Node>newArrayList(expr);
    HashMap<Node, Object> nodeOutputs = new LinkedHashMap<Node, Object>();
    ogw.startWalking(topNodes, nodeOutputs);

    return convert(nodeOutputs);
  }

  // temporary type-safe casting
  protected Map<ASTNode, T> convert(Map<Node, Object> outputs) {
    Map<ASTNode, T> converted = new LinkedHashMap<>();
    for (Map.Entry<Node, Object> entry : outputs.entrySet()) {
      if (entry.getKey() instanceof ASTNode &&
          (entry.getValue() == null || exprFactory.isExprInstance(entry.getValue()))) {
        converted.put((ASTNode)entry.getKey(), (T) entry.getValue());
      } else {
        LOG.warn("Invalid type entry " + entry);
      }
    }
    return converted;
  }

  /**
   * Processor for processing NULL expression.
   */
  public class NullExprProcessor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      T desc = processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      return exprFactory.createNullConstantExpr();
    }

  }

  /**
   * Processor for processing Dynamic expression.
   */
  public class DynamicParameterProcessor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ASTNode node = (ASTNode)nd;
      String indexStr = (node).getText();
      int index = Integer.parseInt(indexStr);
      return exprFactory.createDynamicParamExpr(index);
    }

  }


  /**
   * Factory method to get NullExprProcessor.
   *
   * @return NullExprProcessor.
   */
  protected NullExprProcessor getNullExprProcessor() {
    return new NullExprProcessor();
  }

  /**
   * Factory method to get {@link DynamicParameterProcessor}.
   */
  protected DynamicParameterProcessor getDynamicParameterProcessor() {
    return new DynamicParameterProcessor();
  }

  /**
   * Processor for processing numeric constants.
   */
  public class NumExprProcessor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      T desc = processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      T result = null;
      ASTNode expr = (ASTNode) nd;
      try {
        if (expr.getText().endsWith("L")) {
          // Literal bigint.
          result = exprFactory.createBigintConstantExpr(
              expr.getText().substring(0, expr.getText().length() - 1));
        } else if (expr.getText().endsWith("S")) {
          // Literal smallint.
          result = exprFactory.createSmallintConstantExpr(
              expr.getText().substring(0, expr.getText().length() - 1));
        } else if (expr.getText().endsWith("Y")) {
          // Literal tinyint.
          result = exprFactory.createTinyintConstantExpr(
              expr.getText().substring(0, expr.getText().length() - 1));
        } else if (expr.getText().endsWith("BD")) {
          // Literal decimal
          result = exprFactory.createDecimalConstantExpr(
              expr.getText().substring(0, expr.getText().length() - 2), true);
        } else if (expr.getText().endsWith("F")) {
          // Literal float.
          result = exprFactory.createFloatConstantExpr(
              expr.getText().substring(0, expr.getText().length() - 1));
        } else if (expr.getText().endsWith("D")) {
          // Literal double.
          result = exprFactory.createDoubleConstantExpr(
              expr.getText().substring(0, expr.getText().length() - 1));
        } else {
          // Default behavior
          result = exprFactory.createConstantExpr(expr.getText());
        }
      } catch (NumberFormatException e) {
        // do nothing here, we will throw an exception in the following block
      }
      if (result == null) {
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_NUMERICAL_CONSTANT.getMsg(), expr));
      }
      return result;
    }

  }

  /**
   * Factory method to get NumExprProcessor.
   *
   * @return NumExprProcessor.
   */
  protected NumExprProcessor getNumExprProcessor() {
    return new NumExprProcessor();
  }

  /**
   * Processor for processing string constants.
   */
  public class StrExprProcessor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      T desc = processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode) nd;
      String str = null;

      switch (expr.getToken().getType()) {
        case HiveParser.StringLiteral:
          str = BaseSemanticAnalyzer.unescapeSQLString(expr.getText());
          break;
        case HiveParser.TOK_STRINGLITERALSEQUENCE:
          StringBuilder sb = new StringBuilder();
          for (Node n : expr.getChildren()) {
            sb.append(
                BaseSemanticAnalyzer.unescapeSQLString(((ASTNode) n).getText()));
          }
          str = sb.toString();
          break;
        case HiveParser.TOK_CHARSETLITERAL:
          str = BaseSemanticAnalyzer.charSetString(expr.getChild(0).getText(),
              expr.getChild(1).getText());
          break;
        default:
          // HiveParser.identifier | HiveParse.KW_IF | HiveParse.KW_LEFT |
          // HiveParse.KW_RIGHT
          str = BaseSemanticAnalyzer.unescapeIdentifier(expr.getText().toLowerCase());
          break;
      }
      return exprFactory.createStringConstantExpr(str);
    }

  }

  /**
   * Factory method to get StrExprProcessor.
   *
   * @return StrExprProcessor.
   */
  protected StrExprProcessor getStrExprProcessor() {
    return new StrExprProcessor();
  }

  /**
   * Processor for boolean constants.
   */
  public class BoolExprProcessor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      T desc = processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode) nd;
      String bool = null;

      switch (expr.getToken().getType()) {
        case HiveParser.KW_TRUE:
          bool = Boolean.TRUE.toString();
          break;
        case HiveParser.KW_FALSE:
          bool = Boolean.FALSE.toString();
          break;
        default:
          assert false;
      }
      return exprFactory.createBooleanConstantExpr(bool);
    }

  }

  /**
   * Factory method to get BoolExprProcessor.
   *
   * @return BoolExprProcessor.
   */
  protected BoolExprProcessor getBoolExprProcessor() {
    return new BoolExprProcessor();
  }

  /**
   * Processor for date constants.
   */
  public class DateTimeExprProcessor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      T desc = processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode) nd;
      String timeString = BaseSemanticAnalyzer.stripQuotes(expr.getText());

      // Get the string value and convert to a Date value.
      try {
        if (expr.getType() == HiveParser.TOK_DATELITERAL) {
          return exprFactory.createDateConstantExpr(timeString);
        }
        if (expr.getType() == HiveParser.TOK_TIMESTAMPLITERAL) {
          return exprFactory.createTimestampConstantExpr(timeString);
        }
        if (expr.getType() == HiveParser.TOK_TIMESTAMPLOCALTZLITERAL) {
          HiveConf conf;
          try {
            conf = Hive.get().getConf();
          } catch (HiveException e) {
            throw new SemanticException(e);
          }
          return exprFactory.createTimestampLocalTimeZoneConstantExpr(timeString, conf.getLocalTimeZone());
        }
        throw new IllegalArgumentException("Invalid time literal type " + expr.getType());
      } catch (Exception err) {
        throw new SemanticException(
            "Unable to convert time literal '" + timeString + "' to time value.", err);
      }
    }
  }

  /**
   * Factory method to get DateExprProcessor.
   *
   * @return DateExprProcessor.
   */
  protected DateTimeExprProcessor getDateTimeExprProcessor() {
    return new DateTimeExprProcessor();
  }

  /**
   * Processor for interval constants.
   */
  public class IntervalExprProcessor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      T desc = processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode) nd;
      String intervalString = BaseSemanticAnalyzer.stripQuotes(expr.getText());

      // Get the string value and convert to a Interval value.
      try {
        switch (expr.getType()) {
          case HiveParser.TOK_INTERVAL_YEAR_MONTH_LITERAL:
            return exprFactory.createIntervalYearMonthConstantExpr(intervalString);
          case HiveParser.TOK_INTERVAL_DAY_TIME_LITERAL:
            return exprFactory.createIntervalDayTimeConstantExpr(intervalString);
          case HiveParser.TOK_INTERVAL_YEAR_LITERAL:
            return exprFactory.createIntervalYearConstantExpr(intervalString);
          case HiveParser.TOK_INTERVAL_MONTH_LITERAL:
            return exprFactory.createIntervalMonthConstantExpr(intervalString);
          case HiveParser.TOK_INTERVAL_DAY_LITERAL:
            return exprFactory.createIntervalDayConstantExpr(intervalString);
          case HiveParser.TOK_INTERVAL_HOUR_LITERAL:
            return exprFactory.createIntervalHourConstantExpr(intervalString);
          case HiveParser.TOK_INTERVAL_MINUTE_LITERAL:
            return exprFactory.createIntervalMinuteConstantExpr(intervalString);
          case HiveParser.TOK_INTERVAL_SECOND_LITERAL:
            return exprFactory.createIntervalSecondConstantExpr(intervalString);
          default:
            throw new IllegalArgumentException("Invalid time literal type " + expr.getType());
        }
      } catch (Exception err) {
        throw new SemanticException(
            "Unable to convert interval literal '" + intervalString + "' to interval value.", err);
      }
    }
  }

  /**
   * Factory method to get IntervalExprProcessor.
   *
   * @return IntervalExprProcessor.
   */
  protected IntervalExprProcessor getIntervalExprProcessor() {
    return new IntervalExprProcessor();
  }

  /**
   * Processor for table columns.
   */
  public class ColumnExprProcessor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      T desc = processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode) nd;
      ASTNode parent = stack.size() > 1 ? (ASTNode) stack.get(stack.size() - 2) : null;
      RowResolver input = ctx.getInputRR();
      if (input == null) {
        ctx.setError(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_COLUMN.getMsg(), expr), expr);
        return null;
      }

      if (expr.getType() != HiveParser.TOK_TABLE_OR_COL) {
        ctx.setError(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_COLUMN.getMsg(), expr), expr);
        return null;
      }

      assert (expr.getChildCount() == 1);
      String tableOrCol = BaseSemanticAnalyzer.unescapeIdentifier(expr
          .getChild(0).getText());

      boolean isTableAlias = input.hasTableAlias(tableOrCol);
      ColumnInfo colInfo = null;
      RowResolver usedRR = null;
      int offset = 0;
      try {
        colInfo = input.get(null, tableOrCol);
        usedRR = input;
      } catch (SemanticException semanticException) {
        if (!isTableAlias || parent == null || parent.getType() != HiveParser.DOT) {
          throw semanticException;
        }
      }
      // try outer row resolver
      if (ctx.getOuterRR() != null && colInfo == null && !isTableAlias) {
        RowResolver outerRR = ctx.getOuterRR();
        isTableAlias = outerRR.hasTableAlias(tableOrCol);
        colInfo = outerRR.get(null, tableOrCol);
        usedRR = outerRR;
        offset = input.getColumnInfos().size();
      }

      if (isTableAlias) {
        if (colInfo != null) {
          if (parent != null && parent.getType() == HiveParser.DOT) {
            // It's a table alias.
            return null;
          }
          // It's a column.
          return exprFactory.toExpr(colInfo, usedRR, offset);
        } else {
          // It's a table alias.
          // We will process that later in DOT.
          return null;
        }
      } else {
        if (colInfo == null) {
          // It's not a column or a table alias.
          if (input.getIsExprResolver()) {
            ASTNode exprNode = expr;
            if (!stack.empty()) {
              ASTNode tmp = (ASTNode) stack.pop();
              if (!stack.empty()) {
                exprNode = (ASTNode) stack.peek();
              }
              stack.push(tmp);
            }
            ctx.setError(ASTErrorUtils.getMsg(
                ErrorMsg.NON_KEY_EXPR_IN_GROUPBY.getMsg(), exprNode), expr);
            return null;
          } else {
            List<String> possibleColumnNames = input.getReferenceableColumnAliases(tableOrCol, -1);
            String reason = String.format("(possible column names are: %s)",
                StringUtils.join(possibleColumnNames, ", "));
            ctx.setError(ASTErrorUtils.getMsg(
                ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(), expr.getChild(0), reason),
                expr);
            LOG.debug(ErrorMsg.INVALID_TABLE_OR_COLUMN.toString() + ":"
                + input.toString());
            return null;
          }
        } else {
          // It's a column.
          return exprFactory.toExpr(colInfo, usedRR, offset);
        }
      }
    }

  }

  /**
   * Factory method to get ColumnExprProcessor.
   *
   * @return ColumnExprProcessor.
   */
  protected ColumnExprProcessor getColumnExprProcessor() {
    return new ColumnExprProcessor();
  }

  /**
   * The default processor for typechecking.
   */
  public class DefaultExprProcessor implements SemanticNodeProcessor {

    private boolean isRedundantConversionFunction(ASTNode expr,
        boolean isFunction, List<T> children) {
      if (!isFunction) {
        return false;
      }
      // conversion functions take a single parameter
      if (children.size() != 1) {
        return false;
      }
      String funcText = CONVERSION_FUNCTION_TEXT_MAP.get(((ASTNode) expr
          .getChild(0)).getType());
      // not a conversion function
      if (funcText == null) {
        return false;
      }
      // return true when the child type and the conversion target type is the
      // same
      return exprFactory.getTypeInfo(children.get(0)).getTypeName().equalsIgnoreCase(funcText);
    }

    /**
     * This function create an ExprNodeDesc for a UDF function given the
     * children (arguments). It will insert implicit type conversion functions
     * if necessary.
     *
     * @throws UDFArgumentException
     */
    public T getFuncExprNodeDescWithUdfData(String udfName, TypeInfo typeInfo,
        T... children) throws SemanticException {

      FunctionInfo fi;
      try {
        fi = exprFactory.getFunctionInfo(udfName);
      } catch (SemanticException e) {
        throw new UDFArgumentException(e);
      }
      if (fi == null) {
        throw new UDFArgumentException(udfName + " not found.");
      }

      if (!fi.isGenericUDF()) {
        throw new UDFArgumentException(udfName
            + " is an aggregation function or a table function.");
      }

      List<T> childrenList = new ArrayList<>(children.length);

      childrenList.addAll(Arrays.asList(children));
      return exprFactory.createFuncCallExpr(typeInfo, fi, udfName, childrenList);
    }

    public T getFuncExprNodeDesc(String udfName, T... children) throws SemanticException {
      return getFuncExprNodeDescWithUdfData(udfName, null, children);
    }

    /**
     * @param column  column expression to convert
     * @param tableFieldTypeInfo TypeInfo to convert to
     * @return Expression converting column to the type specified by tableFieldTypeInfo
     */
    public T createConversionCast(T column, PrimitiveTypeInfo tableFieldTypeInfo)
        throws SemanticException {
      // Get base type, since type string may be parameterized
      String baseType = TypeInfoUtils.getBaseName(tableFieldTypeInfo.getTypeName());

      // If the type cast UDF is for a parameterized type, then it should implement
      // the SettableUDF interface so that we can pass in the params.
      // Not sure if this is the cleanest solution, but there does need to be a way
      // to provide the type params to the type cast.
      return getDefaultExprProcessor().getFuncExprNodeDescWithUdfData(baseType, tableFieldTypeInfo, column);
    }

    protected void validateUDF(ASTNode expr, boolean isFunction, TypeCheckCtx ctx, FunctionInfo fi,
        List<T> children) throws SemanticException {
      // Check if a bigint is implicitely cast to a double as part of a comparison
      // Perform the check here instead of in GenericUDFBaseCompare to guarantee it is only run once per operator
      if (exprFactory.isCompareFunction(fi) && children.size() == 2) {
        TypeInfo oiTypeInfo0 = exprFactory.getTypeInfo(children.get(0));
        TypeInfo oiTypeInfo1 = exprFactory.getTypeInfo(children.get(1));

        SessionState ss = SessionState.get();
        Configuration conf = (ss != null) ? ss.getConf() : new Configuration();

        LogHelper console = new LogHelper(LOG);

        if (TypeInfoUtils.isConversionLossy(oiTypeInfo0, oiTypeInfo1)) {
          String error = StrictChecks.checkTypeSafety(conf);
          if (error != null) {
            throw new UDFArgumentException(error);
          }
          String tName0 = oiTypeInfo0.getTypeName();
          String tName1 = oiTypeInfo1.getTypeName();
          console.printError("WARNING: Comparing " + tName0 + " and " + tName1 + " may result in loss of information.");
        }
      }

      // Detect UDTF's in nested SELECT, GROUP BY, etc as they aren't
      // supported
      if (fi.getGenericUDTF() != null) {
        throw new SemanticException(ErrorMsg.UDTF_INVALID_LOCATION.getMsg());
      }
      // UDAF in filter condition, group-by caluse, param of funtion, etc.
      if (fi.getGenericUDAFResolver() != null) {
        if (isFunction) {
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.UDAF_INVALID_LOCATION.getMsg(), (ASTNode) expr.getChild(0)));
        } else {
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.UDAF_INVALID_LOCATION.getMsg(), expr));
        }
      }
      if (!ctx.getAllowStatefulFunctions()) {
        if (exprFactory.isStateful(fi)) {
          throw new SemanticException(ErrorMsg.UDF_STATEFUL_INVALID_LOCATION.getMsg());
        }
      }
    }

    private void insertCast(String funcText, List<T> children) throws SemanticException {
      // substring, concat UDFs expect first argument as string. Therefore this method inserts explicit cast
      // to cast the first operand to string
      if (funcText.equals("substring") || funcText.equals("concat")) {
        if (children.size() > 0 && !isStringType(exprFactory.getTypeInfo(children.get(0)))) {
          T newColumn = createConversionCast(children.get(0), TypeInfoFactory.stringTypeInfo);
          children.set(0, newColumn);
        }
      }

      if (funcText.equalsIgnoreCase("and") || funcText.equalsIgnoreCase("or")
          || funcText.equalsIgnoreCase("not") || funcText.equalsIgnoreCase("!")) {
        // If the current function is a conjunction, the returning types of the children should be booleans.
        // Iterate on the children, if the result of a child expression is not a boolean, try to implicitly
        // convert the result of such a child to a boolean value.
        for (int i = 0; i < children.size(); i++) {
          T child = children.get(i);
          TypeInfo typeInfo = exprFactory.getTypeInfo(child);
          if (!TypeInfoFactory.booleanTypeInfo.accept(typeInfo)) {
            if (typeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE) {
              // For primitive types like string/double/timestamp, try to cast the result of
              // the child expression to a boolean.
              children.set(i, createConversionCast(child, TypeInfoFactory.booleanTypeInfo));
            } else {
              // For complex types like map/list/struct, create a isnotnull function on the child expression.
              child = exprFactory.createFuncCallExpr(TypeInfoFactory.booleanTypeInfo,
                  exprFactory.getFunctionInfo("isnotnull"),"isnotnull", Arrays.asList(child));
              children.set(i, child);
            }
          }
        }
      }
    }

    protected T getXpathOrFuncExprNodeDesc(ASTNode node,
        boolean isFunction, List<T> children, TypeCheckCtx ctx)
        throws SemanticException {
      // return the child directly if the conversion is redundant.
      if (isRedundantConversionFunction(node, isFunction, children)) {
        assert (children.size() == 1);
        assert (children.get(0) != null);
        return children.get(0);
      }
      String funcText = getFunctionText(node, isFunction);
      T expr;
      if (funcText.equals(".")) {
        assert (children.size() == 2);
        // Only allow constant field name for now
        assert (exprFactory.isConstantExpr(children.get(1)));
        T object = children.get(0);

        // Calculate result TypeInfo
        String fieldNameString = exprFactory.getConstantValueAsString(children.get(1));
        TypeInfo objectTypeInfo = exprFactory.getTypeInfo(object);

        // Allow accessing a field of list element structs directly from a list
        boolean isList = (objectTypeInfo.getCategory() == ObjectInspector.Category.LIST);
        if (isList) {
          objectTypeInfo = ((ListTypeInfo) objectTypeInfo).getListElementTypeInfo();
        }
        if (objectTypeInfo.getCategory() != Category.STRUCT) {
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.INVALID_DOT.getMsg(), node));
        }
        TypeInfo t = ((StructTypeInfo) objectTypeInfo).getStructFieldTypeInfo(fieldNameString);
        if (isList) {
          t = TypeInfoFactory.getListTypeInfo(t);
        }

        expr = exprFactory.createNestedColumnRefExpr(t, children.get(0), fieldNameString, isList);
      } else if (funcText.equals("[")) {
        funcText = "index";
        FunctionInfo fi = exprFactory.getFunctionInfo(funcText);

        // "[]" : LSQUARE/INDEX Expression
        if (!ctx.getallowIndexExpr()) {
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.INVALID_FUNCTION.getMsg(), node));
        }

        assert (children.size() == 2);

        // Check whether this is a list or a map
        TypeInfo myt = exprFactory.getTypeInfo(children.get(0));

        if (myt.getCategory() == Category.LIST) {
          // Only allow integer index for now
          if (!TypeInfoUtils.implicitConvertible(exprFactory.getTypeInfo(children.get(1)),
              TypeInfoFactory.intTypeInfo)) {
            throw new SemanticException(SemanticAnalyzer.generateErrorMessage(
                node, ErrorMsg.INVALID_ARRAYINDEX_TYPE.getMsg()));
          }

          // Calculate TypeInfo
          TypeInfo t = node.getTypeInfo() != null ? node.getTypeInfo() : ((ListTypeInfo) myt).getListElementTypeInfo();
          expr = exprFactory.createFuncCallExpr(t, fi, funcText, children);
        } else if (myt.getCategory() == Category.MAP) {
          if (!TypeInfoUtils.implicitConvertible(exprFactory.getTypeInfo(children.get(1)),
              ((MapTypeInfo) myt).getMapKeyTypeInfo())) {
            throw new SemanticException(ASTErrorUtils.getMsg(
                ErrorMsg.INVALID_MAPINDEX_TYPE.getMsg(), node));
          }
          // Calculate TypeInfo
          TypeInfo t = node.getTypeInfo() != null ? node.getTypeInfo() : ((MapTypeInfo) myt).getMapValueTypeInfo();
          expr = exprFactory.createFuncCallExpr(t, fi, funcText, children);
        } else {
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.NON_COLLECTION_TYPE.getMsg(), node, myt.getTypeName()));
        }
      } else {
        // other operators or functions
        FunctionInfo fi = exprFactory.getFunctionInfo(funcText);

        if (fi == null) {
          if (isFunction) {
            throw new SemanticException(ASTErrorUtils.getMsg(
                ErrorMsg.INVALID_FUNCTION.getMsg(), (ASTNode) node.getChild(0)));
          } else {
            throw new SemanticException(ASTErrorUtils.getMsg(
                ErrorMsg.INVALID_FUNCTION.getMsg(), node));
          }
        }

        if (!fi.isNative()) {
          ctx.getUnparseTranslator().addIdentifierTranslation(
              (ASTNode) node.getChild(0));
        }

        // Handle type casts that may contain type parameters
        TypeInfo typeInfo = isFunction ? getTypeInfo((ASTNode) node.getChild(0)) : null;

        insertCast(funcText, children);

        validateUDF(node, isFunction, ctx, fi, children);

        // Try to infer the type of the constant only if there are two
        // nodes, one of them is column and the other is numeric const
        if (exprFactory.isCompareFunction(fi)
            && children.size() == 2
            && ((exprFactory.isConstantExpr(children.get(0))
            && exprFactory.isColumnRefExpr(children.get(1)))
            || (exprFactory.isColumnRefExpr(children.get(0))
            && exprFactory.isConstantExpr(children.get(1))))) {

          int constIdx = exprFactory.isConstantExpr(children.get(0)) ? 0 : 1;

          T constChild = children.get(constIdx);
          T columnChild = children.get(1 - constIdx);

          final PrimitiveTypeInfo colTypeInfo = TypeInfoFactory.getPrimitiveTypeInfo(
              exprFactory.getTypeInfo(columnChild).getTypeName().toLowerCase());
          T newChild = interpretNodeAsConstant(colTypeInfo, constChild);
          if (newChild != null) {
            children.set(constIdx, newChild);
          }
        }
        // The "in" function is sometimes changed to an "or".  Later on, the "or"
        // function is processed a little differently.  We don't want to process this
        // new "or" function differently, so we track it with this variable.
        // TODO: Test to see if this can be removed.
        boolean functionInfoChangedFromIn = false;
        if (exprFactory.isInFunction(fi)) {
          // We will split the IN clause into different IN clauses, one for each
          // different value type. The reason is that Hive and Calcite treat
          // types in IN clauses differently and it is practically impossible
          // to find some correct implementation unless this is done.
          ListMultimap<TypeInfo, T> expressions = ArrayListMultimap.create();
          for (int i = 1; i < children.size(); i++) {
            T columnDesc = children.get(0);
            T valueDesc = interpretNode(columnDesc, children.get(i));
            if (valueDesc != null) {
              // Only add to the expression map if types can be coerced
              TypeInfo targetType = exprFactory.getTypeInfo(valueDesc);
              if (!expressions.containsKey(targetType)) {
                expressions.put(targetType, columnDesc);
              }
              expressions.put(targetType, valueDesc);
            }
          }
          if(expressions.isEmpty()) {
            // We will only hit this when none of the operands inside the "in" clause can be type-coerced
            // That would imply that the result of "in" is a boolean "false"
            // This should not impact those cases where the "in" clause is used on a boolean column and
            // there is no operand in the "in" clause that cannot be type-coerced into boolean because
            // in case of boolean, Hive does not allow such use cases and throws an error
            return exprFactory.createBooleanConstantExpr("false");
          }

          children.clear();
          List<T> newExprs = new ArrayList<>();
          int numEntries = expressions.keySet().size();
          if (numEntries == 1) {
            children.addAll(expressions.asMap().values().iterator().next());
            funcText = "in";
            fi = exprFactory.getFunctionInfo("in");
          } else {
            FunctionInfo inFunctionInfo  = exprFactory.getFunctionInfo("in");
            for (Collection<T> c : expressions.asMap().values()) {
              newExprs.add(exprFactory.createFuncCallExpr(node.getTypeInfo(), inFunctionInfo,
                  "in", (List<T>) c));
            }
            children.addAll(newExprs);
            funcText = "or";
            fi = exprFactory.getFunctionInfo("or");
            functionInfoChangedFromIn = true;
          }
        }
        if (exprFactory.isOrFunction(fi) && !functionInfoChangedFromIn) {
          // flatten OR
          List<T> childrenList = new ArrayList<>(children.size());
          for (T child : children) {
            if (TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.VOID_TYPE_NAME).equals(exprFactory.getTypeInfo(child))) {
              child = exprFactory.setTypeInfo(child, TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME));
            }
            if (exprFactory.isORFuncCallExpr(child)) {
              childrenList.addAll(exprFactory.getExprChildren(child));
            } else {
              childrenList.add(child);
            }
          }
          expr = exprFactory.createFuncCallExpr(node.getTypeInfo(), fi, funcText, childrenList);
        } else if (exprFactory.isAndFunction(fi)) {
          // flatten AND
          List<T> childrenList = new ArrayList<>(children.size());
          for (T child : children) {
            if (TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.VOID_TYPE_NAME).equals(exprFactory.getTypeInfo(child))) {
              child = exprFactory.setTypeInfo(child, TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME));
            }
            if (exprFactory.isANDFuncCallExpr(child)) {
              childrenList.addAll(exprFactory.getExprChildren(child));
            } else {
              childrenList.add(child);
            }
          }
          expr = exprFactory.createFuncCallExpr(node.getTypeInfo(), fi, funcText, childrenList);
        } else if (ctx.isFoldExpr() && exprFactory.convertCASEIntoCOALESCEFuncCallExpr(fi, children)) {
          // Rewrite CASE into COALESCE
          fi = exprFactory.getFunctionInfo("coalesce");
          expr = exprFactory.createFuncCallExpr(node.getTypeInfo(), fi, "coalesce",
              Lists.newArrayList(children.get(0), exprFactory.createBooleanConstantExpr(Boolean.FALSE.toString())));
          if (Boolean.FALSE.equals(exprFactory.getConstantValue(children.get(1)))) {
            fi = exprFactory.getFunctionInfo("not");
            expr = exprFactory.createFuncCallExpr(node.getTypeInfo(), fi, "not", Lists.newArrayList(expr));
          }
        } else if (ctx.isFoldExpr() && exprFactory.convertCASEIntoIFFuncCallExpr(fi, children)) {
          // Rewrite CASE(C,A,B) into IF(C,A,B)
          fi = exprFactory.getFunctionInfo("if");
          expr = exprFactory.createFuncCallExpr(node.getTypeInfo(), fi, "if", children);
        } else {
          TypeInfo t = (node.getTypeInfo() != null) ? node.getTypeInfo() : typeInfo;
          expr = exprFactory.createFuncCallExpr(t, fi, funcText, children);
        }

        if (exprFactory.isSTRUCTFuncCallExpr(expr)) {
          expr = exprFactory.replaceFieldNamesInStruct(expr, ctx.getColumnAliases());
        }

        // If the function is deterministic and the children are constants,
        // we try to fold the expression to remove e.g. cast on constant
        if (ctx.isFoldExpr() && exprFactory.isFuncCallExpr(expr) &&
            exprFactory.isConsistentWithinQuery(fi) &&
            exprFactory.isAllConstants(children)) {
          T constantExpr = exprFactory.foldExpr(expr);
          if (constantExpr != null) {
            expr = constantExpr;
          }
        }
      }

      if (exprFactory.isPOSITIVEFuncCallExpr(expr)) {
        // UDFOPPositive is a no-op.
        assert (exprFactory.getExprChildren(expr).size() == 1);
        expr = exprFactory.getExprChildren(expr).get(0);
      } else if (exprFactory.isNEGATIVEFuncCallExpr(expr)) {
        // UDFOPNegative should always be folded.
        assert (exprFactory.getExprChildren(expr).size() == 1);
        T input = exprFactory.getExprChildren(expr).get(0);
        if (exprFactory.isConstantExpr(input)) {
          T constantExpr = exprFactory.foldExpr(expr);
          if (constantExpr != null) {
            expr = constantExpr;
          }
        }
      }
      assert (expr != null);
      return expr;
    }

    private TypeInfo getTypeInfo(ASTNode funcNameNode) throws SemanticException {
      switch (funcNameNode.getType()) {
        case HiveParser.TOK_CHAR:
          return ParseUtils.getCharTypeInfo(funcNameNode);
        case HiveParser.TOK_VARCHAR:
          return ParseUtils.getVarcharTypeInfo(funcNameNode);
        case HiveParser.TOK_TIMESTAMPLOCALTZ:
          TimestampLocalTZTypeInfo timestampLocalTZTypeInfo = new TimestampLocalTZTypeInfo();
          HiveConf conf;
          try {
            conf = Hive.get().getConf();
          } catch (HiveException e) {
            throw new SemanticException(e);
          }
          timestampLocalTZTypeInfo.setTimeZone(conf.getLocalTimeZone());
          return timestampLocalTZTypeInfo;
        case HiveParser.TOK_DECIMAL:
          return ParseUtils.getDecimalTypeTypeInfo(funcNameNode);
        case HiveParser.TOK_MAP:
        case HiveParser.TOK_LIST:
        case HiveParser.TOK_STRUCT:
          return ParseUtils.getComplexTypeTypeInfo(funcNameNode);
        default:
          return null;
      }
    }
    /**
     * Interprets the given value as the input columnDesc if possible.
     * Otherwise, returns input valueDesc as is.
     */
    private T interpretNode(T columnDesc, T valueDesc)
        throws SemanticException {
      if (exprFactory.isColumnRefExpr(columnDesc)) {
        final TypeInfo info = exprFactory.getTypeInfo(columnDesc);
        switch (info.getCategory()) {
        case MAP:
        case LIST:
        case UNION:
        case STRUCT:
          return valueDesc;
        case PRIMITIVE:
          PrimitiveTypeInfo primitiveInfo = TypeInfoFactory.getPrimitiveTypeInfo(info.getTypeName().toLowerCase());
          return interpretNodeAsConstant(primitiveInfo, valueDesc);
        }
      }
      boolean columnStruct = exprFactory.isSTRUCTFuncCallExpr(columnDesc);
      if (columnStruct) {
        boolean constantValuesStruct = exprFactory.isConstantStruct(valueDesc);
        boolean valuesStruct = exprFactory.isSTRUCTFuncCallExpr(valueDesc);
        if (constantValuesStruct || valuesStruct) {
          List<T> columnChilds = exprFactory.getExprChildren(columnDesc);
          List<TypeInfo> structFieldInfos = exprFactory.getStructTypeInfoList(valueDesc);
          List<String> structFieldNames = exprFactory.getStructNameList(valueDesc);

          if (columnChilds.size() != structFieldInfos.size()) {
            throw new SemanticException(ErrorMsg.INCOMPATIBLE_STRUCT.getMsg(columnChilds + " and " + structFieldInfos));
          }

          if (constantValuesStruct) {
            List<Object> literals = (List<Object>) exprFactory.getConstantValue(valueDesc);
            List<T> constantExpressions = new ArrayList<>();
            List<TypeInfo> newStructFieldInfos = new ArrayList<>();
            for (int i = 0; i < columnChilds.size(); i++) {
              final PrimitiveTypeInfo typeInfo = TypeInfoFactory.getPrimitiveTypeInfo(
                  exprFactory.getTypeInfo(columnChilds.get(i)).getTypeName().toLowerCase());
              T constantExpression = interpretNodeAsConstant(typeInfo,
                  exprFactory.createConstantExpr(structFieldInfos.get(i), literals.get(i)));
              if (constantExpression == null) {
                constantExpression = exprFactory.createConstantExpr(typeInfo, null);
              }
              constantExpressions.add(constantExpression);
              newStructFieldInfos.add(exprFactory.getTypeInfo(constantExpression));
            }
            StructTypeInfo structTypeInfo = new StructTypeInfo();
            structTypeInfo.setAllStructFieldNames(new ArrayList<>(structFieldNames));
            structTypeInfo.setAllStructFieldTypeInfos(new ArrayList<>(newStructFieldInfos));
            return exprFactory.createStructExpr(structTypeInfo, constantExpressions);
          } else { // valuesStruct
            List<T> valueChilds = exprFactory.getExprChildren(valueDesc);
            List<T> newValueChilds = new ArrayList<>();
            List<TypeInfo> newStructFieldInfos = new ArrayList<>();
            for (int i = 0; i < columnChilds.size(); i++) {
              T newValue = interpretNode(columnChilds.get(i), valueChilds.get(i));
              newValueChilds.add(newValue);
              newStructFieldInfos.add(exprFactory.getTypeInfo(columnChilds.get(i)));
            }
            StructTypeInfo structTypeInfo = new StructTypeInfo();
            structTypeInfo.setAllStructFieldNames(new ArrayList<>(structFieldNames));
            structTypeInfo.setAllStructFieldTypeInfos(new ArrayList<>(newStructFieldInfos));
            return exprFactory.createStructExpr(structTypeInfo, newValueChilds);
          }
        }
      }
      return valueDesc;
    }

    @VisibleForTesting
    protected T interpretNodeAsConstant(PrimitiveTypeInfo targetType, T constChild) throws SemanticException {
      // k in (v1, v2) can be treated as k = v1 or k = v2, so isEqual set to true.
      return interpretNodeAsConstant(targetType, constChild, true);
    }

    private T interpretNodeAsConstant(PrimitiveTypeInfo targetType, T constChild,
        boolean isEqual) throws SemanticException {
      if (exprFactory.isConstantExpr(constChild)) {
        // Try to narrow type of constant
        Object constVal = exprFactory.getConstantValue(constChild);
        if (constVal == null) {
          // adjust type of null
          return exprFactory.createConstantExpr(targetType, null);
        }
        PrimitiveTypeInfo sourceType =
            (PrimitiveTypeInfo) exprFactory.getTypeInfo(constChild);
        Object newConst = exprFactory.interpretConstantAsPrimitive(
            targetType, constVal, sourceType, isEqual);
        if (newConst == null) {
          return null;
        }
        if (newConst == constVal) {
          return constChild;
        } else {
          return exprFactory.createConstantExpr(exprFactory.adjustConstantType(targetType, newConst), newConst);
        }
      }
      return constChild;
    }

    /**
     * Returns true if des is a descendant of ans (ancestor)
     */
    private boolean isDescendant(Node ans, Node des) {
      if (ans.getChildren() == null) {
        return false;
      }
      for (Node c : ans.getChildren()) {
        if (c == des) {
          return true;
        }
        if (isDescendant(c, des)) {
          return true;
        }
      }
      return false;
    }

    protected T processQualifiedColRef(TypeCheckCtx ctx, ASTNode expr,
        Object... nodeOutputs) throws SemanticException {
      RowResolver input = ctx.getInputRR();
      String tableAlias = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getChild(0)
          .getText());
      // NOTE: tableAlias must be a valid non-ambiguous table alias,
      // because we've checked that in TOK_TABLE_OR_COL's process method.
      T desc = (T) nodeOutputs[1];
      String colName;
      if (exprFactory.isConstantExpr(desc)) {
        colName = exprFactory.getConstantValueAsString(desc);
      } else if (exprFactory.isColumnRefExpr(desc)) {
        colName = exprFactory.getColumnName(desc, input);
      } else {
        throw new SemanticException("Unexpected ExprNode : " + nodeOutputs[1]);
      }

      ColumnInfo colInfo = input.get(tableAlias, colName);
      RowResolver usedRR = input;
      int offset = 0;

      // Try outer Row resolver
      if (colInfo == null && ctx.getOuterRR() != null) {
        RowResolver outerRR = ctx.getOuterRR();
        colInfo = outerRR.get(tableAlias, colName);
        usedRR = outerRR;
        offset = input.getColumnInfos().size();
      }

      if (colInfo == null) {
        ctx.setError(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_COLUMN.getMsg(), expr.getChild(1)), expr);
        return null;
      }
      return exprFactory.toExpr(colInfo, usedRR, offset);
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                          Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;

      T desc = processGByExpr(nd, procCtx);
      if (desc != null) {
        // Here we know nd represents a group by expression.

        // During the DFS traversal of the AST, a descendant of nd likely set an
        // error because a sub-tree of nd is unlikely to also be a group by
        // expression. For example, in a query such as
        // SELECT *concat(key)* FROM src GROUP BY concat(key), 'key' will be
        // processed before 'concat(key)' and since 'key' is not a group by
        // expression, an error will be set in ctx by ColumnExprProcessor.

        // We can clear the global error when we see that it was set in a
        // descendant node of a group by expression because
        // processGByExpr() returns a ExprNodeDesc that effectively ignores
        // its children. Although the error can be set multiple times by
        // descendant nodes, DFS traversal ensures that the error only needs to
        // be cleared once. Also, for a case like
        // SELECT concat(value, concat(value))... the logic still works as the
        // error is only set with the first 'value'; all node processors quit
        // early if the global error is set.

        if (isDescendant(nd, ctx.getErrorSrcNode())) {
          ctx.setError(null, null);
        }
        return desc;
      }

      if (ctx.getError() != null) {
        return null;
      }

      ASTNode expr = (ASTNode) nd;

      /*
       * A Windowing specification get added as a child to a UDAF invocation to distinguish it
       * from similar UDAFs but on different windows.
       * The UDAF is translated to a WindowFunction invocation in the PTFTranslator.
       * So here we just return null for tokens that appear in a Window Specification.
       * When the traversal reaches up to the UDAF invocation its ExprNodeDesc is build using the
       * ColumnInfo in the InputRR. This is similar to how UDAFs are handled in Select lists.
       * The difference is that there is translation for Window related tokens, so we just
       * return null;
       */
      if (WINDOWING_TOKENS.contains(expr.getType())) {
        if (!ctx.getallowWindowing()) {
          throw new SemanticException(SemanticAnalyzer.generateErrorMessage(expr,
              ErrorMsg.INVALID_FUNCTION.getMsg("Windowing is not supported in the context")));
        }

        return null;
      }

      if (expr.getType() == HiveParser.TOK_SUBQUERY_OP || expr.getType() == HiveParser.TOK_QUERY) {
        return null;
      }

      if (expr.getType() == HiveParser.TOK_TABNAME) {
        return null;
      }

      if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
        if (!ctx.getallowAllColRef()) {
          throw new SemanticException(SemanticAnalyzer.generateErrorMessage(expr,
              ErrorMsg.INVALID_COLUMN
                  .getMsg("All column reference is not supported in the context")));
        }

        RowResolver input = ctx.getInputRR();
        T columnList = exprFactory.createExprsListExpr();
        assert expr.getChildCount() <= 1;
        if (expr.getChildCount() == 1) {
          // table aliased (select a.*, for example)
          ASTNode child = (ASTNode) expr.getChild(0);
          assert child.getType() == HiveParser.TOK_TABNAME;
          assert child.getChildCount() == 1;
          String tableAlias = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText());
          Map<String, ColumnInfo> columns = input.getFieldMap(tableAlias);
          if (columns == null) {
            throw new SemanticException(ASTErrorUtils.getMsg(
                ErrorMsg.INVALID_TABLE_ALIAS.getMsg(), child));
          }
          for (Map.Entry<String, ColumnInfo> colMap : columns.entrySet()) {
            ColumnInfo colInfo = colMap.getValue();
            if (!colInfo.getIsVirtualCol()) {
              exprFactory.addExprToExprsList(columnList, exprFactory.toExpr(colInfo, input, 0));
            }
          }
        } else {
          // all columns (select *, for example)
          for (ColumnInfo colInfo : input.getColumnInfos()) {
            if (!colInfo.getIsVirtualCol()) {
              exprFactory.addExprToExprsList(columnList, exprFactory.toExpr(colInfo, input, 0));
            }
          }
        }
        return columnList;
      }

      // If the first child is a TOK_TABLE_OR_COL, and nodeOutput[0] is NULL,
      // and the operator is a DOT, then it's a table column reference.
      if (expr.getType() == HiveParser.DOT
          && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
          && nodeOutputs[0] == null) {
        return processQualifiedColRef(ctx, expr, nodeOutputs);
      }

      // Return nulls for conversion operators
      if (CONVERSION_FUNCTION_TEXT_MAP.keySet().contains(expr.getType())
          || expr.getToken().getType() == HiveParser.CharSetName
          || expr.getToken().getType() == HiveParser.CharSetLiteral
          || expr.getType() == HiveParser.TOK_TABCOL
          || expr.getType() == HiveParser.TOK_TABCOLLIST) {
        return null;
      }

      boolean isFunction = (expr.getType() == HiveParser.TOK_FUNCTION ||
          expr.getType() == HiveParser.TOK_FUNCTIONSTAR ||
          expr.getType() == HiveParser.TOK_FUNCTIONDI);

      if (!ctx.getAllowDistinctFunctions() && expr.getType() == HiveParser.TOK_FUNCTIONDI) {
        throw new SemanticException(
            SemanticAnalyzer.generateErrorMessage(expr, ErrorMsg.DISTINCT_NOT_SUPPORTED.getMsg()));
      }

      // Create all children
      int childrenBegin = (isFunction ? 1 : 0);
      List<T> children = new ArrayList<T>(
          expr.getChildCount() - childrenBegin);
      for (int ci = childrenBegin; ci < expr.getChildCount(); ci++) {
        if (nodeOutputs[ci] == ALIAS_PLACEHOLDER) {
          continue;
        }

        T nodeOutput = (T) nodeOutputs[ci];
        if (exprFactory.isExprsListExpr(nodeOutput)) {
          children.addAll(exprFactory.getExprChildren(nodeOutput));
        } else {
          children.add(nodeOutput);
        }
      }

      if (expr.getType() == HiveParser.TOK_FUNCTIONSTAR) {
        if (!ctx.getallowFunctionStar()) {
          throw new SemanticException(SemanticAnalyzer.generateErrorMessage(expr,
              ErrorMsg.INVALID_COLUMN
                  .getMsg(".* reference is not supported in the context")));
        }

        RowResolver input = ctx.getInputRR();
        for (ColumnInfo colInfo : input.getColumnInfos()) {
          if (!colInfo.getIsVirtualCol()) {
            children.add(exprFactory.toExpr(colInfo, input, 0));
          }
        }
      }

      // If any of the children contains null, then return a null
      // this is a hack for now to handle the group by case
      if (children.contains(null)) {
        List<String> possibleColumnNames = getReferenceableColumnAliases(ctx);
        String reason = String.format("(possible column names are: %s)",
            StringUtils.join(possibleColumnNames, ", "));
        ctx.setError(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_COLUMN.getMsg(), expr.getChild(0), reason),
            expr);
        return null;
      }

      // Create function desc
      try {
        return getXpathOrFuncExprNodeDesc(expr, isFunction, children, ctx);
      } catch (UDFArgumentTypeException e) {
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_ARGUMENT_TYPE.getMsg(),
            expr.getChild(childrenBegin + e.getArgumentId()), e.getMessage()), e);
      } catch (UDFArgumentLengthException e) {
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_ARGUMENT_LENGTH.getMsg(),
            expr, e.getMessage()), e);
      } catch (UDFArgumentException e) {
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_ARGUMENT.getMsg(),
            expr, e.getMessage()), e);
      }
    }

    protected List<String> getReferenceableColumnAliases(TypeCheckCtx ctx) {
      return ctx.getInputRR().getReferenceableColumnAliases(null, -1);
    }
  }

  /**
   * Factory method to get DefaultExprProcessor.
   *
   * @return DefaultExprProcessor.
   */
  protected DefaultExprProcessor getDefaultExprProcessor() {
    return new DefaultExprProcessor();
  }

  /**
   * Processor for subquery expressions..
   */
  public class SubQueryExprProcessor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                          Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ASTNode expr = (ASTNode) nd;
      ASTNode sqNode = (ASTNode) expr.getParent().getChild(1);

      if (!ctx.getallowSubQueryExpr()) {
        throw new CalciteSubquerySemanticException(SemanticAnalyzer.generateErrorMessage(sqNode,
            ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg("Currently SubQuery expressions are only allowed as " +
                "Where and Having Clause predicates")));
      }

      T desc = processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      //TOK_SUBQUERY_EXPR should have either 2 or 3 children
      assert (expr.getChildren().size() == 3 || expr.getChildren().size() == 2);
      //First child should be operand
      assert (expr.getChild(0).getType() == HiveParser.TOK_SUBQUERY_OP);

      ASTNode subqueryOp = (ASTNode) expr.getChild(0);
      SubqueryType subqueryType = null;
      if ((subqueryOp.getChildCount() > 0) && (subqueryOp.getChild(0).getType() == HiveParser.KW_IN
          || subqueryOp.getChild(0).getType() == HiveParser.TOK_SUBQUERY_OP_NOTIN)) {
        subqueryType = SubqueryType.IN;
      } else if ((subqueryOp.getChildCount() > 0) && (subqueryOp.getChild(0).getType() == HiveParser.KW_EXISTS
          || subqueryOp.getChild(0).getType() == HiveParser.TOK_SUBQUERY_OP_NOTEXISTS)) {
        subqueryType = SubqueryType.EXISTS;
      } else if ((subqueryOp.getChildCount() > 0) && (subqueryOp.getChild(0).getType() == HiveParser.KW_SOME)) {
        subqueryType = SubqueryType.SOME;
      } else if ((subqueryOp.getChildCount() > 0) && (subqueryOp.getChild(0).getType() == HiveParser.KW_ALL)) {
        subqueryType = SubqueryType.ALL;
      } else if (subqueryOp.getChildCount() == 0) {
        subqueryType = SubqueryType.SCALAR;
      }

      T res = exprFactory.createSubqueryExpr(ctx, expr, subqueryType, nodeOutputs);
      if (res == null) {
        /*
         * Restriction.1.h :: SubQueries only supported in the SQL Where Clause.
         */
        ctx.setError(ASTErrorUtils.getMsg(
            ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(), sqNode,
            "Currently only IN & EXISTS SubQuery expressions are allowed"),
            sqNode);
      }
      return res;
    }
  }

  /**
   * Factory method to get SubQueryExprProcessor.
   *
   * @return DateExprProcessor.
   */
  protected SubQueryExprProcessor getSubQueryExprProcessor() {
    return new SubQueryExprProcessor();
  }

  /**
   * Function to do groupby subexpression elimination. This is called by all the
   * processors initially. As an example, consider the query select a+b,
   * count(1) from T group by a+b; Then a+b is already precomputed in the group
   * by operators key, so we substitute a+b in the select list with the internal
   * column name of the a+b expression that appears in the in input row
   * resolver.
   *
   * @param nd      The node that is being inspected.
   * @param procCtx The processor context.
   * @return exprNodeColumnDesc.
   */
  private T processGByExpr(Node nd, Object procCtx) throws SemanticException {
    // We recursively create the exprNodeDesc. Base cases: when we encounter
    // a column ref, we convert that into an exprNodeColumnDesc; when we
    // encounter
    // a constant, we convert that into an exprNodeConstantDesc. For others we
    // just
    // build the exprNodeFuncDesc with recursively built children.
    ASTNode expr = (ASTNode) nd;
    TypeCheckCtx ctx = (TypeCheckCtx) procCtx;

    // bypass only if outerRR is not null. Otherwise we need to look for expressions in outerRR for
    // subqueries e.g. select min(b.value) from table b group by b.key
    //                                  having key in (select .. where a = min(b.value)
    if (!ctx.isUseCaching() && ctx.getOuterRR() == null) {
      return null;
    }

    RowResolver input = ctx.getInputRR();
    T desc = null;

    if ((ctx == null) || (input == null) || (!ctx.getAllowGBExprElimination())) {
      return null;
    }

    // If the current subExpression is pre-calculated, as in Group-By etc.
    ColumnInfo colInfo = input.getExpression(expr);
    RowResolver usedRR = input;
    int offset = 0;

    // try outer row resolver
    RowResolver outerRR = ctx.getOuterRR();
    if (colInfo == null && outerRR != null) {
      colInfo = outerRR.getExpression(expr);
      usedRR = outerRR;
      offset = input.getColumnInfos().size();
    }
    if (colInfo != null) {
      desc = exprFactory.createColumnRefExpr(colInfo, usedRR, offset);
      ASTNode source = input.getExpressionSource(expr);
      if (source != null && ctx.getUnparseTranslator() != null) {
        ctx.getUnparseTranslator().addCopyTranslation(expr, source);
      }
      return desc;
    }
    return desc;
  }

  public static boolean isStringType(TypeInfo typeInfo) {
    if (typeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
      if (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(primitiveCategory) ==
          PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP) {
        return true;
      }
    }
    return false;
  }

  public static String getFunctionText(ASTNode expr, boolean isFunction) {
    String funcText = null;
    if (!isFunction) {
      // For operator, the function name is the operator text, unless it's in
      // our special dictionary
      if (expr.getChildCount() == 1) {
        funcText = SPECIAL_UNARY_OPERATOR_TEXT_MAP.get(expr.getType());
      }
      if (funcText == null) {
        funcText = expr.getText();
      }
    } else {
      // For TOK_FUNCTION, the function name is stored in the first child,
      // unless it's in our
      // special dictionary.
      assert (expr.getChildCount() >= 1);
      int funcType = ((ASTNode) expr.getChild(0)).getType();
      if (funcText == null) {
        funcText = CONVERSION_FUNCTION_TEXT_MAP.get(funcType);
      }
      if (funcText == null) {
        funcText = ((ASTNode) expr.getChild(0)).getText();
      }
    }
    return BaseSemanticAnalyzer.unescapeIdentifier(funcText);
  }

  private SemanticNodeProcessor getValueAliasProcessor() {
    return new ValueAliasProcessor();
  }

  public static class ValueAliasProcessor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
      ASTNode astNode = (ASTNode) nd;
      ((TypeCheckCtx) procCtx).addColumnAlias(astNode.getChild(0).getText());
      return ALIAS_PLACEHOLDER;
    }
  }

}
