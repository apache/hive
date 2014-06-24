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

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

/**
 * The Factory for creating typecheck processors. The typecheck processors are
 * used to processes the syntax trees for Join Condition expressions and convert them into
 * expression Node Descriptor trees. They also introduce the correct conversion
 * functions to do proper implicit conversion.
 */

/**
 * TODO:<br>
 * 1. Migrate common code in to TypeCheckProcFactory<br>
 * 2. Verify if disallowed expressions in join condition would even come here or
 * would result in exception in phase1 analysis<br>
 * 3. IS all of the expressions for "getStrExprProcessor" allowed as part of
 * join condition<br>
 */
public final class JoinCondnTypeCheckProcFactory {

  protected static final Log LOG = LogFactory
                                     .getLog(JoinCondnTypeCheckProcFactory.class
                                         .getName());

  private JoinCondnTypeCheckProcFactory() {
    // prevent instantiation
  }

  public static Map<ASTNode, ExprNodeDesc> genExprNode(ASTNode expr,
      JoinTypeCheckCtx tcCtx) throws SemanticException {
    // Create the walker, the rules dispatcher and the context.
    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    opRules.put(new RuleRegExp("R1", HiveParser.TOK_NULL + "%"),
        getNullExprProcessor());
    opRules.put(new RuleRegExp("R2", HiveParser.Number + "%|"
        + HiveParser.TinyintLiteral + "%|" + HiveParser.SmallintLiteral + "%|"
        + HiveParser.BigintLiteral + "%|" + HiveParser.DecimalLiteral + "%"),
        getNumExprProcessor());
    opRules.put(new RuleRegExp("R3", HiveParser.Identifier + "%|"
        + HiveParser.StringLiteral + "%|" + HiveParser.TOK_CHARSETLITERAL
        + "%|" + HiveParser.TOK_STRINGLITERALSEQUENCE + "%|" + "%|"
        + HiveParser.KW_IF + "%|" + HiveParser.KW_CASE + "%|"
        + HiveParser.KW_WHEN + "%|" + HiveParser.KW_IN + "%|"
        + HiveParser.KW_ARRAY + "%|" + HiveParser.KW_MAP + "%|"
        + HiveParser.KW_STRUCT + "%|" + HiveParser.KW_EXISTS + "%|"
        + HiveParser.TOK_SUBQUERY_OP_NOTIN + "%"), getStrExprProcessor());
    opRules.put(new RuleRegExp("R4", HiveParser.KW_TRUE + "%|"
        + HiveParser.KW_FALSE + "%"), getBoolExprProcessor());
    opRules.put(new RuleRegExp("R5", HiveParser.TOK_DATELITERAL + "%"),
        getDateExprProcessor());
    opRules.put(new RuleRegExp("R6", HiveParser.TOK_TABLE_OR_COL + "%"),
        getColumnExprProcessor());
    opRules.put(new RuleRegExp("R7", HiveParser.TOK_SUBQUERY_OP + "%"),
        getSubQueryExprProcessor());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultExprProcessor(),
        opRules, tcCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(expr);
    HashMap<Node, Object> nodeOutputs = new LinkedHashMap<Node, Object>();
    ogw.startWalking(topNodes, nodeOutputs);

    return convert(nodeOutputs);
  }

  // temporary type-safe casting
  private static Map<ASTNode, ExprNodeDesc> convert(Map<Node, Object> outputs) {
    Map<ASTNode, ExprNodeDesc> converted = new LinkedHashMap<ASTNode, ExprNodeDesc>();
    for (Map.Entry<Node, Object> entry : outputs.entrySet()) {
      if (entry.getKey() instanceof ASTNode
          && (entry.getValue() == null || entry.getValue() instanceof ExprNodeDesc)) {
        converted
            .put((ASTNode) entry.getKey(), (ExprNodeDesc) entry.getValue());
      } else {
        LOG.warn("Invalid type entry " + entry);
      }
    }
    return converted;
  }

  /**
   * Processor for processing NULL expression.
   */
  public static class NullExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      JoinTypeCheckCtx ctx = (JoinTypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      return new ExprNodeNullDesc();
    }

  }

  /**
   * Factory method to get NullExprProcessor.
   * 
   * @return NullExprProcessor.
   */
  public static NullExprProcessor getNullExprProcessor() {
    return new NullExprProcessor();
  }

  /**
   * Processor for processing numeric constants.
   */
  public static class NumExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      JoinTypeCheckCtx ctx = (JoinTypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      Number v = null;
      ASTNode expr = (ASTNode) nd;
      // The expression can be any one of Double, Long and Integer. We
      // try to parse the expression in that order to ensure that the
      // most specific type is used for conversion.
      try {
        if (expr.getText().endsWith("L")) {
          // Literal bigint.
          v = Long.valueOf(expr.getText().substring(0,
              expr.getText().length() - 1));
        } else if (expr.getText().endsWith("S")) {
          // Literal smallint.
          v = Short.valueOf(expr.getText().substring(0,
              expr.getText().length() - 1));
        } else if (expr.getText().endsWith("Y")) {
          // Literal tinyint.
          v = Byte.valueOf(expr.getText().substring(0,
              expr.getText().length() - 1));
        } else if (expr.getText().endsWith("BD")) {
          // Literal decimal
          String strVal = expr.getText().substring(0,
              expr.getText().length() - 2);
          HiveDecimal hd = HiveDecimal.create(strVal);
          int prec = 1;
          int scale = 0;
          if (hd != null) {
            prec = hd.precision();
            scale = hd.scale();
          }
          DecimalTypeInfo typeInfo = TypeInfoFactory.getDecimalTypeInfo(prec,
              scale);
          return new ExprNodeConstantDesc(typeInfo, strVal);
        } else {
          v = Double.valueOf(expr.getText());
          v = Long.valueOf(expr.getText());
          v = Integer.valueOf(expr.getText());
        }
      } catch (NumberFormatException e) {
        // do nothing here, we will throw an exception in the following block
      }
      if (v == null) {
        throw new SemanticException(
            ErrorMsg.INVALID_NUMERICAL_CONSTANT.getMsg(expr));
      }
      return new ExprNodeConstantDesc(v);
    }

  }

  /**
   * Factory method to get NumExprProcessor.
   * 
   * @return NumExprProcessor.
   */
  public static NumExprProcessor getNumExprProcessor() {
    return new NumExprProcessor();
  }

  /**
   * Processor for processing string constants.
   */
  public static class StrExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      JoinTypeCheckCtx ctx = (JoinTypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
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
          sb.append(BaseSemanticAnalyzer.unescapeSQLString(((ASTNode) n)
              .getText()));
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
        str = BaseSemanticAnalyzer.unescapeIdentifier(expr.getText());
        break;
      }
      return new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, str);
    }

  }

  /**
   * Factory method to get StrExprProcessor.
   * 
   * @return StrExprProcessor.
   */
  public static StrExprProcessor getStrExprProcessor() {
    return new StrExprProcessor();
  }

  /**
   * Processor for boolean constants.
   */
  public static class BoolExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      JoinTypeCheckCtx ctx = (JoinTypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ASTNode expr = (ASTNode) nd;
      Boolean bool = null;

      switch (expr.getToken().getType()) {
      case HiveParser.KW_TRUE:
        bool = Boolean.TRUE;
        break;
      case HiveParser.KW_FALSE:
        bool = Boolean.FALSE;
        break;
      default:
        assert false;
      }
      return new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, bool);
    }

  }

  /**
   * Factory method to get BoolExprProcessor.
   * 
   * @return BoolExprProcessor.
   */
  public static BoolExprProcessor getBoolExprProcessor() {
    return new BoolExprProcessor();
  }

  /**
   * Processor for date constants.
   */
  public static class DateExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      JoinTypeCheckCtx ctx = (JoinTypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ASTNode expr = (ASTNode) nd;

      // Get the string value and convert to a Date value.
      try {
        String dateString = BaseSemanticAnalyzer.stripQuotes(expr.getText());
        Date date = Date.valueOf(dateString);
        return new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo, date);
      } catch (IllegalArgumentException err) {
        throw new SemanticException(
            "Unable to convert date literal string to date value.", err);
      }
    }
  }

  /**
   * Factory method to get DateExprProcessor.
   * 
   * @return DateExprProcessor.
   */
  public static DateExprProcessor getDateExprProcessor() {
    return new DateExprProcessor();
  }

  /**
   * Processor for table columns.
   */
  public static class ColumnExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      JoinTypeCheckCtx ctx = (JoinTypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ASTNode expr = (ASTNode) nd;
      ASTNode parent = stack.size() > 1 ? (ASTNode) stack.get(stack.size() - 2)
          : null;

      if (expr.getType() != HiveParser.TOK_TABLE_OR_COL) {
        ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr), expr);
        return null;
      }

      assert (expr.getChildCount() == 1);
      String tableOrCol = BaseSemanticAnalyzer.unescapeIdentifier(expr
          .getChild(0).getText());

      boolean qualifiedAccess = (parent != null && parent.getType() == HiveParser.DOT);

      ColumnInfo colInfo = null;
      if (!qualifiedAccess) {
        colInfo = getColInfo(ctx, null, tableOrCol, expr);
        // It's a column.
        return new ExprNodeColumnDesc(colInfo.getType(),
            colInfo.getInternalName(), colInfo.getTabAlias(),
            colInfo.getIsVirtualCol());
      } else if (hasTableAlias(ctx, tableOrCol, expr)) {
        return null;
      } else {
        ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr), expr);
        return null;
      }
    }
  }

  /**
   * Factory method to get ColumnExprProcessor.
   * 
   * @return ColumnExprProcessor.
   */
  public static ColumnExprProcessor getColumnExprProcessor() {
    return new ColumnExprProcessor();
  }

  /**
   * The default processor for typechecking.
   */
  public static class DefaultExprProcessor implements NodeProcessor {

    static HashMap<Integer, String> specialUnaryOperatorTextHashMap;
    static HashMap<Integer, String> specialFunctionTextHashMap;
    static HashMap<Integer, String> conversionFunctionTextHashMap;
    static HashSet<Integer>         windowingTokens;
    static {
      specialUnaryOperatorTextHashMap = new HashMap<Integer, String>();
      specialUnaryOperatorTextHashMap.put(HiveParser.PLUS, "positive");
      specialUnaryOperatorTextHashMap.put(HiveParser.MINUS, "negative");
      specialFunctionTextHashMap = new HashMap<Integer, String>();
      specialFunctionTextHashMap.put(HiveParser.TOK_ISNULL, "isnull");
      specialFunctionTextHashMap.put(HiveParser.TOK_ISNOTNULL, "isnotnull");
      conversionFunctionTextHashMap = new HashMap<Integer, String>();
      conversionFunctionTextHashMap.put(HiveParser.TOK_BOOLEAN,
          serdeConstants.BOOLEAN_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_TINYINT,
          serdeConstants.TINYINT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_SMALLINT,
          serdeConstants.SMALLINT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_INT,
          serdeConstants.INT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_BIGINT,
          serdeConstants.BIGINT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_FLOAT,
          serdeConstants.FLOAT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_DOUBLE,
          serdeConstants.DOUBLE_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_STRING,
          serdeConstants.STRING_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_CHAR,
          serdeConstants.CHAR_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_VARCHAR,
          serdeConstants.VARCHAR_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_BINARY,
          serdeConstants.BINARY_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_DATE,
          serdeConstants.DATE_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_TIMESTAMP,
          serdeConstants.TIMESTAMP_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_DECIMAL,
          serdeConstants.DECIMAL_TYPE_NAME);

      windowingTokens = new HashSet<Integer>();
      windowingTokens.add(HiveParser.KW_OVER);
      windowingTokens.add(HiveParser.TOK_PARTITIONINGSPEC);
      windowingTokens.add(HiveParser.TOK_DISTRIBUTEBY);
      windowingTokens.add(HiveParser.TOK_SORTBY);
      windowingTokens.add(HiveParser.TOK_CLUSTERBY);
      windowingTokens.add(HiveParser.TOK_WINDOWSPEC);
      windowingTokens.add(HiveParser.TOK_WINDOWRANGE);
      windowingTokens.add(HiveParser.TOK_WINDOWVALUES);
      windowingTokens.add(HiveParser.KW_UNBOUNDED);
      windowingTokens.add(HiveParser.KW_PRECEDING);
      windowingTokens.add(HiveParser.KW_FOLLOWING);
      windowingTokens.add(HiveParser.KW_CURRENT);
      windowingTokens.add(HiveParser.TOK_TABSORTCOLNAMEASC);
      windowingTokens.add(HiveParser.TOK_TABSORTCOLNAMEDESC);
    }

    private static boolean isRedundantConversionFunction(ASTNode expr,
        boolean isFunction, ArrayList<ExprNodeDesc> children) {
      if (!isFunction) {
        return false;
      }
      // conversion functions take a single parameter
      if (children.size() != 1) {
        return false;
      }
      String funcText = conversionFunctionTextHashMap.get(((ASTNode) expr
          .getChild(0)).getType());
      // not a conversion function
      if (funcText == null) {
        return false;
      }
      // return true when the child type and the conversion target type is the
      // same
      return ((PrimitiveTypeInfo) children.get(0).getTypeInfo()).getTypeName()
          .equalsIgnoreCase(funcText);
    }

    public static String getFunctionText(ASTNode expr, boolean isFunction) {
      String funcText = null;
      if (!isFunction) {
        // For operator, the function name is the operator text, unless it's in
        // our special dictionary
        if (expr.getChildCount() == 1) {
          funcText = specialUnaryOperatorTextHashMap.get(expr.getType());
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
        funcText = specialFunctionTextHashMap.get(funcType);
        if (funcText == null) {
          funcText = conversionFunctionTextHashMap.get(funcType);
        }
        if (funcText == null) {
          funcText = ((ASTNode) expr.getChild(0)).getText();
        }
      }
      return BaseSemanticAnalyzer.unescapeIdentifier(funcText);
    }

    /**
     * This function create an ExprNodeDesc for a UDF function given the
     * children (arguments). It will insert implicit type conversion functions
     * if necessary.
     * 
     * @throws UDFArgumentException
     */
    static ExprNodeDesc getFuncExprNodeDescWithUdfData(String udfName,
        TypeInfo typeInfo, ExprNodeDesc... children)
        throws UDFArgumentException {

      FunctionInfo fi = FunctionRegistry.getFunctionInfo(udfName);
      if (fi == null) {
        throw new UDFArgumentException(udfName + " not found.");
      }

      GenericUDF genericUDF = fi.getGenericUDF();
      if (genericUDF == null) {
        throw new UDFArgumentException(udfName
            + " is an aggregation function or a table function.");
      }

      // Add udfData to UDF if necessary
      if (typeInfo != null) {
        if (genericUDF instanceof SettableUDF) {
          ((SettableUDF) genericUDF).setTypeInfo(typeInfo);
        }
      }

      List<ExprNodeDesc> childrenList = new ArrayList<ExprNodeDesc>(
          children.length);
      childrenList.addAll(Arrays.asList(children));
      return ExprNodeGenericFuncDesc.newInstance(genericUDF, childrenList);
    }

    public static ExprNodeDesc getFuncExprNodeDesc(String udfName,
        ExprNodeDesc... children) throws UDFArgumentException {
      return getFuncExprNodeDescWithUdfData(udfName, null, children);
    }

    static ExprNodeDesc getXpathOrFuncExprNodeDesc(ASTNode expr,
        boolean isFunction, ArrayList<ExprNodeDesc> children,
        JoinTypeCheckCtx ctx) throws SemanticException, UDFArgumentException {
      // return the child directly if the conversion is redundant.
      if (isRedundantConversionFunction(expr, isFunction, children)) {
        assert (children.size() == 1);
        assert (children.get(0) != null);
        return children.get(0);
      }
      String funcText = getFunctionText(expr, isFunction);
      ExprNodeDesc desc;
      if (funcText.equals(".")) {
        throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(expr));
      } else if (funcText.equals("[")) {
        // "[]" : LSQUARE/INDEX Expression
        throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(expr));
      } else {
        // other operators or functions
        FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcText);

        if (fi == null) {
          if (isFunction) {
            throw new SemanticException(
                ErrorMsg.INVALID_FUNCTION.getMsg((ASTNode) expr.getChild(0)));
          } else {
            throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(expr));
          }
        }

        // getGenericUDF() actually clones the UDF. Just call it once and reuse.
        GenericUDF genericUDF = fi.getGenericUDF();

        if (genericUDF instanceof GenericUDFOPOr) {
          throw new SemanticException(
              ErrorMsg.INVALID_FUNCTION.getMsg((ASTNode) expr.getChild(0)));
        }

        // Handle type casts that may contain type parameters
        if (isFunction) {
          ASTNode funcNameNode = (ASTNode) expr.getChild(0);
          switch (funcNameNode.getType()) {
          case HiveParser.TOK_CHAR:
            // Add type params
            CharTypeInfo charTypeInfo = ParseUtils
                .getCharTypeInfo(funcNameNode);
            if (genericUDF != null) {
              ((SettableUDF) genericUDF).setTypeInfo(charTypeInfo);
            }
            break;
          case HiveParser.TOK_VARCHAR:
            VarcharTypeInfo varcharTypeInfo = ParseUtils
                .getVarcharTypeInfo(funcNameNode);
            if (genericUDF != null) {
              ((SettableUDF) genericUDF).setTypeInfo(varcharTypeInfo);
            }
            break;
          case HiveParser.TOK_DECIMAL:
            DecimalTypeInfo decTypeInfo = ParseUtils
                .getDecimalTypeTypeInfo(funcNameNode);
            if (genericUDF != null) {
              ((SettableUDF) genericUDF).setTypeInfo(decTypeInfo);
            }
            break;
          default:
            // Do nothing
            break;
          }
        }

        // Join Condition can not contain UDTF
        if (fi.getGenericUDTF() != null) {
          throw new SemanticException(ErrorMsg.UDTF_INVALID_LOCATION.getMsg());
        }

        // UDAF in filter condition, group-by caluse, param of funtion, etc.
        if (fi.isGenericUDAF()) {
          if (isFunction) {
            throw new SemanticException(
                ErrorMsg.UDAF_INVALID_LOCATION.getMsg((ASTNode) expr
                    .getChild(0)));
          } else {
            throw new SemanticException(
                ErrorMsg.UDAF_INVALID_LOCATION.getMsg(expr));
          }
        }

        if (genericUDF != null) {
          if (FunctionRegistry.isStateful(genericUDF)) {
            throw new SemanticException(
                ErrorMsg.UDF_STATEFUL_INVALID_LOCATION.getMsg());
          }
        }

        if (!(genericUDF instanceof GenericUDFOPAnd)) {
          if (!(genericUDF instanceof GenericUDFBaseCompare)) {
            if (genericUDFargsRefersToBothInput(genericUDF, children,
                ctx.getInputRRList())) {
              ctx.setError(ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(expr), expr);
            }
          } else if (genericUDF instanceof GenericUDFBaseCompare) {
            if (children.size() == 2
                && !(children.get(0) instanceof ExprNodeConstantDesc)
                && !(children.get(1) instanceof ExprNodeConstantDesc)) {
              if (comparisonUDFargsRefersToBothInput(
                  (GenericUDFBaseCompare) genericUDF, children,
                  ctx.getInputRRList())) {
                ctx.setError(ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(expr),
                    expr);
                return null;
              }

              if (argsRefersToNeither((GenericUDFBaseCompare) genericUDF,
                  children, ctx.getInputRRList())) {
                ctx.setError(ErrorMsg.INVALID_JOIN_CONDITION_2.getMsg(expr),
                    expr);
                return null;
              }

              if (!(genericUDF instanceof GenericUDFOPEqual)) {
                ctx.setError(ErrorMsg.INVALID_FUNCTION.getMsg(expr), expr);
                return null;
              }
            } else if (children.size() == 2
                && ((children.get(0) instanceof ExprNodeConstantDesc && children
                    .get(1) instanceof ExprNodeColumnDesc) || (children.get(0) instanceof ExprNodeColumnDesc && children
                    .get(1) instanceof ExprNodeConstantDesc))) {
              int constIdx = children.get(0) instanceof ExprNodeConstantDesc ? 0
                  : 1;

              Set<String> inferTypes = new HashSet<String>(Arrays.asList(
                  serdeConstants.TINYINT_TYPE_NAME.toLowerCase(),
                  serdeConstants.SMALLINT_TYPE_NAME.toLowerCase(),
                  serdeConstants.INT_TYPE_NAME.toLowerCase(),
                  serdeConstants.BIGINT_TYPE_NAME.toLowerCase(),
                  serdeConstants.FLOAT_TYPE_NAME.toLowerCase(),
                  serdeConstants.DOUBLE_TYPE_NAME.toLowerCase(),
                  serdeConstants.STRING_TYPE_NAME.toLowerCase()));

              String constType = children.get(constIdx).getTypeString()
                  .toLowerCase();
              String columnType = children.get(1 - constIdx).getTypeString()
                  .toLowerCase();

              if (inferTypes.contains(constType)
                  && inferTypes.contains(columnType)
                  && !columnType.equalsIgnoreCase(constType)) {
                Object originalValue = ((ExprNodeConstantDesc) children
                    .get(constIdx)).getValue();
                String constValue = originalValue.toString();
                boolean triedDouble = false;
                Number value = null;
                try {
                  if (columnType
                      .equalsIgnoreCase(serdeConstants.TINYINT_TYPE_NAME)) {
                    value = new Byte(constValue);
                  } else if (columnType
                      .equalsIgnoreCase(serdeConstants.SMALLINT_TYPE_NAME)) {
                    value = new Short(constValue);
                  } else if (columnType
                      .equalsIgnoreCase(serdeConstants.INT_TYPE_NAME)) {
                    value = new Integer(constValue);
                  } else if (columnType
                      .equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME)) {
                    value = new Long(constValue);
                  } else if (columnType
                      .equalsIgnoreCase(serdeConstants.FLOAT_TYPE_NAME)) {
                    value = new Float(constValue);
                  } else if (columnType
                      .equalsIgnoreCase(serdeConstants.DOUBLE_TYPE_NAME)) {
                    triedDouble = true;
                    value = new Double(constValue);
                  } else if (columnType
                      .equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)) {
                    // Don't scramble the const type information if comparing to
                    // a
                    // string column,
                    // It's not useful to do so; as of now, there is also a hack
                    // in
                    // SemanticAnalyzer#genTablePlan that causes every column to
                    // look like a string
                    // a string down here, so number type information is always
                    // lost
                    // otherwise.
                    boolean isNumber = (originalValue instanceof Number);
                    triedDouble = !isNumber;
                    value = isNumber ? (Number) originalValue : new Double(
                        constValue);
                  }
                } catch (NumberFormatException nfe) {
                  // this exception suggests the precise type inference did not
                  // succeed
                  // we'll try again to convert it to double
                  // however, if we already tried this, or the column is NUMBER
                  // type
                  // and
                  // the operator is EQUAL, return false due to the type
                  // mismatch
                  if (triedDouble
                      || (genericUDF instanceof GenericUDFOPEqual && !columnType
                          .equals(serdeConstants.STRING_TYPE_NAME))) {
                    return new ExprNodeConstantDesc(false);
                  }

                  try {
                    value = new Double(constValue);
                  } catch (NumberFormatException ex) {
                    return new ExprNodeConstantDesc(false);
                  }
                }

                if (value != null) {
                  children.set(constIdx, new ExprNodeConstantDesc(value));
                }
              }
            }
          }
        }

        desc = ExprNodeGenericFuncDesc.newInstance(genericUDF, funcText,
            children);
      }
      // UDFOPPositive is a no-op.
      // However, we still create it, and then remove it here, to make sure we
      // only allow
      // "+" for numeric types.
      if (FunctionRegistry.isOpPositive(desc)) {
        assert (desc.getChildren().size() == 1);
        desc = desc.getChildren().get(0);
      }
      assert (desc != null);
      return desc;
    }

    private static boolean genericUDFargsRefersToBothInput(GenericUDF udf,
        ArrayList<ExprNodeDesc> children, List<RowResolver> inputRRList) {
      boolean argsRefersToBothInput = false;

      Map<Integer, ExprNodeDesc> hasCodeToColDescMap = new HashMap<Integer, ExprNodeDesc>();
      for (ExprNodeDesc child : children) {
        ExprNodeDescUtils.getExprNodeColumnDesc(child, hasCodeToColDescMap);
      }
      Set<Integer> inputRef = getInputRef(hasCodeToColDescMap.values(), inputRRList);

      if (inputRef.size() > 1)
        argsRefersToBothInput = true;

      return argsRefersToBothInput;
    }

    private static boolean comparisonUDFargsRefersToBothInput(
        GenericUDFBaseCompare comparisonUDF, ArrayList<ExprNodeDesc> children,
        List<RowResolver> inputRRList) {
      boolean argsRefersToBothInput = false;

      Map<Integer, ExprNodeDesc> lhsHashCodeToColDescMap = new HashMap<Integer, ExprNodeDesc>();
      Map<Integer, ExprNodeDesc> rhsHashCodeToColDescMap = new HashMap<Integer, ExprNodeDesc>();
      ExprNodeDescUtils.getExprNodeColumnDesc(children.get(0), lhsHashCodeToColDescMap);
      ExprNodeDescUtils.getExprNodeColumnDesc(children.get(1), rhsHashCodeToColDescMap);
      Set<Integer> lhsInputRef = getInputRef(lhsHashCodeToColDescMap.values(), inputRRList);
      Set<Integer> rhsInputRef = getInputRef(rhsHashCodeToColDescMap.values(), inputRRList);

      if (lhsInputRef.size() > 1 || rhsInputRef.size() > 1)
        argsRefersToBothInput = true;

      return argsRefersToBothInput;
    }

    private static boolean argsRefersToNeither(
        GenericUDFBaseCompare comparisonUDF, ArrayList<ExprNodeDesc> children,
        List<RowResolver> inputRRList) {
      boolean argsRefersToNeither = false;

      Map<Integer, ExprNodeDesc> lhsHashCodeToColDescMap = new HashMap<Integer, ExprNodeDesc>();
      Map<Integer, ExprNodeDesc> rhsHashCodeToColDescMap = new HashMap<Integer, ExprNodeDesc>();
      ExprNodeDescUtils.getExprNodeColumnDesc(children.get(0), lhsHashCodeToColDescMap);
      ExprNodeDescUtils.getExprNodeColumnDesc(children.get(1), rhsHashCodeToColDescMap);
      Set<Integer> lhsInputRef = getInputRef(lhsHashCodeToColDescMap.values(), inputRRList);
      Set<Integer> rhsInputRef = getInputRef(rhsHashCodeToColDescMap.values(), inputRRList);

      if (lhsInputRef.size() == 0 && rhsInputRef.size() == 0)
        argsRefersToNeither = true;

      return argsRefersToNeither;
    }

    private static Set<Integer> getInputRef(Collection<ExprNodeDesc> colDescSet,
        List<RowResolver> inputRRList) {
      String tableAlias;
      RowResolver inputRR;
      Set<Integer> inputLineage = new HashSet<Integer>();

      for (ExprNodeDesc col : colDescSet) {
        ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) col;
        tableAlias = colDesc.getTabAlias();

        for (int i = 0; i < inputRRList.size(); i++) {
          inputRR = inputRRList.get(i);
          
          // If table Alias is present check if InputRR has that table and then check for internal name
          // else if table alias is null then check with internal name in all inputRR.
          if (tableAlias != null) {
            if (inputRR.hasTableAlias(tableAlias)) {
              if (inputRR.getInvRslvMap().containsKey(colDesc.getColumn())) {
                inputLineage.add(i);
              }
            }
          } else {
            if (inputRR.getInvRslvMap().containsKey(colDesc.getColumn())) {
              inputLineage.add(i);
            }                
          }
        }
      }
      
      return inputLineage;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      JoinTypeCheckCtx ctx = (JoinTypeCheckCtx) procCtx;

      if (ctx.getError() != null) {
        return null;
      }

      ASTNode expr = (ASTNode) nd;

      /*
       * Windowing is not supported in Join Condition
       */
      if (windowingTokens.contains(expr.getType())) {
        ctx.setError(ErrorMsg.INVALID_FUNCTION.getMsg(expr,
            "Windowing is not supported in Join Condition"), expr);

        return null;
      }

      if (expr.getType() == HiveParser.TOK_TABNAME) {
        return null;
      }

      if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
        ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr,
            "Join Condition does not support * syntax"), expr);

        return null;
      }

      // If the first child is a TOK_TABLE_OR_COL, and nodeOutput[0] is NULL,
      // and the operator is a DOT, then it's a table column reference.
      if (expr.getType() == HiveParser.DOT
          && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
          && nodeOutputs[0] == null) {

        String tableAlias = BaseSemanticAnalyzer.unescapeIdentifier(expr
            .getChild(0).getChild(0).getText());
        // NOTE: tableAlias must be a valid non-ambiguous table alias,
        // because we've checked that in TOK_TABLE_OR_COL's process method.
        ColumnInfo colInfo = getColInfo(ctx, tableAlias,
            ((ExprNodeConstantDesc) nodeOutputs[1]).getValue().toString(), expr);

        if (colInfo == null) {
          ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr.getChild(1)), expr);
          return null;
        }
        return new ExprNodeColumnDesc(colInfo.getType(),
            colInfo.getInternalName(), tableAlias, colInfo.getIsVirtualCol());
      }

      // Return nulls for conversion operators
      if (conversionFunctionTextHashMap.keySet().contains(expr.getType())
          || specialFunctionTextHashMap.keySet().contains(expr.getType())
          || expr.getToken().getType() == HiveParser.CharSetName
          || expr.getToken().getType() == HiveParser.CharSetLiteral) {
        return null;
      }

      boolean isFunction = (expr.getType() == HiveParser.TOK_FUNCTION
          || expr.getType() == HiveParser.TOK_FUNCTIONSTAR || expr.getType() == HiveParser.TOK_FUNCTIONDI);

      // Create all children
      int childrenBegin = (isFunction ? 1 : 0);
      ArrayList<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(
          expr.getChildCount() - childrenBegin);
      for (int ci = childrenBegin; ci < expr.getChildCount(); ci++) {
        if (nodeOutputs[ci] instanceof ExprNodeColumnListDesc) {
          children.addAll(((ExprNodeColumnListDesc) nodeOutputs[ci])
              .getChildren());
        } else {
          children.add((ExprNodeDesc) nodeOutputs[ci]);
        }
      }

      if (expr.getType() == HiveParser.TOK_FUNCTIONSTAR) {
        ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr.getChild(1)), expr);
        return null;
      }

      // If any of the children contains null, then return a null
      // this is a hack for now to handle the group by case
      if (children.contains(null)) {
        List<String> possibleColumnNames = getReferenceableColumnAliases(ctx);
        String reason = String.format("(possible column names are: %s)",
            StringUtils.join(possibleColumnNames, ", "));
        ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr.getChild(0), reason),
            expr);
        return null;
      }

      // Create function desc
      try {
        return getXpathOrFuncExprNodeDesc(expr, isFunction, children, ctx);
      } catch (UDFArgumentTypeException e) {
        throw new SemanticException(ErrorMsg.INVALID_ARGUMENT_TYPE.getMsg(
            expr.getChild(childrenBegin + e.getArgumentId()), e.getMessage()));
      } catch (UDFArgumentLengthException e) {
        throw new SemanticException(ErrorMsg.INVALID_ARGUMENT_LENGTH.getMsg(
            expr, e.getMessage()));
      } catch (UDFArgumentException e) {
        throw new SemanticException(ErrorMsg.INVALID_ARGUMENT.getMsg(expr,
            e.getMessage()));
      }
    }

  }

  /**
   * Factory method to get DefaultExprProcessor.
   * 
   * @return DefaultExprProcessor.
   */
  public static DefaultExprProcessor getDefaultExprProcessor() {
    return new DefaultExprProcessor();
  }

  /**
   * Processor for subquery expressions..
   */
  public static class SubQueryExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      JoinTypeCheckCtx ctx = (JoinTypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ASTNode expr = (ASTNode) nd;
      ASTNode sqNode = (ASTNode) expr.getParent().getChild(1);
      /*
       * Restriction.1.h :: SubQueries not supported in Join Condition.
       */
      ctx.setError(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(sqNode,
          "SubQuery expressions are npt supported in Join Condition"), sqNode);
      return null;
    }
  }

  /**
   * Factory method to get SubQueryExprProcessor.
   * 
   * @return DateExprProcessor.
   */
  public static SubQueryExprProcessor getSubQueryExprProcessor() {
    return new SubQueryExprProcessor();
  }

  private static boolean hasTableAlias(JoinTypeCheckCtx ctx, String tabName,
      ASTNode expr) throws SemanticException {
    int tblAliasCnt = 0;
    for (RowResolver rr : ctx.getInputRRList()) {
      if (rr.hasTableAlias(tabName))
        tblAliasCnt++;
    }

    if (tblAliasCnt > 1) {
      throw new SemanticException(
          ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(expr));
    }

    return (tblAliasCnt == 1) ? true : false;
  }

  private static ColumnInfo getColInfo(JoinTypeCheckCtx ctx, String tabName,
      String colAlias, ASTNode expr) throws SemanticException {
    ColumnInfo tmp;
    ColumnInfo cInfoToRet = null;

    for (RowResolver rr : ctx.getInputRRList()) {
      tmp = rr.get(tabName, colAlias);
      if (tmp != null) {
        if (cInfoToRet != null) {
          throw new SemanticException(
              ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(expr));
        }
        cInfoToRet = tmp;
      }
    }

    return cInfoToRet;
  }

  private static List<String> getReferenceableColumnAliases(JoinTypeCheckCtx ctx) {
    List<String> possibleColumnNames = new ArrayList<String>();
    for (RowResolver rr : ctx.getInputRRList()) {
      possibleColumnNames.addAll(rr.getReferenceableColumnAliases(null, -1));
    }

    return possibleColumnNames;
  }
}
