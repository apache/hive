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

import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * The Factory for creating typecheck processors. The typecheck processors are
 * used to processes the syntax trees for expressions and convert them into
 * expression Node Descriptor trees. They also introduce the correct conversion
 * functions to do proper implicit conversion.
 */
public final class TypeCheckProcFactory {

  protected static final Log LOG = LogFactory.getLog(TypeCheckProcFactory.class
      .getName());

  private TypeCheckProcFactory() {
    // prevent instantiation
  }

  /**
   * Function to do groupby subexpression elimination. This is called by all the
   * processors initially. As an example, consider the query select a+b,
   * count(1) from T group by a+b; Then a+b is already precomputed in the group
   * by operators key, so we substitute a+b in the select list with the internal
   * column name of the a+b expression that appears in the in input row
   * resolver.
   *
   * @param nd
   *          The node that is being inspected.
   * @param procCtx
   *          The processor context.
   *
   * @return exprNodeColumnDesc.
   */
  public static ExprNodeDesc processGByExpr(Node nd, Object procCtx)
      throws SemanticException {
    // We recursively create the exprNodeDesc. Base cases: when we encounter
    // a column ref, we convert that into an exprNodeColumnDesc; when we
    // encounter
    // a constant, we convert that into an exprNodeConstantDesc. For others we
    // just
    // build the exprNodeFuncDesc with recursively built children.
    ASTNode expr = (ASTNode) nd;
    TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
    RowResolver input = ctx.getInputRR();
    ExprNodeDesc desc = null;

    // If the current subExpression is pre-calculated, as in Group-By etc.
    ColumnInfo colInfo = input.getExpression(expr);
    if (colInfo != null) {
      desc = new ExprNodeColumnDesc(colInfo.getType(), colInfo
          .getInternalName(), colInfo.getTabAlias(), colInfo
          .getIsVirtualCol());
      ASTNode source = input.getExpressionSource(expr);
      if (source != null) {
        ctx.getUnparseTranslator().addCopyTranslation(expr, source);
      }
      return desc;
    }
    return desc;
  }

  public static Map<ASTNode, ExprNodeDesc> genExprNode(ASTNode expr,
      TypeCheckCtx tcCtx) throws SemanticException {
    // Create the walker, the rules dispatcher and the context.
    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    opRules.put(new RuleRegExp("R1", HiveParser.TOK_NULL + "%"),
        getNullExprProcessor());
    opRules.put(new RuleRegExp("R2", HiveParser.Number + "%|" +
        HiveParser.TinyintLiteral + "%|" +
        HiveParser.SmallintLiteral + "%|" +
        HiveParser.BigintLiteral + "%|" +
        HiveParser.DecimalLiteral + "%"),
        getNumExprProcessor());
    opRules
        .put(new RuleRegExp("R3", HiveParser.Identifier + "%|"
        + HiveParser.StringLiteral + "%|" + HiveParser.TOK_CHARSETLITERAL + "%|"
        + HiveParser.TOK_STRINGLITERALSEQUENCE + "%|"
        + "%|" + HiveParser.KW_IF + "%|" + HiveParser.KW_CASE + "%|"
        + HiveParser.KW_WHEN + "%|" + HiveParser.KW_IN + "%|"
        + HiveParser.KW_ARRAY + "%|" + HiveParser.KW_MAP + "%|"
        + HiveParser.KW_STRUCT + "%"),
        getStrExprProcessor());
    opRules.put(new RuleRegExp("R4", HiveParser.KW_TRUE + "%|"
        + HiveParser.KW_FALSE + "%"), getBoolExprProcessor());
    opRules.put(new RuleRegExp("R5", HiveParser.TOK_TABLE_OR_COL + "%"),
        getColumnExprProcessor());

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
      if (entry.getKey() instanceof ASTNode && entry.getValue() instanceof ExprNodeDesc) {
        converted.put((ASTNode)entry.getKey(), (ExprNodeDesc)entry.getValue());
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

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
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

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      Number v = null;
      ASTNode expr = (ASTNode) nd;
      // The expression can be any one of Double, Long and Integer. We
      // try to parse the expression in that order to ensure that the
      // most specific type is used for conversion.
      try {
        if (expr.getText().endsWith("L")) {
          // Literal bigint.
          v = Long.valueOf(expr.getText().substring(
                0, expr.getText().length() - 1));
        } else if (expr.getText().endsWith("S")) {
          // Literal smallint.
          v = Short.valueOf(expr.getText().substring(
                0, expr.getText().length() - 1));
        } else if (expr.getText().endsWith("Y")) {
          // Literal tinyint.
          v = Byte.valueOf(expr.getText().substring(
                0, expr.getText().length() - 1));
        } else if (expr.getText().endsWith("BD")) {
          // Literal decimal
          return new ExprNodeConstantDesc(TypeInfoFactory.decimalTypeInfo, 
                expr.getText().substring(0, expr.getText().length() - 2));
        } else {
          v = Double.valueOf(expr.getText());
          v = Long.valueOf(expr.getText());
          v = Integer.valueOf(expr.getText());
        }
      } catch (NumberFormatException e) {
        // do nothing here, we will throw an exception in the following block
      }
      if (v == null) {
        throw new SemanticException(ErrorMsg.INVALID_NUMERICAL_CONSTANT
            .getMsg(expr));
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

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
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
              BaseSemanticAnalyzer.unescapeSQLString(((ASTNode)n).getText()));
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

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
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
   * Processor for table columns.
   */
  public static class ColumnExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode) nd;
      ASTNode parent = stack.size() > 1 ? (ASTNode) stack.get(stack.size() - 2) : null;
      RowResolver input = ctx.getInputRR();

      if (expr.getType() != HiveParser.TOK_TABLE_OR_COL) {
        ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr), expr);
        return null;
      }

      assert (expr.getChildCount() == 1);
      String tableOrCol = BaseSemanticAnalyzer.unescapeIdentifier(expr
          .getChild(0).getText());

      boolean isTableAlias = input.hasTableAlias(tableOrCol);
      ColumnInfo colInfo = input.get(null, tableOrCol);

      if (isTableAlias) {
        if (colInfo != null) {
          if (parent != null && parent.getType() == HiveParser.DOT) {
            // It's a table alias.
            return null;
          }
          // It's a column.
          return new ExprNodeColumnDesc(colInfo.getType(), colInfo
              .getInternalName(), colInfo.getTabAlias(), colInfo
              .getIsVirtualCol());
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
            ctx.setError(ErrorMsg.NON_KEY_EXPR_IN_GROUPBY.getMsg(exprNode), expr);
            return null;
          } else {
            List<String> possibleColumnNames = input.getReferenceableColumnAliases(tableOrCol, -1);
            String reason = String.format("(possible column names are: %s)",
                StringUtils.join(possibleColumnNames, ", "));
            ctx.setError(ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr.getChild(0), reason),
                expr);
            LOG.debug(ErrorMsg.INVALID_TABLE_OR_COLUMN.toString() + ":"
                + input.toString());
            return null;
          }
        } else {
          // It's a column.
          ExprNodeColumnDesc exprNodColDesc = new ExprNodeColumnDesc(colInfo.getType(), colInfo
              .getInternalName(), colInfo.getTabAlias(), colInfo
              .getIsVirtualCol());
          exprNodColDesc.setSkewedCol(colInfo.isSkewedCol());
          return exprNodColDesc;
        }
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
    static HashSet<Integer> windowingTokens;
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
      conversionFunctionTextHashMap.put(HiveParser.TOK_BINARY,
          serdeConstants.BINARY_TYPE_NAME);
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
    public static ExprNodeDesc getFuncExprNodeDesc(String udfName,
        ExprNodeDesc... children) throws UDFArgumentException {

      FunctionInfo fi = FunctionRegistry.getFunctionInfo(udfName);
      if (fi == null) {
        throw new UDFArgumentException(udfName + " not found.");
      }

      GenericUDF genericUDF = fi.getGenericUDF();
      if (genericUDF == null) {
        throw new UDFArgumentException(udfName
            + " is an aggregation function or a table function.");
      }

      List<ExprNodeDesc> childrenList = new ArrayList<ExprNodeDesc>(children.length);
      childrenList.addAll(Arrays.asList(children));
      return ExprNodeGenericFuncDesc.newInstance(genericUDF, childrenList);
    }

    static ExprNodeDesc getXpathOrFuncExprNodeDesc(ASTNode expr,
        boolean isFunction, ArrayList<ExprNodeDesc> children, TypeCheckCtx ctx)
        throws SemanticException, UDFArgumentException {
      // return the child directly if the conversion is redundant.
      if (isRedundantConversionFunction(expr, isFunction, children)) {
        assert (children.size() == 1);
        assert (children.get(0) != null);
        return children.get(0);
      }
      String funcText = getFunctionText(expr, isFunction);
      ExprNodeDesc desc;
      if (funcText.equals(".")) {
        // "." : FIELD Expression
        assert (children.size() == 2);
        // Only allow constant field name for now
        assert (children.get(1) instanceof ExprNodeConstantDesc);
        ExprNodeDesc object = children.get(0);
        ExprNodeConstantDesc fieldName = (ExprNodeConstantDesc) children.get(1);
        assert (fieldName.getValue() instanceof String);

        // Calculate result TypeInfo
        String fieldNameString = (String) fieldName.getValue();
        TypeInfo objectTypeInfo = object.getTypeInfo();

        // Allow accessing a field of list element structs directly from a list
        boolean isList = (object.getTypeInfo().getCategory() == ObjectInspector.Category.LIST);
        if (isList) {
          objectTypeInfo = ((ListTypeInfo) objectTypeInfo)
              .getListElementTypeInfo();
        }
        if (objectTypeInfo.getCategory() != Category.STRUCT) {
          throw new SemanticException(ErrorMsg.INVALID_DOT.getMsg(expr));
        }
        TypeInfo t = ((StructTypeInfo) objectTypeInfo)
            .getStructFieldTypeInfo(fieldNameString);
        if (isList) {
          t = TypeInfoFactory.getListTypeInfo(t);
        }

        desc = new ExprNodeFieldDesc(t, children.get(0), fieldNameString,
            isList);

      } else if (funcText.equals("[")) {
        // "[]" : LSQUARE/INDEX Expression
        assert (children.size() == 2);

        // Check whether this is a list or a map
        TypeInfo myt = children.get(0).getTypeInfo();

        if (myt.getCategory() == Category.LIST) {
          // Only allow integer index for now
          if (!(children.get(1) instanceof ExprNodeConstantDesc)
              || !(((ExprNodeConstantDesc) children.get(1)).getTypeInfo()
              .equals(TypeInfoFactory.intTypeInfo))) {
            throw new SemanticException(SemanticAnalyzer.generateErrorMessage(
                  expr,
                  ErrorMsg.INVALID_ARRAYINDEX_CONSTANT.getMsg()));
          }

          // Calculate TypeInfo
          TypeInfo t = ((ListTypeInfo) myt).getListElementTypeInfo();
          desc = new ExprNodeGenericFuncDesc(t, FunctionRegistry
              .getGenericUDFForIndex(), children);
        } else if (myt.getCategory() == Category.MAP) {
          // Only allow constant map key for now
          if (!(children.get(1) instanceof ExprNodeConstantDesc)) {
            throw new SemanticException(SemanticAnalyzer.generateErrorMessage(
                  expr,
                  ErrorMsg.INVALID_MAPINDEX_CONSTANT.getMsg()));
          }
          if (!(((ExprNodeConstantDesc) children.get(1)).getTypeInfo()
              .equals(((MapTypeInfo) myt).getMapKeyTypeInfo()))) {
            throw new SemanticException(ErrorMsg.INVALID_MAPINDEX_TYPE
                .getMsg(expr));
          }
          // Calculate TypeInfo
          TypeInfo t = ((MapTypeInfo) myt).getMapValueTypeInfo();
          desc = new ExprNodeGenericFuncDesc(t, FunctionRegistry
              .getGenericUDFForIndex(), children);
        } else {
          throw new SemanticException(ErrorMsg.NON_COLLECTION_TYPE.getMsg(expr,
              myt.getTypeName()));
        }
      } else {
        // other operators or functions
        FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcText);

        if (fi == null) {
          if (isFunction) {
            throw new SemanticException(ErrorMsg.INVALID_FUNCTION
                .getMsg((ASTNode) expr.getChild(0)));
          } else {
            throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(expr));
          }
        }

        if (!fi.isNative()) {
          ctx.getUnparseTranslator().addIdentifierTranslation(
              (ASTNode) expr.getChild(0));
        }

        // Detect UDTF's in nested SELECT, GROUP BY, etc as they aren't
        // supported
        if (fi.getGenericUDTF() != null) {
          throw new SemanticException(ErrorMsg.UDTF_INVALID_LOCATION.getMsg());
        }
        // UDAF in filter condition, group-by caluse, param of funtion, etc.
        if (fi.getGenericUDAFResolver() != null) {
          if (isFunction) {
            throw new SemanticException(ErrorMsg.UDAF_INVALID_LOCATION.
                getMsg((ASTNode) expr.getChild(0)));
          } else {
            throw new SemanticException(ErrorMsg.UDAF_INVALID_LOCATION.getMsg(expr));
          }
        }
        if (!ctx.getAllowStatefulFunctions() && (fi.getGenericUDF() != null)) {
          if (FunctionRegistry.isStateful(fi.getGenericUDF())) {
            throw new SemanticException(
              ErrorMsg.UDF_STATEFUL_INVALID_LOCATION.getMsg());
          }
        }

        // Try to infer the type of the constant only if there are two
        // nodes, one of them is column and the other is numeric const
        if (fi.getGenericUDF() instanceof GenericUDFBaseCompare
            && children.size() == 2
            && ((children.get(0) instanceof ExprNodeConstantDesc
                && children.get(1) instanceof ExprNodeColumnDesc)
                || (children.get(0) instanceof ExprNodeColumnDesc
                    && children.get(1) instanceof ExprNodeConstantDesc))) {
          int constIdx =
              children.get(0) instanceof ExprNodeConstantDesc ? 0 : 1;

          Set<String> inferTypes = new HashSet<String>(Arrays.asList(
              serdeConstants.TINYINT_TYPE_NAME.toLowerCase(),
              serdeConstants.SMALLINT_TYPE_NAME.toLowerCase(),
              serdeConstants.INT_TYPE_NAME.toLowerCase(),
              serdeConstants.BIGINT_TYPE_NAME.toLowerCase(),
              serdeConstants.FLOAT_TYPE_NAME.toLowerCase(),
              serdeConstants.DOUBLE_TYPE_NAME.toLowerCase(),
              serdeConstants.STRING_TYPE_NAME.toLowerCase()
              ));

          String constType = children.get(constIdx).getTypeString().toLowerCase();
          String columnType = children.get(1 - constIdx).getTypeString().toLowerCase();

          if (inferTypes.contains(constType) && inferTypes.contains(columnType)
              && !columnType.equalsIgnoreCase(constType)) {
            String constValue =
                ((ExprNodeConstantDesc) children.get(constIdx)).getValue().toString();
            boolean triedDouble = false;

            Number value = null;
            try {
              if (columnType.equalsIgnoreCase(serdeConstants.TINYINT_TYPE_NAME)) {
                value = new Byte(constValue);
              } else if (columnType.equalsIgnoreCase(serdeConstants.SMALLINT_TYPE_NAME)) {
                value = new Short(constValue);
              } else if (columnType.equalsIgnoreCase(serdeConstants.INT_TYPE_NAME)) {
                value = new Integer(constValue);
              } else if (columnType.equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME)) {
                value = new Long(constValue);
              } else if (columnType.equalsIgnoreCase(serdeConstants.FLOAT_TYPE_NAME)) {
                value = new Float(constValue);
              } else if (columnType.equalsIgnoreCase(serdeConstants.DOUBLE_TYPE_NAME)
                  || (columnType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)
                     && !constType.equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME))) {
                // no smart inference for queries like "str_col = bigint_const"
                triedDouble = true;
                value = new Double(constValue);
              }
            } catch (NumberFormatException nfe) {
              // this exception suggests the precise type inference did not succeed
              // we'll try again to convert it to double
              // however, if we already tried this, or the column is NUMBER type and
              // the operator is EQUAL, return false due to the type mismatch
              if (triedDouble ||
                  (fi.getGenericUDF() instanceof GenericUDFOPEqual
                  && !columnType.equals(serdeConstants.STRING_TYPE_NAME))) {
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

        desc = ExprNodeGenericFuncDesc.newInstance(fi.getGenericUDF(), children);
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

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
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
        // error is only set with the first 'value'; all node pocessors quit
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
      if ( windowingTokens.contains(expr.getType())) {
        return null;
      }

      if (expr.getType() == HiveParser.TOK_TABNAME) {
        return null;
      }

      if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
        RowResolver input = ctx.getInputRR();
        ExprNodeColumnListDesc columnList = new ExprNodeColumnListDesc();
        assert expr.getChildCount() <= 1;
        if (expr.getChildCount() == 1) {
          // table aliased (select a.*, for example)
          ASTNode child = (ASTNode) expr.getChild(0);
          assert child.getType() == HiveParser.TOK_TABNAME;
          assert child.getChildCount() == 1;
          String tableAlias = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText());
          HashMap<String, ColumnInfo> columns = input.getFieldMap(tableAlias);
          if (columns == null) {
            throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(child));
          }
          for (Map.Entry<String, ColumnInfo> colMap : columns.entrySet()) {
            ColumnInfo colInfo = colMap.getValue();
            if (!colInfo.getIsVirtualCol()) {
              columnList.addColumn(new ExprNodeColumnDesc(colInfo.getType(),
                  colInfo.getInternalName(), colInfo.getTabAlias(), false));
            }
          }
        } else {
          // all columns (select *, for example)
          for (ColumnInfo colInfo : input.getColumnInfos()) {
            if (!colInfo.getIsVirtualCol()) {
              columnList.addColumn(new ExprNodeColumnDesc(colInfo.getType(),
                  colInfo.getInternalName(), colInfo.getTabAlias(), false));
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

        RowResolver input = ctx.getInputRR();
        String tableAlias = BaseSemanticAnalyzer.unescapeIdentifier(expr
            .getChild(0).getChild(0).getText());
        // NOTE: tableAlias must be a valid non-ambiguous table alias,
        // because we've checked that in TOK_TABLE_OR_COL's process method.
        ColumnInfo colInfo = input.get(tableAlias,
            ((ExprNodeConstantDesc) nodeOutputs[1]).getValue().toString());

        if (colInfo == null) {
          ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr.getChild(1)), expr);
          return null;
        }
        return new ExprNodeColumnDesc(colInfo.getType(), colInfo
            .getInternalName(), colInfo.getTabAlias(), colInfo
            .getIsVirtualCol());
      }

      // Return nulls for conversion operators
      if (conversionFunctionTextHashMap.keySet().contains(expr.getType())
          || specialFunctionTextHashMap.keySet().contains(expr.getType())
          || expr.getToken().getType() == HiveParser.CharSetName
          || expr.getToken().getType() == HiveParser.CharSetLiteral) {
        return null;
      }

      boolean isFunction = (expr.getType() == HiveParser.TOK_FUNCTION ||
          expr.getType() == HiveParser.TOK_FUNCTIONSTAR ||
          expr.getType() == HiveParser.TOK_FUNCTIONDI);

      // Create all children
      int childrenBegin = (isFunction ? 1 : 0);
      ArrayList<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(expr
          .getChildCount()
          - childrenBegin);
      for (int ci = childrenBegin; ci < expr.getChildCount(); ci++) {
        if (nodeOutputs[ci] instanceof ExprNodeColumnListDesc) {
          children.addAll(((ExprNodeColumnListDesc)nodeOutputs[ci]).getChildren());
        } else {
          children.add((ExprNodeDesc) nodeOutputs[ci]);
        }
      }

      if (expr.getType() == HiveParser.TOK_FUNCTIONSTAR) {
        RowResolver input = ctx.getInputRR();
        for (ColumnInfo colInfo : input.getColumnInfos()) {
          if (!colInfo.getIsVirtualCol()) {
            children.add(new ExprNodeColumnDesc(colInfo.getType(),
                colInfo.getInternalName(), colInfo.getTabAlias(), false));
          }
        }
      }

      // If any of the children contains null, then return a null
      // this is a hack for now to handle the group by case
      if (children.contains(null)) {
        RowResolver input = ctx.getInputRR();
        List<String> possibleColumnNames = input.getReferenceableColumnAliases(null, -1);
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
        throw new SemanticException(ErrorMsg.INVALID_ARGUMENT_TYPE.getMsg(expr
            .getChild(childrenBegin + e.getArgumentId()), e.getMessage()));
      } catch (UDFArgumentLengthException e) {
        throw new SemanticException(ErrorMsg.INVALID_ARGUMENT_LENGTH.getMsg(
            expr, e.getMessage()));
      } catch (UDFArgumentException e) {
        throw new SemanticException(ErrorMsg.INVALID_ARGUMENT.getMsg(expr, e
            .getMessage()));
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
}
