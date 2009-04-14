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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFuncDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeIndexDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeNullDesc;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.udf.UDFOPPositive;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

/**
 * The Factory for creating typecheck processors. The typecheck processors are used to
 * processes the syntax trees for expressions and convert them into expression Node
 * Descriptor trees. They also introduce the correct conversion functions to do proper
 * implicit conversion.
 */
public class TypeCheckProcFactory {

  /**
   * Function to do groupby subexpression elimination. This is called by all the processors initially.
   * As an example, consider the query
   *   select a+b, count(1) from T group by a+b;
   * Then a+b is already precomputed in the group by operators key, so we substitute a+b in the select
   * list with the internal column name of the a+b expression that appears in the in input row resolver.
   * 
   * @param nd The node that is being inspected.
   * @param procCtx The processor context.
   * 
   * @return exprNodeColumnDesc.
   */
  public static exprNodeDesc processGByExpr(Node nd, Object procCtx) 
    throws SemanticException {
    //  We recursively create the exprNodeDesc.  Base cases:  when we encounter 
    //  a column ref, we convert that into an exprNodeColumnDesc;  when we encounter 
    //  a constant, we convert that into an exprNodeConstantDesc.  For others we just 
    //  build the exprNodeFuncDesc with recursively built children.
    ASTNode expr = (ASTNode)nd;
    TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
    RowResolver input = ctx.getInputRR();
    exprNodeDesc desc = null;

    //  If the current subExpression is pre-calculated, as in Group-By etc.
    ColumnInfo colInfo = input.get("", expr.toStringTree());
    if (colInfo != null) {
      desc = new exprNodeColumnDesc(colInfo.getType(), colInfo.getInternalName()); 
      return desc;
    }    
    return desc;
  }
  
  /**
   * Processor for processing NULL expression.
   */
  public static class NullExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      exprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }
      
      return new exprNodeNullDesc();
    }
    
  }
  
  /**
   * Factory method to get NullExprProcessor.
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

      exprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }
      
      Number v = null;
      ASTNode expr = (ASTNode)nd;
      // The expression can be any one of Double, Long and Integer. We
      // try to parse the expression in that order to ensure that the
      // most specific type is used for conversion.
      try {
        v = Double.valueOf(expr.getText());
        v = Long.valueOf(expr.getText());
        v = Integer.valueOf(expr.getText());
      } catch (NumberFormatException e) {
        // do nothing here, we will throw an exception in the following block
      }
      if (v == null) {
        throw new SemanticException(ErrorMsg.INVALID_NUMERICAL_CONSTANT.getMsg(expr));
      }
      return new exprNodeConstantDesc(v);
    }
    
  }
  
  /**
   * Factory method to get NumExprProcessor.
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

      exprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }
      
      ASTNode expr = (ASTNode)nd;
      String str = null;
      
      switch (expr.getToken().getType()) {
      case HiveParser.StringLiteral:
        str = BaseSemanticAnalyzer.unescapeSQLString(expr.getText());
        break;
      case HiveParser.TOK_CHARSETLITERAL:
        str = BaseSemanticAnalyzer.charSetString(expr.getChild(0).getText(), expr.getChild(1).getText());
        break;
      default:
        // HiveParser.identifier | HiveParse.KW_IF | HiveParse.KW_LEFT | HiveParse.KW_RIGHT 
        str = BaseSemanticAnalyzer.unescapeIdentifier(expr.getText());
        break;
      }
      return new exprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, str);
    }
    
  }
  
  /**
   * Factory method to get StrExprProcessor.
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

      exprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode)nd;
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
      return new exprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, bool);      
    }
    
  }
  
  /**
   * Factory method to get BoolExprProcessor.
   * @return BoolExprProcessor.
   */
  public static BoolExprProcessor getBoolExprProcessor() {
    return new BoolExprProcessor();
  }
  
  /**
   * Processor for table columns
   */
  public static class ColumnExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      exprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode)nd;
      TypeCheckCtx ctx = (TypeCheckCtx)procCtx;
      RowResolver input = ctx.getInputRR();

      if(expr.getType() != HiveParser.TOK_COLREF) {
        ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr));
        return null;
      }
      
      String tabAlias = null;
      String colName = null;
      
      if (expr.getChildCount() != 1) {
        tabAlias = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getText());
        colName = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(1).getText());
      }
      else {
        colName = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getText());
      }

      if (colName == null) {
        ctx.setError(ErrorMsg.INVALID_XPATH.getMsg(expr));
        return null;
      }

      ColumnInfo colInfo = input.get(tabAlias, colName);

      if (colInfo == null && input.getIsExprResolver()) {
        ctx.setError(ErrorMsg.NON_KEY_EXPR_IN_GROUPBY.getMsg(expr));
        return null;
      }         
      else if (tabAlias != null && !input.hasTableAlias(tabAlias)) {
        ctx.setError(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(expr.getChild(0)));
        return null;
      } else if (colInfo == null) {
        ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(tabAlias == null? expr.getChild(0) : expr.getChild(1)));
        return null;
      }

      return new exprNodeColumnDesc(colInfo.getType(), colInfo.getInternalName());
    }
    
  }
  
  /**
   * Factory method to get ColumnExprProcessor.
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
    static {
      specialUnaryOperatorTextHashMap = new HashMap<Integer, String>();
      specialUnaryOperatorTextHashMap.put(HiveParser.PLUS, "positive");
      specialUnaryOperatorTextHashMap.put(HiveParser.MINUS, "negative");
      specialFunctionTextHashMap = new HashMap<Integer, String>();
      specialFunctionTextHashMap.put(HiveParser.TOK_ISNULL, "isnull");
      specialFunctionTextHashMap.put(HiveParser.TOK_ISNOTNULL, "isnotnull");
      conversionFunctionTextHashMap = new HashMap<Integer, String>();
      conversionFunctionTextHashMap.put(HiveParser.TOK_BOOLEAN, Constants.BOOLEAN_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_TINYINT, Constants.TINYINT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_SMALLINT, Constants.SMALLINT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_INT, Constants.INT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_BIGINT, Constants.BIGINT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_FLOAT, Constants.FLOAT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_DOUBLE, Constants.DOUBLE_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_STRING, Constants.STRING_TYPE_NAME);
    }

    public static boolean isRedundantConversionFunction(ASTNode expr, boolean isFunction, ArrayList<exprNodeDesc> children) {
      if (!isFunction) return false;
      // children is always one less than the expr.getChildCount(), since the latter contains function name.
      assert(children.size() == expr.getChildCount() - 1);
      // conversion functions take a single parameter
      if (children.size() != 1) return false;
      String funcText = conversionFunctionTextHashMap.get(((ASTNode)expr.getChild(0)).getType());
      // not a conversion function 
      if (funcText == null) return false;
      // return true when the child type and the conversion target type is the same
      return ((PrimitiveTypeInfo)children.get(0).getTypeInfo()).getPrimitiveWritableClass().getName().equals(funcText);
    }
    
    public static String getFunctionText(ASTNode expr, boolean isFunction) {
      String funcText = null;
      if (!isFunction) {
        // For operator, the function name is the operator text, unless it's in our special dictionary
        if (expr.getChildCount() == 1) {
          funcText = specialUnaryOperatorTextHashMap.get(expr.getType());
        }
        if (funcText == null) {
          funcText = expr.getText();
        }
      } else {
        // For TOK_FUNCTION, the function name is stored in the first child, unless it's in our
        // special dictionary.
        assert(expr.getChildCount() >= 1);
        int funcType = ((ASTNode)expr.getChild(0)).getType();
        funcText = specialFunctionTextHashMap.get(funcType);
        if (funcText == null) {
          funcText = conversionFunctionTextHashMap.get(funcType);
        }
        if (funcText == null) {
          funcText = ((ASTNode)expr.getChild(0)).getText();
        }
      }
      return funcText;
    }


    
    /**
     * Get the exprNodeDesc
     * @param name
     * @param children
     * @return The expression node descriptor
     */
    public static exprNodeDesc getFuncExprNodeDesc(String name, exprNodeDesc... children) {
      return getFuncExprNodeDesc(name, Arrays.asList(children));
    }
    
    /**
     * This function create an ExprNodeDesc for a UDF function given the children (arguments).
     * It will insert implicit type conversion functions if necessary. 
     * @throws SemanticException 
     */
    public static exprNodeDesc getFuncExprNodeDesc(String udfName, List<exprNodeDesc> children) {

      // Find the corresponding method
      ArrayList<TypeInfo> argumentTypeInfos = new ArrayList<TypeInfo>(children.size());
      for(int i=0; i<children.size(); i++) {
        exprNodeDesc child = children.get(i);
        argumentTypeInfos.add(child.getTypeInfo());
      }
      
      Method udfMethod = FunctionRegistry.getUDFMethod(udfName, argumentTypeInfos);
      if (udfMethod == null) return null;

      ArrayList<exprNodeDesc> ch = SemanticAnalyzer.convertParameters(udfMethod, children);

      // The return type of a function can be of either Java Primitive Type/Class or Writable Class.
      TypeInfo resultTypeInfo = null;
      if (PrimitiveObjectInspectorUtils.isPrimitiveWritableClass(udfMethod.getReturnType())) {
        resultTypeInfo = TypeInfoFactory.getPrimitiveTypeInfoFromPrimitiveWritable(udfMethod.getReturnType()); 
      } else {
        resultTypeInfo = TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(udfMethod.getReturnType());
      }
      
      exprNodeFuncDesc desc = new exprNodeFuncDesc(
        resultTypeInfo,
        FunctionRegistry.getUDFClass(udfName),
        udfMethod, ch);
      return desc;
    }

    static exprNodeDesc getXpathOrFuncExprNodeDesc(ASTNode expr, boolean isFunction,
        ArrayList<exprNodeDesc> children)
        throws SemanticException {
      // return the child directly if the conversion is redundant.
      if (isRedundantConversionFunction(expr, isFunction, children)) {
        assert(children.size() == 1);
        assert(children.get(0) != null);
        return children.get(0);
      }
      String funcText = getFunctionText(expr, isFunction);
      exprNodeDesc desc;
      if (funcText.equals(".")) {
        // "." :  FIELD Expression
        assert(children.size() == 2);
        // Only allow constant field name for now
        assert(children.get(1) instanceof exprNodeConstantDesc);
        exprNodeDesc object = children.get(0);
        exprNodeConstantDesc fieldName = (exprNodeConstantDesc)children.get(1);
        assert(fieldName.getValue() instanceof String);
        
        // Calculate result TypeInfo
        String fieldNameString = (String)fieldName.getValue();
        TypeInfo objectTypeInfo = object.getTypeInfo();
        
        // Allow accessing a field of list element structs directly from a list  
        boolean isList = (object.getTypeInfo().getCategory() == ObjectInspector.Category.LIST);
        if (isList) {
          objectTypeInfo = ((ListTypeInfo)objectTypeInfo).getListElementTypeInfo();
        }
        if (objectTypeInfo.getCategory() != Category.STRUCT) {
          throw new SemanticException(ErrorMsg.INVALID_DOT.getMsg(expr));
        }
        TypeInfo t = ((StructTypeInfo)objectTypeInfo).getStructFieldTypeInfo(fieldNameString);
        if (isList) {
          t = TypeInfoFactory.getListTypeInfo(t);
        }
        
        desc = new exprNodeFieldDesc(t, children.get(0), fieldNameString, isList);
        
      } else if (funcText.equals("[")){
        // "[]" : LSQUARE/INDEX Expression
        assert(children.size() == 2);
        
        // Check whether this is a list or a map
        TypeInfo myt = children.get(0).getTypeInfo();

        if (myt.getCategory() == Category.LIST) {
          // Only allow constant integer index for now
          if (!(children.get(1) instanceof exprNodeConstantDesc)
              || !(((exprNodeConstantDesc)children.get(1)).getTypeInfo().equals(TypeInfoFactory.intTypeInfo))) {
            throw new SemanticException(ErrorMsg.INVALID_ARRAYINDEX_CONSTANT.getMsg(expr));
          }
        
          // Calculate TypeInfo
          TypeInfo t = ((ListTypeInfo)myt).getListElementTypeInfo();
          desc = new exprNodeIndexDesc(t, children.get(0), children.get(1));
        }
        else if (myt.getCategory() == Category.MAP) {
          // Only allow only constant indexes for now
          if (!(children.get(1) instanceof exprNodeConstantDesc)) {
            throw new SemanticException(ErrorMsg.INVALID_MAPINDEX_CONSTANT.getMsg(expr));
          }
          if (!(((exprNodeConstantDesc)children.get(1)).getTypeInfo().equals( 
              ((MapTypeInfo)myt).getMapKeyTypeInfo()))) {
            throw new SemanticException(ErrorMsg.INVALID_MAPINDEX_TYPE.getMsg(expr));
          }
          // Calculate TypeInfo
          TypeInfo t = ((MapTypeInfo)myt).getMapValueTypeInfo();
          
          desc = new exprNodeIndexDesc(t, children.get(0), children.get(1));
        }
        else {
          throw new SemanticException(ErrorMsg.NON_COLLECTION_TYPE.getMsg(expr, 
              myt.getTypeName()));
        }
      } else {
        // other operators or functions
        Class<? extends UDF> udf = FunctionRegistry.getUDFClass(funcText);
        if (udf == null) {
          if (isFunction)
            throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg((ASTNode)expr.getChild(0)));
          else
            throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg((ASTNode)expr));
        }
        
        desc = getFuncExprNodeDesc(funcText, children);
        if (desc == null) {
          ArrayList<Class<?>> argumentClasses = new ArrayList<Class<?>>(children.size());
          for(int i=0; i<children.size(); i++) {
            argumentClasses.add(((PrimitiveTypeInfo)children.get(i).getTypeInfo()).getPrimitiveWritableClass());
          }
    
          if (isFunction) {
            String reason = "Looking for UDF \"" + expr.getChild(0).getText() + "\" with parameters " + argumentClasses;
            throw new SemanticException(ErrorMsg.INVALID_FUNCTION_SIGNATURE.getMsg((ASTNode)expr.getChild(0), reason));
          } else {
            String reason = "Looking for Operator \"" + expr.getText() + "\" with parameters " + argumentClasses;
            throw new SemanticException(ErrorMsg.INVALID_OPERATOR_SIGNATURE.getMsg(expr, reason));
          }
        }
      }
      // UDFOPPositive is a no-op.
      // However, we still create it, and then remove it here, to make sure we only allow
      // "+" for numeric types.
      if (desc instanceof exprNodeFuncDesc) {
        exprNodeFuncDesc funcDesc = (exprNodeFuncDesc)desc;
        if (funcDesc.getUDFClass().equals(UDFOPPositive.class)) {
          assert(funcDesc.getChildren().size() == 1);
          desc = funcDesc.getChildExprs().get(0);
        }
      }
      assert(desc != null);
      return desc;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      ASTNode expr = (ASTNode)nd;
      
      // Return nulls for conversion operators
      if (conversionFunctionTextHashMap.keySet().contains(expr.getType()) ||
          specialFunctionTextHashMap.keySet().contains(expr.getType()) ||
          expr.getToken().getType() == HiveParser.CharSetName ||
          expr.getToken().getType() == HiveParser.CharSetLiteral) {
        return null;
      }
      
      exprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      boolean isFunction = (expr.getType() == HiveParser.TOK_FUNCTION);
      
      // Create all children
      int childrenBegin = (isFunction ? 1 : 0);
      ArrayList<exprNodeDesc> children = new ArrayList<exprNodeDesc>(expr.getChildCount() - childrenBegin);
      for (int ci=childrenBegin; ci<expr.getChildCount(); ci++) {
        children.add((exprNodeDesc)nodeOutputs[ci]);
      }
      
      // If any of the children contains null, then return a null
      // this is a hack for now to handle the group by case
      if (children.contains(null)) {
        return null;
      }
      
      // Create function desc
      return getXpathOrFuncExprNodeDesc(expr, isFunction, children);
    }
    
  }
  
  /**
   * Factory method to get DefaultExprProcessor.
   * @return DefaultExprProcessor.
   */
  public static DefaultExprProcessor getDefaultExprProcessor() {
    return new DefaultExprProcessor();
  }
}
