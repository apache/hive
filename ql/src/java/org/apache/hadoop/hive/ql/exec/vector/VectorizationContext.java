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

package org.apache.hadoop.hive.ql.exec.vector;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ColumnExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprAndExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprOrExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterNotExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsFalse;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsNotNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsTrue;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFCountDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFCountLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFStdPopDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFStdPopLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFStdSampDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFStdSampLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFSumDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFSumLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarPopDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarPopLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarSampDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarSampLong;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.UDFOPDivide;
import org.apache.hadoop.hive.ql.udf.UDFOPMinus;
import org.apache.hadoop.hive.ql.udf.UDFOPMod;
import org.apache.hadoop.hive.ql.udf.UDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.UDFOPNegative;
import org.apache.hadoop.hive.ql.udf.UDFOPPlus;
import org.apache.hadoop.hive.ql.udf.UDFOPPositive;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * Context class for vectorization execution.
 * Main role is to map column names to column indices and serves as a
 * factory class for building vectorized expressions out of descriptors.
 *
 */
public class VectorizationContext {
  private static final Log LOG = LogFactory.getLog(
      VectorizationContext.class.getName());

  //columnName to column position map
  private final Map<String, Integer> columnMap;
  //Next column to be used for intermediate output
  private int nextOutputColumn;
  private OperatorType opType;
  //Map column number to type
  private final Map<Integer, String> outputColumnTypes;

  public VectorizationContext(Map<String, Integer> columnMap,
      int initialOutputCol) {
    this.columnMap = columnMap;
    this.nextOutputColumn = initialOutputCol;
    this.outputColumnTypes = new HashMap<Integer, String>();
  }
  
  public int allocateOutputColumn (String columnName, String columnType) {
    int newColumnIndex = nextOutputColumn++;
    columnMap.put(columnName, newColumnIndex);
    outputColumnTypes.put(newColumnIndex, columnType);
    return newColumnIndex;
  }

  public void setOperatorType(OperatorType opType) {
    this.opType = opType;
  }

  private VectorExpression getVectorExpression(ExprNodeColumnDesc
      exprDesc) {

    int columnNum = columnMap.get(exprDesc.getColumn());
    VectorExpression expr = null;
    switch (opType) {
      case FILTER:
        //Important: It will come here only if the column is being used as a boolean
        expr = new SelectColumnIsTrue(columnNum);
        break;
      case SELECT:
      case GROUPBY:
      case REDUCESINK:
        expr = new IdentityExpression(columnNum, exprDesc.getTypeString());
        break;
    }
    return expr;
  }

  public VectorExpression[] getVectorExpressions(List<ExprNodeDesc> exprNodes) {
    int i = 0;
    VectorExpression[] ret = new VectorExpression[exprNodes.size()];
    for (ExprNodeDesc e : exprNodes) {
      ret[i++] = getVectorExpression(e);
    }
    return ret;
  }

  public VectorExpression getVectorExpression(ExprNodeDesc exprDesc) {
    if (exprDesc instanceof ExprNodeColumnDesc) {
      return getVectorExpression((ExprNodeColumnDesc) exprDesc);
    } else if (exprDesc instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc expr = (ExprNodeGenericFuncDesc) exprDesc;
      return getVectorExpression(expr.getGenericUDF(),
          expr.getChildExprs());
    }
    return null;
  }

  public VectorExpression getUnaryMinusExpression(List<ExprNodeDesc> childExprList) {
    ExprNodeDesc childExpr = childExprList.get(0);
    int inputCol;
    String colType;
    VectorExpression v1 = null;
    int outputCol = this.nextOutputColumn++;
    if (childExpr instanceof ExprNodeGenericFuncDesc) {
      v1 = getVectorExpression(childExpr);
      inputCol = v1.getOutputColumn();
      colType = v1.getOutputType();
    } else if (childExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) childExpr;
      inputCol = columnMap.get(colDesc.getColumn());
      colType = colDesc.getTypeString();
    } else {
      throw new RuntimeException("Expression not supported: "+childExpr);
    }
    String className = getNormalizedTypeName(colType) + "colUnaryMinus";
    this.nextOutputColumn = outputCol+1;
    VectorExpression expr;
    try {
      expr = (VectorExpression) Class.forName(className).
          getDeclaredConstructors()[0].newInstance(inputCol, outputCol);
    } catch (Exception ex) {
      throw new RuntimeException((ex));
    }
    if (v1 != null) {
      expr.setChildExpressions(new VectorExpression [] {v1});
    }
    return expr;
  }

  public VectorExpression getUnaryPlusExpression(List<ExprNodeDesc> childExprList) {
    ExprNodeDesc childExpr = childExprList.get(0);
    int inputCol;
    String colType;
    VectorExpression v1 = null;
    if (childExpr instanceof ExprNodeGenericFuncDesc) {
      v1 = getVectorExpression(childExpr);
      inputCol = v1.getOutputColumn();
      colType = v1.getOutputType();
    } else if (childExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) childExpr;
      inputCol = columnMap.get(colDesc.getColumn());
      colType = colDesc.getTypeString();
    } else {
      throw new RuntimeException("Expression not supported: "+childExpr);
    }
    VectorExpression expr = new IdentityExpression(inputCol, colType);
    if (v1 != null) {
      expr.setChildExpressions(new VectorExpression [] {v1});
    }
    return expr;
  }

  private VectorExpression getVectorExpression(GenericUDF udf,
      List<ExprNodeDesc> childExpr) {
    if (udf instanceof GenericUDFOPLessThan) {
      return getVectorBinaryComparisonFilterExpression("Less", childExpr);
    } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
      return getVectorBinaryComparisonFilterExpression("LessEqual", childExpr);
    } else if (udf instanceof GenericUDFOPEqual) {
      return getVectorBinaryComparisonFilterExpression("Equal", childExpr);
    } else if (udf instanceof GenericUDFOPGreaterThan) {
      return getVectorBinaryComparisonFilterExpression("Greater", childExpr);
    } else if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
      return getVectorBinaryComparisonFilterExpression("GreaterEqual", childExpr);
    } else if (udf instanceof GenericUDFOPNotEqual) {
      return getVectorBinaryComparisonFilterExpression("NotEqual", childExpr);
    } else if (udf instanceof GenericUDFOPNotNull) {
      return getVectorExpression((GenericUDFOPNotNull) udf, childExpr);
    } else if (udf instanceof GenericUDFOPNull) {
      return getVectorExpression((GenericUDFOPNull) udf, childExpr);
    } else if (udf instanceof GenericUDFOPAnd) {
      return getVectorExpression((GenericUDFOPAnd) udf, childExpr);
    } else if (udf instanceof GenericUDFOPNot) {
      return getVectorExpression((GenericUDFOPNot) udf, childExpr);
    } else if (udf instanceof GenericUDFOPOr) {
      return getVectorExpression((GenericUDFOPOr) udf, childExpr);
    } else if (udf instanceof GenericUDFBridge) {
      return getVectorExpression((GenericUDFBridge) udf, childExpr);
    }
    return null;
  }

  private VectorExpression getVectorExpression(GenericUDFBridge udf,
      List<ExprNodeDesc> childExpr) {
    Class<? extends UDF> cl = udf.getUdfClass();
    // (UDFBaseNumericOp.class.isAssignableFrom(cl)) == true
    if (cl.equals(UDFOPPlus.class)) {
      return getBinaryArithmeticExpression("Add", childExpr);
    } else if (cl.equals(UDFOPMinus.class)) {
      return getBinaryArithmeticExpression("Subtract", childExpr);
    } else if (cl.equals(UDFOPMultiply.class)) {
      return getBinaryArithmeticExpression("Multiply", childExpr);
    } else if (cl.equals(UDFOPDivide.class)) {
      return getBinaryArithmeticExpression("Divide", childExpr);
    } else if (cl.equals(UDFOPMod.class)) {
      return getBinaryArithmeticExpression("Modulo", childExpr);
    } else if (cl.equals(UDFOPNegative.class)) {
      return getUnaryMinusExpression(childExpr);
    } else if (cl.equals(UDFOPPositive.class)) {
      return getUnaryPlusExpression(childExpr);
    }
    return null;
  }

  private VectorExpression getBinaryArithmeticExpression(String method,
      List<ExprNodeDesc> childExpr) {
    ExprNodeDesc leftExpr = childExpr.get(0);
    ExprNodeDesc rightExpr = childExpr.get(1);

    VectorExpression expr = null;
    if ( (leftExpr instanceof ExprNodeColumnDesc) &&
        (rightExpr instanceof ExprNodeConstantDesc) ) {
      ExprNodeColumnDesc leftColDesc = (ExprNodeColumnDesc) leftExpr;
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) rightExpr;
      int inputCol = columnMap.get(leftColDesc.getColumn());
      String colType = leftColDesc.getTypeString();
      String scalarType = constDesc.getTypeString();
      String className = getBinaryColumnScalarExpressionClassName(colType,
          scalarType, method);
      int outputCol = this.nextOutputColumn++;
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol,
            getScalarValue(constDesc), outputCol);
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
    } else if ( (rightExpr instanceof ExprNodeColumnDesc) &&
        (leftExpr instanceof ExprNodeConstantDesc) ) {
      ExprNodeColumnDesc rightColDesc = (ExprNodeColumnDesc) rightExpr;
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) leftExpr;
      int inputCol = columnMap.get(rightColDesc.getColumn());
      String colType = rightColDesc.getTypeString();
      String scalarType = constDesc.getTypeString();
      String className = getBinaryColumnScalarExpressionClassName(colType,
          scalarType, method);
      int outputCol = this.nextOutputColumn++;
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol,
            getScalarValue(constDesc), outputCol);
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
    } else if ( (rightExpr instanceof ExprNodeColumnDesc) &&
        (leftExpr instanceof ExprNodeColumnDesc) ) {
      ExprNodeColumnDesc rightColDesc = (ExprNodeColumnDesc) rightExpr;
      ExprNodeColumnDesc leftColDesc = (ExprNodeColumnDesc) leftExpr;
      int inputCol1 = columnMap.get(rightColDesc.getColumn());
      int inputCol2 = columnMap.get(leftColDesc.getColumn());
      String colType1 = rightColDesc.getTypeString();
      String colType2 = leftColDesc.getTypeString();
      String className = getBinaryColumnColumnExpressionClassName(colType1,
          colType2, method);
      int outputCol = this.nextOutputColumn++;
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2,
            outputCol);
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
    } else if ((leftExpr instanceof ExprNodeGenericFuncDesc)
        && (rightExpr instanceof ExprNodeColumnDesc)) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) rightExpr;
      int outputCol = this.nextOutputColumn++;
      VectorExpression v1 = getVectorExpression(leftExpr);
      int inputCol1 = v1.getOutputColumn();
      int inputCol2 = columnMap.get(colDesc.getColumn());
      String colType1 = v1.getOutputType();
      String colType2 = colDesc.getTypeString();
      String className = getBinaryColumnColumnExpressionClassName(colType1,
          colType2, method);
      this.nextOutputColumn = outputCol+1;
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2,
            outputCol);
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
      expr.setChildExpressions(new VectorExpression [] {v1});
    } else if ((leftExpr instanceof ExprNodeGenericFuncDesc)
        && (rightExpr instanceof ExprNodeConstantDesc)) {
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) rightExpr;
      int outputCol = this.nextOutputColumn++;
      VectorExpression v1 = getVectorExpression(leftExpr);
      int inputCol1 = v1.getOutputColumn();
      String colType1 = v1.getOutputType();
      String scalarType = constDesc.getTypeString();
      String className = getBinaryColumnScalarExpressionClassName(colType1,
          scalarType, method);
      this.nextOutputColumn = outputCol+1;
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1,
            getScalarValue(constDesc), outputCol);
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
      expr.setChildExpressions(new VectorExpression [] {v1});
    } else if ((leftExpr instanceof ExprNodeColumnDesc)
        && (rightExpr instanceof ExprNodeGenericFuncDesc)) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) leftExpr;
      int outputCol = this.nextOutputColumn++;
      VectorExpression v2 = getVectorExpression(rightExpr);
      int inputCol1 = columnMap.get(colDesc.getColumn());
      int inputCol2 = v2.getOutputColumn();
      String colType1 = colDesc.getTypeString();
      String colType2 = v2.getOutputType();
      String className = getBinaryColumnColumnExpressionClassName(colType1,
          colType2, method);
      this.nextOutputColumn = outputCol+1;
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2,
            outputCol);
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
      expr.setChildExpressions(new VectorExpression [] {v2});
    } else if ((leftExpr instanceof ExprNodeConstantDesc)
        && (rightExpr instanceof ExprNodeGenericFuncDesc)) {
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) leftExpr;
      int outputCol = this.nextOutputColumn++;
      VectorExpression v2 = getVectorExpression(rightExpr);
      int inputCol2 = v2.getOutputColumn();
      String colType2 = v2.getOutputType();
      String scalarType = constDesc.getTypeString();
      String className = getBinaryScalarColumnExpressionClassName(colType2,
          scalarType, method);
      this.nextOutputColumn = outputCol+1;
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol2,
            getScalarValue(constDesc), outputCol);
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
      expr.setChildExpressions(new VectorExpression [] {v2});
    } else if ((leftExpr instanceof ExprNodeGenericFuncDesc)
        && (rightExpr instanceof ExprNodeGenericFuncDesc)) {
      //For arithmetic expression, the child expressions must be materializing
      //columns
      int outputCol = this.nextOutputColumn++;
      VectorExpression v1 = getVectorExpression(leftExpr);
      VectorExpression v2 = getVectorExpression(rightExpr);
      int inputCol1 = v1.getOutputColumn();
      int inputCol2 = v2.getOutputColumn();
      String colType1 = v1.getOutputType();
      String colType2 = v2.getOutputType();
      String className = getBinaryColumnColumnExpressionClassName(colType1,
          colType2, method);
      //Reclaim the output columns
      this.nextOutputColumn = outputCol+1;
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2,
            outputCol);
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
      expr.setChildExpressions(new VectorExpression [] {v1, v2});
    }
    return expr;

  }

  private VectorExpression getVectorExpression(GenericUDFOPOr udf,
      List<ExprNodeDesc> childExpr) {
    ExprNodeDesc leftExpr = childExpr.get(0);
    ExprNodeDesc rightExpr = childExpr.get(1);

    VectorExpression ve1;
    VectorExpression ve2;
    if (leftExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) leftExpr;
      int inputCol = columnMap.get(colDesc.getColumn());
      ve1 = new SelectColumnIsTrue(inputCol);
    } else {
      ve1 = getVectorExpression(leftExpr);
    }

    if (rightExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) rightExpr;
      int inputCol = columnMap.get(colDesc.getColumn());
      ve2 = new SelectColumnIsTrue(inputCol);
    } else {
      ve2 = getVectorExpression(leftExpr);
    }

    return new FilterExprOrExpr(ve1,ve2);
  }

  private VectorExpression getVectorExpression(GenericUDFOPNot udf,
      List<ExprNodeDesc> childExpr) {
    ExprNodeDesc expr = childExpr.get(0);
    if (expr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) expr;
      int inputCol = columnMap.get(colDesc.getColumn());
      VectorExpression ve = new SelectColumnIsFalse(inputCol);
      return ve;
    } else {
      VectorExpression ve = getVectorExpression(expr);
      new FilterNotExpr(ve);
    }
    return null;
  }

  private VectorExpression getVectorExpression(GenericUDFOPAnd udf,
      List<ExprNodeDesc> childExpr) {
    ExprNodeDesc leftExpr = childExpr.get(0);
    ExprNodeDesc rightExpr = childExpr.get(1);

    VectorExpression ve1;
    VectorExpression ve2;
    if (leftExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) leftExpr;
      int inputCol = columnMap.get(colDesc.getColumn());
      ve1 = new SelectColumnIsTrue(inputCol);
    } else {
      ve1 = getVectorExpression(leftExpr);
    }

    if (rightExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) rightExpr;
      int inputCol = columnMap.get(colDesc.getColumn());
      ve2 = new SelectColumnIsTrue(inputCol);
    } else {
      ve2 = getVectorExpression(leftExpr);
    }

    return new FilterExprAndExpr(ve1,ve2);
  }

  private VectorExpression getVectorExpression(GenericUDFOPNull udf,
      List<ExprNodeDesc> childExpr) {
    ExprNodeDesc expr = childExpr.get(0);
    VectorExpression ve = null;
    if (expr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) expr;
      int inputCol = columnMap.get(colDesc.getColumn());
      ve = new SelectColumnIsNull(inputCol);
    } else {
      //TODO
    }
    return ve;
  }

  private VectorExpression getVectorExpression(GenericUDFOPNotNull udf,
      List<ExprNodeDesc> childExpr) {
    ExprNodeDesc expr = childExpr.get(0);
    if (expr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) expr;
      int inputCol = columnMap.get(colDesc.getColumn());
      VectorExpression ve = new SelectColumnIsNotNull(inputCol);
      return ve;
    } else {
      //TODO
    }
    return null;
  }

  private Object getScalarValue(ExprNodeConstantDesc constDesc) {
    if (constDesc.getTypeString().equalsIgnoreCase("String")) {
      return ((String) constDesc.getValue()).getBytes();
    } else {
      return constDesc.getValue();
    }
  }

  private VectorExpression getVectorBinaryComparisonFilterExpression(String
      opName, List<ExprNodeDesc> childExpr) {

    ExprNodeDesc leftExpr = childExpr.get(0);
    ExprNodeDesc rightExpr = childExpr.get(1);

    VectorExpression expr = null;
    if ( (leftExpr instanceof ExprNodeColumnDesc) &&
        (rightExpr instanceof ExprNodeConstantDesc) ) {
      ExprNodeColumnDesc leftColDesc = (ExprNodeColumnDesc) leftExpr;
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) rightExpr;
      int inputCol = columnMap.get(leftColDesc.getColumn());
      String colType = leftColDesc.getTypeString();
      String scalarType = constDesc.getTypeString();
      String className = getFilterColumnScalarExpressionClassName(colType,
          scalarType, opName);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol,
            getScalarValue(constDesc));
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
    } else if ( (rightExpr instanceof ExprNodeColumnDesc) &&
        (leftExpr instanceof ExprNodeConstantDesc) ) {
      ExprNodeColumnDesc rightColDesc = (ExprNodeColumnDesc) rightExpr;
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) leftExpr;
      int inputCol = columnMap.get(rightColDesc.getColumn());
      String colType = rightColDesc.getTypeString();
      String scalarType = constDesc.getTypeString();
      String className = getFilterColumnScalarExpressionClassName(colType,
          scalarType, opName);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol,
            getScalarValue(constDesc));
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
    } else if ( (rightExpr instanceof ExprNodeColumnDesc) &&
        (leftExpr instanceof ExprNodeColumnDesc) ) {
      ExprNodeColumnDesc rightColDesc = (ExprNodeColumnDesc) rightExpr;
      ExprNodeColumnDesc leftColDesc = (ExprNodeColumnDesc) leftExpr;
      int inputCol1 = columnMap.get(rightColDesc.getColumn());
      int inputCol2 = columnMap.get(leftColDesc.getColumn());
      String colType1 = rightColDesc.getTypeString();
      String colType2 = leftColDesc.getTypeString();
      String className = getFilterColumnColumnExpressionClassName(colType1,
          colType2, opName);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2);
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
    } else if ( (leftExpr instanceof ExprNodeGenericFuncDesc) &&
        (rightExpr instanceof ExprNodeColumnDesc) ) {
      VectorExpression v1 = getVectorExpression((ExprNodeGenericFuncDesc) leftExpr);
      ExprNodeColumnDesc leftColDesc = (ExprNodeColumnDesc) leftExpr;
      int inputCol1 = v1.getOutputColumn();
      int inputCol2 = columnMap.get(leftColDesc.getColumn());
      String colType1 = v1.getOutputType();
      String colType2 = leftColDesc.getTypeString();
      String className = getFilterColumnColumnExpressionClassName(colType1,
          colType2, opName);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2);
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
      expr.setChildExpressions(new VectorExpression [] {v1});
    } else if ( (leftExpr instanceof ExprNodeColumnDesc) &&
        (rightExpr instanceof ExprNodeGenericFuncDesc) ) {
      ExprNodeColumnDesc rightColDesc = (ExprNodeColumnDesc) leftExpr;
      VectorExpression v2 = getVectorExpression((ExprNodeGenericFuncDesc) rightExpr);
      int inputCol1 = columnMap.get(rightColDesc.getColumn());
      int inputCol2 = v2.getOutputColumn();
      String colType1 = rightColDesc.getTypeString();
      String colType2 = v2.getOutputType();
      String className = getFilterColumnColumnExpressionClassName(colType1,
          colType2, opName);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2);
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
      expr.setChildExpressions(new VectorExpression [] {v2});
    } else if ( (leftExpr instanceof ExprNodeGenericFuncDesc) &&
        (rightExpr instanceof ExprNodeConstantDesc) ) {
      VectorExpression v1 = getVectorExpression((ExprNodeGenericFuncDesc) leftExpr);
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) leftExpr;
      int inputCol1 = v1.getOutputColumn();
      String colType1 = v1.getOutputType();
      String scalarType = constDesc.getTypeString();
      String className = getFilterColumnScalarExpressionClassName(colType1,
          scalarType, opName);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1,
            getScalarValue(constDesc));
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
      expr.setChildExpressions(new VectorExpression [] {v1});
    } else if ( (leftExpr instanceof ExprNodeConstantDesc) &&
        (rightExpr instanceof ExprNodeGenericFuncDesc) ) {
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) leftExpr;
      VectorExpression v2 = getVectorExpression((ExprNodeGenericFuncDesc) rightExpr);
      int inputCol2 = v2.getOutputColumn();
      String scalarType = constDesc.getTypeString();
      String colType = v2.getOutputType();
      String className = getFilterScalarColumnExpressionClassName(colType,
          scalarType, opName);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol2,
            getScalarValue(constDesc));
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
      expr.setChildExpressions(new VectorExpression [] {v2});
    } else {
      //For comparison expression, the child expressions must be materializing
      //columns
      VectorExpression v1 = getVectorExpression(leftExpr);
      VectorExpression v2 = getVectorExpression(rightExpr);
      int inputCol1 = v1.getOutputColumn();
      int inputCol2 = v2.getOutputColumn();
      String colType1 = v1.getOutputType();
      String colType2 = v2.getOutputType();
      String className = getFilterColumnColumnExpressionClassName(colType1,
          colType2, opName);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2);
      } catch (Exception ex) {
        throw new RuntimeException((ex));
      }
      expr.setChildExpressions(new VectorExpression [] {v1, v2});
    }
    return expr;
  }

  private String getNormalizedTypeName(String colType) {
    String normalizedType = null;
    if (colType.equalsIgnoreCase("Double") || colType.equalsIgnoreCase("Float")) {
      normalizedType = "Double";
    } else if (colType.equalsIgnoreCase("String")) {
      normalizedType = "String";
    } else {
      normalizedType = "Long";
    }
    return normalizedType;
  }

  private String getFilterColumnColumnExpressionClassName(String colType1,
      String colType2, String opName) {
    StringBuilder b = new StringBuilder();
    b.append("org.apache.hadoop.hive.ql.exec.vector.expressions.gen.");
    if (opType.equals(OperatorType.FILTER)) {
      b.append("Filter");
    }
    b.append(getNormalizedTypeName(colType1));
    b.append("Col");
    b.append(opName);
    b.append(getNormalizedTypeName(colType2));
    b.append("Column");
    return b.toString();
  }

  private String getFilterColumnScalarExpressionClassName(String colType, String
      scalarType, String opName) {
    StringBuilder b = new StringBuilder();
    b.append("org.apache.hadoop.hive.ql.exec.vector.expressions.gen.");
    if (opType.equals(OperatorType.FILTER)) {
      b.append("Filter");
    }
    b.append(getNormalizedTypeName(colType));
    b.append("Col");
    b.append(opName);
    b.append(getNormalizedTypeName(scalarType));
    b.append("Scalar");
    return b.toString();
  }

  private String getFilterScalarColumnExpressionClassName(String colType, String
      scalarType, String opName) {
    StringBuilder b = new StringBuilder();
    b.append("org.apache.hadoop.hive.ql.exec.vector.expressions.gen.");
    if (opType.equals(OperatorType.FILTER)) {
      b.append("Filter");
    }
    b.append(getNormalizedTypeName(scalarType));
    b.append("Scalar");
    b.append(opName);
    b.append(getNormalizedTypeName(colType));
    b.append("Column");
    return b.toString();
  }

  private String getBinaryColumnScalarExpressionClassName(String colType,
      String scalarType, String method) {
    StringBuilder b = new StringBuilder();
    b.append("org.apache.hadoop.hive.ql.exec.vector.expressions.gen.");
    b.append(getNormalizedTypeName(colType));
    b.append("Col");
    b.append(method);
    b.append(getNormalizedTypeName(scalarType));
    b.append("Scalar");
    return b.toString();
  }

  private String getBinaryScalarColumnExpressionClassName(String colType,
      String scalarType, String method) {
    StringBuilder b = new StringBuilder();
    b.append("org.apache.hadoop.hive.ql.exec.vector.expressions.gen.");
    b.append(this.getNormalizedTypeName(scalarType));
    b.append("Scalar");
    b.append(method);
    b.append(this.getNormalizedTypeName(colType));
    b.append("Column");
    return b.toString();
  }

  private String getBinaryColumnColumnExpressionClassName(String colType1,
      String colType2, String method) {
    StringBuilder b = new StringBuilder();
    b.append("org.apache.hadoop.hive.ql.exec.vector.expressions.gen.");
    b.append(getNormalizedTypeName(colType1));
    b.append("Col");
    b.append(method);
    b.append(getNormalizedTypeName(colType2));
    b.append("Column");
    return b.toString();
  }

  static Object[][] aggregatesDefinition = {
    {"min",       "Long",   VectorUDAFMinLong.class},
    {"min",       "Double", VectorUDAFMinDouble.class},
    {"max",       "Long",   VectorUDAFMaxLong.class},
    {"max",       "Double", VectorUDAFMaxDouble.class},
    {"count",     "Long",   VectorUDAFCountLong.class},
    {"count",     "Double", VectorUDAFCountDouble.class},
    {"sum",       "Long",   VectorUDAFSumLong.class},
    {"sum",       "Double", VectorUDAFSumDouble.class},
    {"avg",       "Long",   VectorUDAFAvgLong.class},
    {"avg",       "Double", VectorUDAFAvgDouble.class},
    {"variance",  "Long",   VectorUDAFVarPopLong.class},
    {"var_pop",   "Long",   VectorUDAFVarPopLong.class},
    {"variance",  "Double", VectorUDAFVarPopDouble.class},
    {"var_pop",   "Double", VectorUDAFVarPopDouble.class},
    {"var_samp",  "Long",   VectorUDAFVarSampLong.class},
    {"var_samp" , "Double", VectorUDAFVarSampDouble.class},
    {"std",       "Long",   VectorUDAFStdPopLong.class},
    {"stddev",    "Long",   VectorUDAFStdPopLong.class},
    {"stddev_pop","Long",   VectorUDAFStdPopLong.class},
    {"std",       "Long",   VectorUDAFStdPopDouble.class},
    {"stddev",    "Long",   VectorUDAFStdPopDouble.class},
    {"stddev_pop","Long",   VectorUDAFStdPopDouble.class},
    {"stddev_samp","Long",  VectorUDAFStdSampLong.class},
    {"stddev_samp","Double",VectorUDAFStdSampDouble.class},
  };
  
  public VectorAggregateExpression getAggregatorExpression(AggregationDesc desc) 
      throws HiveException {
    ArrayList<ExprNodeDesc> paramDescList = desc.getParameters();
    VectorExpression[] vectorParams = new VectorExpression[paramDescList.size()];

    for (int i = 0; i< paramDescList.size(); ++i) {
      ExprNodeDesc exprDesc = paramDescList.get(i);
      vectorParams[i] = this.getVectorExpression(exprDesc);
    }
    
    String aggregateName = desc.getGenericUDAFName();
    List<ExprNodeDesc> params = desc.getParameters();
    //TODO: handle length != 1
    assert (params.size() == 1);
    ExprNodeDesc inputExpr = params.get(0);
    String inputType = getNormalizedTypeName(inputExpr.getTypeString());

    for (Object[] aggDef : aggregatesDefinition) {
      if (aggDef[0].equals (aggregateName) &&
          aggDef[1].equals(inputType)) {
        Class<? extends VectorAggregateExpression> aggClass = 
            (Class<? extends VectorAggregateExpression>) (aggDef[2]);
        try
        {
          Constructor<? extends VectorAggregateExpression> ctor = 
              aggClass.getConstructor(VectorExpression.class);
          VectorAggregateExpression aggExpr = ctor.newInstance(vectorParams[0]);
          return aggExpr;
        }
        // TODO: change to 1.7 syntax when possible
        //catch (InvocationTargetException | IllegalAccessException 
        // | NoSuchMethodException | InstantiationException)
        catch (Exception e)
        {
          throw new HiveException("Internal exception for vector aggregate : \"" + 
               aggregateName + "\" for type: \"" + inputType + "", e);
        }
      }
    }

    throw new HiveException("Vector aggregate not implemented: \"" + aggregateName + 
        "\" for type: \"" + inputType + "");
  }
  
  static Object[][] columnTypes = {
    {"Double",  DoubleColumnVector.class},
    {"Long",    LongColumnVector.class},
    {"String",  BytesColumnVector.class},
  };

  public VectorizedRowBatch allocateRowBatch(int rowCount) throws HiveException {
    VectorizedRowBatch ret = new VectorizedRowBatch(nextOutputColumn, rowCount);
    for (int i=0; i < nextOutputColumn; ++i) {
      if (false == outputColumnTypes.containsKey(i)) {
        continue;
      }
      String columnTypeName = outputColumnTypes.get(i);
      for (Object[] columnType: columnTypes) {
        if (columnTypeName.equalsIgnoreCase((String)columnType[0])) {
          Class<? extends ColumnVector> columnTypeClass = (Class<? extends ColumnVector>)columnType[1];
          try {
            Constructor<? extends ColumnVector> ctor = columnTypeClass.getConstructor(int.class);
            ret.cols[i] = ctor.newInstance(rowCount);
          }
          catch(Exception e) {
            throw new HiveException (
                String.format(
                    "Internal exception occured trying to allocate a vectorized column %d of type %s",
                    i, columnTypeName),
                e);
          }
        }
      }
    }
    return ret;
  }

  Object[][] mapObjectInspectors = {
      {"double", PrimitiveObjectInspectorFactory.writableDoubleObjectInspector},
      {"long", PrimitiveObjectInspectorFactory.writableLongObjectInspector},
  };

  public ObjectInspector getVectorRowObjectInspector(List<String> columnNames) throws HiveException {
    List<ObjectInspector> oids = new ArrayList<ObjectInspector>();
    for(String columnName: columnNames) {
      int columnIndex = columnMap.get(columnName);
      String outputType = outputColumnTypes.get(columnIndex);
      ObjectInspector oi = null;
      for(Object[] moi: mapObjectInspectors) {
        if (outputType.equalsIgnoreCase((String) moi[0])) {
          oi = (ObjectInspector) moi[1];
          break;
        }
      }
      if (oi == null) {
        throw new HiveException(String.format("Unsuported type: %s for column %d:%s",
            outputType, columnIndex, columnName));
      }
      oids.add(oi);
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, oids);
  }
}

