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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprAndExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprOrExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterNotExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsFalse;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsNotNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsTrue;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFUnixTimeStampLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFCount;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFCountStar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinString;
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
import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFHour;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFOPDivide;
import org.apache.hadoop.hive.ql.udf.UDFOPMinus;
import org.apache.hadoop.hive.ql.udf.UDFOPMod;
import org.apache.hadoop.hive.ql.udf.UDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.UDFOPNegative;
import org.apache.hadoop.hive.ql.udf.UDFOPPlus;
import org.apache.hadoop.hive.ql.udf.UDFOPPositive;
import org.apache.hadoop.hive.ql.udf.UDFSecond;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.ql.udf.UDFYear;
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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

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
  private final int firstOutputColumnIndex;

  private OperatorType opType;
  //Map column number to type
  private final OutputColumnManager ocm;

  public VectorizationContext(Map<String, Integer> columnMap,
      int initialOutputCol) {
    this.columnMap = columnMap;
    this.ocm = new OutputColumnManager(initialOutputCol);
    this.firstOutputColumnIndex = initialOutputCol;
  }

  private int getInputColumnIndex(String name) {
    if (columnMap == null) {
      //Null is treated as test call, is used for validation test.
      return 0;
    } else {
      return columnMap.get(name);
    }
  }

  private class OutputColumnManager {
    private final int initialOutputCol;
    private int outputColCount = 0;

    OutputColumnManager(int initialOutputCol) {
      this.initialOutputCol = initialOutputCol;
    }

    //The complete list of output columns. These should be added to the
    //Vectorized row batch for processing. The index in the row batch is
    //equal to the index in this array plus initialOutputCol.
    //Start with size 100 and double when needed.
    private String [] outputColumnsTypes = new String[100];

    private final Set<Integer> usedOutputColumns = new HashSet<Integer>();

    int allocateOutputColumn(String columnType) {
      return initialOutputCol + allocateOutputColumnInternal(columnType);
    }

    private int allocateOutputColumnInternal(String columnType) {
      for (int i = 0; i < outputColCount; i++) {
        if (usedOutputColumns.contains(i) ||
            !(outputColumnsTypes)[i].equals(columnType)) {
          continue;
        }
        //Use i
        usedOutputColumns.add(i);
        return i;
      }
      //Out of allocated columns
      if (outputColCount < outputColumnsTypes.length) {
        int newIndex = outputColCount;
        outputColumnsTypes[outputColCount++] = columnType;
        usedOutputColumns.add(newIndex);
        return newIndex;
      } else {
        //Expand the array
        outputColumnsTypes = Arrays.copyOf(outputColumnsTypes, 2*outputColCount);
        int newIndex = outputColCount;
        outputColumnsTypes[outputColCount++] = columnType;
        usedOutputColumns.add(newIndex);
        return newIndex;
      }
    }

    void freeOutputColumn(int index) {
      int colIndex = index-initialOutputCol;
      if (colIndex >= 0) {
        usedOutputColumns.remove(index-initialOutputCol);
      }
    }

    String getOutputColumnType(int index) {
      return outputColumnsTypes[index-initialOutputCol];
    }

    int getNumOfOutputColumn() {
      return outputColCount;
    }
  }

  public void setOperatorType(OperatorType opType) {
    this.opType = opType;
  }

  private VectorExpression getVectorExpression(ExprNodeColumnDesc
      exprDesc) {
    int columnNum = getInputColumnIndex(exprDesc.getColumn());
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

  public VectorExpression[] getVectorExpressions(List<ExprNodeDesc> exprNodes) throws HiveException {
    int i = 0;
    if (null == exprNodes) {
      return new VectorExpression[0];
    }
    VectorExpression[] ret = new VectorExpression[exprNodes.size()];
    for (ExprNodeDesc e : exprNodes) {
      ret[i++] = getVectorExpression(e);
    }
    return ret;
  }

  /**
   * Returns a vector expression for a given expression
   * description.
   * @param exprDesc, Expression description
   * @return {@link VectorExpression}
   * @throws HiveException
   */
  public VectorExpression getVectorExpression(ExprNodeDesc exprDesc) throws HiveException {
    VectorExpression ve = null;
    if (exprDesc instanceof ExprNodeColumnDesc) {
      ve = getVectorExpression((ExprNodeColumnDesc) exprDesc);
    } else if (exprDesc instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc expr = (ExprNodeGenericFuncDesc) exprDesc;
      ve = getVectorExpression(expr.getGenericUDF(),
          expr.getChildExprs());
    } else if (exprDesc instanceof ExprNodeConstantDesc) {
      ve = getConstantVectorExpression((ExprNodeConstantDesc) exprDesc);
    }
    if (ve == null) {
      throw new HiveException("Could not vectorize expression: "+exprDesc.getName());
    }
    return ve;
  }

  /**
   * Handles only the special case of unary operators on a constant.
   * @param exprDesc
   * @return The same expression if no folding done, else return the constant
   *         expression.
   * @throws HiveException
   */
  private ExprNodeDesc foldConstantsForUnaryExpression(ExprNodeDesc exprDesc) throws HiveException {
    if (!(exprDesc instanceof ExprNodeGenericFuncDesc)) {
      return exprDesc;
    }

    if (exprDesc.getChildren() == null || (exprDesc.getChildren().size() != 1) ||
        (!( exprDesc.getChildren().get(0) instanceof ExprNodeConstantDesc))) {
      return exprDesc;
    }

    GenericUDF gudf = ((ExprNodeGenericFuncDesc) exprDesc).getGenericUDF();
    if (!(gudf instanceof GenericUDFBridge)) {
      return exprDesc;
    }

    Class<? extends UDF> cl = ((GenericUDFBridge) gudf).getUdfClass();

    ExprNodeConstantDesc constExpr = (ExprNodeConstantDesc) exprDesc.getChildren().get(0);

    if (cl.equals(UDFOPNegative.class) || cl.equals(UDFOPPositive.class)) {
      ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(exprDesc);
      ObjectInspector output = evaluator.initialize(null);

      Object constant = evaluator.evaluate(null);
      Object java = ObjectInspectorUtils.copyToStandardJavaObject(constant, output);
      return new ExprNodeConstantDesc(java);
    } else {
      return exprDesc;
    }
  }

  private VectorExpression getConstantVectorExpression(ExprNodeConstantDesc exprDesc)
      throws HiveException {
    String type = exprDesc.getTypeString();
    String colVectorType = this.getOutputColType(type, "constant");
    int outCol = ocm.allocateOutputColumn(colVectorType);
    if (type.equalsIgnoreCase("long") || type.equalsIgnoreCase("int") ||
        type.equalsIgnoreCase("short") || type.equalsIgnoreCase("byte")) {
      return new ConstantVectorExpression(outCol,
          ((Number) exprDesc.getValue()).longValue());
    } else if (type.equalsIgnoreCase("double") || type.equalsIgnoreCase("float")) {
      return new ConstantVectorExpression(outCol, ((Number) exprDesc.getValue()).doubleValue());
    } else if (type.equalsIgnoreCase("string")) {
      return new ConstantVectorExpression(outCol, ((String) exprDesc.getValue()).getBytes());
    } else {
      throw new HiveException("Unsupported constant type");
    }
  }

  private VectorExpression getUnaryMinusExpression(List<ExprNodeDesc> childExprList)
      throws HiveException {
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
      throw new HiveException("Expression not supported: "+childExpr);
    }
    int outputCol = ocm.allocateOutputColumn(colType);
    String className = getNormalizedTypeName(colType) + "colUnaryMinus";
    VectorExpression expr;
    try {
      expr = (VectorExpression) Class.forName(className).
          getDeclaredConstructors()[0].newInstance(inputCol, outputCol);
    } catch (Exception ex) {
      throw new HiveException(ex);
    }
    if (v1 != null) {
      expr.setChildExpressions(new VectorExpression [] {v1});
      ocm.freeOutputColumn(v1.getOutputColumn());
    }
    return expr;
  }

  private VectorExpression getUnaryPlusExpression(List<ExprNodeDesc> childExprList)
      throws HiveException {
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
      inputCol = getInputColumnIndex(colDesc.getColumn());
      colType = colDesc.getTypeString();
    } else {
      throw new HiveException("Expression not supported: "+childExpr);
    }
    VectorExpression expr = new IdentityExpression(inputCol, colType);
    if (v1 != null) {
      expr.setChildExpressions(new VectorExpression [] {v1});
    }
    return expr;
  }

  private VectorExpression getVectorExpression(GenericUDF udf,
      List<ExprNodeDesc> childExpr) throws HiveException {
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
    } else if(udf instanceof GenericUDFToUnixTimeStamp) {
      return getVectorExpression((GenericUDFToUnixTimeStamp) udf, childExpr);
    }
    throw new HiveException("Udf: "+udf.getClass().getSimpleName()+", is not supported");
  }

  private VectorExpression getVectorExpression(GenericUDFToUnixTimeStamp udf,
      List<ExprNodeDesc> childExpr) throws HiveException {
    ExprNodeDesc leftExpr = childExpr.get(0);
    leftExpr = foldConstantsForUnaryExpression(leftExpr);
    VectorExpression v1 = getVectorExpression(leftExpr);
    String colType = v1.getOutputType();
    String outputType = "long";
    if(colType.equalsIgnoreCase("timestamp")) {
      int inputCol = v1.getOutputColumn();
      int outputCol = ocm.allocateOutputColumn(outputType);
      try {
        VectorExpression v2 = new VectorUDFUnixTimeStampLong(inputCol, outputCol);
        return v2;
      } catch(Exception e) {
        e.printStackTrace();
        throw new HiveException("Udf: Vector"+udf+", could not be initialized for " + colType, e);
      }
    }
    throw new HiveException("Udf: "+udf.getClass().getSimpleName()+", is not supported");
  }

  private VectorExpression getVectorExpression(GenericUDFBridge udf,
      List<ExprNodeDesc> childExpr) throws HiveException {
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
    } else if (cl.equals(UDFYear.class) ||
        cl.equals(UDFMonth.class) ||
        cl.equals(UDFWeekOfYear.class) ||
        cl.equals(UDFDayOfMonth.class) ||
        cl.equals(UDFHour.class) ||
        cl.equals(UDFMinute.class) ||
        cl.equals(UDFSecond.class)) {
      return getTimestampFieldExpression(cl.getSimpleName(), childExpr);
    }

    throw new HiveException("Udf: "+udf.getClass().getSimpleName()+", is not supported");
  }

  private VectorExpression getTimestampFieldExpression(String udf,
      List<ExprNodeDesc> childExpr) throws HiveException {
    ExprNodeDesc leftExpr = childExpr.get(0);
    leftExpr = foldConstantsForUnaryExpression(leftExpr);
    VectorExpression v1 = getVectorExpression(leftExpr);
    String colType = v1.getOutputType();
    String outputType = "long";
    if(colType.equalsIgnoreCase("timestamp")) {
        int inputCol = v1.getOutputColumn();
        int outputCol = ocm.allocateOutputColumn(outputType);
        String pkg = "org.apache.hadoop.hive.ql.exec.vector.expressions";
        // org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFYearLong
        String vectorUDF = pkg + ".Vector"+udf+"Long";
        try {
          VectorExpression v2 = (VectorExpression)Class.forName(vectorUDF).
              getDeclaredConstructors()[0].newInstance(inputCol,outputCol);
          return v2;
        } catch(Exception e) {
          e.printStackTrace();
          throw new HiveException("Udf: Vector"+udf+", could not be initialized for " + colType, e);
        }
    }
    throw new HiveException("Udf: "+udf+", is not supported for " + colType);
  }

  private VectorExpression getBinaryArithmeticExpression(String method,
      List<ExprNodeDesc> childExpr) throws HiveException {
    ExprNodeDesc leftExpr = childExpr.get(0);
    ExprNodeDesc rightExpr = childExpr.get(1);

    // TODO: Remove this when constant folding is fixed in the optimizer.
    leftExpr = foldConstantsForUnaryExpression(leftExpr);
    rightExpr = foldConstantsForUnaryExpression(rightExpr);

    VectorExpression v1 = null;
    VectorExpression v2 = null;

    VectorExpression expr = null;
    if ( (leftExpr instanceof ExprNodeColumnDesc) &&
        (rightExpr instanceof ExprNodeConstantDesc) ) {
      ExprNodeColumnDesc leftColDesc = (ExprNodeColumnDesc) leftExpr;
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) rightExpr;
      int inputCol = getInputColumnIndex(leftColDesc.getColumn());
      String colType = leftColDesc.getTypeString();
      String scalarType = constDesc.getTypeString();
      String className = getBinaryColumnScalarExpressionClassName(colType,
          scalarType, method);
      int outputCol = ocm.allocateOutputColumn(getOutputColType(colType,
          scalarType, method));
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol,
            getScalarValue(constDesc), outputCol);
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
    } else if ( (leftExpr instanceof ExprNodeConstantDesc) &&
        (rightExpr instanceof ExprNodeColumnDesc) ) {
      ExprNodeColumnDesc rightColDesc = (ExprNodeColumnDesc) rightExpr;
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) leftExpr;
      int inputCol = getInputColumnIndex(rightColDesc.getColumn());
      String colType = rightColDesc.getTypeString();
      String scalarType = constDesc.getTypeString();
      String className = getBinaryScalarColumnExpressionClassName(colType,
          scalarType, method);
      String outputColType = getOutputColType(colType, scalarType, method);
      int outputCol = ocm.allocateOutputColumn(outputColType);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(getScalarValue(constDesc),
            inputCol, outputCol);
      } catch (Exception ex) {
        throw new HiveException("Could not instantiate: "+className, ex);
      }
    } else if ( (rightExpr instanceof ExprNodeColumnDesc) &&
        (leftExpr instanceof ExprNodeColumnDesc) ) {
      ExprNodeColumnDesc leftColDesc = (ExprNodeColumnDesc) leftExpr;
      ExprNodeColumnDesc rightColDesc = (ExprNodeColumnDesc) rightExpr;
      int inputCol1 = getInputColumnIndex(leftColDesc.getColumn());
      int inputCol2 = getInputColumnIndex(rightColDesc.getColumn());
      String colType1 = leftColDesc.getTypeString();
      String colType2 = rightColDesc.getTypeString();
      String outputColType = getOutputColType(colType1, colType2, method);
      String className = getBinaryColumnColumnExpressionClassName(colType1,
          colType2, method);
      int outputCol = ocm.allocateOutputColumn(outputColType);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2,
            outputCol);
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
    } else if ((leftExpr instanceof ExprNodeGenericFuncDesc)
        && (rightExpr instanceof ExprNodeColumnDesc)) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) rightExpr;
      v1 = getVectorExpression(leftExpr);
      int inputCol1 = v1.getOutputColumn();
      int inputCol2 = getInputColumnIndex(colDesc.getColumn());
      String colType1 = v1.getOutputType();
      String colType2 = colDesc.getTypeString();
      String outputColType = getOutputColType(colType1, colType2, method);
      String className = getBinaryColumnColumnExpressionClassName(colType1,
          colType2, method);
      int outputCol = ocm.allocateOutputColumn(outputColType);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2,
            outputCol);
      } catch (Exception ex) {
        throw new HiveException((ex));
      }
      expr.setChildExpressions(new VectorExpression [] {v1});
    } else if ((leftExpr instanceof ExprNodeGenericFuncDesc)
        && (rightExpr instanceof ExprNodeConstantDesc)) {
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) rightExpr;
      v1 = getVectorExpression(leftExpr);
      int inputCol1 = v1.getOutputColumn();
      String colType1 = v1.getOutputType();
      String scalarType = constDesc.getTypeString();
      String outputColType = getOutputColType(colType1, scalarType, method);
      int outputCol = ocm.allocateOutputColumn(outputColType);
      String className = getBinaryColumnScalarExpressionClassName(colType1,
          scalarType, method);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1,
            getScalarValue(constDesc), outputCol);
      } catch (Exception ex) {
        throw new HiveException((ex));
      }
      expr.setChildExpressions(new VectorExpression [] {v1});
    } else if ((leftExpr instanceof ExprNodeColumnDesc)
        && (rightExpr instanceof ExprNodeGenericFuncDesc)) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) leftExpr;
      v2 = getVectorExpression(rightExpr);
      int inputCol1 = getInputColumnIndex(colDesc.getColumn());
      int inputCol2 = v2.getOutputColumn();
      String colType1 = colDesc.getTypeString();
      String colType2 = v2.getOutputType();
      String outputColType = getOutputColType(colType1, colType2, method);
      int outputCol = ocm.allocateOutputColumn(outputColType);
      String className = getBinaryColumnColumnExpressionClassName(colType1,
          colType2, method);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2,
            outputCol);
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
      expr.setChildExpressions(new VectorExpression [] {v2});
    } else if ((leftExpr instanceof ExprNodeConstantDesc)
        && (rightExpr instanceof ExprNodeGenericFuncDesc)) {
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) leftExpr;
      v2 = getVectorExpression(rightExpr);
      int inputCol2 = v2.getOutputColumn();
      String colType2 = v2.getOutputType();
      String scalarType = constDesc.getTypeString();
      String outputColType = getOutputColType(colType2, scalarType, method);
      int outputCol = ocm.allocateOutputColumn(outputColType);
      String className = getBinaryScalarColumnExpressionClassName(colType2,
          scalarType, method);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol2,
            getScalarValue(constDesc), outputCol);
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
      expr.setChildExpressions(new VectorExpression [] {v2});
    } else if ((leftExpr instanceof ExprNodeGenericFuncDesc)
        && (rightExpr instanceof ExprNodeGenericFuncDesc)) {
      //For arithmetic expression, the child expressions must be materializing
      //columns
      v1 = getVectorExpression(leftExpr);
      v2 = getVectorExpression(rightExpr);
      int inputCol1 = v1.getOutputColumn();
      int inputCol2 = v2.getOutputColumn();
      String colType1 = v1.getOutputType();
      String colType2 = v2.getOutputType();
      String outputColType = getOutputColType(colType1, colType2, method);
      int outputCol = ocm.allocateOutputColumn(outputColType);
      String className = getBinaryColumnColumnExpressionClassName(colType1,
          colType2, method);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2,
            outputCol);
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
      expr.setChildExpressions(new VectorExpression [] {v1, v2});
    }
    //Reclaim output columns of children to be re-used later
    if (v1 != null) {
      ocm.freeOutputColumn(v1.getOutputColumn());
    }
    if (v2 != null) {
      ocm.freeOutputColumn(v2.getOutputColumn());
    }
    return expr;
  }

  private VectorExpression getVectorExpression(GenericUDFOPOr udf,
      List<ExprNodeDesc> childExpr) throws HiveException {
    ExprNodeDesc leftExpr = childExpr.get(0);
    ExprNodeDesc rightExpr = childExpr.get(1);

    VectorExpression ve1;
    VectorExpression ve2;
    if (leftExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) leftExpr;
      int inputCol = getInputColumnIndex(colDesc.getColumn());
      ve1 = new SelectColumnIsTrue(inputCol);
    } else {
      ve1 = getVectorExpression(leftExpr);
    }

    if (rightExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) rightExpr;
      int inputCol = getInputColumnIndex(colDesc.getColumn());
      ve2 = new SelectColumnIsTrue(inputCol);
    } else {
      ve2 = getVectorExpression(rightExpr);
    }

    return new FilterExprOrExpr(ve1,ve2);
  }

  private VectorExpression getVectorExpression(GenericUDFOPNot udf,
      List<ExprNodeDesc> childExpr) throws HiveException {
    ExprNodeDesc expr = childExpr.get(0);
    if (expr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) expr;
      int inputCol = getInputColumnIndex(colDesc.getColumn());
      VectorExpression ve = new SelectColumnIsFalse(inputCol);
      return ve;
    } else {
      VectorExpression ve = getVectorExpression(expr);
      return new FilterNotExpr(ve);
    }
  }

  private VectorExpression getVectorExpression(GenericUDFOPAnd udf,
      List<ExprNodeDesc> childExpr) throws HiveException {

    ExprNodeDesc leftExpr = childExpr.get(0);
    ExprNodeDesc rightExpr = childExpr.get(1);

    VectorExpression ve1;
    VectorExpression ve2;
    if (leftExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) leftExpr;
      int inputCol = getInputColumnIndex(colDesc.getColumn());
      ve1 = new SelectColumnIsTrue(inputCol);
    } else {
      ve1 = getVectorExpression(leftExpr);
    }

    if (rightExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) rightExpr;
      int inputCol = getInputColumnIndex(colDesc.getColumn());
      ve2 = new SelectColumnIsTrue(inputCol);
    } else {
      ve2 = getVectorExpression(rightExpr);
    }

    return new FilterExprAndExpr(ve1,ve2);
  }

  private VectorExpression getVectorExpression(GenericUDFOPNull udf,
      List<ExprNodeDesc> childExpr) throws HiveException {
    ExprNodeDesc expr = childExpr.get(0);
    VectorExpression ve = null;
    if (expr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) expr;
      int inputCol = getInputColumnIndex(colDesc.getColumn());
      ve = new SelectColumnIsNull(inputCol);
    } else {
      throw new HiveException("Not supported");
    }
    return ve;
  }

  private VectorExpression getVectorExpression(GenericUDFOPNotNull udf,
      List<ExprNodeDesc> childExpr) throws HiveException {
    ExprNodeDesc expr = childExpr.get(0);
    if (expr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) expr;
      int inputCol = getInputColumnIndex(colDesc.getColumn());
      VectorExpression ve = new SelectColumnIsNotNull(inputCol);
      return ve;
    } else {
      throw new HiveException("Not supported");
    }
  }

  private Object getScalarValue(ExprNodeConstantDesc constDesc) {
    if (constDesc.getTypeString().equalsIgnoreCase("String")) {
      return ((String) constDesc.getValue()).getBytes();
    } else {
      return constDesc.getValue();
    }
  }

  private VectorExpression getVectorBinaryComparisonFilterExpression(String
      opName, List<ExprNodeDesc> childExpr) throws HiveException {

    ExprNodeDesc leftExpr = childExpr.get(0);
    ExprNodeDesc rightExpr = childExpr.get(1);

    // TODO: Remove this when constant folding is fixed in the optimizer.
    leftExpr = foldConstantsForUnaryExpression(leftExpr);
    rightExpr = foldConstantsForUnaryExpression(rightExpr);

    VectorExpression expr = null;
    VectorExpression v1 = null;
    VectorExpression v2 = null;
    if ( (leftExpr instanceof ExprNodeColumnDesc) &&
        (rightExpr instanceof ExprNodeConstantDesc) ) {
      ExprNodeColumnDesc leftColDesc = (ExprNodeColumnDesc) leftExpr;
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) rightExpr;
      int inputCol = getInputColumnIndex(leftColDesc.getColumn());
      String colType = leftColDesc.getTypeString();
      String scalarType = constDesc.getTypeString();
      String className = getFilterColumnScalarExpressionClassName(colType,
          scalarType, opName);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol,
            getScalarValue(constDesc));
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
    } else if ( (rightExpr instanceof ExprNodeColumnDesc) &&
        (leftExpr instanceof ExprNodeConstantDesc) ) {
      ExprNodeColumnDesc rightColDesc = (ExprNodeColumnDesc) rightExpr;
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) leftExpr;
      int inputCol = getInputColumnIndex(rightColDesc.getColumn());
      String colType = rightColDesc.getTypeString();
      String scalarType = constDesc.getTypeString();
      String className = getFilterColumnScalarExpressionClassName(colType,
          scalarType, opName);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol,
            getScalarValue(constDesc));
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
    } else if ( (rightExpr instanceof ExprNodeColumnDesc) &&
        (leftExpr instanceof ExprNodeColumnDesc) ) {
      ExprNodeColumnDesc leftColDesc = (ExprNodeColumnDesc) leftExpr;
      ExprNodeColumnDesc rightColDesc = (ExprNodeColumnDesc) rightExpr;
      int inputCol1 = getInputColumnIndex(leftColDesc.getColumn());
      int inputCol2 = getInputColumnIndex(rightColDesc.getColumn());
      String colType1 = leftColDesc.getTypeString();
      String colType2 = rightColDesc.getTypeString();
      String className = getFilterColumnColumnExpressionClassName(colType1,
          colType2, opName);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2);
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
    } else if ( (leftExpr instanceof ExprNodeGenericFuncDesc) &&
        (rightExpr instanceof ExprNodeColumnDesc) ) {
      v1 = getVectorExpression((ExprNodeGenericFuncDesc) leftExpr);
      ExprNodeColumnDesc rightColDesc = (ExprNodeColumnDesc) rightExpr;
      int inputCol1 = v1.getOutputColumn();
      int inputCol2 = getInputColumnIndex(rightColDesc.getColumn());
      String colType1 = v1.getOutputType();
      String colType2 = rightColDesc.getTypeString();
      String className = getFilterColumnColumnExpressionClassName(colType1,
          colType2, opName);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2);
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
      expr.setChildExpressions(new VectorExpression [] {v1});
    } else if ( (leftExpr instanceof ExprNodeColumnDesc) &&
        (rightExpr instanceof ExprNodeGenericFuncDesc) ) {
      ExprNodeColumnDesc rightColDesc = (ExprNodeColumnDesc) leftExpr;
      v2 = getVectorExpression((ExprNodeGenericFuncDesc) rightExpr);
      int inputCol1 = getInputColumnIndex(rightColDesc.getColumn());
      int inputCol2 = v2.getOutputColumn();
      String colType1 = rightColDesc.getTypeString();
      String colType2 = v2.getOutputType();
      String className = getFilterColumnColumnExpressionClassName(colType1,
          colType2, opName);
      try {
        expr = (VectorExpression) Class.forName(className).
            getDeclaredConstructors()[0].newInstance(inputCol1, inputCol2);
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
      expr.setChildExpressions(new VectorExpression [] {v2});
    } else if ( (leftExpr instanceof ExprNodeGenericFuncDesc) &&
        (rightExpr instanceof ExprNodeConstantDesc) ) {
      v1 = getVectorExpression((ExprNodeGenericFuncDesc) leftExpr);
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) rightExpr;
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
        throw new HiveException(ex);
      }
      expr.setChildExpressions(new VectorExpression [] {v1});
    } else if ( (leftExpr instanceof ExprNodeConstantDesc) &&
        (rightExpr instanceof ExprNodeGenericFuncDesc) ) {
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) leftExpr;
      v2 = getVectorExpression((ExprNodeGenericFuncDesc) rightExpr);
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
        throw new HiveException(ex);
      }
      expr.setChildExpressions(new VectorExpression [] {v2});
    } else {
      //For comparison expression, the child expressions must be materializing
      //columns
      v1 = getVectorExpression(leftExpr);
      v2 = getVectorExpression(rightExpr);
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
        throw new HiveException(ex);
      }
      expr.setChildExpressions(new VectorExpression [] {v1, v2});
    }
    if (v1 != null) {
      ocm.freeOutputColumn(v1.getOutputColumn());
    }
    if (v2 != null) {
      ocm.freeOutputColumn(v2.getOutputColumn());
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
    String normColType = getNormalizedTypeName(colType);
    String normScalarType = getNormalizedTypeName(scalarType);
    if (normColType.equalsIgnoreCase("long") && normScalarType.equalsIgnoreCase("long")
        && method.equalsIgnoreCase("divide")) {
      b.append("org.apache.hadoop.hive.ql.exec.vector.expressions.");
    } else {
      b.append("org.apache.hadoop.hive.ql.exec.vector.expressions.gen.");
    }
    b.append(normColType);
    b.append("Col");
    b.append(method);
    b.append(normScalarType);
    b.append("Scalar");
    return b.toString();
  }

  private String getBinaryScalarColumnExpressionClassName(String colType,
      String scalarType, String method) {
    StringBuilder b = new StringBuilder();
    String normColType = getNormalizedTypeName(colType);
    String normScalarType = getNormalizedTypeName(scalarType);
    if (normColType.equalsIgnoreCase("long") && normScalarType.equalsIgnoreCase("long")
        && method.equalsIgnoreCase("divide")) {
      b.append("org.apache.hadoop.hive.ql.exec.vector.expressions.");
    } else {
      b.append("org.apache.hadoop.hive.ql.exec.vector.expressions.gen.");
    }
    b.append(normScalarType);
    b.append("Scalar");
    b.append(method);
    b.append(normColType);
    b.append("Column");
    return b.toString();
  }

  private String getBinaryColumnColumnExpressionClassName(String colType1,
      String colType2, String method) {
    StringBuilder b = new StringBuilder();
    String normColType1 = getNormalizedTypeName(colType1);
    String normColType2 = getNormalizedTypeName(colType2);
    if (normColType1.equalsIgnoreCase("long") && normColType2.equalsIgnoreCase("long")
        && method.equalsIgnoreCase("divide")) {
      b.append("org.apache.hadoop.hive.ql.exec.vector.expressions.");
    } else {
      b.append("org.apache.hadoop.hive.ql.exec.vector.expressions.gen.");
    }
    b.append(normColType1);
    b.append("Col");
    b.append(method);
    b.append(normColType2);
    b.append("Column");
    return b.toString();
  }

  private String getOutputColType(String inputType1, String inputType2, String method) {
    if (method.equalsIgnoreCase("divide") || inputType1.equalsIgnoreCase("double") ||
        inputType2.equalsIgnoreCase("double") || inputType1.equalsIgnoreCase("float") ||
        inputType2.equalsIgnoreCase("float")) {
      return "double";
    } else {
      if (inputType1.equalsIgnoreCase("string") || inputType2.equalsIgnoreCase("string")) {
        return "string";
      } else {
        return "long";
      }
    }
  }

  private String getOutputColType(String inputType, String method) {
    if (inputType.equalsIgnoreCase("float") || inputType.equalsIgnoreCase("double")) {
      return "double";
    } else if (inputType.equalsIgnoreCase("string")) {
      return "string";
    } else {
      return "long";
    }
  }

  static Object[][] aggregatesDefinition = {
    {"min",       "Long",   VectorUDAFMinLong.class},
    {"min",       "Double", VectorUDAFMinDouble.class},
    {"min",       "String", VectorUDAFMinString.class},
    {"max",       "Long",   VectorUDAFMaxLong.class},
    {"max",       "Double", VectorUDAFMaxDouble.class},
    {"max",       "String", VectorUDAFMaxString.class},
    {"count",     null,     VectorUDAFCountStar.class},
    {"count",     "Long",   VectorUDAFCount.class},
    {"count",     "Double", VectorUDAFCount.class},
    {"count",     "String", VectorUDAFCount.class},
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
    String inputType = null;

    if (paramDescList.size() > 0) {
      ExprNodeDesc inputExpr = paramDescList.get(0);
      inputType = getNormalizedTypeName(inputExpr.getTypeString());
    }

    for (Object[] aggDef : aggregatesDefinition) {
      if (aggregateName.equalsIgnoreCase((String) aggDef[0]) &&
          ((aggDef[1] == null && inputType == null) ||
          (aggDef[1] != null && aggDef[1].equals(inputType)))) {
        Class<? extends VectorAggregateExpression> aggClass =
            (Class<? extends VectorAggregateExpression>) (aggDef[2]);
        try
        {
          Constructor<? extends VectorAggregateExpression> ctor =
              aggClass.getConstructor(VectorExpression.class);
          VectorAggregateExpression aggExpr = ctor.newInstance(
              vectorParams.length > 0 ? vectorParams[0] : null);
          aggExpr.init(desc);
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

  private VectorizedRowBatch allocateRowBatch(int rowCount) throws HiveException {
    int columnCount = firstOutputColumnIndex + ocm.getNumOfOutputColumn();
    VectorizedRowBatch ret = new VectorizedRowBatch(columnCount, rowCount);
    for (int i=0; i < columnCount; ++i) {
      String columnTypeName = ocm.getOutputColumnType(i);
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

  public Map<Integer, String> getOutputColumnTypeMap() {
    Map<Integer, String> map = new HashMap<Integer, String>();
    for (int i = 0; i < ocm.outputColCount; i++) {
      String type = ocm.outputColumnsTypes[i];
      map.put(i+this.firstOutputColumnIndex, type);
    }
    return map;
  }

  public ColumnVector allocateColumnVector(String type, int defaultSize) {
    if (type.equalsIgnoreCase("double")) {
      return new DoubleColumnVector(defaultSize);
    } else if (type.equalsIgnoreCase("string")) {
      return new BytesColumnVector(defaultSize);
    } else {
      return new LongColumnVector(defaultSize);
    }
  }


  public void addToColumnMap(String columnName, int outputColumn) {
    if (columnMap != null) {
      columnMap.put(columnName, outputColumn);
    }
  }
}

