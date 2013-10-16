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
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterConstantBooleanVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprAndExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprOrExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterStringColLikeStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FuncRand;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ISetDoubleArg;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ISetLongArg;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterStringColRegExpStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsNotNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsTrue;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringConcatColCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringConcatColScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringConcatScalarCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringSubstrColStart;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringSubstrColStartLen;
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
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastLongToBooleanViaLongToLong;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFArgDesc;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcat;
import org.apache.hadoop.hive.ql.udf.UDFAcos;
import org.apache.hadoop.hive.ql.udf.UDFAsin;
import org.apache.hadoop.hive.ql.udf.UDFAtan;
import org.apache.hadoop.hive.ql.udf.UDFBin;
import org.apache.hadoop.hive.ql.udf.UDFCeil;
import org.apache.hadoop.hive.ql.udf.UDFConv;
import org.apache.hadoop.hive.ql.udf.UDFCos;
import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFDegrees;
import org.apache.hadoop.hive.ql.udf.UDFExp;
import org.apache.hadoop.hive.ql.udf.UDFFloor;
import org.apache.hadoop.hive.ql.udf.UDFHex;
import org.apache.hadoop.hive.ql.udf.UDFHour;
import org.apache.hadoop.hive.ql.udf.UDFLTrim;
import org.apache.hadoop.hive.ql.udf.UDFLength;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.UDFLn;
import org.apache.hadoop.hive.ql.udf.UDFLog;
import org.apache.hadoop.hive.ql.udf.UDFLog10;
import org.apache.hadoop.hive.ql.udf.UDFLog2;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFOPDivide;
import org.apache.hadoop.hive.ql.udf.UDFOPMinus;
import org.apache.hadoop.hive.ql.udf.UDFOPMod;
import org.apache.hadoop.hive.ql.udf.UDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.UDFOPNegative;
import org.apache.hadoop.hive.ql.udf.UDFOPPlus;
import org.apache.hadoop.hive.ql.udf.UDFOPPositive;
import org.apache.hadoop.hive.ql.udf.UDFPower;
import org.apache.hadoop.hive.ql.udf.UDFRegExp;
import org.apache.hadoop.hive.ql.udf.UDFRTrim;
import org.apache.hadoop.hive.ql.udf.UDFRadians;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.UDFRound;
import org.apache.hadoop.hive.ql.udf.UDFSecond;
import org.apache.hadoop.hive.ql.udf.UDFSign;
import org.apache.hadoop.hive.ql.udf.UDFSin;
import org.apache.hadoop.hive.ql.udf.UDFSqrt;
import org.apache.hadoop.hive.ql.udf.UDFSubstr;
import org.apache.hadoop.hive.ql.udf.UDFTan;
import org.apache.hadoop.hive.ql.udf.UDFToBoolean;
import org.apache.hadoop.hive.ql.udf.UDFToByte;
import org.apache.hadoop.hive.ql.udf.UDFToInteger;
import org.apache.hadoop.hive.ql.udf.UDFToLong;
import org.apache.hadoop.hive.ql.udf.UDFToShort;
import org.apache.hadoop.hive.ql.udf.UDFToFloat;
import org.apache.hadoop.hive.ql.udf.UDFToDouble;
import org.apache.hadoop.hive.ql.udf.UDFToString;
import org.apache.hadoop.hive.ql.udf.UDFTrim;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLower;
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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

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

  // Package where custom (hand-built) vector expression classes are located.
  private static final String CUSTOM_EXPR_PACKAGE =
      "org.apache.hadoop.hive.ql.exec.vector.expressions";

  // Package where vector expression packages generated from templates are located.
  private static final String GENERATED_EXPR_PACKAGE =
      "org.apache.hadoop.hive.ql.exec.vector.expressions.gen";

  public VectorizationContext(Map<String, Integer> columnMap,
      int initialOutputCol) {
    this.columnMap = columnMap;
    this.ocm = new OutputColumnManager(initialOutputCol);
    this.firstOutputColumnIndex = initialOutputCol;
  }

  private int getInputColumnIndex(String name) {
      return columnMap.get(name);
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
      int relativeCol = allocateOutputColumnInternal(columnType);
      return initialOutputCol + relativeCol;
    }

    private int allocateOutputColumnInternal(String columnType) {
      for (int i = 0; i < outputColCount; i++) {
        if (usedOutputColumns.contains(i) ||
            !(outputColumnsTypes)[i].equalsIgnoreCase(columnType)) {
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
      if (isCustomUDF(expr) || isLegacyPathUDF(expr)) {
        ve = getCustomUDFExpression(expr);
      } else {
        ve = getVectorExpression(expr.getGenericUDF(),
            expr.getChildExprs());
      }
    } else if (exprDesc instanceof ExprNodeConstantDesc) {
      ve = getConstantVectorExpression((ExprNodeConstantDesc) exprDesc);
    }
    if (ve == null) {
      throw new HiveException("Could not vectorize expression: "+exprDesc.getName());
    }
    return ve;
  }

  /* Return true if this is one of a small set of functions for which
   * it is significantly easier to use the old code path in vectorized
   * mode instead of implementing a new, optimized VectorExpression.
   *
   * Depending on performance requirements and frequency of use, these
   * may be implemented in the future with an optimized VectorExpression.
   */
  public static boolean isLegacyPathUDF(ExprNodeGenericFuncDesc expr) {
    GenericUDF gudf = expr.getGenericUDF();
    if (gudf instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) gudf;
      Class<? extends UDF> udfClass = bridge.getUdfClass();
      if (udfClass.equals(UDFHex.class)
          || udfClass.equals(UDFConv.class)
          || isCastToIntFamily(udfClass) && arg0Type(expr).equals("string")
          || isCastToFloatFamily(udfClass) && arg0Type(expr).equals("string")
          || udfClass.equals(UDFToString.class) &&
               (arg0Type(expr).equals("timestamp")
                   || arg0Type(expr).equals("double")
                   || arg0Type(expr).equals("float"))) {
        return true;
      }
    } else if (gudf instanceof GenericUDFTimestamp && arg0Type(expr).equals("string")) {
      return true;
    }
    return false;
  }

  public static boolean isCastToIntFamily(Class<? extends UDF> udfClass) {
    return udfClass.equals(UDFToByte.class)
        || udfClass.equals(UDFToShort.class)
        || udfClass.equals(UDFToInteger.class)
        || udfClass.equals(UDFToLong.class);

    // Boolean is purposely excluded.
  }

  public static boolean isCastToFloatFamily(Class<? extends UDF> udfClass) {
    return udfClass.equals(UDFToDouble.class)
        || udfClass.equals(UDFToFloat.class);
  }

  // Return the type string of the first argument (argument 0).
  public static String arg0Type(ExprNodeGenericFuncDesc expr) {
    String type = expr.getChildExprs().get(0).getTypeString();
    return type;
  }

  // Return true if this is a custom UDF or custom GenericUDF.
  // This is for use only in the planner. It will fail in a task.
  public static boolean isCustomUDF(ExprNodeGenericFuncDesc expr) {
    String udfName = expr.getFuncText();
    if (udfName == null) {
      return false;
    }
    FunctionInfo funcInfo = FunctionRegistry.getFunctionInfo(udfName);
    if (funcInfo == null) {
      return false;
    }
    boolean isNativeFunc = funcInfo.isNative();
    return !isNativeFunc;
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
    } else if (type.equalsIgnoreCase("boolean")) {
      if (this.opType == OperatorType.FILTER) {
        if (((Boolean) exprDesc.getValue()).booleanValue()) {
          return new FilterConstantBooleanVectorExpression(1);
        } else {
          return new FilterConstantBooleanVectorExpression(0);
        }
      } else {
        if (((Boolean) exprDesc.getValue()).booleanValue()) {
          return new ConstantVectorExpression(outCol, 1);
        } else {
          return new ConstantVectorExpression(outCol, 0);
        }
      }
    } else {
      throw new HiveException("Unsupported constant type: "+type.toString());
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
      inputCol = getInputColumnIndex(colDesc.getColumn());
      colType = colDesc.getTypeString();
    } else {
      throw new HiveException("Expression not supported: "+childExpr);
    }
    String outputColumnType = getNormalizedTypeName(colType);
    int outputCol = ocm.allocateOutputColumn(outputColumnType);
    String className = "org.apache.hadoop.hive.ql.exec.vector.expressions.gen."
       + outputColumnType + "ColUnaryMinus";
    VectorExpression expr;
    try {
      expr = (VectorExpression) getConstructor(className).newInstance(inputCol, outputCol);
    } catch (Exception ex) {
      throw new HiveException(ex);
    }
    if (v1 != null) {
      expr.setChildExpressions(new VectorExpression [] {v1});
      ocm.freeOutputColumn(v1.getOutputColumn());
    }
    return expr;
  }

  /* For functions that take one argument, and can be translated using a vector
   * expression class of the form
   *   <packagePrefix>.<classPrefix><argumentType>To<resultType>
   * The argumentType is inferred from the input expression.
   */
  private VectorExpression getUnaryFunctionExpression(
      String classPrefix,
      String resultType,
      List<ExprNodeDesc> childExprList,
      String packagePrefix)
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
    String funcInputColType = getNormalizedTypeName(colType);
    int outputCol = ocm.allocateOutputColumn(resultType);
    String className = packagePrefix + "."
        + classPrefix + funcInputColType + "To" + resultType;
    VectorExpression expr;
    try {
      expr = (VectorExpression) getConstructor(className).newInstance(inputCol, outputCol);
    } catch (Exception ex) {
      throw new HiveException(ex);
    }
    if (v1 != null) {
      expr.setChildExpressions(new VectorExpression [] {v1});
      ocm.freeOutputColumn(v1.getOutputColumn());
    }
    return expr;
  }

  // Used as a fast path for operations that don't modify their input, like unary +
  // and casting boolean to long.
  private VectorExpression getIdentityExpression(List<ExprNodeDesc> childExprList)
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
    } else if (udf instanceof GenericUDFLower) {
      return getUnaryStringExpression("StringLower", "String", childExpr);
    } else if (udf instanceof GenericUDFUpper) {
      return getUnaryStringExpression("StringUpper", "String", childExpr);
    } else if (udf instanceof GenericUDFConcat) {
      return getConcatExpression(childExpr);
    } else if (udf instanceof GenericUDFAbs) {
      return getUnaryAbsExpression(childExpr);
    } else if (udf instanceof GenericUDFTimestamp) {
      return getCastToTimestamp(childExpr);
    }

    throw new HiveException("Udf: "+udf.getClass().getSimpleName()+", is not supported");
  }

  private VectorExpression getUnaryAbsExpression(List<ExprNodeDesc> childExpr)
      throws HiveException {
    String argType = childExpr.get(0).getTypeString();
    if (isIntFamily(argType)) {
      return getUnaryFunctionExpression("FuncAbs", "Long", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (isFloatFamily(argType)) {
      return getUnaryFunctionExpression("FuncAbs", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    }

    throw new HiveException("Udf: Abs() not supported for argument type " + argType);
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
      return getIdentityExpression(childExpr);
    } else if (cl.equals(UDFYear.class) ||
        cl.equals(UDFMonth.class) ||
        cl.equals(UDFWeekOfYear.class) ||
        cl.equals(UDFDayOfMonth.class) ||
        cl.equals(UDFHour.class) ||
        cl.equals(UDFMinute.class) ||
        cl.equals(UDFSecond.class)) {
      return getTimestampFieldExpression(cl.getSimpleName(), childExpr);
    } else if (cl.equals(UDFLike.class)) {
      return getLikeExpression(childExpr, true);
    } else if (cl.equals(UDFRegExp.class)) {
      return getLikeExpression(childExpr, false);
    } else if (cl.equals(UDFLength.class)) {
      return getUnaryStringExpression("StringLength", "Long", childExpr);
    } else if (cl.equals(UDFSubstr.class)) {
      return getSubstrExpression(childExpr);
    } else if (cl.equals(UDFLTrim.class)) {
      return getUnaryStringExpression("StringLTrim", "String", childExpr);
    } else if (cl.equals(UDFRTrim.class)) {
      return getUnaryStringExpression("StringRTrim", "String", childExpr);
    } else if (cl.equals(UDFTrim.class)) {
      return getUnaryStringExpression("StringTrim", "String", childExpr);
    } else if (cl.equals(UDFSin.class)) {
      return getUnaryFunctionExpression("FuncSin", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFCos.class)) {
      return getUnaryFunctionExpression("FuncCos", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFTan.class)) {
      return getUnaryFunctionExpression("FuncTan", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFAsin.class)) {
      return getUnaryFunctionExpression("FuncASin", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFAcos.class)) {
      return getUnaryFunctionExpression("FuncACos", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFAtan.class)) {
      return getUnaryFunctionExpression("FuncATan", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFFloor.class)) {
      return getUnaryFunctionExpression("FuncFloor", "Long", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFCeil.class)) {
      return getUnaryFunctionExpression("FuncCeil", "Long", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFDegrees.class)) {
      return getUnaryFunctionExpression("FuncDegrees", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFRadians.class)) {
      return getUnaryFunctionExpression("FuncRadians", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFLn.class)) {
      return getUnaryFunctionExpression("FuncLn", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFLog2.class)) {
      return getUnaryFunctionExpression("FuncLog2", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFLog10.class)) {
      return getUnaryFunctionExpression("FuncLog10", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFSign.class)) {
      return getUnaryFunctionExpression("FuncSign", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFSqrt.class)) {
      return getUnaryFunctionExpression("FuncSqrt", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFExp.class)) {
      return getUnaryFunctionExpression("FuncExp", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (cl.equals(UDFLog.class)) {
      return getLogWithBaseExpression(childExpr);
    } else if (cl.equals(UDFPower.class)) {
      return getPowerExpression(childExpr);
    } else if (cl.equals(UDFRound.class)) {
      return getRoundExpression(childExpr);
    } else if (cl.equals(UDFRand.class)) {
      return getRandExpression(childExpr);
    } else if (cl.equals(UDFBin.class)) {
      return getUnaryStringExpression("FuncBin", "String", childExpr);
    } else if (isCastToIntFamily(cl)) {
      return getCastToLongExpression(childExpr);
    } else if (cl.equals(UDFToBoolean.class)) {
      return getCastToBoolean(childExpr);
    } else if (isCastToFloatFamily(cl)) {
      return getCastToDoubleExpression(childExpr);
    } else if (cl.equals(UDFToString.class)) {
      return getCastToString(childExpr);
    }

    throw new HiveException("Udf: "+udf.getClass().getSimpleName()+", is not supported");
  }

  private VectorExpression getCastToTimestamp(List<ExprNodeDesc> childExpr)
      throws HiveException {
    String inputType = childExpr.get(0).getTypeString();
    if (isIntFamily(inputType)) {
      return getUnaryFunctionExpression("CastLongToTimestampVia", "Long", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (isFloatFamily(inputType)) {
      return getUnaryFunctionExpression("CastDoubleToTimestampVia", "Long", childExpr,
          GENERATED_EXPR_PACKAGE);
    }
    // The string type is deliberately omitted -- it's handled elsewhere. See isLegacyPathUDF.

    throw new HiveException("Unhandled cast input type: " + inputType);
  }

  private VectorExpression getCastToString(List<ExprNodeDesc> childExpr)
      throws HiveException {
    String inputType = childExpr.get(0).getTypeString();
    if (inputType.equals("boolean")) {
      // Boolean must come before the integer family. It's a special case.
      return getUnaryFunctionExpression("CastBooleanToStringVia", "String", childExpr,
          CUSTOM_EXPR_PACKAGE);
    } else if (isIntFamily(inputType)) {
      return getUnaryFunctionExpression("Cast", "String", childExpr,
          CUSTOM_EXPR_PACKAGE);
    }
    /* The string type is deliberately omitted -- the planner removes string to string casts.
     * Timestamp, float, and double types are handled by the legacy code path. See isLegacyPathUDF.
     */

    throw new HiveException("Unhandled cast input type: " + inputType);
  }

  private VectorExpression getCastToDoubleExpression(List<ExprNodeDesc> childExpr)
      throws HiveException {
    String inputType = childExpr.get(0).getTypeString();
    if (isIntFamily(inputType)) {
      return getUnaryFunctionExpression("Cast", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (inputType.equals("timestamp")) {
      return getUnaryFunctionExpression("CastTimestampToDoubleVia", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (isFloatFamily(inputType)) {

      // float types require no conversion, so use a no-op
      return getIdentityExpression(childExpr);
    }
    // The string type is deliberately omitted -- it's handled elsewhere. See isLegacyPathUDF.

    throw new HiveException("Unhandled cast input type: " + inputType);
  }

  private VectorExpression getCastToBoolean(List<ExprNodeDesc> childExpr)
      throws HiveException {
    String inputType = childExpr.get(0).getTypeString();
    if (isFloatFamily(inputType)) {
      return getUnaryFunctionExpression("CastDoubleToBooleanVia", "Long", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (isIntFamily(inputType) || inputType.equals("timestamp")) {
      return getUnaryFunctionExpression("CastLongToBooleanVia", "Long", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (inputType.equals("string")) {

      // string casts to false if it is 0 characters long, otherwise true
      VectorExpression lenExpr = getUnaryStringExpression("StringLength", "Long", childExpr);

      int outputCol = ocm.allocateOutputColumn("integer");
      VectorExpression lenToBoolExpr =
          new CastLongToBooleanViaLongToLong(lenExpr.getOutputColumn(), outputCol);
      lenToBoolExpr.setChildExpressions(new VectorExpression[] {lenExpr});
      ocm.freeOutputColumn(lenExpr.getOutputColumn());
      return lenToBoolExpr;
    }
    // cast(booleanExpr as boolean) case is omitted because planner removes it as a no-op

    throw new HiveException("Unhandled cast input type: " + inputType);
  }

  private VectorExpression getCastToLongExpression(List<ExprNodeDesc> childExpr)
      throws HiveException {
    String inputType = childExpr.get(0).getTypeString();
    if (isFloatFamily(inputType)) {
      return getUnaryFunctionExpression("Cast", "Long", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (inputType.equals("timestamp")) {
      return getUnaryFunctionExpression("CastTimestampToLongVia", "Long", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (isIntFamily(inputType)) {

      // integer and boolean types require no conversion, so use a no-op
      return getIdentityExpression(childExpr);
    }
    // string type is deliberately omitted -- it's handled elsewhere. See isLegacyPathUDF.

    throw new HiveException("Unhandled cast input type: " + inputType);
  }

  private VectorExpression getRandExpression(List<ExprNodeDesc> childExpr)
    throws HiveException {

    // prepare one output column
    int outputCol = ocm.allocateOutputColumn("Double");
    if (childExpr == null || childExpr.size() == 0) {

      // make no-argument vectorized Rand expression
      return new FuncRand(outputCol);
    } else if (childExpr.size() == 1) {

      // Make vectorized Rand expression with seed
      long seed = getLongScalar(childExpr.get(0));
      return new FuncRand(seed, outputCol);
    }

    throw new HiveException("Vectorization error. Rand has more than 1 argument.");
  }

  private VectorExpression getRoundExpression(List<ExprNodeDesc> childExpr)
    throws HiveException {

    // Handle one-argument case
    if (childExpr.size() == 1) {
      return getUnaryFunctionExpression("FuncRound", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    }

    // Handle two-argument case

    // Try to get the second argument (the number of digits)
    long numDigits = getLongScalar(childExpr.get(1));

    // Use the standard logic for a unary function to handle the first argument.
    VectorExpression e = getUnaryFunctionExpression("RoundWithNumDigits", "Double", childExpr,
        CUSTOM_EXPR_PACKAGE);

    // Set second argument for this special case
    ((ISetLongArg) e).setArg(numDigits);
    return e;
  }

  private VectorExpression getPowerExpression(List<ExprNodeDesc> childExpr)
      throws HiveException {
    String argType = childExpr.get(0).getTypeString();

    // Try to get the second argument, typically a constant value (the power).
    double power = getDoubleScalar(childExpr.get(1));

    // Use the standard logic for a unary function to handle the first argument.
    VectorExpression e = getUnaryFunctionExpression("FuncPower", "Double", childExpr,
        CUSTOM_EXPR_PACKAGE);

    // Set the second argument for this special case
    ((ISetDoubleArg) e).setArg(power);
    return e;
  }

  private VectorExpression getLogWithBaseExpression(List<ExprNodeDesc> childExpr)
      throws HiveException {
    if (childExpr.size() == 1) {

      // No base provided, so this is equivalent to Ln
      return getUnaryFunctionExpression("FuncLn", "Double", childExpr,
          GENERATED_EXPR_PACKAGE);
    } else if (childExpr.size() == 2) {
      String argType = childExpr.get(0).getTypeString();

      // Try to get the second argument, typically a constant value (the base)
      double base = getDoubleScalar(childExpr.get(1));

      // Use the standard logic for a unary function to handle the first argument.
      VectorExpression e = getUnaryFunctionExpression("FuncLogWithBase", "Double", childExpr,
          CUSTOM_EXPR_PACKAGE);

      // set the second argument for this special case
      ((ISetDoubleArg) e).setArg(base);
      return e;
    }

    throw new HiveException("Udf: Log could not be vectorized");
  }

  private double getDoubleScalar(ExprNodeDesc expr) throws HiveException {
    if (!(expr instanceof ExprNodeConstantDesc)) {
      throw new HiveException("Constant value expected for UDF argument. " +
          "Non-constant argument not supported for vectorization.");
    }
    ExprNodeConstantDesc constExpr = (ExprNodeConstantDesc) expr;
    Object obj = getScalarValue(constExpr);
    if (obj instanceof Double) {
      return ((Double) obj).doubleValue();
    } else if (obj instanceof DoubleWritable) {
      return ((DoubleWritable) obj).get();
    } else if (obj instanceof Integer) {
      return (double) ((Integer) obj).longValue();
    } else if (obj instanceof IntWritable) {
      return (double) ((IntWritable) obj).get();
    }

    throw new HiveException("Udf: unhandled constant type for scalar argument."
        + "Expecting double or integer");
  }

  private long getLongScalar(ExprNodeDesc expr) throws HiveException {
    if (!(expr instanceof ExprNodeConstantDesc)) {
      throw new HiveException("Constant value expected for UDF argument. " +
          "Non-constant argument not supported for vectorization.");
    }
    ExprNodeConstantDesc constExpr = (ExprNodeConstantDesc) expr;
    Object obj = getScalarValue(constExpr);
    if (obj instanceof Integer) {
      return (long) ((Integer) obj).longValue();
    } else if (obj instanceof IntWritable) {
      return (long) ((IntWritable) obj).get();
    } else if (obj instanceof Long) {
      return ((Long) obj).longValue();
    } else if (obj instanceof LongWritable) {
      return ((LongWritable) obj).get();
    }

    throw new HiveException("Udf: unhandled constant type for scalar argument."
        + "Expecting integer or bigint");
  }

  /* Return a vector expression for string concatenation, including the column-scalar,
   * scalar-column, and column-column cases.
   */
  private VectorExpression getConcatExpression(List<ExprNodeDesc> childExprList)
      throws HiveException {
    ExprNodeDesc left = childExprList.get(0);
    ExprNodeDesc right = childExprList.get(1);
    int inputColLeft = -1;
    int inputColRight = -1;
    VectorExpression vLeft = null;
    VectorExpression vRight = null;
    VectorExpression expr = null;

    // Generate trees to evaluate non-leaf inputs, if there are any.
    if (left instanceof ExprNodeGenericFuncDesc) {
      vLeft = getVectorExpression(left);
      inputColLeft = vLeft.getOutputColumn();
    }

    if (right instanceof ExprNodeGenericFuncDesc) {
      vRight = getVectorExpression(right);
      inputColRight = vRight.getOutputColumn();
    }

    // Handle case for left input a column and right input a constant
    if ((left instanceof ExprNodeColumnDesc || inputColLeft != -1) &&
        right instanceof ExprNodeConstantDesc) {
      if (inputColLeft == -1) {
        inputColLeft = getInputColumnIndex(((ExprNodeColumnDesc) left).getColumn());
      }
      int outputCol = ocm.allocateOutputColumn("String");
      byte[] constant = (byte[]) getScalarValue((ExprNodeConstantDesc) right);
      expr = new StringConcatColScalar(inputColLeft, outputCol, constant);
      if (vLeft != null) {
        expr.setChildExpressions(new VectorExpression [] {vLeft});
      }
    }

    // Handle case for left input a constant and right input a column
    else if ((left instanceof ExprNodeConstantDesc) &&
        (right instanceof ExprNodeColumnDesc || inputColRight != -1)) {
      if (inputColRight == -1) {
        inputColRight = getInputColumnIndex(((ExprNodeColumnDesc) right).getColumn());
      }
      int outputCol = ocm.allocateOutputColumn("String");
      byte[] constant = (byte[]) getScalarValue((ExprNodeConstantDesc) left);
      expr = new StringConcatScalarCol(constant, inputColRight, outputCol);
      if (vRight != null) {
        expr.setChildExpressions(new VectorExpression [] {vRight});
      }
    }

    // Handle case where both left and right inputs are columns
    else if ((left instanceof ExprNodeColumnDesc || inputColLeft != -1) &&
        (right instanceof ExprNodeColumnDesc || inputColRight != -1)) {
      if (inputColLeft == -1) {
        inputColLeft = getInputColumnIndex(((ExprNodeColumnDesc) left).getColumn());
      }
      if (inputColRight == -1) {
        inputColRight = getInputColumnIndex(((ExprNodeColumnDesc) right).getColumn());
      }
      int outputCol = ocm.allocateOutputColumn("String");
      expr = new StringConcatColCol(inputColLeft, inputColRight, outputCol);
      if (vLeft == null && vRight != null) {
        expr.setChildExpressions(new VectorExpression [] {vRight});
      } else if (vLeft != null && vRight == null) {
        expr.setChildExpressions(new VectorExpression [] {vLeft});
      } else if (vLeft != null && vRight != null) {

        // Both left and right have child expressions
        expr.setChildExpressions(new VectorExpression [] {vLeft, vRight});
      }
    } else {
      throw new HiveException("Failed to vectorize CONCAT()");
    }

    // Free output columns if inputs have non-leaf expression trees.
    if (vLeft != null) {
      ocm.freeOutputColumn(vLeft.getOutputColumn());
    }
    if (vRight != null) {
      ocm.freeOutputColumn(vRight.getOutputColumn());
    }
    return expr;
  }

  /*
   * Return vector expression for a custom (i.e. not built-in) UDF.
   */
  private VectorExpression getCustomUDFExpression(ExprNodeGenericFuncDesc expr)
      throws HiveException {

    //GenericUDFBridge udfBridge = (GenericUDFBridge) expr.getGenericUDF();
    List<ExprNodeDesc> childExprList = expr.getChildExprs();

    // argument descriptors
    VectorUDFArgDesc[] argDescs = new VectorUDFArgDesc[expr.getChildExprs().size()];
    for (int i = 0; i < argDescs.length; i++) {
      argDescs[i] = new VectorUDFArgDesc();
    }

    // positions of variable arguments (columns or non-constant expressions)
    List<Integer> variableArgPositions = new ArrayList<Integer>();

    // Column numbers of batch corresponding to expression result arguments
    List<Integer> exprResultColumnNums = new ArrayList<Integer>();

    // Prepare children
    List<VectorExpression> vectorExprs = new ArrayList<VectorExpression>();

    for (int i = 0; i < childExprList.size(); i++) {
      ExprNodeDesc child = childExprList.get(i);
      if (child instanceof ExprNodeGenericFuncDesc) {
        VectorExpression e = getVectorExpression(child);
        vectorExprs.add(e);
        variableArgPositions.add(i);
        exprResultColumnNums.add(e.getOutputColumn());
        argDescs[i].setVariable(e.getOutputColumn());
      } else if (child instanceof ExprNodeColumnDesc) {
        variableArgPositions.add(i);
        argDescs[i].setVariable(getInputColumnIndex(((ExprNodeColumnDesc) child).getColumn()));
      } else if (child instanceof ExprNodeConstantDesc) {

        // this is a constant
        argDescs[i].setConstant((ExprNodeConstantDesc) child);
      } else {
        throw new HiveException("Unable to vectorize Custom UDF");
      }
    }

    // Allocate output column and get column number;
    int outputCol = -1;
    String resultColVectorType;
    String resultType = expr.getTypeInfo().getTypeName();
    if (resultType.equalsIgnoreCase("string")) {
      resultColVectorType = "String";
    } else if (isIntFamily(resultType)) {
      resultColVectorType = "Long";
    } else if (isFloatFamily(resultType)) {
      resultColVectorType = "Double";
    } else if (resultType.equalsIgnoreCase("timestamp")) {
      resultColVectorType = "Long";
    } else {
      throw new HiveException("Unable to vectorize due to unsupported custom UDF return type "
                                + resultType);
    }
    outputCol = ocm.allocateOutputColumn(resultColVectorType);

    // Make vectorized operator
    VectorExpression ve;
    ve = new VectorUDFAdaptor(expr, outputCol, resultColVectorType, argDescs);

    // Set child expressions
    VectorExpression[] childVEs = null;
    if (exprResultColumnNums.size() != 0) {
      childVEs = new VectorExpression[exprResultColumnNums.size()];
      for (int i = 0; i < childVEs.length; i++) {
        childVEs[i] = vectorExprs.get(i);
      }
    }
    ve.setChildExpressions(childVEs);

    // Free output columns if inputs have non-leaf expression trees.
    for (Integer i : exprResultColumnNums) {
      ocm.freeOutputColumn(i);
    }
    return ve;
  }

  // return true if this is any kind of float
  public static boolean isFloatFamily(String resultType) {
    return resultType.equalsIgnoreCase("double")
        || resultType.equalsIgnoreCase("float");
  }

  // Return true if this data type is handled in the output vector as an integer.
  public static boolean isIntFamily(String resultType) {
    return resultType.equalsIgnoreCase("tinyint")
        || resultType.equalsIgnoreCase("smallint")
        || resultType.equalsIgnoreCase("int")
        || resultType.equalsIgnoreCase("bigint")
        || resultType.equalsIgnoreCase("boolean");
  }

  /* Return a unary string vector expression. This is used for functions like
   * UPPER() and LOWER().
   */
  private VectorExpression getUnaryStringExpression(String vectorExprClassName,
      String resultType, // result type name
      List<ExprNodeDesc> childExprList) throws HiveException {

      return getUnaryExpression(vectorExprClassName, resultType, childExprList,
          CUSTOM_EXPR_PACKAGE);
  }

  private VectorExpression getUnaryExpression(String vectorExprClassName,
      String resultType,           // result type name
      List<ExprNodeDesc> childExprList,
      String packagePathPrefix     // prefix of package path name
      ) throws HiveException {

    /* Create an instance of the class vectorExprClassName for the input column or expression result
     * and return it.
     */

    ExprNodeDesc childExpr = childExprList.get(0);
    int inputCol;
    VectorExpression v1 = null;
    if (childExpr instanceof ExprNodeGenericFuncDesc) {
      v1 = getVectorExpression(childExpr);
      inputCol = v1.getOutputColumn();
    } else if (childExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) childExpr;
      inputCol = getInputColumnIndex(colDesc.getColumn());
    } else {
      // constant argument case not supported
      throw new HiveException("Expression not supported: "+childExpr);
    }
    String outputColumnType = getNormalizedTypeName(resultType);
    int outputCol = ocm.allocateOutputColumn(outputColumnType);
    String className = packagePathPrefix + "." + vectorExprClassName;
    VectorExpression expr;
    try {
      expr = (VectorExpression) getConstructor(className).newInstance(inputCol, outputCol);
    } catch (Exception ex) {
      throw new HiveException(ex);
    }
    if (v1 != null) {
      expr.setChildExpressions(new VectorExpression [] {v1});
      ocm.freeOutputColumn(v1.getOutputColumn());
    }
    return expr;
  }


  private VectorExpression getSubstrExpression(
      List<ExprNodeDesc> childExprList) throws HiveException {

    ExprNodeDesc childExpr = childExprList.get(0);
    ExprNodeDesc startExpr = childExprList.get(1);
    startExpr = foldConstantsForUnaryExpression(startExpr);

    // Get second and optionally third arguments
    int start;
    if (startExpr instanceof ExprNodeConstantDesc) {
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) startExpr;
      start = ((Integer) constDesc.getValue()).intValue();
    } else {
      throw new HiveException("Cannot vectorize non-constant start argument for SUBSTR");
    }
    ExprNodeDesc lengthExpr = null;
    int length = 0;
    if (childExprList.size() == 3) {
      lengthExpr = childExprList.get(2);
      lengthExpr = foldConstantsForUnaryExpression(lengthExpr);
      if (lengthExpr instanceof ExprNodeConstantDesc) {
        ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) lengthExpr;
        length = ((Integer) constDesc.getValue()).intValue();
      } else {
        throw new HiveException("Cannot vectorize non-constant length argument for SUBSTR");
      }
    }

    // Prepare first argument (whether it is a column or an expression)
    int inputCol;
    VectorExpression v1 = null;
    if (childExpr instanceof ExprNodeGenericFuncDesc) {
      v1 = getVectorExpression(childExpr);
      inputCol = v1.getOutputColumn();
    } else if (childExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) childExpr;
      inputCol = getInputColumnIndex(colDesc.getColumn());
    } else {
      throw new HiveException("Expression not supported: " + childExpr);
    }
    int outputCol = ocm.allocateOutputColumn("String");

    // Create appropriate vector expression for 2 or 3 argument version of SUBSTR()
    VectorExpression expr = null;
    if (childExprList.size() == 2) {
      expr = new StringSubstrColStart(inputCol, start, outputCol);
    } else if (childExprList.size() == 3) {
      expr = new StringSubstrColStartLen(inputCol, start, length, outputCol);
    } else {
      throw new HiveException("Invalid number of arguments for SUBSTR()");
    }

    if (v1 != null) {
      expr.setChildExpressions(new VectorExpression [] {v1});
      ocm.freeOutputColumn(v1.getOutputColumn());
    }
    return expr;
  }

  /**
   * Returns a vector expression for a LIKE or REGEXP expression
   * @param childExpr A list of child expressions
   * @param isLike {@code true}: the expression is LIKE.
   *               {@code false}: the expression is REGEXP.
   * @return A {@link FilterStringColLikeStringScalar} or
   *         a {@link FilterStringColRegExpStringScalar}
   * @throws HiveException
   */
  private VectorExpression getLikeExpression(List<ExprNodeDesc> childExpr, boolean isLike) throws HiveException {
    ExprNodeDesc leftExpr = childExpr.get(0);
    ExprNodeDesc rightExpr = childExpr.get(1);

    VectorExpression v1 = null;
    VectorExpression expr = null;
    int inputCol;
    ExprNodeConstantDesc constDesc;

    if ((leftExpr instanceof ExprNodeColumnDesc) &&
        (rightExpr instanceof ExprNodeConstantDesc) ) {
      ExprNodeColumnDesc leftColDesc = (ExprNodeColumnDesc) leftExpr;
      constDesc = (ExprNodeConstantDesc) rightExpr;
      inputCol = getInputColumnIndex(leftColDesc.getColumn());
      if (isLike) {
        expr = (VectorExpression) new FilterStringColLikeStringScalar(inputCol,
            new Text((byte[]) getScalarValue(constDesc)));
      } else {
        expr = (VectorExpression) new FilterStringColRegExpStringScalar(inputCol,
            new Text((byte[]) getScalarValue(constDesc)));
      }
    } else if ((leftExpr instanceof ExprNodeGenericFuncDesc) &&
               (rightExpr instanceof ExprNodeConstantDesc)) {
      v1 = getVectorExpression(leftExpr);
      inputCol = v1.getOutputColumn();
      constDesc = (ExprNodeConstantDesc) rightExpr;
      if (isLike) {
        expr = (VectorExpression) new FilterStringColLikeStringScalar(inputCol,
            new Text((byte[]) getScalarValue(constDesc)));
      } else {
        expr = (VectorExpression) new FilterStringColRegExpStringScalar(inputCol,
            new Text((byte[]) getScalarValue(constDesc)));
      }
    }
    // TODO add logic to handle cases where left input is an expression.
    if (expr == null) {
      throw new HiveException("Vector LIKE filter expression could not be initialized");
    }
    if (v1 != null) {
      expr.setChildExpressions(new VectorExpression [] {v1});
      ocm.freeOutputColumn(v1.getOutputColumn());
    }
    return expr;
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
          VectorExpression v2 = (VectorExpression) getConstructor(vectorUDF).
              newInstance(inputCol,outputCol);
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
        expr = (VectorExpression) getConstructor(className).newInstance(inputCol,
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
        expr = (VectorExpression) getConstructor(className).newInstance(getScalarValue(constDesc),
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
        expr = (VectorExpression) getConstructor(className).newInstance(inputCol1, inputCol2,
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
        expr = (VectorExpression) getConstructor(className).newInstance(inputCol1, inputCol2,
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
        expr = (VectorExpression) getConstructor(className).newInstance(inputCol1,
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
        expr = (VectorExpression) getConstructor(className).newInstance(inputCol1, inputCol2,
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
        expr = (VectorExpression) getConstructor(className).newInstance(getScalarValue(constDesc),
                inputCol2, outputCol);
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
        expr = (VectorExpression) getConstructor(className).newInstance(inputCol1, inputCol2,
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
    throw new HiveException("Not is not supported");
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

  private Object getScalarValue(ExprNodeConstantDesc constDesc)
      throws HiveException {
    if (constDesc.getTypeString().equalsIgnoreCase("String")) {
      try {
         byte[] bytes = ((String) constDesc.getValue()).getBytes("UTF-8");
         return bytes;
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
    } else if (constDesc.getTypeString().equalsIgnoreCase("boolean")) {
      if (constDesc.getValue().equals(Boolean.valueOf(true))) {
        return 1;
      } else {
        return 0;
      }
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
        Constructor<?> ctor = getConstructor(className);
        expr = (VectorExpression) ctor.newInstance(inputCol,
            getScalarValue(constDesc));
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
    } else if ((leftExpr instanceof ExprNodeConstantDesc) &&
        (rightExpr instanceof ExprNodeColumnDesc)) {
      ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) leftExpr;
      ExprNodeColumnDesc rightColDesc = (ExprNodeColumnDesc) rightExpr;
      int inputCol = getInputColumnIndex(rightColDesc.getColumn());
      String colType = rightColDesc.getTypeString();
      String scalarType = constDesc.getTypeString();
      String className = getFilterScalarColumnExpressionClassName(colType,
          scalarType, opName);
      try {
        //Constructor<?>
        expr = (VectorExpression) getConstructor(className).newInstance(inputCol,
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
        expr = (VectorExpression) getConstructor(className).newInstance(inputCol1, inputCol2);
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
        expr = (VectorExpression) getConstructor(className).newInstance(inputCol1, inputCol2);
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
        expr = (VectorExpression) getConstructor(className).newInstance(inputCol1, inputCol2);
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
        expr = (VectorExpression) getConstructor(className).newInstance(inputCol1,
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
        expr = (VectorExpression) getConstructor(className).newInstance(inputCol2,
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
        expr = (VectorExpression) getConstructor(className).newInstance(inputCol1, inputCol2);
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

  private Constructor<?> getConstructor(String className) throws HiveException {
    try {
      Class<?> cl = Class.forName(className);
      Constructor<?> [] ctors = cl.getDeclaredConstructors();
      Constructor<?> defaultCtor = cl.getConstructor();
      for (Constructor<?> ctor : ctors) {
        if (!ctor.equals(defaultCtor)) {
          return ctor;
        }
      }
      throw new HiveException("Only default constructor found");
    } catch (Exception ex) {
      throw new HiveException(ex);
    }
  }

  private String getNormalizedTypeName(String colType) throws HiveException {
    validateInputType(colType);
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
      String colType2, String opName) throws HiveException {
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
      scalarType, String opName) throws HiveException {
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
      scalarType, String opName) throws HiveException {
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
      String scalarType, String method) throws HiveException {
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
      String scalarType, String method) throws HiveException {
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
      String colType2, String method) throws HiveException {
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

  private String getOutputColType(String inputType1, String inputType2, String method)
      throws HiveException {
    validateInputType(inputType1);
    validateInputType(inputType2);
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

  private void validateInputType(String inputType) throws HiveException {
    if (! (inputType.equalsIgnoreCase("float") ||
        inputType.equalsIgnoreCase("double") ||
        inputType.equalsIgnoreCase("string") ||
        inputType.equalsIgnoreCase("tinyint") ||
            inputType.equalsIgnoreCase("smallint") ||
            inputType.equalsIgnoreCase("short") ||
            inputType.equalsIgnoreCase("byte") ||
            inputType.equalsIgnoreCase("int") ||
            inputType.equalsIgnoreCase("long") ||
            inputType.equalsIgnoreCase("bigint") ||
            inputType.equalsIgnoreCase("boolean") ||
            inputType.equalsIgnoreCase("timestamp") ) ) {
      throw new HiveException("Unsupported input type: "+inputType);
    }
  }

  private String getOutputColType(String inputType, String method) throws HiveException {
    validateInputType(inputType);
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
    {"std",       "Double", VectorUDAFStdPopDouble.class},
    {"stddev",    "Double", VectorUDAFStdPopDouble.class},
    {"stddev_pop","Double", VectorUDAFStdPopDouble.class},
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

