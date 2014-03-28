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
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.InputExpressionType;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Mode;
import org.apache.hadoop.hive.ql.exec.vector.expressions.*;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFAvgDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFCount;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFCountStar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFSumDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFStdPopDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFStdPopDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFStdPopLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFStdSampDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFStdSampDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFStdSampLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFSumDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFSumLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarPopDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarPopDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarPopLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarSampDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarSampDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarSampLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.*;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFArgDesc;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.ql.udf.UDFConv;
import org.apache.hadoop.hive.ql.udf.UDFHex;
import org.apache.hadoop.hive.ql.udf.UDFSign;
import org.apache.hadoop.hive.ql.udf.UDFToBoolean;
import org.apache.hadoop.hive.ql.udf.UDFToByte;
import org.apache.hadoop.hive.ql.udf.UDFToDouble;
import org.apache.hadoop.hive.ql.udf.UDFToFloat;
import org.apache.hadoop.hive.ql.udf.UDFToInteger;
import org.apache.hadoop.hive.ql.udf.UDFToLong;
import org.apache.hadoop.hive.ql.udf.UDFToShort;
import org.apache.hadoop.hive.ql.udf.UDFToString;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Context class for vectorization execution.
 * Main role is to map column names to column indices and serves as a
 * factory class for building vectorized expressions out of descriptors.
 *
 */
public class VectorizationContext {

  private static final Log LOG = LogFactory.getLog(
      VectorizationContext.class.getName());

  VectorExpressionDescriptor vMap;

  //columnName to column position map
  private final Map<String, Integer> columnMap;
  private final int firstOutputColumnIndex;

  public static final Pattern decimalTypePattern = Pattern.compile("decimal.*",
      Pattern.CASE_INSENSITIVE);

  //Map column number to type
  private final OutputColumnManager ocm;

  // File key is used by operators to retrieve the scratch vectors
  // from mapWork at runtime. The operators that modify the structure of
  // a vector row batch, need to allocate scratch vectors as well. Every
  // operator that creates a new Vectorization context should set a unique
  // fileKey.
  private String fileKey = null;

  // Set of UDF classes for type casting data types in row-mode.
  private static Set<Class<?>> castExpressionUdfs = new HashSet<Class<?>>();
  static {
    castExpressionUdfs.add(GenericUDFToDecimal.class);
    castExpressionUdfs.add(GenericUDFToBinary.class);
    castExpressionUdfs.add(GenericUDFToDate.class);
    castExpressionUdfs.add(GenericUDFToUnixTimeStamp.class);
    castExpressionUdfs.add(GenericUDFToUtcTimestamp.class);
    castExpressionUdfs.add(GenericUDFToChar.class);
    castExpressionUdfs.add(GenericUDFToVarchar.class);
    castExpressionUdfs.add(GenericUDFTimestamp.class);
    castExpressionUdfs.add(UDFToByte.class);
    castExpressionUdfs.add(UDFToBoolean.class);
    castExpressionUdfs.add(UDFToDouble.class);
    castExpressionUdfs.add(UDFToFloat.class);
    castExpressionUdfs.add(UDFToString.class);
    castExpressionUdfs.add(UDFToInteger.class);
    castExpressionUdfs.add(UDFToLong.class);
    castExpressionUdfs.add(UDFToShort.class);
  }

  public VectorizationContext(Map<String, Integer> columnMap,
      int initialOutputCol) {
    this.columnMap = columnMap;
    this.ocm = new OutputColumnManager(initialOutputCol);
    this.firstOutputColumnIndex = initialOutputCol;
    vMap = new VectorExpressionDescriptor();
  }

  /**
   * This constructor inherits the OutputColumnManger and from
   * the 'parent' constructor, therefore this should be used only by operators
   * that don't create a new vectorized row batch. This should be used only by
   * operators that want to modify the columnName map without changing the row batch.
   */
  public VectorizationContext(VectorizationContext parent) {
    this.columnMap = new HashMap<String, Integer>(parent.columnMap);
    this.ocm = parent.ocm;
    this.firstOutputColumnIndex = parent.firstOutputColumnIndex;
    vMap = new VectorExpressionDescriptor();
  }

  public String getFileKey() {
    return fileKey;
  }

  public void setFileKey(String fileKey) {
    this.fileKey = fileKey;
  }

  protected int getInputColumnIndex(String name) {
    if (!columnMap.containsKey(name)) {
      LOG.error(String.format("The column %s is not in the vectorization context column map.", name));
    }
    return columnMap.get(name);
  }

  protected int getInputColumnIndex(ExprNodeColumnDesc colExpr) {
    return columnMap.get(colExpr.getColumn());
  }

  private static class OutputColumnManager {
    private final int initialOutputCol;
    private int outputColCount = 0;

    protected OutputColumnManager(int initialOutputCol) {
      this.initialOutputCol = initialOutputCol;
    }

    //The complete list of output columns. These should be added to the
    //Vectorized row batch for processing. The index in the row batch is
    //equal to the index in this array plus initialOutputCol.
    //Start with size 100 and double when needed.
    private String [] outputColumnsTypes = new String[100];

    private final Set<Integer> usedOutputColumns = new HashSet<Integer>();

    int allocateOutputColumn(String columnType) {
      if (initialOutputCol < 0) {
        // This is a test
        return 0;
      }
      int relativeCol = allocateOutputColumnInternal(columnType);
      return initialOutputCol + relativeCol;
    }

    private int allocateOutputColumnInternal(String columnType) {
      for (int i = 0; i < outputColCount; i++) {

        // Re-use an existing, available column of the same required type.
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
      if (initialOutputCol < 0) {
        // This is a test
        return;
      }
      int colIndex = index-initialOutputCol;
      if (colIndex >= 0) {
        usedOutputColumns.remove(index-initialOutputCol);
      }
    }
  }

  private VectorExpression getColumnVectorExpression(ExprNodeColumnDesc
      exprDesc, Mode mode) {
    int columnNum = getInputColumnIndex(exprDesc.getColumn());
    VectorExpression expr = null;
    switch (mode) {
      case FILTER:
        //Important: It will come here only if the column is being used as a boolean
        expr = new SelectColumnIsTrue(columnNum);
        break;
      case PROJECTION:
        expr = new IdentityExpression(columnNum, exprDesc.getTypeString());
        break;
    }
    return expr;
  }

  public VectorExpression[] getVectorExpressions(List<ExprNodeDesc> exprNodes) throws HiveException {
    return getVectorExpressions(exprNodes, Mode.PROJECTION);
  }

  public VectorExpression[] getVectorExpressions(List<ExprNodeDesc> exprNodes, Mode mode)
    throws HiveException {

    int i = 0;
    if (null == exprNodes) {
      return new VectorExpression[0];
    }
    VectorExpression[] ret = new VectorExpression[exprNodes.size()];
    for (ExprNodeDesc e : exprNodes) {
      ret[i++] = getVectorExpression(e, mode);
    }
    return ret;
  }

  public VectorExpression getVectorExpression(ExprNodeDesc exprDesc) throws HiveException {
    return getVectorExpression(exprDesc, Mode.PROJECTION);
  }

  /**
   * Returns a vector expression for a given expression
   * description.
   * @param exprDesc, Expression description
   * @param mode
   * @return {@link VectorExpression}
   * @throws HiveException
   */
  public VectorExpression getVectorExpression(ExprNodeDesc exprDesc, Mode mode) throws HiveException {
    VectorExpression ve = null;
    if (exprDesc instanceof ExprNodeColumnDesc) {
      ve = getColumnVectorExpression((ExprNodeColumnDesc) exprDesc, mode);
    } else if (exprDesc instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc expr = (ExprNodeGenericFuncDesc) exprDesc;
      if (isCustomUDF(expr) || isNonVectorizedPathUDF(expr)) {
        ve = getCustomUDFExpression(expr);
      } else {

        // Add cast expression if needed. Child expressions of a udf may return different data types
        // and that would require converting their data types to evaluate the udf.
        // For example decimal column added to an integer column would require integer column to be
        // cast to decimal.
        List<ExprNodeDesc> childExpressions = getChildExpressionsWithImplicitCast(expr.getGenericUDF(),
            exprDesc.getChildren(), exprDesc.getTypeInfo());
        ve = getGenericUdfVectorExpression(expr.getGenericUDF(),
            childExpressions, mode, exprDesc.getTypeInfo());
      }
    } else if (exprDesc instanceof ExprNodeConstantDesc) {
      ve = getConstantVectorExpression(((ExprNodeConstantDesc) exprDesc).getValue(), exprDesc.getTypeInfo(),
          mode);
    }
    if (ve == null) {
      throw new HiveException("Could not vectorize expression: "+exprDesc.getName());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Input Expression = " + exprDesc.getTypeInfo()
          + ", Vectorized Expression = " + ve.toString());
    }
    return ve;
  }

  /**
   * Given a udf and its children, return the common type to which the children's type should be
   * cast.
   */
  private TypeInfo getCommonTypeForChildExpressions(GenericUDF genericUdf, List<ExprNodeDesc> children,
      TypeInfo returnType) {
    TypeInfo commonType;
    if (genericUdf instanceof GenericUDFBaseCompare) {

      // Apply comparison rules
      TypeInfo tLeft = children.get(0).getTypeInfo();
      TypeInfo tRight = children.get(1).getTypeInfo();
      commonType = FunctionRegistry.getCommonClassForComparison(tLeft, tRight);
      if (commonType == null) {
        commonType = returnType;
      }
    } else if (genericUdf instanceof GenericUDFIn) {

      // Cast to the type of the first child
      return children.get(0).getTypeInfo();
    } else {
      // The children type should be converted to return type
      commonType = returnType;
    }
    return commonType;
  }

  /**
   * Add a cast expression to the expression tree if needed. The output of child expressions of a given UDF might
   * need a cast if their return type is different from the return type of the UDF.
   *
   * @param genericUDF The given UDF
   * @param children Child expressions of the UDF that might require a cast.
   * @param returnType The return type of the UDF.
   * @return List of child expressions added with cast.
   */
  private List<ExprNodeDesc> getChildExpressionsWithImplicitCast(GenericUDF genericUDF,
      List<ExprNodeDesc> children, TypeInfo returnType) throws HiveException {
    if (isExcludedFromCast(genericUDF)) {

      // No implicit cast needed
      return children;
    }
    if (children == null) {
      return null;
    }

    TypeInfo commonType = getCommonTypeForChildExpressions(genericUDF, children, returnType);

    if (commonType == null) {

      // Couldn't determine common type, don't cast
      return children;
    }

    List<ExprNodeDesc> childrenWithCasts = new ArrayList<ExprNodeDesc>();
    boolean atleastOneCastNeeded = false;
    for (ExprNodeDesc child : children) {
      ExprNodeDesc castExpression = getImplicitCastExpression(genericUDF, child, commonType);
      if (castExpression != null) {
        atleastOneCastNeeded = true;
        childrenWithCasts.add(castExpression);
      } else {
        childrenWithCasts.add(child);
      }
    }
    if (atleastOneCastNeeded) {
      return childrenWithCasts;
    } else {
      return children;
    }
  }

  private boolean isExcludedFromCast(GenericUDF genericUDF) {
    boolean ret = castExpressionUdfs.contains(genericUDF.getClass())
        || (genericUDF instanceof GenericUDFRound) || (genericUDF instanceof GenericUDFBetween);

    if (ret) {
      return ret;
    }

    if (genericUDF instanceof GenericUDFBridge) {
      Class<?> udfClass = ((GenericUDFBridge) genericUDF).getUdfClass();
      return castExpressionUdfs.contains(udfClass)
          || UDFSign.class.isAssignableFrom(udfClass);
    }
    return false;
  }

  /**
   * Creates a DecimalTypeInfo object with appropriate precision and scale for the given
   * inputTypeInfo.
   */
  private TypeInfo updatePrecision(TypeInfo inputTypeInfo, DecimalTypeInfo returnType) {
    if (!(inputTypeInfo instanceof PrimitiveTypeInfo)) {
      return returnType;
    }
    PrimitiveTypeInfo ptinfo = (PrimitiveTypeInfo) inputTypeInfo;
    int precision = getPrecisionForType(ptinfo);
    int scale = HiveDecimalUtils.getScaleForType(ptinfo);
    return new DecimalTypeInfo(precision, scale);
  }

  /**
   * The GenericUDFs might need their children output to be cast to the given castType.
   * This method returns a cast expression that would achieve the required casting.
   */
  private ExprNodeDesc getImplicitCastExpression(GenericUDF udf, ExprNodeDesc child, TypeInfo castType)
      throws HiveException {
    TypeInfo inputTypeInfo = child.getTypeInfo();
    String inputTypeString = inputTypeInfo.getTypeName();
    String castTypeString = castType.getTypeName();

    if (inputTypeString.equals(castTypeString)) {
      // Nothing to be done
      return null;
    }
    boolean inputTypeDecimal = false;
    boolean castTypeDecimal = false;
    if (decimalTypePattern.matcher(inputTypeString).matches()) {
      inputTypeDecimal = true;
    }
    if (decimalTypePattern.matcher(castTypeString).matches()) {
      castTypeDecimal = true;
    }

    if (castTypeDecimal && !inputTypeDecimal) {

      // Cast the input to decimal
      // If castType is decimal, try not to lose precision for numeric types.
      castType = updatePrecision(inputTypeInfo, (DecimalTypeInfo) castType);
      GenericUDFToDecimal castToDecimalUDF = new GenericUDFToDecimal();
      castToDecimalUDF.setTypeInfo(castType);
      List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
      children.add(child);
      ExprNodeDesc desc = new ExprNodeGenericFuncDesc(castType, castToDecimalUDF, children);
      return desc;
    } else if (!castTypeDecimal && inputTypeDecimal) {

      // Cast decimal input to returnType
      GenericUDF genericUdf = getGenericUDFForCast(castType);
      List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
      children.add(child);
      ExprNodeDesc desc = new ExprNodeGenericFuncDesc(castType, genericUdf, children);
      return desc;
    } else {

      // Casts to exact types including long to double etc. are needed in some special cases.
      if (udf instanceof GenericUDFCoalesce) {
        GenericUDF genericUdf = getGenericUDFForCast(castType);
        List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
        children.add(child);
        ExprNodeDesc desc = new ExprNodeGenericFuncDesc(castType, genericUdf, children);
        return desc;
      }
    }
    return null;
  }
  
  private int getPrecisionForType(PrimitiveTypeInfo typeInfo) {
    if (isFloatFamily(typeInfo.getTypeName())) {
      return HiveDecimal.MAX_PRECISION;
    }
    return HiveDecimalUtils.getPrecisionForType(typeInfo);
  }

  private GenericUDF getGenericUDFForCast(TypeInfo castType) throws HiveException {
    UDF udfClass = null;
    GenericUDF genericUdf = null;
    switch (((PrimitiveTypeInfo) castType).getPrimitiveCategory()) {
      case BYTE:
        udfClass = new UDFToByte();
        break;
      case SHORT:
        udfClass = new UDFToShort();
        break;
      case INT:
        udfClass = new UDFToInteger();
        break;
      case LONG:
        udfClass = new UDFToLong();
        break;
      case FLOAT:
        udfClass = new UDFToFloat();
        break;
      case DOUBLE:
        udfClass = new UDFToDouble();
        break;
      case STRING:
        udfClass = new UDFToString();
        break;
      case BOOLEAN:
        udfClass = new UDFToBoolean();
        break;
      case DATE:
        genericUdf = new GenericUDFToDate();
        break;
      case TIMESTAMP:
        genericUdf = new GenericUDFToUnixTimeStamp();
        break;
      case BINARY:
        genericUdf = new GenericUDFToBinary();
        break;
      case DECIMAL:
        genericUdf = new GenericUDFToDecimal();
        break;
    }
    if (genericUdf == null) {
      if (udfClass == null) {
        throw new HiveException("Could not add implicit cast for type "+castType.getTypeName());
      }
      genericUdf = new GenericUDFBridge();
      ((GenericUDFBridge) genericUdf).setUdfClassName(udfClass.getClass().getName());
    }
    if (genericUdf instanceof SettableUDF) {
	((SettableUDF)genericUdf).setTypeInfo(castType);
    }    
    return genericUdf;
  }


  /* Return true if this is one of a small set of functions for which
   * it is significantly easier to use the old code path in vectorized
   * mode instead of implementing a new, optimized VectorExpression.
   *
   * Depending on performance requirements and frequency of use, these
   * may be implemented in the future with an optimized VectorExpression.
   */
  public static boolean isNonVectorizedPathUDF(ExprNodeGenericFuncDesc expr) {
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
    } else if ((gudf instanceof GenericUDFTimestamp && arg0Type(expr).equals("string"))

            /* GenericUDFCase and GenericUDFWhen are implemented with the UDF Adaptor because
             * of their complexity and generality. In the future, variations of these
             * can be optimized to run faster for the vectorized code path. For example,
             * CASE col WHEN 1 then "one" WHEN 2 THEN "two" ELSE "other" END
             * is an example of a GenericUDFCase that has all constant arguments
             * except for the first argument. This is probably a common case and a
             * good candidate for a fast, special-purpose VectorExpression. Then
             * the UDF Adaptor code path could be used as a catch-all for
             * non-optimized general cases.
             */
            || gudf instanceof GenericUDFCase
            || gudf instanceof GenericUDFWhen) {
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
    String type = expr.getChildren().get(0).getTypeString();
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
  ExprNodeDesc foldConstantsForUnaryExpression(ExprNodeDesc exprDesc) throws HiveException {
    if (!(exprDesc instanceof ExprNodeGenericFuncDesc)) {
      return exprDesc;
    }
    
    if (exprDesc.getChildren() == null || (exprDesc.getChildren().size() != 1) ) {
      return exprDesc;
    }

    ExprNodeConstantDesc foldedChild = null;
    if (!( exprDesc.getChildren().get(0) instanceof ExprNodeConstantDesc)) {

      // try recursive folding
      ExprNodeDesc expr = foldConstantsForUnaryExpression(exprDesc.getChildren().get(0));
      if (expr instanceof ExprNodeConstantDesc) {
        foldedChild = (ExprNodeConstantDesc) expr;
      }
    } else {
      foldedChild = (ExprNodeConstantDesc) exprDesc.getChildren().get(0);
    }

    if (foldedChild == null) {
      return exprDesc;
    }

    ObjectInspector childoi = foldedChild.getWritableObjectInspector();
    GenericUDF gudf = ((ExprNodeGenericFuncDesc) exprDesc).getGenericUDF();

    if (gudf instanceof GenericUDFOPNegative || gudf instanceof GenericUDFOPPositive
        || castExpressionUdfs.contains(gudf.getClass())
        || ((gudf instanceof GenericUDFBridge)
            && castExpressionUdfs.contains(((GenericUDFBridge) gudf).getUdfClass()))) {
      ExprNodeEvaluator<?> evaluator = ExprNodeEvaluatorFactory.get(exprDesc);
      ObjectInspector output = evaluator.initialize(childoi);
      Object constant = evaluator.evaluate(null);
      Object java = ObjectInspectorUtils.copyToStandardJavaObject(constant, output);  
      return new ExprNodeConstantDesc(exprDesc.getTypeInfo(), java);
     }

    return exprDesc;
  }

  /* Fold simple unary expressions in all members of the input list and return new list
   * containing results.
   */
  private List<ExprNodeDesc> foldConstantsForUnaryExprs(List<ExprNodeDesc> childExpr)
      throws HiveException {
    List<ExprNodeDesc> constantFoldedChildren = new ArrayList<ExprNodeDesc>();
    if (childExpr != null) {
      for (ExprNodeDesc expr : childExpr) {
        expr = this.foldConstantsForUnaryExpression(expr);
        constantFoldedChildren.add(expr);
      }
    }
    return constantFoldedChildren;
  }

  private VectorExpression getConstantVectorExpression(Object constantValue, TypeInfo typeInfo,
      Mode mode) throws HiveException {
    String type = typeInfo.getTypeName();
    String colVectorType = getNormalizedTypeName(type);
    int outCol = -1;
    if (mode == Mode.PROJECTION) {
      outCol = ocm.allocateOutputColumn(colVectorType);
    }
    if (decimalTypePattern.matcher(type).matches()) {
      VectorExpression ve = new ConstantVectorExpression(outCol, (Decimal128) constantValue);
      ve.setOutputType(typeInfo.getTypeName());
      return ve;
    } else if (type.equalsIgnoreCase("long") || type.equalsIgnoreCase("int") ||
        type.equalsIgnoreCase("short") || type.equalsIgnoreCase("byte")) {
      return new ConstantVectorExpression(outCol,
          ((Number) constantValue).longValue());
    } else if (type.equalsIgnoreCase("double") || type.equalsIgnoreCase("float")) {
      return new ConstantVectorExpression(outCol, ((Number) constantValue).doubleValue());
    } else if (type.equalsIgnoreCase("string")) {
      return new ConstantVectorExpression(outCol, ((String) constantValue).getBytes());
    } else if (type.equalsIgnoreCase("boolean")) {
      if (mode == Mode.FILTER) {
        if (((Boolean) constantValue).booleanValue()) {
          return new FilterConstantBooleanVectorExpression(1);
        } else {
          return new FilterConstantBooleanVectorExpression(0);
        }
      } else {
        if (((Boolean) constantValue).booleanValue()) {
          return new ConstantVectorExpression(outCol, 1);
        } else {
          return new ConstantVectorExpression(outCol, 0);
        }
      }
    }
    throw new HiveException("Unsupported constant type: "+type.toString());
  }

  /**
   * Used as a fast path for operations that don't modify their input, like unary +
   * and casting boolean to long. IdentityExpression and its children are always
   * projections.
   */
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

  private VectorExpression getVectorExpressionForUdf(Class<?> udf, List<ExprNodeDesc> childExpr, Mode mode,
      TypeInfo returnType) throws HiveException {
    int numChildren = (childExpr == null) ? 0 : childExpr.size();
    if (numChildren > VectorExpressionDescriptor.MAX_NUM_ARGUMENTS) {
      return null;
    }
    VectorExpressionDescriptor.Builder builder = new VectorExpressionDescriptor.Builder();
    builder.setNumArguments(numChildren);
    builder.setMode(mode);
    for (int i = 0; i < numChildren; i++) {
      ExprNodeDesc child = childExpr.get(i);
      builder.setArgumentType(i, child.getTypeString());
      if ((child instanceof ExprNodeGenericFuncDesc) || (child instanceof ExprNodeColumnDesc)) {
        builder.setInputExpressionType(i, InputExpressionType.COLUMN);
      } else if (child instanceof ExprNodeConstantDesc) {
        builder.setInputExpressionType(i, InputExpressionType.SCALAR);
      } else {
        throw new HiveException("Cannot handle expression type: " + child.getClass().getSimpleName());
      }
    }
    VectorExpressionDescriptor.Descriptor descriptor = builder.build();
    Class<?> vclass = this.vMap.getVectorExpressionClass(udf, descriptor);
    if (vclass == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No vector udf found for "+udf.getSimpleName() + ", descriptor: "+descriptor);
      }
      return null;
    }
    Mode childrenMode = getChildrenMode(mode, udf);
    return createVectorExpression(vclass, childExpr, childrenMode, returnType);
  }

  private VectorExpression createVectorExpression(Class<?> vectorClass,
      List<ExprNodeDesc> childExpr, Mode childrenMode, TypeInfo returnType) throws HiveException {
    int numChildren = childExpr == null ? 0: childExpr.size();
    VectorExpression.Type [] inputTypes = new VectorExpression.Type[numChildren];
    List<VectorExpression> children = new ArrayList<VectorExpression>();
    Object[] arguments = new Object[numChildren];
    try {
      for (int i = 0; i < numChildren; i++) {
        ExprNodeDesc child = childExpr.get(i);
        inputTypes[i] = VectorExpression.Type.getValue(child.getTypeInfo().getTypeName());
        if (child instanceof ExprNodeGenericFuncDesc) {
          VectorExpression vChild = getVectorExpression(child, childrenMode);
            children.add(vChild);
            arguments[i] = vChild.getOutputColumn();
        } else if (child instanceof ExprNodeColumnDesc) {
          int colIndex = getInputColumnIndex((ExprNodeColumnDesc) child);
            if (childrenMode == Mode.FILTER) {
              // In filter mode, the column must be a boolean
              children.add(new SelectColumnIsTrue(colIndex));
            }
            arguments[i] = colIndex;
        } else if (child instanceof ExprNodeConstantDesc) {
          Object scalarValue = getVectorTypeScalarValue((ExprNodeConstantDesc) child);
          arguments[i] = scalarValue;
        } else {
          throw new HiveException("Cannot handle expression type: " + child.getClass().getSimpleName());
        }
      }
      VectorExpression  vectorExpression = instantiateExpression(vectorClass, returnType, arguments);
      vectorExpression.setInputTypes(inputTypes);
      if ((vectorExpression != null) && !children.isEmpty()) {
        vectorExpression.setChildExpressions(children.toArray(new VectorExpression[0]));
      }
      return vectorExpression;
    } catch (Exception ex) {
      throw new HiveException(ex);
    } finally {
      for (VectorExpression ve : children) {
        ocm.freeOutputColumn(ve.getOutputColumn());
      }
    }
  }

  private Mode getChildrenMode(Mode mode, Class<?> udf) {
    if (mode.equals(Mode.FILTER) && (udf.equals(GenericUDFOPAnd.class) || udf.equals(GenericUDFOPOr.class))) {
      return Mode.FILTER;
    }
    return Mode.PROJECTION;
  }

  private VectorExpression instantiateExpression(Class<?> vclass, TypeInfo returnType, Object...args)
      throws HiveException {
    VectorExpression ve = null;
    Constructor<?> ctor = getConstructor(vclass);
    int numParams = ctor.getParameterTypes().length;
    int argsLength = (args == null) ? 0 : args.length;
    try {
      if (numParams == 0) {
        ve = (VectorExpression) ctor.newInstance();
      } else if (numParams == argsLength) {
        ve = (VectorExpression) ctor.newInstance(args);
      } else if (numParams == argsLength + 1) {
        // Additional argument is needed, which is the outputcolumn.
        String outType;

        // Special handling for decimal because decimal types need scale and precision parameter.
        // This special handling should be avoided by using returnType uniformly for all cases.
        if (returnType != null) {
          outType = getNormalizedTypeName(returnType.getTypeName()).toLowerCase();
        } else {
          outType = ((VectorExpression) vclass.newInstance()).getOutputType();
        }
        int outputCol = ocm.allocateOutputColumn(outType);
        Object [] newArgs = Arrays.copyOf(args, numParams);
        newArgs[numParams-1] = outputCol;
        ve = (VectorExpression) ctor.newInstance(newArgs);
        ve.setOutputType(outType);
      }
    } catch (Exception ex) {
      throw new HiveException("Could not instantiate " + vclass.getSimpleName(), ex);
    }
    return ve;
  }

  private VectorExpression getGenericUdfVectorExpression(GenericUDF udf,
      List<ExprNodeDesc> childExpr, Mode mode, TypeInfo returnType) throws HiveException {

    List<ExprNodeDesc> constantFoldedChildren = foldConstantsForUnaryExprs(childExpr);
    childExpr = constantFoldedChildren;
    //First handle special cases
    if (udf instanceof GenericUDFBetween) {
      return getBetweenFilterExpression(childExpr, mode, returnType);
    } else if (udf instanceof GenericUDFIn) {
      return getInExpression(childExpr, mode, returnType);
    } else if (udf instanceof GenericUDFOPPositive) {
      return getIdentityExpression(childExpr);
    } else if (udf instanceof GenericUDFCoalesce) {

      // Coalesce is a special case because it can take variable number of arguments.
      return getCoalesceExpression(childExpr, returnType);
    } else if (udf instanceof GenericUDFBridge) {
      VectorExpression v = getGenericUDFBridgeVectorExpression((GenericUDFBridge) udf, childExpr, mode,
          returnType);
      if (v != null) {
        return v;
      }
    } else if (udf instanceof GenericUDFToDecimal) {
      return getCastToDecimal(childExpr, returnType);
    }

    // Now do a general lookup
    Class<?> udfClass = udf.getClass();
    if (udf instanceof GenericUDFBridge) {
      udfClass = ((GenericUDFBridge) udf).getUdfClass();
    }

    VectorExpression ve = getVectorExpressionForUdf(udfClass, constantFoldedChildren, mode, returnType);

    if (ve == null) {
      throw new HiveException("Udf: "+udf.getClass().getSimpleName()+", is not supported");
    }

    return ve;
  }

  private VectorExpression getCoalesceExpression(List<ExprNodeDesc> childExpr, TypeInfo returnType)
      throws HiveException {
    int[] inputColumns = new int[childExpr.size()];
    VectorExpression[] vectorChildren = null;
    try {
      vectorChildren = getVectorExpressions(childExpr, Mode.PROJECTION);

      int i = 0;
      for (VectorExpression ve : vectorChildren) {
        inputColumns[i++] = ve.getOutputColumn();
      }

      int outColumn = ocm.allocateOutputColumn(getNormalizedTypeName(returnType.getTypeName()));
      VectorCoalesce vectorCoalesce = new VectorCoalesce(inputColumns, outColumn);
      vectorCoalesce.setOutputType(returnType.getTypeName());
      vectorCoalesce.setChildExpressions(vectorChildren);
      return vectorCoalesce;
    } finally {
      // Free the output columns of the child expressions.
      if (vectorChildren != null) {
        for (VectorExpression v : vectorChildren) {
          ocm.freeOutputColumn(v.getOutputColumn());
        }
      }
    }
  }

  /**
   * Create a filter or boolean-valued expression for column IN ( <list-of-constants> )
   */
  private VectorExpression getInExpression(List<ExprNodeDesc> childExpr, Mode mode, TypeInfo returnType)
      throws HiveException {
    ExprNodeDesc colExpr = childExpr.get(0);

    TypeInfo colTypeInfo = colExpr.getTypeInfo();
    String colType = colExpr.getTypeString();

    // prepare arguments for createVectorExpression
    List<ExprNodeDesc> childrenForInList =
        foldConstantsForUnaryExprs(childExpr.subList(1, childExpr.size()));

    /* This method assumes that the IN list has no NULL entries. That is enforced elsewhere,
     * in the Vectorizer class. If NULL is passed in as a list entry, behavior is not defined.
     * If in the future, NULL values are allowed in the IN list, be sure to handle 3-valued
     * logic correctly. E.g. NOT (col IN (null)) should be considered UNKNOWN, so that would
     * become FALSE in the WHERE clause, and cause the row in question to be filtered out.
     * See the discussion in Jira HIVE-5583.
     */

    VectorExpression expr = null;

    // determine class
    Class<?> cl = null;
    if (isIntFamily(colType)) {
      cl = (mode == Mode.FILTER ? FilterLongColumnInList.class : LongColumnInList.class);
      long[] inVals = new long[childrenForInList.size()];
      for (int i = 0; i != inVals.length; i++) {
        inVals[i] = getIntFamilyScalarAsLong((ExprNodeConstantDesc) childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), Mode.PROJECTION, returnType);
      ((ILongInExpr) expr).setInListValues(inVals);
    } else if (isTimestampFamily(colType)) {
      cl = (mode == Mode.FILTER ? FilterLongColumnInList.class : LongColumnInList.class);
      long[] inVals = new long[childrenForInList.size()];
      for (int i = 0; i != inVals.length; i++) {
        inVals[i] = getTimestampScalar(childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), Mode.PROJECTION, returnType);
      ((ILongInExpr) expr).setInListValues(inVals);
    } else if (isStringFamily(colType)) {
      cl = (mode == Mode.FILTER ? FilterStringColumnInList.class : StringColumnInList.class);
      byte[][] inVals = new byte[childrenForInList.size()][];
      for (int i = 0; i != inVals.length; i++) {
        inVals[i] = getStringScalarAsByteArray((ExprNodeConstantDesc) childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), Mode.PROJECTION, returnType);
      ((IStringInExpr) expr).setInListValues(inVals);
    } else if (isFloatFamily(colType)) {
      cl = (mode == Mode.FILTER ? FilterDoubleColumnInList.class : DoubleColumnInList.class);
      double[] inValsD = new double[childrenForInList.size()];
      for (int i = 0; i != inValsD.length; i++) {
        inValsD[i] = getNumericScalarAsDouble(childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), Mode.PROJECTION, returnType);
      ((IDoubleInExpr) expr).setInListValues(inValsD);
    } else if (isDecimalFamily(colType)) {
      cl = (mode == Mode.FILTER ? FilterDecimalColumnInList.class : DecimalColumnInList.class);
      Decimal128[] inValsD = new Decimal128[childrenForInList.size()];
      for (int i = 0; i != inValsD.length; i++) {
        inValsD[i] = (Decimal128) getVectorTypeScalarValue(
            (ExprNodeConstantDesc)  childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), Mode.PROJECTION, returnType);
      ((IDecimalInExpr) expr).setInListValues(inValsD);
    } else if (isDateFamily(colType)) {
      cl = (mode == Mode.FILTER ? FilterLongColumnInList.class : LongColumnInList.class);
      long[] inVals = new long[childrenForInList.size()];
      for (int i = 0; i != inVals.length; i++) {
        inVals[i] = (Integer) getVectorTypeScalarValue((ExprNodeConstantDesc) childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), Mode.PROJECTION, returnType);
      ((ILongInExpr) expr).setInListValues(inVals);
    }

    // Return the desired VectorExpression if found. Otherwise, return null to cause
    // execution to fall back to row mode.
    return expr;
  }

  private byte[] getStringScalarAsByteArray(ExprNodeConstantDesc exprNodeConstantDesc)
      throws HiveException {
    Object o = getScalarValue(exprNodeConstantDesc);
    if (!(o instanceof byte[])) {
      throw new HiveException("Expected constant argument of type string");
    }
    return (byte[]) o;
  }

  /**
   * Invoke special handling for expressions that can't be vectorized by regular
   * descriptor based lookup.
   */
  private VectorExpression getGenericUDFBridgeVectorExpression(GenericUDFBridge udf,
      List<ExprNodeDesc> childExpr, Mode mode, TypeInfo returnType) throws HiveException {
    Class<? extends UDF> cl = udf.getUdfClass();
    if (isCastToIntFamily(cl)) {
      return getCastToLongExpression(childExpr);
    } else if (cl.equals(UDFToBoolean.class)) {
      return getCastToBoolean(childExpr);
    } else if (isCastToFloatFamily(cl)) {
      return getCastToDoubleExpression(cl, childExpr, returnType);
    } else if (cl.equals(UDFToString.class)) {
      return getCastToString(childExpr, returnType);
    }
    return null;
  }

  private VectorExpression getCastToDecimal(List<ExprNodeDesc> childExpr, TypeInfo returnType)
      throws HiveException {
    ExprNodeDesc child = childExpr.get(0);
    String inputType = childExpr.get(0).getTypeString();
    if (child instanceof ExprNodeConstantDesc) {
      // Return a constant vector expression
      Object constantValue = ((ExprNodeConstantDesc) child).getValue();
      Decimal128 decimalValue = castConstantToDecimal(constantValue, child.getTypeInfo());
      return getConstantVectorExpression(decimalValue, returnType, Mode.PROJECTION);
    }
    if (isIntFamily(inputType)) {
      return createVectorExpression(CastLongToDecimal.class, childExpr, Mode.PROJECTION, returnType);
    } else if (isFloatFamily(inputType)) {
      return createVectorExpression(CastDoubleToDecimal.class, childExpr, Mode.PROJECTION, returnType);
    } else if (decimalTypePattern.matcher(inputType).matches()) {
      return createVectorExpression(CastDecimalToDecimal.class, childExpr, Mode.PROJECTION,
          returnType);
    } else if (isStringFamily(inputType)) {
      return createVectorExpression(CastStringToDecimal.class, childExpr, Mode.PROJECTION, returnType);
    } else if (isDatetimeFamily(inputType)) {
      return createVectorExpression(CastTimestampToDecimal.class, childExpr, Mode.PROJECTION, returnType);
    }
    throw new HiveException("Unhandled cast input type: " + inputType);
  }

  private Decimal128 castConstantToDecimal(Object scalar, TypeInfo type) throws HiveException {
    PrimitiveTypeInfo ptinfo = (PrimitiveTypeInfo) type;
    String typename = type.getTypeName();
    Decimal128 d = new Decimal128();
    int scale = HiveDecimalUtils.getScaleForType(ptinfo);
    switch (ptinfo.getPrimitiveCategory()) {
      case FLOAT:
        float floatVal = ((Float) scalar).floatValue();
        d.update(floatVal, (short) scale);
        break;
      case DOUBLE:
        double doubleVal = ((Double) scalar).doubleValue();
        d.update(doubleVal, (short) scale);
        break;
      case BYTE:
        byte byteVal = ((Byte) scalar).byteValue();
        d.update(byteVal, (short) scale);
        break;
      case SHORT:
        short shortVal = ((Short) scalar).shortValue();
        d.update(shortVal, (short) scale);
        break;
      case INT:
        int intVal = ((Integer) scalar).intValue();
        d.update(intVal, (short) scale);
        break;
      case LONG:
        long longVal = ((Long) scalar).longValue();
        d.update(longVal, (short) scale);
        break;
      case DECIMAL:
        HiveDecimal decimalVal = (HiveDecimal) scalar;
        d.update(decimalVal.unscaledValue(), (short) scale);
        break;
      default:
        throw new HiveException("Unsupported type "+typename+" for cast to Decimal128");
    }
    return d;
  }

  private VectorExpression getCastToString(List<ExprNodeDesc> childExpr, TypeInfo returnType)
      throws HiveException {
    String inputType = childExpr.get(0).getTypeString();
    if (inputType.equals("boolean")) {
      // Boolean must come before the integer family. It's a special case.
      return createVectorExpression(CastBooleanToStringViaLongToString.class, childExpr, Mode.PROJECTION, null);
    } else if (isIntFamily(inputType)) {
      return createVectorExpression(CastLongToString.class, childExpr, Mode.PROJECTION, null);
    } else if (isDecimalFamily(inputType)) {
      return createVectorExpression(CastDecimalToString.class, childExpr, Mode.PROJECTION, returnType);
    } else if (isDateFamily(inputType)) {
      return createVectorExpression(CastDateToString.class, childExpr, Mode.PROJECTION, returnType);
    }
    /* The string type is deliberately omitted -- the planner removes string to string casts.
     * Timestamp, float, and double types are handled by the legacy code path. See isLegacyPathUDF.
     */

    throw new HiveException("Unhandled cast input type: " + inputType);
  }

  private VectorExpression getCastToDoubleExpression(Class<?> udf, List<ExprNodeDesc> childExpr,
      TypeInfo returnType) throws HiveException {
    String inputType = childExpr.get(0).getTypeString();
    if (isIntFamily(inputType)) {
      return createVectorExpression(CastLongToDouble.class, childExpr, Mode.PROJECTION, returnType);
    } else if (inputType.equals("timestamp")) {
      return createVectorExpression(CastTimestampToDoubleViaLongToDouble.class, childExpr, Mode.PROJECTION,
          returnType);
    } else if (isFloatFamily(inputType)) {

      // float types require no conversion, so use a no-op
      return getIdentityExpression(childExpr);
    } 
    // The string type is deliberately omitted -- it's handled elsewhere. See isLegacyPathUDF.

    return null;
  }

  private VectorExpression getCastToBoolean(List<ExprNodeDesc> childExpr)
      throws HiveException {
    String inputType = childExpr.get(0).getTypeString();
    // Long and double are handled using descriptors, string needs to be specially handled.
    if (inputType.equals("string")) {
      // string casts to false if it is 0 characters long, otherwise true
      VectorExpression lenExpr = createVectorExpression(StringLength.class, childExpr,
          Mode.PROJECTION, null);

      int outputCol = ocm.allocateOutputColumn("integer");
      VectorExpression lenToBoolExpr =
          new CastLongToBooleanViaLongToLong(lenExpr.getOutputColumn(), outputCol);
      lenToBoolExpr.setChildExpressions(new VectorExpression[] {lenExpr});
      ocm.freeOutputColumn(lenExpr.getOutputColumn());
      return lenToBoolExpr;
    }
    // cast(booleanExpr as boolean) case is omitted because planner removes it as a no-op

    return null;
  }

  private VectorExpression getCastToLongExpression(List<ExprNodeDesc> childExpr)
      throws HiveException {
    String inputType = childExpr.get(0).getTypeString();
    // Float family, timestamp are handled via descriptor based lookup, int family needs
    // special handling.
    if (isIntFamily(inputType)) {
      // integer and boolean types require no conversion, so use a no-op
      return getIdentityExpression(childExpr);
    }
    // string type is deliberately omitted -- it's handled elsewhere. See isLegacyPathUDF.

    return null;
  }

  /* Get a [NOT] BETWEEN filter expression. This is treated as a special case
   * because the NOT is actually specified in the expression tree as the first argument,
   * and we don't want any runtime cost for that. So creating the VectorExpression
   * needs to be done differently than the standard way where all arguments are
   * passed to the VectorExpression constructor.
   */
  private VectorExpression getBetweenFilterExpression(List<ExprNodeDesc> childExpr, Mode mode, TypeInfo returnType)
      throws HiveException {

    if (mode == Mode.PROJECTION) {

      // Projection mode is not yet supported for [NOT] BETWEEN. Return null so Vectorizer
      // knows to revert to row-at-a-time execution.
      return null;
    }

    boolean notKeywordPresent = (Boolean) ((ExprNodeConstantDesc) childExpr.get(0)).getValue();
    ExprNodeDesc colExpr = childExpr.get(1);

    // The children after not, might need a cast. Get common types for the two comparisons.
    // Casting for 'between' is handled here as a special case, because the first child is for NOT and doesn't need
    // cast
    TypeInfo commonType = FunctionRegistry.getCommonClassForComparison(childExpr.get(1).getTypeInfo(),
        childExpr.get(2).getTypeInfo());
    if (commonType == null) {

      // Can't vectorize
      return null;
    }
    commonType = FunctionRegistry.getCommonClassForComparison(commonType, childExpr.get(3).getTypeInfo());
    if (commonType == null) {

      // Can't vectorize
      return null;
    }

    List<ExprNodeDesc> castChildren = new ArrayList<ExprNodeDesc>();

    for (ExprNodeDesc desc: childExpr.subList(1, 4)) {
      if (commonType.equals(desc.getTypeInfo())) {
        castChildren.add(desc);
      } else {
        GenericUDF castUdf = getGenericUDFForCast(commonType);
        ExprNodeGenericFuncDesc engfd = new ExprNodeGenericFuncDesc(commonType, castUdf,
            Arrays.asList(new ExprNodeDesc[] { desc }));
        castChildren.add(engfd);
      }
    }
    String colType = commonType.getTypeName();

    // prepare arguments for createVectorExpression
    List<ExprNodeDesc> childrenAfterNot = foldConstantsForUnaryExprs(castChildren);

    // determine class
    Class<?> cl = null;
    if (isIntFamily(colType) && !notKeywordPresent) {
      cl = FilterLongColumnBetween.class;
    } else if (isIntFamily(colType) && notKeywordPresent) {
      cl = FilterLongColumnNotBetween.class;
    } else if (isFloatFamily(colType) && !notKeywordPresent) {
      cl = FilterDoubleColumnBetween.class;
    } else if (isFloatFamily(colType) && notKeywordPresent) {
      cl = FilterDoubleColumnNotBetween.class;
    } else if (colType.equals("string") && !notKeywordPresent) {
      cl = FilterStringColumnBetween.class;
    } else if (colType.equals("string") && notKeywordPresent) {
      cl = FilterStringColumnNotBetween.class;
    } else if (colType.equals("timestamp")) {

      // Get timestamp boundary values as longs instead of the expected strings
      long left = getTimestampScalar(childExpr.get(2));
      long right = getTimestampScalar(childExpr.get(3));
      childrenAfterNot = new ArrayList<ExprNodeDesc>();
      childrenAfterNot.add(colExpr);
      childrenAfterNot.add(new ExprNodeConstantDesc(left));
      childrenAfterNot.add(new ExprNodeConstantDesc(right));
      if (notKeywordPresent) {
        cl = FilterLongColumnNotBetween.class;
      } else {
        cl = FilterLongColumnBetween.class;
      }
    } else if (isDecimalFamily(colType) && !notKeywordPresent) {
      cl = FilterDecimalColumnBetween.class;
    } else if (isDecimalFamily(colType) && notKeywordPresent) {
      cl = FilterDecimalColumnNotBetween.class;
    } else if (isDateFamily(colType) && !notKeywordPresent) {
      cl = FilterLongColumnBetween.class;
    } else if (isDateFamily(colType) && notKeywordPresent) {
      cl = FilterLongColumnNotBetween.class;
    }
    return createVectorExpression(cl, childrenAfterNot, Mode.PROJECTION, returnType);
  }

  /*
   * Return vector expression for a custom (i.e. not built-in) UDF.
   */
  private VectorExpression getCustomUDFExpression(ExprNodeGenericFuncDesc expr)
      throws HiveException {

    //GenericUDFBridge udfBridge = (GenericUDFBridge) expr.getGenericUDF();
    List<ExprNodeDesc> childExprList = expr.getChildren();

    // argument descriptors
    VectorUDFArgDesc[] argDescs = new VectorUDFArgDesc[expr.getChildren().size()];
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
        VectorExpression e = getVectorExpression(child, Mode.PROJECTION);
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
    String resultType = expr.getTypeInfo().getTypeName();
    String resultColVectorType = getNormalizedTypeName(resultType);

    outputCol = ocm.allocateOutputColumn(resultColVectorType);

    // Make vectorized operator
    VectorExpression ve = new VectorUDFAdaptor(expr, outputCol, resultColVectorType, argDescs);

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

  public static boolean isStringFamily(String resultType) {
    return resultType.equalsIgnoreCase("string");
  }

  public static boolean isDatetimeFamily(String resultType) {
    return resultType.equalsIgnoreCase("timestamp") || resultType.equalsIgnoreCase("date");
  }

  public static boolean isTimestampFamily(String resultType) {
    return resultType.equalsIgnoreCase("timestamp");
  }
  
  public static boolean isDateFamily(String resultType) {
    return resultType.equalsIgnoreCase("date");
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
        || resultType.equalsIgnoreCase("boolean")
        || resultType.equalsIgnoreCase("long");
  }

  public static boolean isDecimalFamily(String colType) {
      return decimalTypePattern.matcher(colType).matches();
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
    } else if (decimalTypePattern.matcher(constDesc.getTypeString()).matches()) {
      HiveDecimal hd = (HiveDecimal) constDesc.getValue();
      Decimal128 dvalue = new Decimal128();
      dvalue.update(hd.unscaledValue(), (short) hd.scale());
      return dvalue;
    } else {
      return constDesc.getValue();
    }
  }

  private long getIntFamilyScalarAsLong(ExprNodeConstantDesc constDesc)
      throws HiveException {
    Object o = getScalarValue(constDesc);
    if (o instanceof Integer) {
      return (Integer) o;
    } else if (o instanceof Long) {
      return (Long) o;
    }
    throw new HiveException("Unexpected type when converting to long : "+o.getClass().getSimpleName());
  }

  private double getNumericScalarAsDouble(ExprNodeDesc constDesc)
      throws HiveException {
    Object o = getScalarValue((ExprNodeConstantDesc) constDesc);
    if (o instanceof Double) {
      return (Double) o;
    } else if (o instanceof Float) {
      return (Float) o;
    } else if (o instanceof Integer) {
      return (Integer) o;
    } else if (o instanceof Long) {
      return (Long) o;
    }
    throw new HiveException("Unexpected type when converting to double");
  }

  private Object getVectorTypeScalarValue(ExprNodeConstantDesc constDesc) throws HiveException {
    String t = constDesc.getTypeInfo().getTypeName();
    if (isTimestampFamily(t)) {
      return TimestampUtils.getTimeNanoSec((Timestamp) getScalarValue(constDesc));
    } else if (isDateFamily(t)) {
      return DateWritable.dateToDays((Date) getScalarValue(constDesc));
    } else {
      return getScalarValue(constDesc);
    }
  }

  // Get a timestamp as a long in number of nanos, from a string constant or cast
  private long getTimestampScalar(ExprNodeDesc expr) throws HiveException {
    if (expr instanceof ExprNodeGenericFuncDesc &&
        ((ExprNodeGenericFuncDesc) expr).getGenericUDF() instanceof GenericUDFTimestamp) {
      return evaluateCastToTimestamp(expr);
    }
    if (!(expr instanceof ExprNodeConstantDesc)) {
      throw new HiveException("Constant timestamp value expected for expression argument. " +
          "Non-constant argument not supported for vectorization.");
    }
    ExprNodeConstantDesc constExpr = (ExprNodeConstantDesc) expr;
    if (constExpr.getTypeString().equals("string")) {

      // create expression tree with type cast from string to timestamp
      ExprNodeGenericFuncDesc expr2 = new ExprNodeGenericFuncDesc();
      GenericUDFTimestamp f = new GenericUDFTimestamp();
      expr2.setGenericUDF(f);
      ArrayList<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
      children.add(expr);
      expr2.setChildren(children);

      // initialize and evaluate
      return evaluateCastToTimestamp(expr2);
    }

    throw new HiveException("Udf: unhandled constant type for scalar argument. "
        + "Expecting string.");
  }

  private long evaluateCastToTimestamp(ExprNodeDesc expr) throws HiveException {
    ExprNodeGenericFuncDesc expr2 = (ExprNodeGenericFuncDesc) expr;
    ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(expr2);
    ObjectInspector output = evaluator.initialize(null);
    Object constant = evaluator.evaluate(null);
    Object java = ObjectInspectorUtils.copyToStandardJavaObject(constant, output);

    if (!(java instanceof Timestamp)) {
      throw new HiveException("Udf: failed to convert to timestamp");
    }
    Timestamp ts = (Timestamp) java;
    return TimestampUtils.getTimeNanoSec(ts);
  }

  private Constructor<?> getConstructor(Class<?> cl) throws HiveException {
    try {
      Constructor<?> [] ctors = cl.getDeclaredConstructors();
      if (ctors.length == 1) {
        return ctors[0];
      }
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

  static String getNormalizedTypeName(String colType){
    String normalizedType = null;
    if (colType.equalsIgnoreCase("Double") || colType.equalsIgnoreCase("Float")) {
      normalizedType = "Double";
    } else if (colType.equalsIgnoreCase("String")) {
      normalizedType = "String";
   } else if (decimalTypePattern.matcher(colType).matches()) {
      //Return the decimal type as is, it includes scale and precision.
      normalizedType = colType;
    } else {
      normalizedType = "Long";
    }
    return normalizedType;
  }

  static Object[][] aggregatesDefinition = {
    {"min",       "Long",   VectorUDAFMinLong.class},
    {"min",       "Double", VectorUDAFMinDouble.class},
    {"min",       "String", VectorUDAFMinString.class},
    {"min",       "Decimal",VectorUDAFMinDecimal.class},
    {"max",       "Long",   VectorUDAFMaxLong.class},
    {"max",       "Double", VectorUDAFMaxDouble.class},
    {"max",       "String", VectorUDAFMaxString.class},
    {"max",       "Decimal",VectorUDAFMaxDecimal.class},
    {"count",     null,     VectorUDAFCountStar.class},
    {"count",     "Long",   VectorUDAFCount.class},
    {"count",     "Double", VectorUDAFCount.class},
    {"count",     "String", VectorUDAFCount.class},
    {"count",     "Decimal",VectorUDAFCount.class},
    {"sum",       "Long",   VectorUDAFSumLong.class},
    {"sum",       "Double", VectorUDAFSumDouble.class},
    {"sum",       "Decimal",VectorUDAFSumDecimal.class},
    {"avg",       "Long",   VectorUDAFAvgLong.class},
    {"avg",       "Double", VectorUDAFAvgDouble.class},
    {"avg",       "Decimal",VectorUDAFAvgDecimal.class},
    {"variance",  "Long",   VectorUDAFVarPopLong.class},
    {"var_pop",   "Long",   VectorUDAFVarPopLong.class},
    {"variance",  "Double", VectorUDAFVarPopDouble.class},
    {"var_pop",   "Double", VectorUDAFVarPopDouble.class},
    {"variance",  "Decimal",VectorUDAFVarPopDecimal.class},
    {"var_pop",   "Decimal",VectorUDAFVarPopDecimal.class},
    {"var_samp",  "Long",   VectorUDAFVarSampLong.class},
    {"var_samp" , "Double", VectorUDAFVarSampDouble.class},
    {"var_samp" , "Decimal",VectorUDAFVarSampDecimal.class},
    {"std",       "Long",   VectorUDAFStdPopLong.class},
    {"stddev",    "Long",   VectorUDAFStdPopLong.class},
    {"stddev_pop","Long",   VectorUDAFStdPopLong.class},
    {"std",       "Double", VectorUDAFStdPopDouble.class},
    {"stddev",    "Double", VectorUDAFStdPopDouble.class},
    {"stddev_pop","Double", VectorUDAFStdPopDouble.class},
    {"std",       "Decimal",VectorUDAFStdPopDecimal.class},
    {"stddev",    "Decimal",VectorUDAFStdPopDecimal.class},
    {"stddev_pop","Decimal",VectorUDAFStdPopDecimal.class},
    {"stddev_samp","Long",  VectorUDAFStdSampLong.class},
    {"stddev_samp","Double",VectorUDAFStdSampDouble.class},
    {"stddev_samp","Decimal",VectorUDAFStdSampDecimal.class},
  };

  public VectorAggregateExpression getAggregatorExpression(AggregationDesc desc)
      throws HiveException {

    ArrayList<ExprNodeDesc> paramDescList = desc.getParameters();
    VectorExpression[] vectorParams = new VectorExpression[paramDescList.size()];

    for (int i = 0; i< paramDescList.size(); ++i) {
      ExprNodeDesc exprDesc = paramDescList.get(i);
      vectorParams[i] = this.getVectorExpression(exprDesc, Mode.PROJECTION);
    }

    String aggregateName = desc.getGenericUDAFName();
    String inputType = null;

    if (paramDescList.size() > 0) {
      ExprNodeDesc inputExpr = paramDescList.get(0);
      inputType = getNormalizedTypeName(inputExpr.getTypeString());
      if (decimalTypePattern.matcher(inputType).matches()) {
        inputType = "Decimal";
      }
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
        } catch (Exception e) {
          throw new HiveException("Internal exception for vector aggregate : \"" +
               aggregateName + "\" for type: \"" + inputType + "", e);
        }
      }
    }

    throw new HiveException("Vector aggregate not implemented: \"" + aggregateName +
        "\" for type: \"" + inputType + "");
  }

  public Map<Integer, String> getOutputColumnTypeMap() {
    Map<Integer, String> map = new HashMap<Integer, String>();
    for (int i = 0; i < ocm.outputColCount; i++) {
      String type = ocm.outputColumnsTypes[i];
      map.put(i+this.firstOutputColumnIndex, type);
    }
    return map;
  }

  public Map<String, Integer> getColumnMap() {
    return columnMap;
  }

  public void addToColumnMap(String columnName, int outputColumn) throws HiveException {
    if (columnMap.containsKey(columnName) && (columnMap.get(columnName) != outputColumn)) {
      throw new HiveException(String.format("Column %s is already mapped to %d. Cannot remap to %d.",
          columnName, columnMap.get(columnName), outputColumn));
    }
    columnMap.put(columnName, outputColumn);
  }
 }
