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

package org.apache.hadoop.hive.ql.exec.vector;

import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.common.type.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.ArgumentType;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.InputExpressionType;
import org.apache.hadoop.hive.ql.exec.vector.expressions.*;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.*;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFArgDesc;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicValueDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.udf.*;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.AnnotationUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Context class for vectorization execution.
 * Main role is to map column names to column indices and serves as a
 * factory class for building vectorized expressions out of descriptors.
 *
 */
public class VectorizationContext {

  private static final Logger LOG = LoggerFactory.getLogger(
      VectorizationContext.class.getName());

  private final String contextName;
  private final int level;

  VectorExpressionDescriptor vMap;

  private final List<String> initialColumnNames;
  private List<TypeInfo> initialTypeInfos;
  private List<DataTypePhysicalVariation> initialDataTypePhysicalVariations;

  private List<Integer> projectedColumns;
  private List<String> projectionColumnNames;
  private Map<String, Integer> projectionColumnMap;

  //columnName to column position map
  // private final Map<String, Integer> columnMap;
  private int firstOutputColumnIndex;

  public enum HiveVectorAdaptorUsageMode {
    NONE,
    CHOSEN,
    ALL;

    public static HiveVectorAdaptorUsageMode getHiveConfValue(HiveConf hiveConf) {
      String string = HiveConf.getVar(hiveConf,
          HiveConf.ConfVars.HIVE_VECTOR_ADAPTOR_USAGE_MODE);
      return valueOf(string.toUpperCase());
    }
  }

  private HiveVectorAdaptorUsageMode hiveVectorAdaptorUsageMode;
  private boolean testVectorAdaptorOverride;

  public enum HiveVectorIfStmtMode {
    ADAPTOR,
    GOOD,
    BETTER;

    public static HiveVectorIfStmtMode getHiveConfValue(HiveConf hiveConf) {
      String string = HiveConf.getVar(hiveConf,
          HiveConf.ConfVars.HIVE_VECTORIZED_IF_EXPR_MODE);
      return valueOf(string.toUpperCase());
    }
  }

  private HiveVectorIfStmtMode hiveVectorIfStmtMode;

  //when set to true use the overflow checked vector expressions
  private boolean useCheckedVectorExpressions;

  private boolean reuseScratchColumns =
      HiveConf.ConfVars.HIVE_VECTORIZATION_TESTING_REUSE_SCRATCH_COLUMNS.defaultBoolVal;

  private boolean adaptorSuppressEvaluateExceptions;

  private void setHiveConfVars(HiveConf hiveConf) {
    hiveVectorAdaptorUsageMode = HiveVectorAdaptorUsageMode.getHiveConfValue(hiveConf);
    testVectorAdaptorOverride =
        HiveConf.getBoolVar(hiveConf, ConfVars.HIVE_TEST_VECTOR_ADAPTOR_OVERRIDE);
    hiveVectorIfStmtMode = HiveVectorIfStmtMode.getHiveConfValue(hiveConf);
    this.reuseScratchColumns =
        HiveConf.getBoolVar(hiveConf, ConfVars.HIVE_VECTORIZATION_TESTING_REUSE_SCRATCH_COLUMNS);
    this.ocm.setReuseColumns(reuseScratchColumns);
    useCheckedVectorExpressions =
        HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_VECTORIZATION_USE_CHECKED_EXPRESSIONS);
    adaptorSuppressEvaluateExceptions =
        HiveConf.getBoolVar(
            hiveConf, HiveConf.ConfVars.HIVE_VECTORIZED_ADAPTOR_SUPPRESS_EVALUATE_EXCEPTIONS);
  }

  private void copyHiveConfVars(VectorizationContext vContextEnvironment) {
    hiveVectorAdaptorUsageMode = vContextEnvironment.hiveVectorAdaptorUsageMode;
    testVectorAdaptorOverride = vContextEnvironment.testVectorAdaptorOverride;
    hiveVectorIfStmtMode = vContextEnvironment.hiveVectorIfStmtMode;
    this.reuseScratchColumns = vContextEnvironment.reuseScratchColumns;
    useCheckedVectorExpressions = vContextEnvironment.useCheckedVectorExpressions;
    adaptorSuppressEvaluateExceptions = vContextEnvironment.adaptorSuppressEvaluateExceptions;
    this.ocm.setReuseColumns(reuseScratchColumns);
  }

  // Convenient constructor for initial batch creation takes
  // a list of columns names and maps them to 0..n-1 indices.
  public VectorizationContext(
      String contextName,
      List<String> initialColumnNames,
      List<TypeInfo> initialTypeInfos,
      List<DataTypePhysicalVariation> initialDataTypePhysicalVariations,
      HiveConf hiveConf) {
    this.contextName = contextName;
    level = 0;
    this.initialColumnNames = initialColumnNames;
    this.initialTypeInfos = initialTypeInfos;
    this.initialDataTypePhysicalVariations = initialDataTypePhysicalVariations;
    this.projectionColumnNames = initialColumnNames;

    projectedColumns = new ArrayList<Integer>();
    projectionColumnMap = new HashMap<String, Integer>();
    for (int i = 0; i < this.projectionColumnNames.size(); i++) {
      projectedColumns.add(i);
      projectionColumnMap.put(projectionColumnNames.get(i), i);
    }

    int firstOutputColumnIndex = projectedColumns.size();
    this.ocm = new OutputColumnManager(firstOutputColumnIndex);
    this.firstOutputColumnIndex = firstOutputColumnIndex;
    vMap = new VectorExpressionDescriptor();

    if (hiveConf != null) {
      setHiveConfVars(hiveConf);
    }
  }

  // Convenient constructor for initial batch creation takes
  // a list of columns names and maps them to 0..n-1 indices.
  public VectorizationContext(String contextName, List<String> initialColumnNames,
      HiveConf hiveConf) {
    this.contextName = contextName;
    level = 0;
    this.initialColumnNames = initialColumnNames;
    this.projectionColumnNames = initialColumnNames;

    projectedColumns = new ArrayList<Integer>();
    projectionColumnMap = new HashMap<String, Integer>();
    for (int i = 0; i < this.projectionColumnNames.size(); i++) {
      projectedColumns.add(i);
      projectionColumnMap.put(projectionColumnNames.get(i), i);
    }

    int firstOutputColumnIndex = projectedColumns.size();
    this.ocm = new OutputColumnManager(firstOutputColumnIndex);
    this.firstOutputColumnIndex = firstOutputColumnIndex;
    vMap = new VectorExpressionDescriptor();

    if (hiveConf != null) {
      setHiveConfVars(hiveConf);
    }
  }

  public VectorizationContext(String contextName, List<String> initialColumnNames,
      VectorizationContext vContextEnvironment) {
    this(contextName, initialColumnNames, (HiveConf) null);
    copyHiveConfVars(vContextEnvironment);
  }

  @VisibleForTesting
  public VectorizationContext(String contextName, List<String> initialColumnNames) {
    this(contextName, initialColumnNames, (HiveConf) null);
  }

  // Constructor to with the individual addInitialColumn method
  // followed by a call to finishedAddingInitialColumns.
  public VectorizationContext(String contextName, HiveConf hiveConf) {
    this.contextName = contextName;
    level = 0;
    initialColumnNames = new ArrayList<String>();
    projectedColumns = new ArrayList<Integer>();
    projectionColumnNames = new ArrayList<String>();
    projectionColumnMap = new HashMap<String, Integer>();
    this.ocm = new OutputColumnManager(0);
    this.firstOutputColumnIndex = 0;
    vMap = new VectorExpressionDescriptor();

    if (hiveConf != null) {
      setHiveConfVars(hiveConf);
    }

  }

  @VisibleForTesting
  public VectorizationContext(String contextName) {
    this(contextName, (HiveConf) null);
  }

  // Constructor useful making a projection vectorization context.  E.g. VectorSelectOperator.
  // Use with resetProjectionColumns and addProjectionColumn.
  // Keeps existing output column map, etc.
  public VectorizationContext(String contextName, VectorizationContext vContext) {
    this.contextName = contextName;
    level = vContext.level + 1;
    this.initialColumnNames = vContext.initialColumnNames;
    this.initialTypeInfos = vContext.initialTypeInfos;
    this.initialDataTypePhysicalVariations = vContext.initialDataTypePhysicalVariations;
    this.projectedColumns = new ArrayList<Integer>();
    this.projectionColumnNames = new ArrayList<String>();
    this.projectionColumnMap = new HashMap<String, Integer>();

    this.ocm = vContext.ocm;
    this.firstOutputColumnIndex = vContext.firstOutputColumnIndex;
    vMap = new VectorExpressionDescriptor();

    copyHiveConfVars(vContext);
  }

  // Add an initial column to a vectorization context when
  // a vectorized row batch is being created.
  public void addInitialColumn(String columnName) {
    initialColumnNames.add(columnName);
    int index = projectedColumns.size();
    projectedColumns.add(index);
    projectionColumnNames.add(columnName);
    projectionColumnMap.put(columnName, index);
  }

  // Finishes the vectorization context after all the initial
  // columns have been added.
  @VisibleForTesting
  public void finishedAddingInitialColumns() {
    int firstOutputColumnIndex = projectedColumns.size();
    this.ocm = new OutputColumnManager(firstOutputColumnIndex);
    this.ocm.setReuseColumns(this.reuseScratchColumns);
    this.firstOutputColumnIndex = firstOutputColumnIndex;
  }

  // Empties the projection columns.
  public void resetProjectionColumns() {
    projectedColumns = new ArrayList<Integer>();
    projectionColumnNames = new ArrayList<String>();
    projectionColumnMap = new HashMap<String, Integer>();
  }

  // Add a projection column to a projection vectorization context.
  public void addProjectionColumn(String columnName, int vectorBatchColIndex) {
    if (vectorBatchColIndex < 0) {
      throw new RuntimeException("Negative projected column number");
    }
    projectedColumns.add(vectorBatchColIndex);
    projectionColumnNames.add(columnName);
    projectionColumnMap.put(columnName, vectorBatchColIndex);
  }

  public void setInitialTypeInfos(List<TypeInfo> initialTypeInfos) {
    this.initialTypeInfos = initialTypeInfos;
    final int size = initialTypeInfos.size();
    initialDataTypePhysicalVariations = new ArrayList<DataTypePhysicalVariation>(size);
    for (int i = 0; i < size; i++) {
      initialDataTypePhysicalVariations.add(DataTypePhysicalVariation.NONE);
    }
  }

  public void setInitialDataTypePhysicalVariations(
      List<DataTypePhysicalVariation> initialDataTypePhysicalVariations) {
    this.initialDataTypePhysicalVariations = initialDataTypePhysicalVariations;
  }

  public List<String> getInitialColumnNames() {
    return initialColumnNames;
  }

  public List<Integer> getProjectedColumns() {
    return projectedColumns;
  }

  public List<String> getProjectionColumnNames() {
    return projectionColumnNames;
  }

  public Map<String, Integer> getProjectionColumnMap() {
    return projectionColumnMap;
  }

  public TypeInfo[] getInitialTypeInfos() {
    return initialTypeInfos.toArray(new TypeInfo[0]);
  }

  public TypeInfo getTypeInfo(int columnNum) throws HiveException {
    if (initialTypeInfos == null) {
      throw new HiveException("initialTypeInfos array is null in contextName " + contextName);
    }
    final int initialSize = initialTypeInfos.size();
    if (columnNum < initialSize) {
      return initialTypeInfos.get(columnNum);
    } else {
      String typeName = ocm.getScratchTypeName(columnNum);

      // Replace unparsable synonyms.
      typeName = VectorizationContext.mapTypeNameSynonyms(typeName);

      // Make CHAR and VARCHAR type info parsable.
      if (typeName.equals("char")) {
        typeName = "char(" + HiveChar.MAX_CHAR_LENGTH + ")";
      } else if (typeName.equals("varchar")) {
        typeName = "varchar(" + HiveVarchar.MAX_VARCHAR_LENGTH + ")";
      }

      TypeInfo typeInfo =
          TypeInfoUtils.getTypeInfoFromTypeString(typeName);
      return typeInfo;
    }
  }

  public DataTypePhysicalVariation getDataTypePhysicalVariation(int columnNum) throws HiveException {
    if (initialDataTypePhysicalVariations == null) {
      return null;
    }
    if (columnNum < initialDataTypePhysicalVariations.size()) {
      return initialDataTypePhysicalVariations.get(columnNum);
    }
    return ocm.getDataTypePhysicalVariation(columnNum);
  }

  public TypeInfo[] getAllTypeInfos() throws HiveException {
    final int size = initialTypeInfos.size() + ocm.outputColCount;

    TypeInfo[] result = new TypeInfo[size];
    for (int i = 0; i < size; i++) {
      result[i] = getTypeInfo(i);
    }
    return result;
  }

  public static final Pattern decimalTypePattern = Pattern.compile("decimal.*",
      Pattern.CASE_INSENSITIVE);

  public static final Pattern charTypePattern = Pattern.compile("char.*",
	      Pattern.CASE_INSENSITIVE);

  public static final Pattern varcharTypePattern = Pattern.compile("varchar.*",
	      Pattern.CASE_INSENSITIVE);

  public static final Pattern charVarcharTypePattern = Pattern.compile("char.*|varchar.*",
      Pattern.CASE_INSENSITIVE);

  public static final Pattern structTypePattern = Pattern.compile("struct.*",
      Pattern.CASE_INSENSITIVE);

  public static final Pattern listTypePattern = Pattern.compile("array.*",
      Pattern.CASE_INSENSITIVE);

  public static final Pattern mapTypePattern = Pattern.compile("map.*",
      Pattern.CASE_INSENSITIVE);

  //Map column number to type (this is always non-null for a useful vec context)
  private OutputColumnManager ocm;

  // Set of UDF classes for type casting data types in row-mode.
  private static Set<Class<?>> castExpressionUdfs = new HashSet<Class<?>>();
  static {
    castExpressionUdfs.add(GenericUDFToString.class);
    castExpressionUdfs.add(GenericUDFToDecimal.class);
    castExpressionUdfs.add(GenericUDFToBinary.class);
    castExpressionUdfs.add(GenericUDFToDate.class);
    castExpressionUdfs.add(GenericUDFToUnixTimeStamp.class);
    castExpressionUdfs.add(GenericUDFToUtcTimestamp.class);
    castExpressionUdfs.add(GenericUDFToChar.class);
    castExpressionUdfs.add(GenericUDFToVarchar.class);
    castExpressionUdfs.add(GenericUDFTimestamp.class);
    castExpressionUdfs.add(GenericUDFToIntervalYearMonth.class);
    castExpressionUdfs.add(GenericUDFToIntervalDayTime.class);
    castExpressionUdfs.add(UDFToByte.class);
    castExpressionUdfs.add(UDFToBoolean.class);
    castExpressionUdfs.add(UDFToDouble.class);
    castExpressionUdfs.add(UDFToFloat.class);
    castExpressionUdfs.add(UDFToInteger.class);
    castExpressionUdfs.add(UDFToLong.class);
    castExpressionUdfs.add(UDFToShort.class);
  }

  // Set of GenericUDFs which require need implicit type casting of decimal parameters.
  // Vectorization for mathmatical functions currently depends on decimal params automatically
  // being converted to the return type (see getImplicitCastExpression()), which is not correct
  // in the general case. This set restricts automatic type conversion to just these functions.
  private static Set<Class<?>> udfsNeedingImplicitDecimalCast = new HashSet<Class<?>>();
  static {
    udfsNeedingImplicitDecimalCast.add(GenericUDFOPPlus.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFOPMinus.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFOPMultiply.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFOPDivide.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFOPMod.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFRound.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFBRound.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFFloor.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFCbrt.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFCeil.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFAbs.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFPosMod.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFPower.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFFactorial.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFOPPositive.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFOPNegative.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFCoalesce.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFElt.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFGreatest.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFLeast.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFIn.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFOPEqual.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFOPEqualNS.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFOPNotEqual.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFOPLessThan.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFOPEqualOrLessThan.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFOPGreaterThan.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFOPEqualOrGreaterThan.class);
    udfsNeedingImplicitDecimalCast.add(GenericUDFBetween.class);
    udfsNeedingImplicitDecimalCast.add(UDFSqrt.class);
    udfsNeedingImplicitDecimalCast.add(UDFRand.class);
    udfsNeedingImplicitDecimalCast.add(UDFLn.class);
    udfsNeedingImplicitDecimalCast.add(UDFLog2.class);
    udfsNeedingImplicitDecimalCast.add(UDFSin.class);
    udfsNeedingImplicitDecimalCast.add(UDFAsin.class);
    udfsNeedingImplicitDecimalCast.add(UDFCos.class);
    udfsNeedingImplicitDecimalCast.add(UDFAcos.class);
    udfsNeedingImplicitDecimalCast.add(UDFLog10.class);
    udfsNeedingImplicitDecimalCast.add(UDFLog.class);
    udfsNeedingImplicitDecimalCast.add(UDFExp.class);
    udfsNeedingImplicitDecimalCast.add(UDFDegrees.class);
    udfsNeedingImplicitDecimalCast.add(UDFRadians.class);
    udfsNeedingImplicitDecimalCast.add(UDFAtan.class);
    udfsNeedingImplicitDecimalCast.add(UDFTan.class);
    udfsNeedingImplicitDecimalCast.add(UDFOPLongDivide.class);
  }

  protected boolean needsImplicitCastForDecimal(GenericUDF udf) {
    Class<?> udfClass = udf.getClass();
    if (udf instanceof GenericUDFBridge) {
      udfClass = ((GenericUDFBridge) udf).getUdfClass();
    }
    return udfsNeedingImplicitDecimalCast.contains(udfClass);
  }

  public int getInputColumnIndex(String name) throws HiveException {
    if (name == null) {
      throw new HiveException("Null column name");
    }
    if (!projectionColumnMap.containsKey(name)) {
      throw new HiveException(String.format("The column %s is not in the vectorization context column map %s.",
                 name, projectionColumnMap.toString()));
    }
    final int projectedColumnNum = projectionColumnMap.get(name);
    if (projectedColumnNum < 0) {
      throw new HiveException("Negative projected column number");
    }
    return projectedColumnNum;
  }

  protected int getInputColumnIndex(ExprNodeColumnDesc colExpr) throws HiveException {
    // Call the regular method since it does error checking.
    return getInputColumnIndex(colExpr.getColumn());
  }

  private static class OutputColumnManager {
    private final int initialOutputCol;
    private int outputColCount = 0;
    private boolean reuseScratchColumns = true;

    protected OutputColumnManager(int initialOutputCol) {
      this.initialOutputCol = initialOutputCol;
    }

    //The complete list of output columns. These should be added to the
    //Vectorized row batch for processing. The index in the row batch is
    //equal to the index in this array plus initialOutputCol.
    //Start with size 100 and double when needed.
    private String[] scratchVectorTypeNames = new String[100];
    private DataTypePhysicalVariation[] scratchDataTypePhysicalVariations =
        new DataTypePhysicalVariation[100];

    private final Set<Integer> usedOutputColumns = new HashSet<Integer>();

    int allocateOutputColumn(TypeInfo typeInfo) throws HiveException {
      return allocateOutputColumn(typeInfo, DataTypePhysicalVariation.NONE);
    }

    int allocateOutputColumn(TypeInfo typeInfo,
        DataTypePhysicalVariation dataTypePhysicalVariation) throws HiveException {

        if (initialOutputCol < 0) {
          // This is a test calling.
          return 0;
        }

        // CONCERN: We currently differentiate DECIMAL columns by their precision and scale...,
        // which could lead to a lot of extra unnecessary scratch columns.
        String vectorTypeName = getScratchName(typeInfo);
        int relativeCol = allocateOutputColumnInternal(vectorTypeName, dataTypePhysicalVariation);
        return initialOutputCol + relativeCol;
      }

    private int allocateOutputColumnInternal(String columnType, DataTypePhysicalVariation dataTypePhysicalVariation) {
      for (int i = 0; i < outputColCount; i++) {

        // Re-use an existing, available column of the same required type.
        if (usedOutputColumns.contains(i) ||
            !(scratchVectorTypeNames[i].equalsIgnoreCase(columnType) &&
              scratchDataTypePhysicalVariations[i] == dataTypePhysicalVariation)) {
          continue;
        }
        //Use i
        usedOutputColumns.add(i);
        return i;
      }
      //Out of allocated columns
      if (outputColCount < scratchVectorTypeNames.length) {
        int newIndex = outputColCount;
        scratchVectorTypeNames[outputColCount] = columnType;
        scratchDataTypePhysicalVariations[outputColCount++] = dataTypePhysicalVariation;
        usedOutputColumns.add(newIndex);
        return newIndex;
      } else {
        //Expand the array
        scratchVectorTypeNames = Arrays.copyOf(scratchVectorTypeNames, 2*outputColCount);
        scratchDataTypePhysicalVariations = Arrays.copyOf(scratchDataTypePhysicalVariations, 2*outputColCount);
        int newIndex = outputColCount;
        scratchVectorTypeNames[outputColCount] = columnType;
        scratchDataTypePhysicalVariations[outputColCount++] = dataTypePhysicalVariation;
        usedOutputColumns.add(newIndex);
        return newIndex;
      }
    }

    void freeOutputColumn(int index) {
      if (initialOutputCol < 0 || reuseScratchColumns == false) {
        // This is a test
        return;
      }
      int colIndex = index-initialOutputCol;
      if (colIndex >= 0) {
        usedOutputColumns.remove(index-initialOutputCol);
      }
    }

    public int[] currentScratchColumns() {
      TreeSet<Integer> treeSet = new TreeSet<Integer>();
      for (Integer col : usedOutputColumns) {
        treeSet.add(initialOutputCol + col);
      }
      return ArrayUtils.toPrimitive(treeSet.toArray(new Integer[0]));
    }

    public String getScratchTypeName(int columnNum) {
      return scratchVectorTypeNames[columnNum - initialOutputCol];
    }

    public DataTypePhysicalVariation getDataTypePhysicalVariation(int columnNum) {
      if (scratchDataTypePhysicalVariations == null) {
        return null;
      }
      return scratchDataTypePhysicalVariations[columnNum - initialOutputCol];
    }

    // Allow debugging by disabling column reuse (input cols are never reused by design, only
    // scratch cols are)
    public void setReuseColumns(boolean reuseColumns) {
      this.reuseScratchColumns = reuseColumns;
    }
  }

  public int allocateScratchColumn(TypeInfo typeInfo) throws HiveException {
    return ocm.allocateOutputColumn(typeInfo);
  }

  public int[] currentScratchColumns() {
    return ocm.currentScratchColumns();
  }

  private VectorExpression getFilterOnBooleanColumnExpression(ExprNodeColumnDesc exprDesc,
      int columnNum) throws HiveException {
    VectorExpression expr = null;

    // Evaluate the column as a boolean, converting if necessary.
    TypeInfo typeInfo = exprDesc.getTypeInfo();
    if (typeInfo.getCategory() == Category.PRIMITIVE &&
        ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory() == PrimitiveCategory.BOOLEAN) {
      expr = new SelectColumnIsTrue(columnNum);

      expr.setInputTypeInfos(typeInfo);
      expr.setInputDataTypePhysicalVariations(DataTypePhysicalVariation.NONE);

    } else {
      // Ok, we need to convert.
      ArrayList<ExprNodeDesc> exprAsList = new ArrayList<ExprNodeDesc>(1);
      exprAsList.add(exprDesc);

      // First try our cast method that will handle a few special cases.
      VectorExpression castToBooleanExpr = getCastToBoolean(exprAsList);
      if (castToBooleanExpr == null) {

        // Ok, try the UDF.
        castToBooleanExpr = getVectorExpressionForUdf(null, UDFToBoolean.class, exprAsList,
            VectorExpressionDescriptor.Mode.PROJECTION, TypeInfoFactory.booleanTypeInfo);
        if (castToBooleanExpr == null) {
          throw new HiveException("Cannot vectorize converting expression " +
              exprDesc.getExprString() + " to boolean");
        }
      }

      final int outputColumnNum = castToBooleanExpr.getOutputColumnNum();

      expr = new SelectColumnIsTrue(outputColumnNum);

      expr.setChildExpressions(new VectorExpression[] {castToBooleanExpr});

      expr.setInputTypeInfos(castToBooleanExpr.getOutputTypeInfo());
      expr.setInputDataTypePhysicalVariations(DataTypePhysicalVariation.NONE);
    }
    return expr;
  }

  private VectorExpression getColumnVectorExpression(ExprNodeColumnDesc exprDesc,
      VectorExpressionDescriptor.Mode mode) throws HiveException {
    int columnNum = getInputColumnIndex(exprDesc.getColumn());
    VectorExpression expr = null;
    switch (mode) {
    case FILTER:
      expr = getFilterOnBooleanColumnExpression(exprDesc, columnNum);
      break;
    case PROJECTION:
      {
        expr = new IdentityExpression(columnNum);

        TypeInfo identityTypeInfo = exprDesc.getTypeInfo();
        DataTypePhysicalVariation identityDataTypePhysicalVariation =
            getDataTypePhysicalVariation(columnNum);

        expr.setInputTypeInfos(identityTypeInfo);
        expr.setInputDataTypePhysicalVariations(identityDataTypePhysicalVariation);

        expr.setOutputTypeInfo(identityTypeInfo);
        expr.setOutputDataTypePhysicalVariation(identityDataTypePhysicalVariation);
      }
      break;
    default:
      throw new RuntimeException("Unexpected mode " + mode);
    }
    return expr;
  }

  public VectorExpression[] getVectorExpressionsUpConvertDecimal64(List<ExprNodeDesc> exprNodes)
      throws HiveException {
    VectorExpression[] vecExprs =
        getVectorExpressions(exprNodes, VectorExpressionDescriptor.Mode.PROJECTION);
    final int size = vecExprs.length;
    for (int i = 0; i < size; i++) {
      VectorExpression vecExpr = vecExprs[i];
      if (vecExpr.getOutputColumnVectorType() == ColumnVector.Type.DECIMAL_64) {
        vecExprs[i] = wrapWithDecimal64ToDecimalConversion(vecExpr);
      }
    }
    return vecExprs;
  }

  public VectorExpression[] getVectorExpressions(List<ExprNodeDesc> exprNodes) throws HiveException {
    return getVectorExpressions(exprNodes, VectorExpressionDescriptor.Mode.PROJECTION);
  }

  public VectorExpression[] getVectorExpressions(List<ExprNodeDesc> exprNodes, VectorExpressionDescriptor.Mode mode)
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
    return getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.PROJECTION);
  }

  /**
   * Returns a vector expression for a given expression
   * description.
   * @param exprDesc, Expression description
   * @param mode
   * @return {@link VectorExpression}
   * @throws HiveException
   */
  public VectorExpression getVectorExpression(ExprNodeDesc exprDesc, VectorExpressionDescriptor.Mode mode) throws HiveException {
    VectorExpression ve = null;
    if (exprDesc instanceof ExprNodeColumnDesc) {
      ve = getColumnVectorExpression((ExprNodeColumnDesc) exprDesc, mode);
    } else if (exprDesc instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc expr = (ExprNodeGenericFuncDesc) exprDesc;
      // push not through between...
      if ("not".equals(expr.getFuncText())) {
        if (expr.getChildren() != null && expr.getChildren().size() == 1) {
          ExprNodeDesc child = expr.getChildren().get(0);
          if (child instanceof ExprNodeGenericFuncDesc) {
            ExprNodeGenericFuncDesc childExpr = (ExprNodeGenericFuncDesc) child;
            if ("between".equals(childExpr.getFuncText())) {
              ExprNodeConstantDesc flag = (ExprNodeConstantDesc) childExpr.getChildren().get(0);
              List<ExprNodeDesc> newChildren = new ArrayList<>();
              if (Boolean.TRUE.equals(flag.getValue())) {
                newChildren.add(new ExprNodeConstantDesc(Boolean.FALSE));
              } else {
                newChildren.add(new ExprNodeConstantDesc(Boolean.TRUE));
              }
              newChildren
                  .addAll(childExpr.getChildren().subList(1, childExpr.getChildren().size()));
              expr.setTypeInfo(childExpr.getTypeInfo());
              expr.setGenericUDF(childExpr.getGenericUDF());
              expr.setChildren(newChildren);
            }
          }
        }
      }
      // Add cast expression if needed. Child expressions of a udf may return different data types
      // and that would require converting their data types to evaluate the udf.
      // For example decimal column added to an integer column would require integer column to be
      // cast to decimal.
      // Note: this is a no-op for custom UDFs
      List<ExprNodeDesc> childExpressions = getChildExpressionsWithImplicitCast(expr.getGenericUDF(),
          exprDesc.getChildren(), exprDesc.getTypeInfo());

      // Are we forcing the usage of VectorUDFAdaptor for test purposes?
      if (!testVectorAdaptorOverride) {
        ve = getGenericUdfVectorExpression(expr.getGenericUDF(),
            childExpressions, mode, exprDesc.getTypeInfo());
      }
      if (ve == null) {
        // Ok, no vectorized class available.  No problem -- try to use the VectorUDFAdaptor
        // when configured.
        //
        // NOTE: We assume if hiveVectorAdaptorUsageMode has not been set it because we are
        // executing a test that didn't create a HiveConf, etc.  No usage of VectorUDFAdaptor in
        // that case.
        if (hiveVectorAdaptorUsageMode != null) {
          switch (hiveVectorAdaptorUsageMode) {
          case NONE:
            // No VectorUDFAdaptor usage.
            throw new HiveException(
                "Could not vectorize expression (mode = " + mode.name() + "): " + exprDesc.toString()
                  + " because hive.vectorized.adaptor.usage.mode=none");
          case CHOSEN:
            if (isNonVectorizedPathUDF(expr, mode)) {
              ve = getCustomUDFExpression(expr, mode);
            } else {
              throw new HiveException(
                  "Could not vectorize expression (mode = " + mode.name() + "): " + exprDesc.toString()
                    + " because hive.vectorized.adaptor.usage.mode=chosen"
                    + " and the UDF wasn't one of the chosen ones");
            }
            break;
          case ALL:
            if (LOG.isDebugEnabled()) {
              LOG.debug("We will try to use the VectorUDFAdaptor for " + exprDesc.toString()
                  + " because hive.vectorized.adaptor.usage.mode=all");
            }
            ve = getCustomUDFExpression(expr, mode);
            break;
          default:
            throw new RuntimeException("Unknown hive vector adaptor usage mode " +
              hiveVectorAdaptorUsageMode.name());
          }
          if (ve == null) {
            throw new HiveException(
                "Unable vectorize expression (mode = " + mode.name() + "): " + exprDesc.toString()
                  + " even for the VectorUDFAdaptor");
          }
        }
      }
    } else if (exprDesc instanceof ExprNodeConstantDesc) {
      ve = getConstantVectorExpression(((ExprNodeConstantDesc) exprDesc).getValue(), exprDesc.getTypeInfo(),
          mode);
    } else if (exprDesc instanceof ExprNodeDynamicValueDesc) {
      ve = getDynamicValueVectorExpression((ExprNodeDynamicValueDesc) exprDesc, mode);
    } else if (exprDesc instanceof ExprNodeFieldDesc) {
      // Get the GenericUDFStructField to process the field of Struct type
      ve = getGenericUDFStructField((ExprNodeFieldDesc)exprDesc,
          mode, exprDesc.getTypeInfo());
    }
    if (ve == null) {
      throw new HiveException(
          "Could not vectorize expression (mode = " + mode.name() + "): " + exprDesc.toString());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Input Expression = " + exprDesc.toString()
          + ", Vectorized Expression = " + ve.toString());
    }

    return ve;
  }

  private VectorExpression getGenericUDFStructField(ExprNodeFieldDesc exprNodeFieldDesc,
      VectorExpressionDescriptor.Mode mode, TypeInfo returnType) throws HiveException {
    // set the arguments for GenericUDFStructField
    List<ExprNodeDesc> children = new ArrayList<>(2);
    children.add(exprNodeFieldDesc.getDesc());
    children.add(new ExprNodeConstantDesc(getStructFieldIndex(exprNodeFieldDesc)));

    return getVectorExpressionForUdf(null, GenericUDFStructField.class, children, mode, returnType);
  }

  /**
   * The field of Struct is stored in StructColumnVector.fields[index].
   * Check the StructTypeInfo.getAllStructFieldNames() and compare to the field name, get the index.
   */
  private int getStructFieldIndex(ExprNodeFieldDesc exprNodeFieldDesc) throws HiveException {
    ExprNodeDesc structNodeDesc = exprNodeFieldDesc.getDesc();
    String fieldName = exprNodeFieldDesc.getFieldName();
    StructTypeInfo structTypeInfo = (StructTypeInfo) structNodeDesc.getTypeInfo();
    int index = 0;
    boolean isFieldExist = false;
    for (String fn : structTypeInfo.getAllStructFieldNames()) {
      if (fieldName.equals(fn)) {
        isFieldExist = true;
        break;
      }
      index++;
    }
    if (isFieldExist) {
      return index;
    } else {
      throw new HiveException("Could not vectorize expression:" + exprNodeFieldDesc.toString()
          + ", the field " + fieldName + " doesn't exist.");
    }
  }

  /**
   * Given a udf and its children, return the common type to which the children's type should be
   * cast.
   */
  private TypeInfo getCommonTypeForChildExpressions(GenericUDF genericUdf,
      List<ExprNodeDesc> children, TypeInfo returnType) throws HiveException {
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
      TypeInfo colTi = children.get(0).getTypeInfo();
      if (colTi.getCategory() != Category.PRIMITIVE) {
        return colTi; // Handled later, only struct will be supported.
      }
      TypeInfo opTi = GenericUDFUtils.deriveInType(children);
      if (opTi == null || opTi.getCategory() != Category.PRIMITIVE) {
        throw new HiveException("Cannot vectorize IN() - common type is " + opTi);
      }
      if (((PrimitiveTypeInfo)colTi).getPrimitiveCategory() !=
          ((PrimitiveTypeInfo)opTi).getPrimitiveCategory()) {
        throw new HiveException("Cannot vectorize IN() - casting a column is not supported. "
            + "Column type is " + colTi + " but the common type is " + opTi);
      }
      return colTi;
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

    if (isCustomUDF(genericUDF.getUdfName())) {
      // no implicit casts possible
      return children;
    }

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
    if (genericUDF instanceof GenericUDFElt) {
      int i = 0;
      for (ExprNodeDesc child : children) {
        TypeInfo castType = commonType;
        if (i++ == 0) {
          castType = isIntFamily(child.getTypeString()) ? child.getTypeInfo() : TypeInfoFactory.intTypeInfo;
        }
        ExprNodeDesc castExpression = getImplicitCastExpression(genericUDF, child, castType);
        if (castExpression != null) {
          atleastOneCastNeeded = true;
          childrenWithCasts.add(castExpression);
        } else {
          childrenWithCasts.add(child);
        }
      }
    } else {
      for (ExprNodeDesc child : children) {
        ExprNodeDesc castExpression = getImplicitCastExpression(genericUDF, child, commonType);
        if (castExpression != null) {
          atleastOneCastNeeded = true;
          childrenWithCasts.add(castExpression);
        } else {
          childrenWithCasts.add(child);
        }
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
    // TODO: precision and scale would be practically invalid for string conversion (38,38)
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
      if (needsImplicitCastForDecimal(udf)) {
        // Cast the input to decimal
        // If castType is decimal, try not to lose precision for numeric types.
        castType = updatePrecision(inputTypeInfo, (DecimalTypeInfo) castType);
        GenericUDFToDecimal castToDecimalUDF = new GenericUDFToDecimal();
        castToDecimalUDF.setTypeInfo(castType);
        List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
        children.add(child);
        ExprNodeDesc desc = new ExprNodeGenericFuncDesc(castType, castToDecimalUDF, children);
        return desc;
      }
    } else if (!castTypeDecimal && inputTypeDecimal) {
      if (needsImplicitCastForDecimal(udf)) {
        // Cast decimal input to returnType
        GenericUDF genericUdf = getGenericUDFForCast(castType);
        List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
        children.add(child);
        ExprNodeDesc desc = new ExprNodeGenericFuncDesc(castType, genericUdf, children);
        return desc;
      }
    } else {

      // Casts to exact types including long to double etc. are needed in some special cases.
      if (udf instanceof GenericUDFCoalesce || udf instanceof GenericUDFNvl
          || udf instanceof GenericUDFElt) {
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

  public static GenericUDF getGenericUDFForCast(TypeInfo castType) throws HiveException {
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
        genericUdf = new GenericUDFToString();
        break;
      case CHAR:
        genericUdf = new GenericUDFToChar();
        break;
      case VARCHAR:
        genericUdf = new GenericUDFToVarchar();
        break;
      case BOOLEAN:
        udfClass = new UDFToBoolean();
        break;
      case DATE:
        genericUdf = new GenericUDFToDate();
        break;
      case TIMESTAMP:
        genericUdf = new GenericUDFTimestamp();
        break;
      case INTERVAL_YEAR_MONTH:
        genericUdf = new GenericUDFToIntervalYearMonth();
        break;
      case INTERVAL_DAY_TIME:
        genericUdf = new GenericUDFToIntervalDayTime();
        break;
      case BINARY:
        genericUdf = new GenericUDFToBinary();
        break;
      case DECIMAL:
        genericUdf = new GenericUDFToDecimal();
        break;
      case VOID:
      case UNKNOWN:
        // fall-through to throw exception, its not expected for execution to reach here.
        break;
    }
    if (genericUdf == null) {
      if (udfClass == null) {
        throw new HiveException("Could not add implicit cast for type "+castType.getTypeName());
      }
      GenericUDFBridge genericUDFBridge = new GenericUDFBridge();
      genericUDFBridge.setUdfClassName(udfClass.getClass().getName());
      genericUDFBridge.setUdfName(udfClass.getClass().getSimpleName());
      genericUdf = genericUDFBridge;
    }
    if (genericUdf instanceof SettableUDF) {
      ((SettableUDF) genericUdf).setTypeInfo(castType);
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
  public static boolean isNonVectorizedPathUDF(ExprNodeGenericFuncDesc expr,
      VectorExpressionDescriptor.Mode mode) {
    GenericUDF gudf = expr.getGenericUDF();
    if (gudf instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) gudf;
      Class<? extends UDF> udfClass = bridge.getUdfClass();
      if (udfClass.equals(UDFHex.class)
          || udfClass.equals(UDFRegExpExtract.class)
          || udfClass.equals(UDFRegExpReplace.class)
          || udfClass.equals(UDFConv.class)
          || udfClass.equals(UDFFromUnixTime.class) && isIntFamily(arg0Type(expr))
          || isCastToIntFamily(udfClass) && isStringFamily(arg0Type(expr))
          || isCastToFloatFamily(udfClass) && isStringFamily(arg0Type(expr))) {
        return true;
      }
    } else if ((gudf instanceof GenericUDFTimestamp && isStringFamily(arg0Type(expr)))

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
    } else if ((gudf instanceof GenericUDFToString
                   || gudf instanceof GenericUDFToChar
                   || gudf instanceof GenericUDFToVarchar) &&
               (arg0Type(expr).equals("timestamp")
                   || arg0Type(expr).equals("double")
                   || arg0Type(expr).equals("float"))) {
      return true;
    } else if (gudf instanceof GenericUDFBetween && (mode == VectorExpressionDescriptor.Mode.PROJECTION)) {
      // between has 4 args here, but can be vectorized like this
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
  // This two functions are for use only in the planner. It will fail in a task.
  public static boolean isCustomUDF(ExprNodeGenericFuncDesc expr) {
    return isCustomUDF(expr.getFuncText());
  }

  private static boolean isCustomUDF(String udfName) {
    if (udfName == null) {
      return false;
    }
    FunctionInfo funcInfo;
    try {
      funcInfo = FunctionRegistry.getFunctionInfo(udfName);
    } catch (SemanticException e) {
      LOG.warn("Failed to load " + udfName, e);
      funcInfo = null;
    }
    if (funcInfo == null) {
      return false;
    }
    boolean isNativeFunc = funcInfo.isNative();
    return !isNativeFunc;
  }

  /**
   * Handles only the special cases of cast/+ve/-ve operator on a constant.
   * @param exprDesc
   * @return The same expression if no evaluation done, else return the constant
   *         expression.
   * @throws HiveException
   */
  ExprNodeDesc evaluateCastOnConstants(ExprNodeDesc exprDesc) throws HiveException {
    if (!(exprDesc instanceof ExprNodeGenericFuncDesc)) {
      return exprDesc;
    }

    if (exprDesc.getChildren() == null || (exprDesc.getChildren().size() != 1) ) {
      return exprDesc;
    }

    ExprNodeConstantDesc foldedChild = null;
    if (!( exprDesc.getChildren().get(0) instanceof ExprNodeConstantDesc)) {

      // try recursive folding
      ExprNodeDesc expr = evaluateCastOnConstants(exprDesc.getChildren().get(0));
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

    // Only evaluate +ve/-ve or cast on constant or recursive casting.
    if (gudf instanceof GenericUDFOPNegative || gudf instanceof GenericUDFOPPositive ||
        castExpressionUdfs.contains(gudf.getClass())
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

  /* For cast on constant operator in all members of the input list and return new list
   * containing results.
   */
  private List<ExprNodeDesc> evaluateCastOnConstants(List<ExprNodeDesc> childExpr)
      throws HiveException {
    List<ExprNodeDesc> evaluatedChildren = new ArrayList<ExprNodeDesc>();
    if (childExpr != null) {
        for (ExprNodeDesc expr : childExpr) {
          expr = this.evaluateCastOnConstants(expr);
          evaluatedChildren.add(expr);
        }
    }
    return evaluatedChildren;
  }

  private VectorExpression getConstantVectorExpression(Object constantValue, TypeInfo typeInfo,
      VectorExpressionDescriptor.Mode mode) throws HiveException {
    String typeName =  typeInfo.getTypeName();
    VectorExpressionDescriptor.ArgumentType vectorArgType =
        VectorExpressionDescriptor.ArgumentType.fromHiveTypeName(typeName);
    if (vectorArgType == VectorExpressionDescriptor.ArgumentType.NONE) {
      throw new HiveException("No vector argument type for type name " + typeName);
    }
    int outCol = -1;
    if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
      outCol = ocm.allocateOutputColumn(typeInfo);
    }
    if (constantValue == null) {
      return new ConstantVectorExpression(outCol, typeInfo, true);
    }

    // Boolean is special case.
    if (typeName.equalsIgnoreCase("boolean")) {
      if (mode == VectorExpressionDescriptor.Mode.FILTER) {
        if (((Boolean) constantValue).booleanValue()) {
          return new FilterConstantBooleanVectorExpression(1);
        } else {
          return new FilterConstantBooleanVectorExpression(0);
        }
      } else {
        if (((Boolean) constantValue).booleanValue()) {
          return new ConstantVectorExpression(outCol, 1, typeInfo);
        } else {
          return new ConstantVectorExpression(outCol, 0, typeInfo);
        }
      }
    }

    switch (vectorArgType) {
    case INT_FAMILY:
      return new ConstantVectorExpression(outCol, ((Number) constantValue).longValue(), typeInfo);
    case DATE:
      return new ConstantVectorExpression(outCol, DateWritableV2.dateToDays((Date) constantValue), typeInfo);
    case TIMESTAMP:
      return new ConstantVectorExpression(outCol,
          ((org.apache.hadoop.hive.common.type.Timestamp) constantValue).toSqlTimestamp(), typeInfo);
    case INTERVAL_YEAR_MONTH:
      return new ConstantVectorExpression(outCol,
          ((HiveIntervalYearMonth) constantValue).getTotalMonths(), typeInfo);
    case INTERVAL_DAY_TIME:
      return new ConstantVectorExpression(outCol, (HiveIntervalDayTime) constantValue, typeInfo);
    case FLOAT_FAMILY:
      return new ConstantVectorExpression(outCol, ((Number) constantValue).doubleValue(), typeInfo);
    case DECIMAL:
      return new ConstantVectorExpression(outCol, (HiveDecimal) constantValue, typeInfo);
    case STRING:
      return new ConstantVectorExpression(outCol, ((String) constantValue).getBytes(), typeInfo);
    case CHAR:
      return new ConstantVectorExpression(outCol, ((HiveChar) constantValue), typeInfo);
    case VARCHAR:
      return new ConstantVectorExpression(outCol, ((HiveVarchar) constantValue), typeInfo);
    default:
      throw new HiveException("Unsupported constant type: " + typeName + ", object class " + constantValue.getClass().getSimpleName());
    }
  }

  private VectorExpression getDynamicValueVectorExpression(ExprNodeDynamicValueDesc dynamicValueExpr,
      VectorExpressionDescriptor.Mode mode) throws HiveException {
    String typeName =  dynamicValueExpr.getTypeInfo().getTypeName();
    VectorExpressionDescriptor.ArgumentType vectorArgType = VectorExpressionDescriptor.ArgumentType.fromHiveTypeName(typeName);
    if (vectorArgType == VectorExpressionDescriptor.ArgumentType.NONE) {
      throw new HiveException("No vector argument type for type name " + typeName);
    }
    int outCol = -1;
    if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
      outCol = ocm.allocateOutputColumn(dynamicValueExpr.getTypeInfo());
    }

    return new DynamicValueVectorExpression(outCol, dynamicValueExpr.getTypeInfo(), dynamicValueExpr.getDynamicValue());
  }

  /**
   * Used as a fast path for operations that don't modify their input, like unary +
   * and casting boolean to long. IdentityExpression and its children are always
   * projections.
   */
  private VectorExpression getIdentityExpression(List<ExprNodeDesc> childExprList)
      throws HiveException {
    ExprNodeDesc childExpr = childExprList.get(0);
    int identityCol;
    TypeInfo identityTypeInfo;
    DataTypePhysicalVariation identityDataTypePhysicalVariation;
    VectorExpression v1 = null;
    if (childExpr instanceof ExprNodeGenericFuncDesc) {
      v1 = getVectorExpression(childExpr);
      identityCol = v1.getOutputColumnNum();
      identityTypeInfo = v1.getOutputTypeInfo();
      identityDataTypePhysicalVariation = v1.getOutputDataTypePhysicalVariation();
    } else if (childExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) childExpr;
      identityCol = getInputColumnIndex(colDesc.getColumn());
      identityTypeInfo = colDesc.getTypeInfo();

      // CONSIDER: Validation of type information

      identityDataTypePhysicalVariation = getDataTypePhysicalVariation(identityCol);
    } else {
      throw new HiveException("Expression not supported: "+childExpr);
    }

    VectorExpression ve = new IdentityExpression(identityCol);

    if (v1 != null) {
      ve.setChildExpressions(new VectorExpression [] {v1});
    }

    ve.setInputTypeInfos(identityTypeInfo);
    ve.setInputDataTypePhysicalVariations(identityDataTypePhysicalVariation);

    ve.setOutputTypeInfo(identityTypeInfo);
    ve.setOutputDataTypePhysicalVariation(identityDataTypePhysicalVariation);

    return ve;
  }


  private boolean checkExprNodeDescForDecimal64(ExprNodeDesc exprNodeDesc) throws HiveException {
    if (exprNodeDesc instanceof ExprNodeColumnDesc) {
      int colIndex = getInputColumnIndex((ExprNodeColumnDesc) exprNodeDesc);
      DataTypePhysicalVariation dataTypePhysicalVariation = getDataTypePhysicalVariation(colIndex);
      return (dataTypePhysicalVariation == DataTypePhysicalVariation.DECIMAL_64);
    } else if (exprNodeDesc instanceof ExprNodeGenericFuncDesc) {

      // Is the result Decimal64 precision?
      TypeInfo returnType = exprNodeDesc.getTypeInfo();
      if (!checkTypeInfoForDecimal64(returnType)) {
        return false;
      }
      DecimalTypeInfo returnDecimalType = (DecimalTypeInfo) returnType;

      GenericUDF udf = ((ExprNodeGenericFuncDesc) exprNodeDesc).getGenericUDF();
      Class<?> udfClass = udf.getClass();

      // We have a class-level annotation that says whether the UDF's vectorization expressions
      // support Decimal64.
      VectorizedExpressionsSupportDecimal64 annotation =
          AnnotationUtils.getAnnotation(udfClass, VectorizedExpressionsSupportDecimal64.class);
      if (annotation == null) {
        return false;
      }

      // Carefully check the children to make sure they are Decimal64.
      List<ExprNodeDesc> children = exprNodeDesc.getChildren();
      for (ExprNodeDesc childExprNodeDesc : children) {

        // Some cases were converted before calling getVectorExpressionForUdf.
        // So, emulate those cases first.

        if (childExprNodeDesc instanceof ExprNodeConstantDesc) {
          DecimalTypeInfo childDecimalTypeInfo =
              decimalTypeFromCastToDecimal(childExprNodeDesc, returnDecimalType);
          if (childDecimalTypeInfo == null) {
            return false;
          }
          if (!checkTypeInfoForDecimal64(childDecimalTypeInfo)) {
            return false;
          }
          continue;
        }

        // Otherwise, recurse.
        if (!checkExprNodeDescForDecimal64(childExprNodeDesc)) {
          return false;
        }
      }
      return true;
    } else if (exprNodeDesc instanceof ExprNodeConstantDesc) {
      return checkTypeInfoForDecimal64(exprNodeDesc.getTypeInfo());
    }
    return false;
  }

  private boolean checkTypeInfoForDecimal64(TypeInfo typeInfo) {
    if (typeInfo instanceof DecimalTypeInfo) {
      DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
      return HiveDecimalWritable.isPrecisionDecimal64(decimalTypeInfo.precision());
    }
    return false;
  }

  public boolean haveCandidateForDecimal64VectorExpression(int numChildren,
      List<ExprNodeDesc> childExpr, TypeInfo returnType) throws HiveException {

    // For now, just 2 Decimal64 inputs and a Decimal64 or boolean output.
    return (numChildren == 2 &&
        checkExprNodeDescForDecimal64(childExpr.get(0)) &&
        checkExprNodeDescForDecimal64(childExpr.get(1)) &&
        (checkTypeInfoForDecimal64(returnType) ||
            returnType.equals(TypeInfoFactory.booleanTypeInfo)));
  }

  private VectorExpression getDecimal64VectorExpressionForUdf(GenericUDF genericUdf,
      Class<?> udfClass, List<ExprNodeDesc> childExpr, int numChildren,
      VectorExpressionDescriptor.Mode mode, TypeInfo returnType) throws HiveException {

    ExprNodeDesc child1 = childExpr.get(0);
    ExprNodeDesc child2 = childExpr.get(1);

    DecimalTypeInfo decimalTypeInfo1 = (DecimalTypeInfo) child1.getTypeInfo();
    DecimalTypeInfo decimalTypeInfo2 = (DecimalTypeInfo) child2.getTypeInfo();

    DataTypePhysicalVariation dataTypePhysicalVariation1 = DataTypePhysicalVariation.DECIMAL_64;
    DataTypePhysicalVariation dataTypePhysicalVariation2 = DataTypePhysicalVariation.DECIMAL_64;

    final int scale1 = decimalTypeInfo1.scale();
    final int scale2 = decimalTypeInfo2.scale();

    VectorExpressionDescriptor.Builder builder = new VectorExpressionDescriptor.Builder();
    builder.setNumArguments(numChildren);
    builder.setMode(mode);

    boolean isColumnScaleEstablished = false;
    int columnScale = 0;
    boolean hasScalar = false;
    builder.setArgumentType(0, ArgumentType.DECIMAL_64);
    if (child1 instanceof ExprNodeGenericFuncDesc ||
        child1 instanceof ExprNodeColumnDesc) {
      builder.setInputExpressionType(0, InputExpressionType.COLUMN);
      isColumnScaleEstablished = true;
      columnScale = scale1;
    } else if (child1 instanceof ExprNodeConstantDesc) {
      if (isNullConst(child1)) {

        // Cannot handle NULL scalar parameter.
        return null;
      }
      hasScalar = true;
      builder.setInputExpressionType(0, InputExpressionType.SCALAR);
    } else {

      // Currently, only functions, columns, and scalars supported.
      return null;
    }

    builder.setArgumentType(1, ArgumentType.DECIMAL_64);
    if (child2 instanceof ExprNodeGenericFuncDesc ||
        child2 instanceof ExprNodeColumnDesc) {
      builder.setInputExpressionType(1, InputExpressionType.COLUMN);
      if (!isColumnScaleEstablished) {
        isColumnScaleEstablished = true;
        columnScale = scale2;
      } else if (columnScale != scale2) {

        // We only support Decimal64 on 2 columns when the have the same scale.
        return null;
      }
    } else if (child2 instanceof ExprNodeConstantDesc) {
      // Cannot have SCALAR, SCALAR.
      if (!isColumnScaleEstablished) {
        return null;
      }
      if (isNullConst(child2)) {

        // Cannot handle NULL scalar parameter.
        return null;
      }
      hasScalar = true;
      builder.setInputExpressionType(1, InputExpressionType.SCALAR);
    } else {

      // Currently, only functions, columns, and scalars supported.
      return null;
    }

    VectorExpressionDescriptor.Descriptor descriptor = builder.build();
    Class<?> vectorClass =
        this.vMap.getVectorExpressionClass(udfClass, descriptor, useCheckedVectorExpressions);
    if (vectorClass == null) {
      return null;
    }

    VectorExpressionDescriptor.Mode childrenMode = getChildrenMode(mode, udfClass);

    /*
     * Custom build arguments.
     */

    List<VectorExpression> children = new ArrayList<VectorExpression>();
    Object[] arguments = new Object[numChildren];

    for (int i = 0; i < numChildren; i++) {
      ExprNodeDesc child = childExpr.get(i);
      if (child instanceof ExprNodeGenericFuncDesc) {
        VectorExpression vChild = getVectorExpression(child, childrenMode);
        children.add(vChild);
        arguments[i] = vChild.getOutputColumnNum();
      } else if (child instanceof ExprNodeColumnDesc) {
        int colIndex = getInputColumnIndex((ExprNodeColumnDesc) child);
        if (childrenMode == VectorExpressionDescriptor.Mode.FILTER) {

          VectorExpression filterExpr =
              getFilterOnBooleanColumnExpression((ExprNodeColumnDesc) child, colIndex);
          if (filterExpr == null) {
            return null;
          }

          children.add(filterExpr);
        }
        arguments[i] = colIndex;
      } else {
        Preconditions.checkState(child instanceof ExprNodeConstantDesc);
        ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) child;
        HiveDecimal hiveDecimal = (HiveDecimal) constDesc.getValue();
        if (hiveDecimal.scale() > columnScale) {

          // For now, bail out on decimal constants with larger scale than column scale.
          return null;
        }
        final long decimal64Scalar = new HiveDecimalWritable(hiveDecimal).serialize64(columnScale);
        arguments[i] = decimal64Scalar;
      }
    }

    /*
     * Instantiate Decimal64 vector expression.
     *
     * The instantiateExpression method sets the output column and type information.
     */
    VectorExpression  vectorExpression =
        instantiateExpression(vectorClass, returnType, DataTypePhysicalVariation.DECIMAL_64, arguments);
    if (vectorExpression == null) {
      handleCouldNotInstantiateVectorExpression(vectorClass, returnType, DataTypePhysicalVariation.DECIMAL_64, arguments);
    }

    vectorExpression.setInputTypeInfos(decimalTypeInfo1, decimalTypeInfo2);
    vectorExpression.setInputDataTypePhysicalVariations(dataTypePhysicalVariation1, dataTypePhysicalVariation2);

    if ((vectorExpression != null) && !children.isEmpty()) {
      vectorExpression.setChildExpressions(children.toArray(new VectorExpression[0]));
    }

    return vectorExpression;
  }

  private VectorExpression getVectorExpressionForUdf(GenericUDF genericUdf,
      Class<?> udfClass, List<ExprNodeDesc> childExpr, VectorExpressionDescriptor.Mode mode,
      TypeInfo returnType) throws HiveException {

    int numChildren = (childExpr == null) ? 0 : childExpr.size();

    if (numChildren > 2 && genericUdf != null && mode == VectorExpressionDescriptor.Mode.FILTER &&
        ((genericUdf instanceof GenericUDFOPOr) || (genericUdf instanceof GenericUDFOPAnd))) {

      // Special case handling for Multi-OR and Multi-AND.

      for (int i = 0; i < numChildren; i++) {
        ExprNodeDesc child = childExpr.get(i);
        String childTypeString = child.getTypeString();
        if (childTypeString == null) {
          throw new HiveException("Null child type name string");
        }
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(childTypeString);
        Type columnVectorType = VectorizationContext.getColumnVectorTypeFromTypeInfo(typeInfo);
        if (columnVectorType != ColumnVector.Type.LONG){
          return null;
        }
        if (!(child instanceof ExprNodeGenericFuncDesc) && !(child instanceof ExprNodeColumnDesc)) {
          return null;
        }
      }
      Class<?> vclass;
      if (genericUdf instanceof GenericUDFOPOr) {
        vclass = FilterExprOrExpr.class;
      } else if (genericUdf instanceof GenericUDFOPAnd) {
        vclass = FilterExprAndExpr.class;
      } else {
        throw new RuntimeException("Unexpected multi-child UDF");
      }
      VectorExpressionDescriptor.Mode childrenMode = getChildrenMode(mode, udfClass);
      return createVectorExpression(vclass, childExpr, childrenMode, returnType);
    }
    if (numChildren > VectorExpressionDescriptor.MAX_NUM_ARGUMENTS) {
      return null;
    }

    // Should we intercept here for a possible Decimal64 vector expression class?
    if (haveCandidateForDecimal64VectorExpression(numChildren, childExpr, returnType)) {
      VectorExpression result = getDecimal64VectorExpressionForUdf(genericUdf, udfClass,
          childExpr, numChildren, mode, returnType);
      if (result != null) {
        return result;
      }
      // Otherwise, fall through and proceed with non-Decimal64 vector expression classes...
    }

    VectorExpressionDescriptor.Builder builder = new VectorExpressionDescriptor.Builder();
    builder.setNumArguments(numChildren);
    builder.setMode(mode);
    for (int i = 0; i < numChildren; i++) {
      ExprNodeDesc child = childExpr.get(i);
      TypeInfo childTypeInfo = child.getTypeInfo();
      String childTypeString = childTypeInfo.toString();
      if (childTypeString == null) {
        throw new HiveException("Null child type name string");
      }
      String undecoratedTypeName = getUndecoratedName(childTypeString);
      if (undecoratedTypeName == null) {
        throw new HiveException("No match for type string " + childTypeString + " from undecorated type name method");
      }
      builder.setArgumentType(i, undecoratedTypeName);
      if ((child instanceof ExprNodeGenericFuncDesc) || (child instanceof ExprNodeColumnDesc)
          || (child instanceof ExprNodeFieldDesc)) {
        builder.setInputExpressionType(i, InputExpressionType.COLUMN);
      } else if (child instanceof ExprNodeConstantDesc) {
        if (isNullConst(child)) {
          // Cannot handle NULL scalar parameter.
          return null;
        }
        builder.setInputExpressionType(i, InputExpressionType.SCALAR);
      } else if (child instanceof ExprNodeDynamicValueDesc) {
        builder.setInputExpressionType(i, InputExpressionType.DYNAMICVALUE);
      } else {
        throw new HiveException("Cannot handle expression type: " + child.getClass().getSimpleName());
      }
    }
    VectorExpressionDescriptor.Descriptor descriptor = builder.build();
    Class<?> vclass =
        this.vMap.getVectorExpressionClass(udfClass, descriptor, useCheckedVectorExpressions);
    if (vclass == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No vector udf found for "+udfClass.getSimpleName() + ", descriptor: "+descriptor);
      }
      return null;
    }
    VectorExpressionDescriptor.Mode childrenMode = getChildrenMode(mode, udfClass);
    return createVectorExpression(vclass, childExpr, childrenMode, returnType);
  }

  private VectorExpression createDecimal64ToDecimalConversion(int colIndex, TypeInfo resultTypeInfo)
      throws HiveException {
    Object [] conversionArgs = new Object[1];
    conversionArgs[0] = colIndex;
    VectorExpression vectorExpression =
        instantiateExpression(
            ConvertDecimal64ToDecimal.class,
            resultTypeInfo,
            DataTypePhysicalVariation.NONE,
            conversionArgs);
    if (vectorExpression == null) {
      handleCouldNotInstantiateVectorExpression(
          ConvertDecimal64ToDecimal.class, resultTypeInfo, DataTypePhysicalVariation.NONE,
          conversionArgs);
    }

    vectorExpression.setInputTypeInfos(resultTypeInfo);
    vectorExpression.setInputDataTypePhysicalVariations(DataTypePhysicalVariation.DECIMAL_64);

    return vectorExpression;
  }

  public VectorExpression wrapWithDecimal64ToDecimalConversion(VectorExpression inputExpression)
      throws HiveException {

    VectorExpression wrapExpression = createDecimal64ToDecimalConversion(
        inputExpression.getOutputColumnNum(), inputExpression.getOutputTypeInfo());
    if (inputExpression instanceof IdentityExpression) {
      return wrapExpression;
    }

    // CONCERN: Leaking scratch column?
    VectorExpression[] child = new VectorExpression[1];
    child[0] = inputExpression;
    wrapExpression.setChildExpressions(child);

    return wrapExpression;
  }

  private VectorExpression createVectorExpression(Class<?> vectorClass,
      List<ExprNodeDesc> childExpr, VectorExpressionDescriptor.Mode childrenMode, TypeInfo returnType) throws HiveException {
    int numChildren = childExpr == null ? 0: childExpr.size();

    TypeInfo[] inputTypeInfos = new TypeInfo[numChildren];
    DataTypePhysicalVariation[] inputDataTypePhysicalVariations = new DataTypePhysicalVariation[numChildren];

    List<VectorExpression> children = new ArrayList<VectorExpression>();
    Object[] arguments = new Object[numChildren];

    for (int i = 0; i < numChildren; i++) {
      ExprNodeDesc child = childExpr.get(i);
      TypeInfo childTypeInfo = child.getTypeInfo();

      inputTypeInfos[i] = childTypeInfo;
      inputDataTypePhysicalVariations[i] = DataTypePhysicalVariation.NONE;   // Assume.

      if ((child instanceof ExprNodeGenericFuncDesc) || (child instanceof ExprNodeFieldDesc)) {
        VectorExpression vChild = getVectorExpression(child, childrenMode);
          children.add(vChild);
          arguments[i] = vChild.getOutputColumnNum();

          // Update.
          inputDataTypePhysicalVariations[i] = vChild.getOutputDataTypePhysicalVariation();
      } else if (child instanceof ExprNodeColumnDesc) {
        int colIndex = getInputColumnIndex((ExprNodeColumnDesc) child);

        // CONSIDER: Validate type information

        if (childTypeInfo instanceof DecimalTypeInfo) {

          // In this method, we must only process non-Decimal64 column vectors.
          // Convert Decimal64 columns to regular decimal.
          DataTypePhysicalVariation dataTypePhysicalVariation = getDataTypePhysicalVariation(colIndex);
          if (dataTypePhysicalVariation != null && dataTypePhysicalVariation == DataTypePhysicalVariation.DECIMAL_64) {

            // FUTURE: Can we reuse this conversion?
            VectorExpression vChild = createDecimal64ToDecimalConversion(colIndex, childTypeInfo);
            children.add(vChild);
            arguments[i] = vChild.getOutputColumnNum();

            // Update.
            inputDataTypePhysicalVariations[i] = vChild.getOutputDataTypePhysicalVariation();
            continue;
          }
        }
        if (childrenMode == VectorExpressionDescriptor.Mode.FILTER) {

          // In filter mode, the column must be a boolean
          SelectColumnIsTrue selectColumnIsTrue = new SelectColumnIsTrue(colIndex);

          selectColumnIsTrue.setInputTypeInfos(childTypeInfo);
          selectColumnIsTrue.setInputDataTypePhysicalVariations(DataTypePhysicalVariation.NONE);

          children.add(selectColumnIsTrue);
        }
        arguments[i] = colIndex;
      } else if (child instanceof ExprNodeConstantDesc) {
        Object scalarValue = getVectorTypeScalarValue((ExprNodeConstantDesc) child);
        arguments[i] = (null == scalarValue) ? getConstantVectorExpression(null, child.getTypeInfo(), childrenMode) : scalarValue;
      } else if (child instanceof ExprNodeDynamicValueDesc) {
        arguments[i] = ((ExprNodeDynamicValueDesc) child).getDynamicValue();
      } else {
        throw new HiveException("Cannot handle expression type: " + child.getClass().getSimpleName());
      }
    }
    VectorExpression vectorExpression = instantiateExpression(vectorClass, returnType, DataTypePhysicalVariation.NONE, arguments);
    if (vectorExpression == null) {
      handleCouldNotInstantiateVectorExpression(vectorClass, returnType, DataTypePhysicalVariation.NONE, arguments);
    }

    vectorExpression.setInputTypeInfos(inputTypeInfos);
    vectorExpression.setInputDataTypePhysicalVariations(inputDataTypePhysicalVariations);

    if ((vectorExpression != null) && !children.isEmpty()) {
      vectorExpression.setChildExpressions(children.toArray(new VectorExpression[0]));
    }

    for (VectorExpression ve : children) {
      ocm.freeOutputColumn(ve.getOutputColumnNum());
    }

    return vectorExpression;
  }

  private void handleCouldNotInstantiateVectorExpression(Class<?> vectorClass, TypeInfo returnType,
      DataTypePhysicalVariation dataTypePhysicalVariation, Object[] arguments) throws HiveException {
    String displayString = "Could not instantiate vector expression class " + vectorClass.getName() +
        " for arguments " + Arrays.toString(arguments) + " return type " +
        VectorExpression.getTypeName(returnType, dataTypePhysicalVariation);
    throw new HiveException(displayString);
  }

  private VectorExpressionDescriptor.Mode getChildrenMode(VectorExpressionDescriptor.Mode mode, Class<?> udf) {
    if (mode.equals(VectorExpressionDescriptor.Mode.FILTER) && (udf.equals(GenericUDFOPAnd.class) || udf.equals(GenericUDFOPOr.class))) {
      return VectorExpressionDescriptor.Mode.FILTER;
    }
    return VectorExpressionDescriptor.Mode.PROJECTION;
  }

  private String getNewInstanceArgumentString(Object [] args) {
    if (args == null) {
      return "arguments: NULL";
    }
    ArrayList<String> argClasses = new ArrayList<String>();
    for (Object obj : args) {
      argClasses.add(obj.getClass().getSimpleName());
    }
    return "arguments: " + Arrays.toString(args) + ", argument classes: " + argClasses.toString();
  }

  private static final int STACK_LENGTH_LIMIT = 15;

  public static String getStackTraceAsSingleLine(Throwable e) {
    StringBuilder sb = new StringBuilder();
    sb.append(e);
    sb.append(" stack trace: ");
    StackTraceElement[] stackTrace = e.getStackTrace();
    int length = stackTrace.length;
    boolean isTruncated = false;
    if (length > STACK_LENGTH_LIMIT) {
      length = STACK_LENGTH_LIMIT;
      isTruncated = true;
    }
    for (int i = 0; i < length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(stackTrace[i]);
    }
    if (isTruncated) {
      sb.append(", ...");
    }

    // Attempt to cleanup stack trace elements that vary by VM.
    String cleaned = sb.toString().replaceAll("GeneratedConstructorAccessor[0-9]*", "GeneratedConstructorAccessor<omitted>");

    return cleaned;
  }

  public VectorExpression instantiateExpression(Class<?> vclass, TypeInfo returnTypeInfo,
      DataTypePhysicalVariation returnDataTypePhysicalVariation, Object...args)
      throws HiveException {
    VectorExpression ve = null;
    Constructor<?> ctor = getConstructor(vclass);
    int numParams = ctor.getParameterTypes().length;
    int argsLength = (args == null) ? 0 : args.length;
    if (numParams == 0) {
      try {
        ve = (VectorExpression) ctor.newInstance();
      } catch (Exception ex) {
        throw new HiveException("Could not instantiate " + vclass.getSimpleName() + " with 0 arguments, exception: " +
            getStackTraceAsSingleLine(ex));
      }
    } else if (numParams == argsLength) {
      try {
        ve = (VectorExpression) ctor.newInstance(args);
      } catch (Exception ex) {
          throw new HiveException("Could not instantiate " + vclass.getSimpleName() + " with " + getNewInstanceArgumentString(args) + ", exception: " +
              getStackTraceAsSingleLine(ex));
      }
    } else if (numParams == argsLength + 1) {
      // Additional argument is needed, which is the outputcolumn.
      Object [] newArgs = null;
      try {
        if (returnTypeInfo == null) {
          throw new HiveException("Missing output type information");
        }
        String returnTypeName = returnTypeInfo.getTypeName();
        returnTypeName = VectorizationContext.mapTypeNameSynonyms(returnTypeName);

        // Special handling for decimal because decimal types need scale and precision parameter.
        // This special handling should be avoided by using returnType uniformly for all cases.
        final int outputColumnNum =
            ocm.allocateOutputColumn(returnTypeInfo, returnDataTypePhysicalVariation);

        newArgs = Arrays.copyOf(args, numParams);
        newArgs[numParams-1] = outputColumnNum;

        ve = (VectorExpression) ctor.newInstance(newArgs);

        /*
         * Caller is responsible for setting children and input type information.
         */
        ve.setOutputTypeInfo(returnTypeInfo);
        ve.setOutputDataTypePhysicalVariation(returnDataTypePhysicalVariation);

      } catch (Exception ex) {
          throw new HiveException("Could not instantiate " + vclass.getSimpleName() + " with arguments " + getNewInstanceArgumentString(newArgs) + ", exception: " +
              getStackTraceAsSingleLine(ex));
      }
    }
    // Add maxLength parameter to UDFs that have CHAR or VARCHAR output.
    if (ve instanceof TruncStringOutput) {
      TruncStringOutput truncStringOutput = (TruncStringOutput) ve;
      if (returnTypeInfo instanceof BaseCharTypeInfo) {
        BaseCharTypeInfo baseCharTypeInfo = (BaseCharTypeInfo) returnTypeInfo;
        truncStringOutput.setMaxLength(baseCharTypeInfo.getLength());
      }
    }
    return ve;
  }

  private VectorExpression getGenericUdfVectorExpression(GenericUDF udf,
      List<ExprNodeDesc> childExpr, VectorExpressionDescriptor.Mode mode, TypeInfo returnType) throws HiveException {

    List<ExprNodeDesc> castedChildren = evaluateCastOnConstants(childExpr);
    childExpr = castedChildren;

    //First handle special cases.  If one of the special case methods cannot handle it,
    // it returns null.
    VectorExpression ve = null;
    if (udf instanceof GenericUDFBetween && mode == VectorExpressionDescriptor.Mode.FILTER) {
      ve = getBetweenFilterExpression(childExpr, mode, returnType);
    } else if (udf instanceof GenericUDFIn) {
      ve = getInExpression(childExpr, mode, returnType);
    } else if (udf instanceof GenericUDFIf) {
      ve = getIfExpression((GenericUDFIf) udf, childExpr, mode, returnType);
    } else if (udf instanceof GenericUDFWhen) {
      ve = getWhenExpression(childExpr, mode, returnType);
    } else if (udf instanceof GenericUDFOPPositive) {
      ve = getIdentityExpression(childExpr);
    } else if (udf instanceof GenericUDFCoalesce || udf instanceof GenericUDFNvl) {

      // Coalesce is a special case because it can take variable number of arguments.
      // Nvl is a specialization of the Coalesce.
      ve = getCoalesceExpression(childExpr, returnType);
    } else if (udf instanceof GenericUDFElt) {

      // Elt is a special case because it can take variable number of arguments.
      ve = getEltExpression(childExpr, returnType);
    } else if (udf instanceof GenericUDFBridge) {
      ve = getGenericUDFBridgeVectorExpression((GenericUDFBridge) udf, childExpr, mode,
          returnType);
    } else if (udf instanceof GenericUDFToString) {
      ve = getCastToString(childExpr, returnType);
    } else if (udf instanceof GenericUDFToDecimal) {
      ve = getCastToDecimal(childExpr, returnType);
    } else if (udf instanceof GenericUDFToChar) {
      ve = getCastToChar(childExpr, returnType);
    } else if (udf instanceof GenericUDFToVarchar) {
      ve = getCastToVarChar(childExpr, returnType);
    } else if (udf instanceof GenericUDFTimestamp) {
      ve = getCastToTimestamp((GenericUDFTimestamp)udf, childExpr, mode, returnType);
    }
    if (ve != null) {
      return ve;
    }
    // Now do a general lookup
    Class<?> udfClass = udf.getClass();
    boolean isSubstituted = false;
    if (udf instanceof GenericUDFBridge) {
      udfClass = ((GenericUDFBridge) udf).getUdfClass();
      isSubstituted = true;
    }

    ve = getVectorExpressionForUdf((!isSubstituted ? udf : null),
        udfClass, castedChildren, mode, returnType);

    return ve;
  }

  private VectorExpression getCastToTimestamp(GenericUDFTimestamp udf,
      List<ExprNodeDesc> childExpr, VectorExpressionDescriptor.Mode mode, TypeInfo returnType) throws HiveException {
    VectorExpression ve = getVectorExpressionForUdf(udf, udf.getClass(), childExpr, mode, returnType);

    // Replace with the milliseconds conversion
    if (!udf.isIntToTimestampInSeconds() && ve instanceof CastLongToTimestamp) {
      ve = createVectorExpression(CastMillisecondsLongToTimestamp.class,
          childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    }

    return ve;
  }

  private void freeNonColumns(VectorExpression[] vectorChildren) {
    if (vectorChildren == null) {
      return;
    }
    for (VectorExpression v : vectorChildren) {
      if (!(v instanceof IdentityExpression)) {
        ocm.freeOutputColumn(v.getOutputColumnNum());
      }
    }
  }

  private VectorExpression getCoalesceExpression(List<ExprNodeDesc> childExpr, TypeInfo returnType)
      throws HiveException {
    int[] inputColumns = new int[childExpr.size()];
    VectorExpression[] vectorChildren =
        getVectorExpressions(childExpr, VectorExpressionDescriptor.Mode.PROJECTION);

    final int size = vectorChildren.length;
    TypeInfo[] inputTypeInfos = new TypeInfo[size];
    DataTypePhysicalVariation[] inputDataTypePhysicalVariations = new DataTypePhysicalVariation[size];
    int i = 0;
    for (VectorExpression ve : vectorChildren) {
      inputColumns[i] = ve.getOutputColumnNum();
      inputTypeInfos[i] = ve.getOutputTypeInfo();
      inputDataTypePhysicalVariations[i++] = ve.getOutputDataTypePhysicalVariation();
    }

    final int outputColumnNum = ocm.allocateOutputColumn(returnType);
    VectorCoalesce vectorCoalesce = new VectorCoalesce(inputColumns, outputColumnNum);

    vectorCoalesce.setChildExpressions(vectorChildren);

    vectorCoalesce.setInputTypeInfos(inputTypeInfos);
    vectorCoalesce.setInputDataTypePhysicalVariations(inputDataTypePhysicalVariations);

    vectorCoalesce.setOutputTypeInfo(returnType);
    vectorCoalesce.setOutputDataTypePhysicalVariation(DataTypePhysicalVariation.NONE);

    freeNonColumns(vectorChildren);
    return vectorCoalesce;
  }

  private VectorExpression getEltExpression(List<ExprNodeDesc> childExpr, TypeInfo returnType)
      throws HiveException {
    int[] inputColumns = new int[childExpr.size()];
    VectorExpression[] vectorChildren =
        getVectorExpressions(childExpr, VectorExpressionDescriptor.Mode.PROJECTION);

    final int size = vectorChildren.length;
    TypeInfo[] inputTypeInfos = new TypeInfo[size];
    DataTypePhysicalVariation[] inputDataTypePhysicalVariations = new DataTypePhysicalVariation[size];
    int i = 0;
    for (VectorExpression ve : vectorChildren) {
      inputColumns[i] = ve.getOutputColumnNum();
      inputTypeInfos[i] = ve.getOutputTypeInfo();
      inputDataTypePhysicalVariations[i++] = ve.getOutputDataTypePhysicalVariation();
    }

    final int outputColumnNum = ocm.allocateOutputColumn(returnType);
    VectorElt vectorElt = new VectorElt(inputColumns, outputColumnNum);

    vectorElt.setChildExpressions(vectorChildren);

    vectorElt.setInputTypeInfos(inputTypeInfos);
    vectorElt.setInputDataTypePhysicalVariations(inputDataTypePhysicalVariations);

    vectorElt.setOutputTypeInfo(returnType);
    vectorElt.setOutputDataTypePhysicalVariation(DataTypePhysicalVariation.NONE);

    freeNonColumns(vectorChildren);
    return vectorElt;
  }

  public enum InConstantType {
    INT_FAMILY,
    TIMESTAMP,
    DATE,
    FLOAT_FAMILY,
    STRING_FAMILY,
    DECIMAL
  }

  public static InConstantType getInConstantTypeFromPrimitiveCategory(PrimitiveCategory primitiveCategory) {

    switch (primitiveCategory) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return InConstantType.INT_FAMILY;

    case DATE:
      return InConstantType.DATE;

    case TIMESTAMP:
      return InConstantType.TIMESTAMP;

    case FLOAT:
    case DOUBLE:
      return InConstantType.FLOAT_FAMILY;

    case STRING:
    case CHAR:
    case VARCHAR:
    case BINARY:
      return InConstantType.STRING_FAMILY;

    case DECIMAL:
      return InConstantType.DECIMAL;


    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY_TIME:
      // UNDONE: Fall through for these... they don't appear to be supported yet.
    default:
      throw new RuntimeException("Unexpected primitive type category " + primitiveCategory);
    }
  }

  private VectorExpression getStructInExpression(List<ExprNodeDesc> childExpr, ExprNodeDesc colExpr,
      TypeInfo colTypeInfo, List<ExprNodeDesc> inChildren, VectorExpressionDescriptor.Mode mode, TypeInfo returnType)
          throws HiveException {

    VectorExpression expr = null;

    StructTypeInfo structTypeInfo = (StructTypeInfo) colTypeInfo;

    ArrayList<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
    final int fieldCount = fieldTypeInfos.size();
    ColumnVector.Type[] fieldVectorColumnTypes = new ColumnVector.Type[fieldCount];
    InConstantType[] fieldInConstantTypes = new InConstantType[fieldCount];
    for (int f = 0; f < fieldCount; f++) {
      TypeInfo fieldTypeInfo = fieldTypeInfos.get(f);
      // Only primitive fields supports for now.
      if (fieldTypeInfo.getCategory() != Category.PRIMITIVE) {
        return null;
      }

      // We are going to serialize using the 4 basic types.
      ColumnVector.Type fieldVectorColumnType = getColumnVectorTypeFromTypeInfo(fieldTypeInfo);
      fieldVectorColumnTypes[f] = fieldVectorColumnType;

      // We currently evaluate the IN (..) constants in special ways.
      PrimitiveCategory fieldPrimitiveCategory =
          ((PrimitiveTypeInfo) fieldTypeInfo).getPrimitiveCategory();
      InConstantType inConstantType = getInConstantTypeFromPrimitiveCategory(fieldPrimitiveCategory);
      fieldInConstantTypes[f] = inConstantType;
    }

    Output buffer = new Output();
    BinarySortableSerializeWrite binarySortableSerializeWrite =
        new BinarySortableSerializeWrite(fieldCount);

    final int inChildrenCount = inChildren.size();
    byte[][] serializedInChildren = new byte[inChildrenCount][];
    try {
      for (int i = 0; i < inChildrenCount; i++) {
        final ExprNodeDesc node = inChildren.get(i);
        final Object[] constants;

        if (node instanceof ExprNodeConstantDesc) {
          ExprNodeConstantDesc constNode = (ExprNodeConstantDesc) node;
          ConstantObjectInspector output = constNode.getWritableObjectInspector();
          constants = ((List<?>) output.getWritableConstantValue()).toArray();
        } else {
          ExprNodeGenericFuncDesc exprNode = (ExprNodeGenericFuncDesc) node;
          ExprNodeEvaluator<?> evaluator = ExprNodeEvaluatorFactory
              .get(exprNode);
          ObjectInspector output = evaluator.initialize(exprNode
              .getWritableObjectInspector());
          constants = (Object[]) evaluator.evaluate(null);
        }

        binarySortableSerializeWrite.set(buffer);
        for (int f = 0; f < fieldCount; f++) {
          Object constant = constants[f];
          if (constant == null) {
            binarySortableSerializeWrite.writeNull();
          } else {
            InConstantType inConstantType = fieldInConstantTypes[f];
            switch (inConstantType) {
            case STRING_FAMILY:
              {
                byte[] bytes;
                if (constant instanceof Text) {
                  Text text = (Text) constant;
                  bytes = text.getBytes();
                  binarySortableSerializeWrite.writeString(bytes, 0, text.getLength());
                } else {
                  throw new HiveException("Unexpected constant String type " +
                      constant.getClass().getSimpleName());
                }
              }
              break;
            case INT_FAMILY:
              {
                long value;
                if (constant instanceof IntWritable) {
                  value = ((IntWritable) constant).get();
                } else if (constant instanceof LongWritable) {
                  value = ((LongWritable) constant).get();
                } else {
                  throw new HiveException("Unexpected constant Long type " +
                      constant.getClass().getSimpleName());
                }
                binarySortableSerializeWrite.writeLong(value);
              }
              break;

            case FLOAT_FAMILY:
              {
                double value;
                if (constant instanceof DoubleWritable) {
                  value = ((DoubleWritable) constant).get();
                } else {
                  throw new HiveException("Unexpected constant Double type " +
                      constant.getClass().getSimpleName());
                }
                binarySortableSerializeWrite.writeDouble(value);
              }
              break;

            // UNDONE...
            case DATE:
            case TIMESTAMP:
            case DECIMAL:
            default:
              throw new RuntimeException("Unexpected IN constant type " + inConstantType.name());
            }
          }
        }
        serializedInChildren[i] = Arrays.copyOfRange(buffer.getData(), 0, buffer.getLength());
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }

    // Create a single child representing the scratch column where we will
    // generate the serialized keys of the batch.
    int scratchBytesCol = ocm.allocateOutputColumn(TypeInfoFactory.stringTypeInfo);

    Class<?> cl = (mode == VectorExpressionDescriptor.Mode.FILTER ? FilterStructColumnInList.class : StructColumnInList.class);

    expr = createVectorExpression(cl, null, VectorExpressionDescriptor.Mode.PROJECTION, returnType);

    ((IStringInExpr) expr).setInListValues(serializedInChildren);

    ((IStructInExpr) expr).setScratchBytesColumn(scratchBytesCol);
    ((IStructInExpr) expr).setStructColumnExprs(this, colExpr.getChildren(),
        fieldVectorColumnTypes);

    return expr;
  }

  /**
   * Create a filter or boolean-valued expression for column IN ( <list-of-constants> )
   */
  private VectorExpression getInExpression(List<ExprNodeDesc> childExpr,
      VectorExpressionDescriptor.Mode mode, TypeInfo returnType) throws HiveException {
    ExprNodeDesc colExpr = childExpr.get(0);
    List<ExprNodeDesc> inChildren = childExpr.subList(1, childExpr.size());

    String colType = colExpr.getTypeString();
    colType = VectorizationContext.mapTypeNameSynonyms(colType);
    TypeInfo colTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(colType);
    Category category = colTypeInfo.getCategory();
    if (category == Category.STRUCT) {
      return getStructInExpression(childExpr, colExpr, colTypeInfo, inChildren, mode, returnType);
    } else if (category != Category.PRIMITIVE) {
      return null;
    }

    // prepare arguments for createVectorExpression
    List<ExprNodeDesc> childrenForInList =  evaluateCastOnConstants(inChildren);

    /* This method assumes that the IN list has no NULL entries. That is enforced elsewhere,
     * in the Vectorizer class. If NULL is passed in as a list entry, behavior is not defined.
     * If in the future, NULL values are allowed in the IN list, be sure to handle 3-valued
     * logic correctly. E.g. NOT (col IN (null)) should be considered UNKNOWN, so that would
     * become FALSE in the WHERE clause, and cause the row in question to be filtered out.
     * See the discussion in Jira HIVE-5583.
     */

    VectorExpression expr = null;

    // Validate the IN items are only constants.
    for (ExprNodeDesc inListChild : childrenForInList) {
      if (!(inListChild instanceof ExprNodeConstantDesc)) {
        throw new HiveException("Vectorizing IN expression only supported for constant values");
      }
    }

    // determine class
    Class<?> cl = null;
    // TODO: the below assumes that all the arguments to IN are of the same type;
    //       non-vectorized validates that explicitly during UDF init.
    if (isIntFamily(colType)) {
      cl = (mode == VectorExpressionDescriptor.Mode.FILTER ? FilterLongColumnInList.class : LongColumnInList.class);
      long[] inVals = new long[childrenForInList.size()];
      for (int i = 0; i != inVals.length; i++) {
        inVals[i] = getIntFamilyScalarAsLong((ExprNodeConstantDesc) childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), VectorExpressionDescriptor.Mode.PROJECTION, returnType);
      ((ILongInExpr) expr).setInListValues(inVals);
    } else if (isTimestampFamily(colType)) {
      cl = (mode == VectorExpressionDescriptor.Mode.FILTER ? FilterTimestampColumnInList.class : TimestampColumnInList.class);
      Timestamp[] inVals = new Timestamp[childrenForInList.size()];
      for (int i = 0; i != inVals.length; i++) {
        inVals[i] = getTimestampScalar(childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), VectorExpressionDescriptor.Mode.PROJECTION, returnType);
      ((ITimestampInExpr) expr).setInListValues(inVals);
    } else if (isStringFamily(colType)) {
      cl = (mode == VectorExpressionDescriptor.Mode.FILTER ? FilterStringColumnInList.class : StringColumnInList.class);
      byte[][] inVals = new byte[childrenForInList.size()][];
      for (int i = 0; i != inVals.length; i++) {
        inVals[i] = getStringScalarAsByteArray((ExprNodeConstantDesc) childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), VectorExpressionDescriptor.Mode.PROJECTION, returnType);
      ((IStringInExpr) expr).setInListValues(inVals);
    } else if (isFloatFamily(colType)) {
      cl = (mode == VectorExpressionDescriptor.Mode.FILTER ? FilterDoubleColumnInList.class : DoubleColumnInList.class);
      double[] inValsD = new double[childrenForInList.size()];
      for (int i = 0; i != inValsD.length; i++) {
        inValsD[i] = getNumericScalarAsDouble(childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), VectorExpressionDescriptor.Mode.PROJECTION, returnType);
      ((IDoubleInExpr) expr).setInListValues(inValsD);
    } else if (isDecimalFamily(colType)) {
      cl = (mode == VectorExpressionDescriptor.Mode.FILTER ? FilterDecimalColumnInList.class : DecimalColumnInList.class);
      HiveDecimal[] inValsD = new HiveDecimal[childrenForInList.size()];
      for (int i = 0; i != inValsD.length; i++) {
        inValsD[i] = (HiveDecimal) getVectorTypeScalarValue(
            (ExprNodeConstantDesc)  childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), VectorExpressionDescriptor.Mode.PROJECTION, returnType);
      ((IDecimalInExpr) expr).setInListValues(inValsD);
    } else if (isDateFamily(colType)) {
      cl = (mode == VectorExpressionDescriptor.Mode.FILTER ? FilterLongColumnInList.class : LongColumnInList.class);
      long[] inVals = new long[childrenForInList.size()];
      for (int i = 0; i != inVals.length; i++) {
        inVals[i] = (Long) getVectorTypeScalarValue((ExprNodeConstantDesc) childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), VectorExpressionDescriptor.Mode.PROJECTION, returnType);
      ((ILongInExpr) expr).setInListValues(inVals);
    }

    // Return the desired VectorExpression if found. Otherwise, return null to cause
    // execution to fall back to row mode.
    return expr;
  }

  private byte[] getStringScalarAsByteArray(ExprNodeConstantDesc exprNodeConstantDesc)
      throws HiveException {
    Object o = getScalarValue(exprNodeConstantDesc);
    if (o instanceof byte[]) {
      return (byte[]) o;
    } else if (o instanceof HiveChar) {
      HiveChar hiveChar = (HiveChar) o;
      try {
        return hiveChar.getStrippedValue().getBytes("UTF-8");
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
    } else if (o instanceof HiveVarchar) {
      HiveVarchar hiveVarchar = (HiveVarchar) o;
      try {
        return hiveVarchar.getValue().getBytes("UTF-8");
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
    } else {
      throw new HiveException("Expected constant argument of string family but found " +
          o.getClass().getSimpleName());
    }
  }

  private PrimitiveCategory getAnyIntegerPrimitiveCategoryFromUdfClass(Class<? extends UDF> udfClass) {
    if (udfClass.equals(UDFToByte.class)) {
      return PrimitiveCategory.BYTE;
    } else if (udfClass.equals(UDFToShort.class)) {
      return PrimitiveCategory.SHORT;
    } else if (udfClass.equals(UDFToInteger.class)) {
      return PrimitiveCategory.INT;
    } else if (udfClass.equals(UDFToLong.class)) {
      return PrimitiveCategory.LONG;
    } else {
      throw new RuntimeException("Unexpected any integery UDF class " + udfClass.getName());
    }
  }

  /**
   * Invoke special handling for expressions that can't be vectorized by regular
   * descriptor based lookup.
   */
  private VectorExpression getGenericUDFBridgeVectorExpression(GenericUDFBridge udf,
      List<ExprNodeDesc> childExpr, VectorExpressionDescriptor.Mode mode, TypeInfo returnType) throws HiveException {
    Class<? extends UDF> cl = udf.getUdfClass();
    VectorExpression ve = null;
    if (isCastToIntFamily(cl)) {
      PrimitiveCategory integerPrimitiveCategory =
          getAnyIntegerPrimitiveCategoryFromUdfClass(cl);
      ve = getCastToLongExpression(childExpr, integerPrimitiveCategory);
    } else if (cl.equals(UDFToBoolean.class)) {
      ve = getCastToBoolean(childExpr);
    } else if (isCastToFloatFamily(cl)) {
      ve = getCastToDoubleExpression(cl, childExpr, returnType);
    }
    if (ve == null && childExpr instanceof ExprNodeGenericFuncDesc) {
      ve = getCustomUDFExpression((ExprNodeGenericFuncDesc) childExpr, mode);
    }
    return ve;
  }

  private HiveDecimal castConstantToDecimal(Object scalar, TypeInfo type) throws HiveException {

    if (null == scalar) {
      return null;
    }
    PrimitiveTypeInfo ptinfo = (PrimitiveTypeInfo) type;
    String typename = type.getTypeName();
    HiveDecimal rawDecimal;
    PrimitiveCategory primitiveCategory = ptinfo.getPrimitiveCategory();
    switch (primitiveCategory) {
    case FLOAT:
      rawDecimal = HiveDecimal.create(String.valueOf(scalar));
      break;
    case DOUBLE:
      rawDecimal = HiveDecimal.create(String.valueOf(scalar));
      break;
    case BYTE:
      rawDecimal = HiveDecimal.create((Byte) scalar);
      break;
    case SHORT:
      rawDecimal = HiveDecimal.create((Short) scalar);
      break;
    case INT:
      rawDecimal = HiveDecimal.create((Integer) scalar);
      break;
    case LONG:
      rawDecimal = HiveDecimal.create((Long) scalar);
      break;
    case STRING:
      rawDecimal = HiveDecimal.create((String) scalar);
      break;
    case CHAR:
      rawDecimal = HiveDecimal.create(((HiveChar) scalar).getStrippedValue());
      break;
    case VARCHAR:
      rawDecimal = HiveDecimal.create(((HiveVarchar) scalar).getValue());
      break;
    case DECIMAL:
      rawDecimal = (HiveDecimal) scalar;
      break;
    default:
      throw new HiveException("Unsupported primitive category " + primitiveCategory + " for cast to HiveDecimal");
    }
    if (rawDecimal == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Casting constant scalar " + scalar + " to HiveDecimal resulted in null");
      }
      return null;
    }
    return rawDecimal;
  }

  private String castConstantToString(Object scalar, TypeInfo type) throws HiveException {
    if (null == scalar) {
      return null;
    }
    PrimitiveTypeInfo ptinfo = (PrimitiveTypeInfo) type;
    String typename = type.getTypeName();
    switch (ptinfo.getPrimitiveCategory()) {
    case FLOAT:
    case DOUBLE:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return ((Number) scalar).toString();
    case DECIMAL:
      HiveDecimal decimalVal = (HiveDecimal) scalar;
      DecimalTypeInfo decType = (DecimalTypeInfo) type;
      return decimalVal.toFormatString(decType.getScale());
    default:
      throw new HiveException("Unsupported type "+typename+" for cast to String");
    }
  }

  private Double castConstantToDouble(Object scalar, TypeInfo type) throws HiveException {
    if (null == scalar) {
      return null;
    }
    PrimitiveTypeInfo ptinfo = (PrimitiveTypeInfo) type;
    String typename = type.getTypeName();
    PrimitiveCategory primitiveCategory = ptinfo.getPrimitiveCategory();
    switch (primitiveCategory) {
    case FLOAT:
    case DOUBLE:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return ((Number) scalar).doubleValue();
    case STRING:
      return Double.valueOf((String) scalar);
    case CHAR:
      return Double.valueOf(((HiveChar) scalar).getStrippedValue());
    case VARCHAR:
      return Double.valueOf(((HiveVarchar) scalar).getValue());
    case DECIMAL:
      HiveDecimal decimalVal = (HiveDecimal) scalar;
      return decimalVal.doubleValue();
    default:
      throw new HiveException("Unsupported primitive category " + primitiveCategory + " for cast to DOUBLE");
    }
  }

  private Long castConstantToLong(Object scalar, TypeInfo type,
      PrimitiveCategory integerPrimitiveCategory) throws HiveException {
    if (null == scalar) {
      return null;
    }
    PrimitiveTypeInfo ptinfo = (PrimitiveTypeInfo) type;
    String typename = type.getTypeName();
    PrimitiveCategory primitiveCategory = ptinfo.getPrimitiveCategory();
    switch (primitiveCategory) {
    case FLOAT:
    case DOUBLE:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return ((Number) scalar).longValue();
    case STRING:
    case CHAR:
    case VARCHAR:
      {
        final long longValue;
        if (primitiveCategory == PrimitiveCategory.STRING) {
          longValue = Long.valueOf((String) scalar);
        } else if (primitiveCategory == PrimitiveCategory.CHAR) {
          longValue = Long.valueOf(((HiveChar) scalar).getStrippedValue());
        } else {
          longValue = Long.valueOf(((HiveVarchar) scalar).getValue());
        }
        switch (integerPrimitiveCategory) {
        case BYTE:
          if (longValue != ((byte) longValue)) {
            // Accurate byte value cannot be obtained.
            return null;
          }
          break;
        case SHORT:
          if (longValue != ((short) longValue)) {
            // Accurate short value cannot be obtained.
            return null;
          }
          break;
        case INT:
          if (longValue != ((int) longValue)) {
            // Accurate int value cannot be obtained.
            return null;
          }
          break;
        case LONG:
          // No range check needed.
          break;
        default:
          throw new RuntimeException("Unexpected integer primitive type " + integerPrimitiveCategory);
        }
        return longValue;
      }
    case DECIMAL:
      HiveDecimal decimalVal = (HiveDecimal) scalar;
      switch (integerPrimitiveCategory) {
      case BYTE:
        if (!decimalVal.isByte()) {
          // Accurate byte value cannot be obtained.
          return null;
        }
        break;
      case SHORT:
        if (!decimalVal.isShort()) {
          // Accurate short value cannot be obtained.
          return null;
        }
        break;
      case INT:
        if (!decimalVal.isInt()) {
          // Accurate int value cannot be obtained.
          return null;
        }
        break;
      case LONG:
        if (!decimalVal.isLong()) {
          // Accurate long value cannot be obtained.
          return null;
        }
        break;
      default:
        throw new RuntimeException("Unexpected integer primitive type " + integerPrimitiveCategory);
      }
      // We only store longs in our LongColumnVector.
      return decimalVal.longValue();
    default:
      throw new HiveException("Unsupported primitive category " + primitiveCategory + " for cast to LONG");
    }
  }

  /*
   * This method must return the decimal TypeInfo for what getCastToDecimal will produce.
   */
  private DecimalTypeInfo decimalTypeFromCastToDecimal(ExprNodeDesc exprNodeDesc,
      DecimalTypeInfo returnDecimalType) throws HiveException {

    if (exprNodeDesc instanceof ExprNodeConstantDesc) {
      // Return a constant vector expression
       Object constantValue = ((ExprNodeConstantDesc) exprNodeDesc).getValue();
       HiveDecimal decimalValue = castConstantToDecimal(constantValue, exprNodeDesc.getTypeInfo());
       if (decimalValue == null) {
         // Return something.
         return returnDecimalType;
       }
       return new DecimalTypeInfo(decimalValue.precision(), decimalValue.scale());
     }
     String inputType = exprNodeDesc.getTypeString();
     if (isIntFamily(inputType) ||
         isFloatFamily(inputType) ||
         decimalTypePattern.matcher(inputType).matches() ||
         isStringFamily(inputType) ||
         inputType.equals("timestamp")) {
      return returnDecimalType;
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
      HiveDecimal decimalValue = castConstantToDecimal(constantValue, child.getTypeInfo());
      return getConstantVectorExpression(decimalValue, returnType, VectorExpressionDescriptor.Mode.PROJECTION);
    }
    if (isIntFamily(inputType)) {
      return createVectorExpression(CastLongToDecimal.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (inputType.equals("float")) {
      return createVectorExpression(CastFloatToDecimal.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (inputType.equals("double")) {
      return createVectorExpression(CastDoubleToDecimal.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (decimalTypePattern.matcher(inputType).matches()) {
      if (child instanceof ExprNodeColumnDesc) {
        int colIndex = getInputColumnIndex((ExprNodeColumnDesc) child);
        DataTypePhysicalVariation dataTypePhysicalVariation = getDataTypePhysicalVariation(colIndex);
        if (dataTypePhysicalVariation == DataTypePhysicalVariation.DECIMAL_64) {

          // Do Decimal64 conversion instead.
          return createDecimal64ToDecimalConversion(colIndex, returnType);
        } else {
          return createVectorExpression(CastDecimalToDecimal.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
              returnType);
        }
      } else {
        return createVectorExpression(CastDecimalToDecimal.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
            returnType);
      }
    } else if (isStringFamily(inputType)) {
      return createVectorExpression(CastStringToDecimal.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (inputType.equals("timestamp")) {
      return createVectorExpression(CastTimestampToDecimal.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    }
    return null;
  }

  private VectorExpression getCastToString(List<ExprNodeDesc> childExpr, TypeInfo returnType)
      throws HiveException {
    ExprNodeDesc child = childExpr.get(0);
    String inputType = childExpr.get(0).getTypeString();
    if (child instanceof ExprNodeConstantDesc) {
        // Return a constant vector expression
        Object constantValue = ((ExprNodeConstantDesc) child).getValue();
        String strValue = castConstantToString(constantValue, child.getTypeInfo());
        return getConstantVectorExpression(strValue, returnType, VectorExpressionDescriptor.Mode.PROJECTION);
    }
    if (inputType.equals("boolean")) {
      // Boolean must come before the integer family. It's a special case.
      return createVectorExpression(CastBooleanToStringViaLongToString.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isIntFamily(inputType)) {
      return createVectorExpression(CastLongToString.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (inputType.equals("float")) {
      return createVectorExpression(CastFloatToString.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (inputType.equals("double")) {
      return createVectorExpression(CastDoubleToString.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isDecimalFamily(inputType)) {
      return createVectorExpression(CastDecimalToString.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isDateFamily(inputType)) {
      return createVectorExpression(CastDateToString.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isTimestampFamily(inputType)) {
      return createVectorExpression(CastTimestampToString.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isStringFamily(inputType)) {
      return createVectorExpression(CastStringGroupToString.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    }
    return null;
  }

  private VectorExpression getCastToChar(List<ExprNodeDesc> childExpr, TypeInfo returnType)
      throws HiveException {
    ExprNodeDesc child = childExpr.get(0);
    String inputType = childExpr.get(0).getTypeString();
    if (child instanceof ExprNodeConstantDesc) {
      // Don't do constant folding here.  Wait until the optimizer is changed to do it.
      // Family of related JIRAs: HIVE-7421, HIVE-7422, and HIVE-7424.
      return null;
    }
    if (inputType.equals("boolean")) {
      // Boolean must come before the integer family. It's a special case.
      return createVectorExpression(CastBooleanToCharViaLongToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isIntFamily(inputType)) {
      return createVectorExpression(CastLongToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (inputType.equals("float")) {
      return createVectorExpression(CastFloatToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (inputType.equals("double")) {
      return createVectorExpression(CastDoubleToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isDecimalFamily(inputType)) {
      return createVectorExpression(CastDecimalToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isDateFamily(inputType)) {
      return createVectorExpression(CastDateToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isTimestampFamily(inputType)) {
      return createVectorExpression(CastTimestampToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isStringFamily(inputType)) {
      return createVectorExpression(CastStringGroupToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    }
    return null;
  }

  private VectorExpression getCastToVarChar(List<ExprNodeDesc> childExpr, TypeInfo returnType)
      throws HiveException {
    ExprNodeDesc child = childExpr.get(0);
    String inputType = childExpr.get(0).getTypeString();
    if (child instanceof ExprNodeConstantDesc) {
      // Don't do constant folding here.  Wait until the optimizer is changed to do it.
      // Family of related JIRAs: HIVE-7421, HIVE-7422, and HIVE-7424.
      return null;
    }
    if (inputType.equals("boolean")) {
      // Boolean must come before the integer family. It's a special case.
      return createVectorExpression(CastBooleanToVarCharViaLongToVarChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isIntFamily(inputType)) {
      return createVectorExpression(CastLongToVarChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (inputType.equals("float")) {
      return createVectorExpression(CastFloatToVarChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (inputType.equals("double")) {
      return createVectorExpression(CastDoubleToVarChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isDecimalFamily(inputType)) {
      return createVectorExpression(CastDecimalToVarChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isDateFamily(inputType)) {
      return createVectorExpression(CastDateToVarChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isTimestampFamily(inputType)) {
      return createVectorExpression(CastTimestampToVarChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    } else if (isStringFamily(inputType)) {
      return createVectorExpression(CastStringGroupToVarChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
    }
    return null;
  }

  private VectorExpression getCastToDoubleExpression(Class<?> udf, List<ExprNodeDesc> childExpr,
      TypeInfo returnType) throws HiveException {
    ExprNodeDesc child = childExpr.get(0);
    String inputType = childExpr.get(0).getTypeString();
    if (child instanceof ExprNodeConstantDesc) {
        // Return a constant vector expression
        Object constantValue = ((ExprNodeConstantDesc) child).getValue();
        Double doubleValue = castConstantToDouble(constantValue, child.getTypeInfo());
        return getConstantVectorExpression(doubleValue, returnType, VectorExpressionDescriptor.Mode.PROJECTION);
    }
    if (isIntFamily(inputType)) {
      if (udf.equals(UDFToFloat.class)) {
        // In order to convert from integer to float correctly, we need to apply the float cast not the double cast (HIVE-13338).
        return createVectorExpression(CastLongToFloatViaLongToDouble.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
      } else {
        return createVectorExpression(CastLongToDouble.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
      }
    } else if (inputType.equals("timestamp")) {
      return createVectorExpression(CastTimestampToDouble.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType);
    } else if (isFloatFamily(inputType)) {

      // float types require no conversion, so use a no-op
      return getIdentityExpression(childExpr);
    }
    return null;
  }

  private VectorExpression getCastToBoolean(List<ExprNodeDesc> childExpr)
      throws HiveException {
    ExprNodeDesc child = childExpr.get(0);
    TypeInfo inputTypeInfo = child.getTypeInfo();
    String inputType = inputTypeInfo.toString();
    if (child instanceof ExprNodeConstantDesc) {
      if (null == ((ExprNodeConstantDesc)child).getValue()) {
        return getConstantVectorExpression(null, TypeInfoFactory.booleanTypeInfo, VectorExpressionDescriptor.Mode.PROJECTION);
      }
      // Don't do constant folding here.  Wait until the optimizer is changed to do it.
      // Family of related JIRAs: HIVE-7421, HIVE-7422, and HIVE-7424.
      return null;
    }
    // Long and double are handled using descriptors, string needs to be specially handled.
    if (isStringFamily(inputType)) {

      VectorExpression lenExpr = createVectorExpression(CastStringToBoolean.class, childExpr,
          VectorExpressionDescriptor.Mode.PROJECTION, TypeInfoFactory.booleanTypeInfo);

      return lenExpr;
    }
    return null;
  }

  private VectorExpression getCastToLongExpression(List<ExprNodeDesc> childExpr, PrimitiveCategory integerPrimitiveCategory)
      throws HiveException {
    ExprNodeDesc child = childExpr.get(0);
    String inputType = childExpr.get(0).getTypeString();
    if (child instanceof ExprNodeConstantDesc) {
        // Return a constant vector expression
        Object constantValue = ((ExprNodeConstantDesc) child).getValue();
        Long longValue = castConstantToLong(constantValue, child.getTypeInfo(), integerPrimitiveCategory);
        return getConstantVectorExpression(longValue, TypeInfoFactory.longTypeInfo, VectorExpressionDescriptor.Mode.PROJECTION);
    }
    // Float family, timestamp are handled via descriptor based lookup, int family needs
    // special handling.
    if (isIntFamily(inputType)) {
      // integer and boolean types require no conversion, so use a no-op
      return getIdentityExpression(childExpr);
    }
    return null;
  }

  /* Get a [NOT] BETWEEN filter expression. This is treated as a special case
   * because the NOT is actually specified in the expression tree as the first argument,
   * and we don't want any runtime cost for that. So creating the VectorExpression
   * needs to be done differently than the standard way where all arguments are
   * passed to the VectorExpression constructor.
   */
  private VectorExpression getBetweenFilterExpression(List<ExprNodeDesc> childExpr, VectorExpressionDescriptor.Mode mode, TypeInfo returnType)
      throws HiveException {

    if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {

      // Projection mode is not yet supported for [NOT] BETWEEN. Return null so Vectorizer
      // knows to revert to row-at-a-time execution.
      return null;
    }

    boolean hasDynamicValues = false;

    // We don't currently support the BETWEEN ends being columns.  They must be scalars.
    if ((childExpr.get(2) instanceof ExprNodeDynamicValueDesc) &&
        (childExpr.get(3) instanceof ExprNodeDynamicValueDesc)) {
      hasDynamicValues = true;
    } else if (!(childExpr.get(2) instanceof ExprNodeConstantDesc) ||
        !(childExpr.get(3) instanceof ExprNodeConstantDesc)) {
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
    List<ExprNodeDesc> childrenAfterNot = evaluateCastOnConstants(castChildren);

    // determine class
    Class<?> cl = null;
    if (isIntFamily(colType) && !notKeywordPresent) {
      cl = (hasDynamicValues ?
          FilterLongColumnBetweenDynamicValue.class :
          FilterLongColumnBetween.class);
    } else if (isIntFamily(colType) && notKeywordPresent) {
      cl = FilterLongColumnNotBetween.class;
    } else if (isFloatFamily(colType) && !notKeywordPresent) {
      cl = (hasDynamicValues ?
          FilterDoubleColumnBetweenDynamicValue.class :
          FilterDoubleColumnBetween.class);
    } else if (isFloatFamily(colType) && notKeywordPresent) {
      cl = FilterDoubleColumnNotBetween.class;
    } else if (colType.equals("string") && !notKeywordPresent) {
      cl = (hasDynamicValues ?
          FilterStringColumnBetweenDynamicValue.class :
          FilterStringColumnBetween.class);
    } else if (colType.equals("string") && notKeywordPresent) {
      cl = FilterStringColumnNotBetween.class;
    } else if (varcharTypePattern.matcher(colType).matches() && !notKeywordPresent) {
      cl = (hasDynamicValues ?
          FilterVarCharColumnBetweenDynamicValue.class :
          FilterVarCharColumnBetween.class);
    } else if (varcharTypePattern.matcher(colType).matches() && notKeywordPresent) {
      cl = FilterVarCharColumnNotBetween.class;
    } else if (charTypePattern.matcher(colType).matches() && !notKeywordPresent) {
      cl =  (hasDynamicValues ?
          FilterCharColumnBetweenDynamicValue.class :
          FilterCharColumnBetween.class);
    } else if (charTypePattern.matcher(colType).matches() && notKeywordPresent) {
      cl = FilterCharColumnNotBetween.class;
    } else if (colType.equals("timestamp") && !notKeywordPresent) {
      cl = (hasDynamicValues ?
          FilterTimestampColumnBetweenDynamicValue.class :
          FilterTimestampColumnBetween.class);
    } else if (colType.equals("timestamp") && notKeywordPresent) {
      cl = FilterTimestampColumnNotBetween.class;
    } else if (isDecimalFamily(colType) && !notKeywordPresent) {
      cl = (hasDynamicValues ?
          FilterDecimalColumnBetweenDynamicValue.class :
          FilterDecimalColumnBetween.class);
    } else if (isDecimalFamily(colType) && notKeywordPresent) {
      cl = FilterDecimalColumnNotBetween.class;
    } else if (isDateFamily(colType) && !notKeywordPresent) {
      cl =  (hasDynamicValues ?
          FilterDateColumnBetweenDynamicValue.class :
          FilterLongColumnBetween.class);
    } else if (isDateFamily(colType) && notKeywordPresent) {
      cl = FilterLongColumnNotBetween.class;
    }
    return createVectorExpression(cl, childrenAfterNot, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
  }

  private boolean isCondExpr(ExprNodeDesc exprNodeDesc) {
    if (exprNodeDesc instanceof ExprNodeConstantDesc ||
        exprNodeDesc instanceof ExprNodeColumnDesc) {
      return false;
    }
    return true;   // Requires conditional evaluation for good performance.
  }

  private boolean isNullConst(ExprNodeDesc exprNodeDesc) {
    //null constant could be typed so we need to check the value
    if (exprNodeDesc instanceof ExprNodeConstantDesc &&
        ((ExprNodeConstantDesc) exprNodeDesc).getValue() == null) {
      return true;
    }
    return false;
  }

  private VectorExpression getIfExpression(GenericUDFIf genericUDFIf, List<ExprNodeDesc> childExpr,
      VectorExpressionDescriptor.Mode mode, TypeInfo returnType) throws HiveException {

    if (mode != VectorExpressionDescriptor.Mode.PROJECTION) {
      return null;
    }

    // Add HiveConf variable with 3 modes:
    //   1) adaptor: Always use VectorUDFAdaptor for IF statements.
    //
    //   2) good: Vectorize but don't optimize conditional expressions
    //
    //   3) better: Vectorize and Optimize conditional expressions.
    //

    if (hiveVectorIfStmtMode == HiveVectorIfStmtMode.ADAPTOR) {
      return null;
    }

    // Align the THEN/ELSE types.
    childExpr =
        getChildExpressionsWithImplicitCast(
            genericUDFIf,
            childExpr,
            returnType);

    final ExprNodeDesc ifDesc = childExpr.get(0);
    final ExprNodeDesc thenDesc = childExpr.get(1);
    final ExprNodeDesc elseDesc = childExpr.get(2);

    final boolean isThenNullConst = isNullConst(thenDesc);
    final boolean isElseNullConst = isNullConst(elseDesc);
    if (isThenNullConst && isElseNullConst) {

      // THEN NULL ELSE NULL: An unusual "case", but possible.
      final int outputColumnNum = ocm.allocateOutputColumn(returnType);

      final VectorExpression resultExpr =
          new IfExprNullNull(
            outputColumnNum);

      resultExpr.setOutputTypeInfo(returnType);
      resultExpr.setOutputDataTypePhysicalVariation(DataTypePhysicalVariation.NONE);

      return resultExpr;
    }

    final boolean isThenCondExpr = isCondExpr(thenDesc);
    final boolean isElseCondExpr = isCondExpr(elseDesc);

    final boolean isOnlyGood = (hiveVectorIfStmtMode == HiveVectorIfStmtMode.GOOD);

    if (isThenNullConst) {
      final VectorExpression whenExpr = getVectorExpression(ifDesc, mode);
      final VectorExpression elseExpr = getVectorExpression(elseDesc, mode);

      final int outputColumnNum = ocm.allocateOutputColumn(returnType);

      final VectorExpression resultExpr;
      if (!isElseCondExpr || isOnlyGood) {
        resultExpr =
            new IfExprNullColumn(
              whenExpr.getOutputColumnNum(),
              elseExpr.getOutputColumnNum(),
              outputColumnNum);
      } else {
        resultExpr =
            new IfExprNullCondExpr(
              whenExpr.getOutputColumnNum(),
              elseExpr.getOutputColumnNum(),
              outputColumnNum);
      }

      resultExpr.setChildExpressions(new VectorExpression[] {whenExpr, elseExpr});

      resultExpr.setInputTypeInfos(
          whenExpr.getOutputTypeInfo(),
          TypeInfoFactory.voidTypeInfo,
          elseExpr.getOutputTypeInfo());
      resultExpr.setInputDataTypePhysicalVariations(
          whenExpr.getOutputDataTypePhysicalVariation(),
          DataTypePhysicalVariation.NONE,
          elseExpr.getOutputDataTypePhysicalVariation());

      resultExpr.setOutputTypeInfo(returnType);
      resultExpr.setOutputDataTypePhysicalVariation(DataTypePhysicalVariation.NONE);

      return resultExpr;
    }

    if (isElseNullConst) {
      final VectorExpression whenExpr = getVectorExpression(ifDesc, mode);
      final VectorExpression thenExpr = getVectorExpression(thenDesc, mode);

      final int outputColumnNum = ocm.allocateOutputColumn(returnType);

      final VectorExpression resultExpr;
      if (!isThenCondExpr || isOnlyGood) {
        resultExpr =
            new IfExprColumnNull(
              whenExpr.getOutputColumnNum(),
              thenExpr.getOutputColumnNum(),
              outputColumnNum);
      } else {
        resultExpr =
            new IfExprCondExprNull(
              whenExpr.getOutputColumnNum(),
              thenExpr.getOutputColumnNum(),
              outputColumnNum);
      }

      resultExpr.setChildExpressions(new VectorExpression[] {whenExpr, thenExpr});

      resultExpr.setInputTypeInfos(
          whenExpr.getOutputTypeInfo(),
          thenExpr.getOutputTypeInfo(),
          TypeInfoFactory.voidTypeInfo);
      resultExpr.setInputDataTypePhysicalVariations(
          whenExpr.getOutputDataTypePhysicalVariation(),
          thenExpr.getOutputDataTypePhysicalVariation(),
          DataTypePhysicalVariation.NONE);

      resultExpr.setOutputTypeInfo(returnType);
      resultExpr.setOutputDataTypePhysicalVariation(DataTypePhysicalVariation.NONE);

      return resultExpr;
    }

    if ((isThenCondExpr || isElseCondExpr) && !isOnlyGood) {
      final VectorExpression whenExpr = getVectorExpression(ifDesc, mode);
      final VectorExpression thenExpr = getVectorExpression(thenDesc, mode);
      final VectorExpression elseExpr = getVectorExpression(elseDesc, mode);

      // Only proceed if the THEN/ELSE types were aligned.
      if (thenExpr.getOutputColumnVectorType() == elseExpr.getOutputColumnVectorType()) {

        final int outputColumnNum = ocm.allocateOutputColumn(returnType);

        final VectorExpression resultExpr;
        if (isThenCondExpr && isElseCondExpr) {
          resultExpr =
              new IfExprCondExprCondExpr(
                whenExpr.getOutputColumnNum(),
                thenExpr.getOutputColumnNum(),
                elseExpr.getOutputColumnNum(),
                outputColumnNum);
        } else if (isThenCondExpr) {
          resultExpr =
              new IfExprCondExprColumn(
                whenExpr.getOutputColumnNum(),
                thenExpr.getOutputColumnNum(),
                elseExpr.getOutputColumnNum(),
                outputColumnNum);
        } else {
          resultExpr =
              new IfExprColumnCondExpr(
                whenExpr.getOutputColumnNum(),
                thenExpr.getOutputColumnNum(),
                elseExpr.getOutputColumnNum(),
                outputColumnNum);
        }

        resultExpr.setChildExpressions(new VectorExpression[] {whenExpr, thenExpr, elseExpr});

        resultExpr.setInputTypeInfos(
            whenExpr.getOutputTypeInfo(),
            thenExpr.getOutputTypeInfo(),
            elseExpr.getOutputTypeInfo());
        resultExpr.setInputDataTypePhysicalVariations(
            whenExpr.getOutputDataTypePhysicalVariation(),
            thenExpr.getOutputDataTypePhysicalVariation(),
            elseExpr.getOutputDataTypePhysicalVariation());

        resultExpr.setOutputTypeInfo(returnType);
        resultExpr.setOutputDataTypePhysicalVariation(DataTypePhysicalVariation.NONE);

        return resultExpr;
      }
    }

    Class<?> udfClass = genericUDFIf.getClass();
    return getVectorExpressionForUdf(
        genericUDFIf, udfClass, childExpr, mode, returnType);
  }

  private VectorExpression getWhenExpression(List<ExprNodeDesc> childExpr,
      VectorExpressionDescriptor.Mode mode, TypeInfo returnType) throws HiveException {

    if (mode != VectorExpressionDescriptor.Mode.PROJECTION) {
      return null;
    }
    final int size = childExpr.size();

    final ExprNodeDesc whenDesc = childExpr.get(0);
    final ExprNodeDesc thenDesc = childExpr.get(1);
    final ExprNodeDesc elseDesc;

    if (size == 2) {
      elseDesc = new ExprNodeConstantDesc(returnType, null);
    } else if (size == 3) {
      elseDesc = childExpr.get(2);
    } else {
      final GenericUDFWhen udfWhen = new GenericUDFWhen();
      elseDesc = new ExprNodeGenericFuncDesc(returnType, udfWhen, udfWhen.getUdfName(),
          childExpr.subList(2, childExpr.size()));
    }

    // Transform CASE WHEN with just a THEN/ELSE into an IF statement.
    final GenericUDFIf genericUDFIf = new GenericUDFIf();
    final List<ExprNodeDesc> ifChildExpr =
        Arrays.<ExprNodeDesc>asList(whenDesc, thenDesc, elseDesc);
    return getIfExpression(genericUDFIf, ifChildExpr, mode, returnType);
  }

  /*
   * Return vector expression for a custom (i.e. not built-in) UDF.
   */
  private VectorExpression getCustomUDFExpression(ExprNodeGenericFuncDesc expr, VectorExpressionDescriptor.Mode mode)
      throws HiveException {

    boolean isFilter = false;    // Assume.
    if (mode == VectorExpressionDescriptor.Mode.FILTER) {

      // Is output type a BOOLEAN?
      TypeInfo resultTypeInfo = expr.getTypeInfo();
      if (resultTypeInfo.getCategory() == Category.PRIMITIVE &&
          ((PrimitiveTypeInfo) resultTypeInfo).getPrimitiveCategory() == PrimitiveCategory.BOOLEAN) {
        isFilter = true;
      } else {
        return null;
      }
    }

    //GenericUDFBridge udfBridge = (GenericUDFBridge) expr.getGenericUDF();
    List<ExprNodeDesc> childExprList = expr.getChildren();
    final int childrenCount = childExprList.size();

    // argument descriptors
    VectorUDFArgDesc[] argDescs = new VectorUDFArgDesc[childrenCount];
    for (int i = 0; i < argDescs.length; i++) {
      argDescs[i] = new VectorUDFArgDesc();
    }

    // positions of variable arguments (columns or non-constant expressions)
    List<Integer> variableArgPositions = new ArrayList<Integer>();

    // Column numbers of batch corresponding to expression result arguments
    List<Integer> exprResultColumnNums = new ArrayList<Integer>();

    // Prepare children
    List<VectorExpression> vectorExprs = new ArrayList<VectorExpression>();

    TypeInfo[] inputTypeInfos = new TypeInfo[childrenCount];
    DataTypePhysicalVariation[] inputDataTypePhysicalVariations = new DataTypePhysicalVariation[childrenCount];

    for (int i = 0; i < childrenCount; i++) {
      ExprNodeDesc child = childExprList.get(i);
      inputTypeInfos[i] = child.getTypeInfo();
      inputDataTypePhysicalVariations[i] = DataTypePhysicalVariation.NONE;

      if (child instanceof ExprNodeGenericFuncDesc) {
        VectorExpression e = getVectorExpression(child, VectorExpressionDescriptor.Mode.PROJECTION);
        vectorExprs.add(e);
        variableArgPositions.add(i);
        exprResultColumnNums.add(e.getOutputColumnNum());
        argDescs[i].setVariable(e.getOutputColumnNum());
      } else if (child instanceof ExprNodeColumnDesc) {
        variableArgPositions.add(i);
        argDescs[i].setVariable(getInputColumnIndex(((ExprNodeColumnDesc) child).getColumn()));
      } else if (child instanceof ExprNodeConstantDesc) {
        // this is a constant (or null)
        if (child.getTypeInfo().getCategory() != Category.PRIMITIVE) {

          // Complex type constants currently not supported by VectorUDFArgDesc.prepareConstant.
          throw new HiveException(
              "Unable to vectorize custom UDF. Complex type constants not supported: " + child);
        }
        argDescs[i].setConstant((ExprNodeConstantDesc) child);
      } else if (child instanceof ExprNodeDynamicValueDesc) {
        VectorExpression e = getVectorExpression(child, VectorExpressionDescriptor.Mode.PROJECTION);
        vectorExprs.add(e);
        variableArgPositions.add(i);
        exprResultColumnNums.add(e.getOutputColumnNum());
        argDescs[i].setVariable(e.getOutputColumnNum());
      } else if (child instanceof ExprNodeFieldDesc) {
        // Get the GenericUDFStructField to process the field of Struct type
        VectorExpression e =
            getGenericUDFStructField(
                (ExprNodeFieldDesc) child, VectorExpressionDescriptor.Mode.PROJECTION,
                child.getTypeInfo());
        vectorExprs.add(e);
        variableArgPositions.add(i);
        exprResultColumnNums.add(e.getOutputColumnNum());
        argDescs[i].setVariable(e.getOutputColumnNum());
      } else {
        throw new HiveException("Unable to vectorize custom UDF. Encountered unsupported expr desc : "
            + child);
      }
    }

    // Allocate output column and get column number;
    TypeInfo resultTypeInfo = expr.getTypeInfo();
    String resultTypeName = resultTypeInfo.getTypeName();

    final int outputColumnNum = ocm.allocateOutputColumn(expr.getTypeInfo());

    // Make vectorized operator
    VectorUDFAdaptor ve = new VectorUDFAdaptor(expr, outputColumnNum, resultTypeName, argDescs);
    ve.setSuppressEvaluateExceptions(adaptorSuppressEvaluateExceptions);

    // Set child expressions
    VectorExpression[] childVEs = null;
    if (exprResultColumnNums.size() != 0) {
      childVEs = new VectorExpression[exprResultColumnNums.size()];
      for (int i = 0; i < childVEs.length; i++) {
        childVEs[i] = vectorExprs.get(i);
      }
    }
    ve.setChildExpressions(childVEs);

    ve.setInputTypeInfos(inputTypeInfos);
    ve.setInputDataTypePhysicalVariations(inputDataTypePhysicalVariations);

    ve.setOutputTypeInfo(resultTypeInfo);
    ve.setOutputDataTypePhysicalVariation(DataTypePhysicalVariation.NONE);

    // Free output columns if inputs have non-leaf expression trees.
    for (Integer i : exprResultColumnNums) {
      ocm.freeOutputColumn(i);
    }

    if (isFilter) {
      SelectColumnIsTrue filterVectorExpr = new SelectColumnIsTrue(outputColumnNum);

      filterVectorExpr.setChildExpressions(new VectorExpression[] {ve});

      filterVectorExpr.setInputTypeInfos(ve.getOutputTypeInfo());
      filterVectorExpr.setInputDataTypePhysicalVariations(ve.getOutputDataTypePhysicalVariation());

      return filterVectorExpr;
    } else {
      return ve;
    }
  }

  public static boolean isStringFamily(String resultType) {
    return resultType.equalsIgnoreCase("string") || charVarcharTypePattern.matcher(resultType).matches() ||
           resultType.equalsIgnoreCase("string_family");
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

  public static boolean isIntervalYearMonthFamily(String resultType) {
    return resultType.equalsIgnoreCase("interval_year_month");
  }

  public static boolean isIntervalDayTimeFamily(String resultType) {
    return resultType.equalsIgnoreCase("interval_day_time");
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
    String typeString = constDesc.getTypeString();
    if (typeString.equalsIgnoreCase("String")) {
      return ((String) constDesc.getValue()).getBytes(StandardCharsets.UTF_8);
    } else if (charTypePattern.matcher(typeString).matches()) {
      return ((HiveChar) constDesc.getValue()).getStrippedValue().getBytes(StandardCharsets.UTF_8);
    } else if (varcharTypePattern.matcher(typeString).matches()) {
      return ((HiveVarchar) constDesc.getValue()).getValue().getBytes(StandardCharsets.UTF_8);
    } else if (typeString.equalsIgnoreCase("boolean")) {
      if (constDesc.getValue().equals(Boolean.valueOf(true))) {
        return 1;
      } else {
        return 0;
      }
    } else if (decimalTypePattern.matcher(typeString).matches()) {
      return constDesc.getValue();
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
    TypeInfo typeInfo = constDesc.getTypeInfo();
    PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
    Object scalarValue = getScalarValue(constDesc);
    switch (primitiveCategory) {
      case DATE:
        return new Long(DateWritableV2.dateToDays((Date) scalarValue));
      case TIMESTAMP:
        return ((org.apache.hadoop.hive.common.type.Timestamp) scalarValue).toSqlTimestamp();
      case INTERVAL_YEAR_MONTH:
        return ((HiveIntervalYearMonth) scalarValue).getTotalMonths();
      default:
        return scalarValue;
    }
  }

  // Get a timestamp from a string constant or cast
  private Timestamp getTimestampScalar(ExprNodeDesc expr) throws HiveException {
    if (expr instanceof ExprNodeGenericFuncDesc &&
        ((ExprNodeGenericFuncDesc) expr).getGenericUDF() instanceof GenericUDFTimestamp) {
      return evaluateCastToTimestamp(expr);
    }
    if (!(expr instanceof ExprNodeConstantDesc)) {
      throw new HiveException("Constant timestamp value expected for expression argument. " +
          "Non-constant argument not supported for vectorization.");
    }
    ExprNodeConstantDesc constExpr = (ExprNodeConstantDesc) expr;
    String constTypeString = constExpr.getTypeString();
    if (isStringFamily(constTypeString) || isDatetimeFamily(constTypeString)) {

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
        + "Expecting string/date/timestamp.");
  }

  private Timestamp evaluateCastToTimestamp(ExprNodeDesc expr) throws HiveException {
    ExprNodeGenericFuncDesc expr2 = (ExprNodeGenericFuncDesc) expr;
    ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(expr2);
    ObjectInspector output = evaluator.initialize(null);
    Object constant = evaluator.evaluate(null);
    Object java = ObjectInspectorUtils.copyToStandardJavaObject(constant, output);

    if (!(java instanceof org.apache.hadoop.hive.common.type.Timestamp)) {
      throw new HiveException("Udf: failed to convert to timestamp");
    }
    Timestamp ts = ((org.apache.hadoop.hive.common.type.Timestamp) java).toSqlTimestamp();
    return ts;
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

  static String getScratchName(TypeInfo typeInfo) throws HiveException {
    // For now, leave DECIMAL precision/scale in the name so DecimalColumnVector scratch columns
    // don't need their precision/scale adjusted...
    if (typeInfo.getCategory() == Category.PRIMITIVE &&
        ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory() == PrimitiveCategory.DECIMAL) {
      return typeInfo.getTypeName();
    }

    // And, for Complex Types, also leave the children types in place...
    if (typeInfo.getCategory() != Category.PRIMITIVE) {
      return typeInfo.getTypeName();
    }

    Type columnVectorType = VectorizationContext.getColumnVectorTypeFromTypeInfo(typeInfo);
    return columnVectorType.name().toLowerCase();
  }

  static String getUndecoratedName(String hiveTypeName) throws HiveException {
    VectorExpressionDescriptor.ArgumentType argType = VectorExpressionDescriptor.ArgumentType.fromHiveTypeName(hiveTypeName);
    switch (argType) {
    case INT_FAMILY:
      return "Long";
    case FLOAT_FAMILY:
      return "Double";
    case DECIMAL:
      return "Decimal";
    case STRING:
      return "String";
    case CHAR:
      return "Char";
    case VARCHAR:
      return "VarChar";
    case BINARY:
      return "Binary";
    case DATE:
      return "Date";
    case TIMESTAMP:
      return "Timestamp";
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY_TIME:
      return hiveTypeName;
    case STRUCT:
      return "Struct";
    case LIST:
      return "List";
    case MAP:
      return "Map";
    default:
      throw new HiveException("Unexpected hive type name " + hiveTypeName);
    }
  }

  public static String mapTypeNameSynonyms(String typeName) {
    typeName = typeName.toLowerCase();
    if (typeName.equals("long")) {
      return "bigint";
    } else if (typeName.equals("string_family")) {
      return "string";
    } else {
      return typeName;
    }
  }

  public static ColumnVector.Type getColumnVectorTypeFromTypeInfo(TypeInfo typeInfo)
      throws HiveException {
    return getColumnVectorTypeFromTypeInfo(typeInfo, DataTypePhysicalVariation.NONE);
  }

  public static ColumnVector.Type getColumnVectorTypeFromTypeInfo(TypeInfo typeInfo,
      DataTypePhysicalVariation dataTypePhysicalVariation)
          throws HiveException {
    switch (typeInfo.getCategory()) {
      case STRUCT:
        return Type.STRUCT;
      case UNION:
        return Type.UNION;
      case LIST:
        return Type.LIST;
      case MAP:
        return Type.MAP;
      case PRIMITIVE: {
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();

        switch (primitiveCategory) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case DATE:
        case INTERVAL_YEAR_MONTH:
          return ColumnVector.Type.LONG;

        case TIMESTAMP:
          return ColumnVector.Type.TIMESTAMP;

        case INTERVAL_DAY_TIME:
          return ColumnVector.Type.INTERVAL_DAY_TIME;

        case FLOAT:
        case DOUBLE:
          return ColumnVector.Type.DOUBLE;

        case STRING:
        case CHAR:
        case VARCHAR:
        case BINARY:
          return ColumnVector.Type.BYTES;

        case DECIMAL:
          if (dataTypePhysicalVariation != null &&
              dataTypePhysicalVariation == DataTypePhysicalVariation.DECIMAL_64) {
            return ColumnVector.Type.DECIMAL_64;
          } else {
            return ColumnVector.Type.DECIMAL;
          }

        case VOID:
          return ColumnVector.Type.VOID;

        default:
          throw new HiveException("Unexpected primitive type category " + primitiveCategory);
        }
      }
      default:
        throw new HiveException("Unexpected type category " +
            typeInfo.getCategory());
    }
  }

  public int firstOutputColumnIndex() {
    return firstOutputColumnIndex;
  }

  public String[] getScratchColumnTypeNames() {
    String[] result = new String[ocm.outputColCount];
    for (int i = 0; i < ocm.outputColCount; i++) {
      String vectorTypeName = ocm.scratchVectorTypeNames[i];
      String typeName;
      if (vectorTypeName.equalsIgnoreCase("bytes")) {
        // Use hive type name.
        typeName = "string";
      } else if (vectorTypeName.equalsIgnoreCase("long")) {
        // Use hive type name.
        typeName = "bigint";
      } else {
        typeName = vectorTypeName;
      }
      result[i] =  typeName;
    }
    return result;
  }

  public DataTypePhysicalVariation[] getScratchDataTypePhysicalVariations() {
    return Arrays.copyOf(ocm.scratchDataTypePhysicalVariations, ocm.outputColCount);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(32);
    sb.append("Context name ").append(contextName).append(", level " + level + ", ");

    Comparator<Integer> comparerInteger = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
          return o1.compareTo(o2);
        }};

    Map<Integer, String> sortedColumnMap = new TreeMap<Integer, String>(comparerInteger);
    for (Map.Entry<String, Integer> entry : projectionColumnMap.entrySet()) {
      sortedColumnMap.put(entry.getValue(), entry.getKey());
    }
    sb.append("sorted projectionColumnMap ").append(sortedColumnMap).append(", ");

    sb.append("initial column names ").append(initialColumnNames.toString()).append(",");
    sb.append("initial type infos ").append(initialTypeInfos.toString()).append(", ");

    sb.append("scratchColumnTypeNames ").append(Arrays.toString(getScratchColumnTypeNames()));

    return sb.toString();
  }
}
