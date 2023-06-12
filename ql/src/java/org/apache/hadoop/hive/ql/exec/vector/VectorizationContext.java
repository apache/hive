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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.vector.expressions.BucketNumExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastBooleanToCharViaLongToChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastBooleanToStringViaLongToString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastBooleanToVarCharViaLongToVarChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastCharToBinary;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDateToChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDateToCharWithFormat;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDateToString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDateToStringWithFormat;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDateToVarChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDateToVarCharWithFormat;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToVarChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDoubleToChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDoubleToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDoubleToString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDoubleToVarChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastFloatToChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastFloatToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastFloatToString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastFloatToVarChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastLongToChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastLongToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastLongToDecimal64;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastLongToString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastLongToTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastLongToVarChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastMillisecondsLongToTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringGroupToChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringGroupToVarChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToBoolean;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToDateWithFormat;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToTimestampWithFormat;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToCharWithFormat;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToStringWithFormat;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToVarChar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToVarCharWithFormat;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConvertDecimal64ToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.Decimal64ColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.DecimalColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.DoubleColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.DynamicValueVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterConstantBooleanVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterDecimal64ColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterDecimal64ColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterDecimal64ColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterDecimalColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterDoubleColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprAndExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprOrExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterLongColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterStringColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterStructColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterTimestampColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.GroupingColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.GroupingColumns;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IDecimalInExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IDoubleInExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ILongInExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IStringInExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IStructInExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ITimestampInExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprColumnCondExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprColumnNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprCondExprColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprCondExprCondExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprCondExprNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprNullColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprNullCondExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprNullNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsTrue;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StructColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.TimestampColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.TruncStringOutput;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorCoalesce;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorElt;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastLongToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastLongToFloatViaLongToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CharColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CharColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.Decimal64ColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.Decimal64ColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterCharColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterCharColumnBetweenDynamicValue;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterCharColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDateColumnBetweenDynamicValue;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDecimalColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDecimalColumnBetweenDynamicValue;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDecimalColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColumnBetweenDynamicValue;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColumnBetweenDynamicValue;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColumnBetweenDynamicValue;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterTimestampColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterTimestampColumnBetweenDynamicValue;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterTimestampColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterVarCharColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterVarCharColumnBetweenDynamicValue;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterVarCharColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.TimestampColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.TimestampColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.VarCharColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.VarCharColumnNotBetween;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
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

/**
 * Context class for vectorization execution.
 * Main role is to map column names to column indices and serves as a
 * factory class for building vectorized expressions out of descriptors.
 *
 */
@SuppressWarnings("ALL") public class VectorizationContext {

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

    projectedColumns = new ArrayList<>();
    projectionColumnMap = new HashMap<>();
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
    this.tryDecimal64Cast = false;
  }

  // Convenient constructor for initial batch creation takes
  // a list of columns names and maps them to 0..n-1 indices.
  public VectorizationContext(String contextName, List<String> initialColumnNames,
      HiveConf hiveConf) {
    this.contextName = contextName;
    level = 0;
    this.initialColumnNames = initialColumnNames;
    this.projectionColumnNames = initialColumnNames;

    projectedColumns = new ArrayList<>();
    projectionColumnMap = new HashMap<>();
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
    this.tryDecimal64Cast = false;
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
    initialColumnNames = new ArrayList<>();
    projectedColumns = new ArrayList<>();
    projectionColumnNames = new ArrayList<>();
    projectionColumnMap = new HashMap<>();
    this.ocm = new OutputColumnManager(0);
    this.tryDecimal64Cast = false;
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
    this.projectedColumns = new ArrayList<>();
    this.projectionColumnNames = new ArrayList<>();
    this.projectionColumnMap = new HashMap<>();

    this.ocm = vContext.ocm;
    this.tryDecimal64Cast = false;
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
    projectedColumns = new ArrayList<>();
    projectionColumnNames = new ArrayList<>();
    projectionColumnMap = new HashMap<>();
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
    initialDataTypePhysicalVariations = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      initialDataTypePhysicalVariations.add(DataTypePhysicalVariation.NONE);
    }
  }

  public void setInitialDataTypePhysicalVariations(
      List<DataTypePhysicalVariation> initialDataTypePhysicalVariations) {
    this.initialDataTypePhysicalVariations = initialDataTypePhysicalVariations;
  }

  @SuppressWarnings("unused") public List<String> getInitialColumnNames() {
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

      return TypeInfoUtils.getTypeInfoFromTypeString(typeName);
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

  public DataTypePhysicalVariation[] getAllDataTypePhysicalVariations() throws HiveException {
    final int size = initialTypeInfos.size() + ocm.outputColCount;

    DataTypePhysicalVariation[] result = new DataTypePhysicalVariation[size];
    for (int i = 0; i < size; i++) {
      result[i] = getDataTypePhysicalVariation(i);
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

  //Can cast to decimal64
  boolean tryDecimal64Cast;

  // Set of UDF classes for type casting data types in row-mode.
  private static final Set<Class<?>> castExpressionUdfs = new HashSet<>();
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
  private static final Set<Class<?>> udfsNeedingImplicitDecimalCast = new HashSet<>();
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
    udfsNeedingImplicitDecimalCast.add(GenericUDFWhen.class);
    udfsNeedingImplicitDecimalCast.add(UDFSqrt.class);
    udfsNeedingImplicitDecimalCast.add(UDFRand.class);
    udfsNeedingImplicitDecimalCast.add(UDFLn.class);
    udfsNeedingImplicitDecimalCast.add(UDFLog2.class);
    udfsNeedingImplicitDecimalCast.add(UDFSin.class);
    udfsNeedingImplicitDecimalCast.add(UDFAsin.class);
    udfsNeedingImplicitDecimalCast.add(UDFSinh.class);
    udfsNeedingImplicitDecimalCast.add(UDFCos.class);
    udfsNeedingImplicitDecimalCast.add(UDFCosh.class);
    udfsNeedingImplicitDecimalCast.add(UDFAcos.class);
    udfsNeedingImplicitDecimalCast.add(UDFLog10.class);
    udfsNeedingImplicitDecimalCast.add(UDFLog.class);
    udfsNeedingImplicitDecimalCast.add(UDFExp.class);
    udfsNeedingImplicitDecimalCast.add(UDFDegrees.class);
    udfsNeedingImplicitDecimalCast.add(UDFRadians.class);
    udfsNeedingImplicitDecimalCast.add(UDFAtan.class);
    udfsNeedingImplicitDecimalCast.add(UDFTan.class);
    udfsNeedingImplicitDecimalCast.add(UDFTanh.class);
    udfsNeedingImplicitDecimalCast.add(UDFOPLongDivide.class);
  }

  private static final long[] POWEROFTENTABLE = {
      1L,                   // 0
      10L,
      100L,
      1_000L,
      10_000L,
      100_000L,
      1_000_000L,
      10_000_000L,
      100_000_000L,           // 8
      1_000_000_000L,
      10_000_000_000L,
      100_000_000_000L,
      1_000_000_000_000L,
      10_000_000_000_000L,
      100_000_000_000_000L,
      1_000_000_000_000_000L,
      10_000_000_000_000_000L,   // 16
      100_000_000_000_000_000L,
      1_000_000_000_000_000_000L, // 18
  };

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
    private boolean[] scratchColumnTrackWasUsed = new boolean[100];

    private final Set<Integer> usedOutputColumns = new HashSet<>();
    private boolean[] markedScratchColumns;

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
        if (scratchColumnTrackWasUsed[i]) {
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
        scratchDataTypePhysicalVariations[outputColCount] = dataTypePhysicalVariation;
        scratchColumnTrackWasUsed[outputColCount++] = true;
        usedOutputColumns.add(newIndex);
        return newIndex;
      } else {
        //Expand the array
        scratchVectorTypeNames = Arrays.copyOf(scratchVectorTypeNames, 2*outputColCount);
        scratchDataTypePhysicalVariations = Arrays.copyOf(scratchDataTypePhysicalVariations, 2*outputColCount);
        scratchColumnTrackWasUsed = Arrays.copyOf(scratchColumnTrackWasUsed, 2*outputColCount);
        int newIndex = outputColCount;
        scratchVectorTypeNames[outputColCount] = columnType;
        scratchDataTypePhysicalVariations[outputColCount] = dataTypePhysicalVariation;
        scratchColumnTrackWasUsed[outputColCount++] = true;
        usedOutputColumns.add(newIndex);
        return newIndex;
      }
    }

    void freeOutputColumn(int index) {
      if (initialOutputCol < 0 || !reuseScratchColumns) {
        // This is a test
        return;
      }
      int colIndex = index-initialOutputCol;
      if (colIndex >= 0) {
        usedOutputColumns.remove(index-initialOutputCol);
      }
    }

    public int[] currentScratchColumns() {
      TreeSet<Integer> treeSet = new TreeSet<>();
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

    public void freeMarkedScratchColumns() {
      if (markedScratchColumns == null) {
        throw new RuntimeException("Illegal call");
      }
      for (int i = 0; i < markedScratchColumns.length; i++) {
        if (markedScratchColumns[i]) {
          scratchColumnTrackWasUsed[i] = false;
        }
      }
      markedScratchColumns = null;
    }

    public void markScratchColumns() {
      if (markedScratchColumns != null) {
        throw new RuntimeException("Illegal call");
      }
      markedScratchColumns = Arrays.copyOf(scratchColumnTrackWasUsed, scratchColumnTrackWasUsed.length);
    }

    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append(String.format(
          "OutputColumnManager: initialOutputCol: %d, outputColCount: %d, usedOutputColumns#: %d"
              + ", reuseScratchColumns: %s, output cols:",
          initialOutputCol, outputColCount, usedOutputColumns.size(), reuseScratchColumns));
      for (int i = 0; i < outputColCount; i++) {
        builder.append(String.format(
            "\n%d (%d): used: %s, scratchVectorTypeName: %s" + ", physicalVariation: %s, trackWasUsed: %s", i,
            i + initialOutputCol, usedOutputColumns.contains(i), scratchVectorTypeNames[i],
            scratchDataTypePhysicalVariations[i], scratchColumnTrackWasUsed[i]));
      }
      return builder.toString();
    }
  }

  public int allocateScratchColumn(TypeInfo typeInfo) throws HiveException {
    return ocm.allocateOutputColumn(typeInfo);
  }

  public int[] currentScratchColumns() {
    return ocm.currentScratchColumns();
  }

  /**
   * Marks all actual scratch columns.
   *
   * They can be decomissioned with {@link #freeMarkedScratchColumns()}.
   */
  public void markActualScratchColumns() {
    ocm.markScratchColumns();
  }

  /**
   * Frees up actually marked scract columns.
   */
  public void freeMarkedScratchColumns() {
    ocm.freeMarkedScratchColumns();
  }

  private VectorExpression getFilterOnBooleanColumnExpression(ExprNodeColumnDesc exprDesc,
      int columnNum) throws HiveException {
    final VectorExpression expr;

    // Evaluate the column as a boolean, converting if necessary.
    TypeInfo typeInfo = exprDesc.getTypeInfo();
    if (typeInfo.getCategory() == Category.PRIMITIVE &&
        ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory() == PrimitiveCategory.BOOLEAN) {
      expr = new SelectColumnIsTrue(columnNum);

      expr.setInputTypeInfos(typeInfo);
      expr.setInputDataTypePhysicalVariations(DataTypePhysicalVariation.NONE);

    } else {
      // Ok, we need to convert.
      List<ExprNodeDesc> exprAsList = Collections.singletonList(exprDesc);
      expr = getCastToBooleanExpression(exprAsList, VectorExpressionDescriptor.Mode.FILTER);
      if (expr == null) {
        throw new HiveException("Cannot vectorize converting expression " +
            exprDesc.getExprString() + " to boolean");
      }
    }
    return expr;
  }

  private VectorExpression getColumnVectorExpression(ExprNodeColumnDesc exprDesc,
      VectorExpressionDescriptor.Mode mode) throws HiveException {
    int columnNum = getInputColumnIndex(exprDesc.getColumn());
    VectorExpression expr;
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

    List<ExprNodeDesc> childrenWithCasts = new ArrayList<>();
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
    } else if (genericUDF instanceof GenericUDFIf) {
      int i = 0;
      for (ExprNodeDesc child : children) {
        if (i++ == 0) {

          // Skip boolean predicate.
          childrenWithCasts.add(child);
          continue;
        }
        if (child instanceof ExprNodeConstantDesc &&
            ((ExprNodeConstantDesc) child).getValue() == null) {

          // Don't cast NULL using ConstantValueExpression but replace the NULL expression with the
          // desired type set. This lets the doGetIfExpression logic use IfExprCondExprNull, etc.
          childrenWithCasts.add(new ExprNodeConstantDesc(commonType, null));
          atleastOneCastNeeded = true;
          continue;
        }
        ExprNodeDesc castExpression = getImplicitCastExpression(genericUDF, child, commonType);
        if (castExpression != null) {
          atleastOneCastNeeded = true;
          childrenWithCasts.add(castExpression);
        } else {
          childrenWithCasts.add(child);
        }
      }
    } else if(genericUDF instanceof GenericUDFWhen) {
      boolean hasElseClause = children.size() % 2 == 1 ;
      for (int i=0; i<children.size(); i++) {
        ExprNodeDesc castExpression = null;
        if (i % 2 == 1) {
          castExpression = getImplicitCastExpression(genericUDF, children.get(i), commonType);
        }
        if(hasElseClause && i == children.size()-1) {
          castExpression = getImplicitCastExpression(genericUDF, children.get(i), commonType);
        }
        if (castExpression != null) {
          atleastOneCastNeeded = true;
          childrenWithCasts.add(castExpression);
        } else {
          childrenWithCasts.add(children.get(i));
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
      return true;
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
        List<ExprNodeDesc> children = new ArrayList<>();
        children.add(child);
        return new ExprNodeGenericFuncDesc(castType, castToDecimalUDF, children);
      }
    } else if (!castTypeDecimal && inputTypeDecimal) {
      if (needsImplicitCastForDecimal(udf)) {
        // Cast decimal input to returnType
        GenericUDF genericUdf = getGenericUDFForCast(castType);
        List<ExprNodeDesc> children = new ArrayList<>();
        children.add(child);
        return new ExprNodeGenericFuncDesc(castType, genericUdf, children);
      }
    } else {

      // Casts to exact types including long to double etc. are needed in some special cases.
      if (udf instanceof GenericUDFCoalesce || udf instanceof GenericUDFElt
          || udf instanceof GenericUDFIf) {
        GenericUDF genericUdf = getGenericUDFForCast(castType);
        List<ExprNodeDesc> children = new ArrayList<>();
        children.add(child);
        return new ExprNodeGenericFuncDesc(castType, genericUdf, children);
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
      return udfClass.equals(UDFHex.class)
          || udfClass.equals(UDFRegExpExtract.class)
          || udfClass.equals(UDFRegExpReplace.class)
          || udfClass.equals(UDFConv.class)
          || isCastToIntFamily(udfClass) && isStringFamily(arg0Type(expr))
          || isCastToFloatFamily(udfClass) && isStringFamily(arg0Type(expr));
    } else if (gudf instanceof GenericUDFFromUnixTime && isIntFamily(arg0Type(expr))
          || (gudf instanceof GenericUDFTimestamp && isStringFamily(arg0Type(expr)))

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
    } else // between has 4 args here, but can be vectorized like this
      if ((gudf instanceof GenericUDFToString
                   || gudf instanceof GenericUDFToChar
                   || gudf instanceof GenericUDFToVarchar) &&
               (arg0Type(expr).equals("timestamp")
                   || arg0Type(expr).equals("double")
                   || arg0Type(expr).equals("float"))) {
      return true;
    } else if (gudf instanceof GenericUDFBetween && (mode == VectorExpressionDescriptor.Mode.PROJECTION)) {
      return true;
    } else if (gudf instanceof GenericUDFConcat && (mode == VectorExpressionDescriptor.Mode.PROJECTION)) {
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

  public static boolean isCastToBoolean(Class<? extends UDF> udfClass) {
    return udfClass.equals(UDFToBoolean.class);
  }

  public static boolean isCastToFloatFamily(Class<? extends UDF> udfClass) {
    return udfClass.equals(UDFToDouble.class)
        || udfClass.equals(UDFToFloat.class);
  }

  // Return the type string of the first argument (argument 0).
  public static String arg0Type(ExprNodeGenericFuncDesc expr) {
    return expr.getChildren().get(0).getTypeString();
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
    List<ExprNodeDesc> evaluatedChildren = new ArrayList<>();
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
      if (typeInfo.getCategory() != Category.PRIMITIVE) {
        throw new HiveException("Complex type constants (" + typeInfo.getCategory() + ") not supported for type name " + typeName);
      }
      if (mode == VectorExpressionDescriptor.Mode.FILTER) {
        return new FilterConstantBooleanVectorExpression(0);
      } else {
        return new ConstantVectorExpression(outCol, typeInfo, true);
      }
    }

    // Boolean is special case.
    if (typeName.equalsIgnoreCase("boolean")) {
      if (mode == VectorExpressionDescriptor.Mode.FILTER) {
        if ((Boolean) constantValue) {
          return new FilterConstantBooleanVectorExpression(1);
        } else {
          return new FilterConstantBooleanVectorExpression(0);
        }
      } else {
        if ((Boolean) constantValue) {
          return new ConstantVectorExpression(outCol, 1, typeInfo);
        } else {
          return new ConstantVectorExpression(outCol, 0, typeInfo);
        }
      }
    }

    return ConstantVectorExpression.create(outCol, constantValue, typeInfo);
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

    if (childExprList.size() != 1) {
      return null;
    }
    ExprNodeDesc childExpr = childExprList.get(0);
    if (!(childExpr instanceof ExprNodeColumnDesc)) {

      // Some vector operators like VectorSelectOperator optimize out IdentityExpression out of
      // their vector expression list and don't evaluate the children, so just return the
      // child expression here instead of IdentityExpression.
      return getVectorExpression(childExpr);
    }

    int identityCol;
    TypeInfo identityTypeInfo;
    DataTypePhysicalVariation identityDataTypePhysicalVariation;

    ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) childExpr;
    identityCol = getInputColumnIndex(colDesc.getColumn());
    identityTypeInfo = colDesc.getTypeInfo();

    identityDataTypePhysicalVariation = getDataTypePhysicalVariation(identityCol);

    VectorExpression ve = new IdentityExpression(identityCol);

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
      if (udf instanceof GenericUDFToDecimal) {
        return true;
      }
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

  private VectorExpression getDecimal64VectorExpressionForUdf(GenericUDF genericUdf,
      Class<?> udfClass, List<ExprNodeDesc> childExprs, int numChildren,
      VectorExpressionDescriptor.Mode mode, TypeInfo returnTypeInfo) throws HiveException {

    VectorExpressionDescriptor.Builder builder = new VectorExpressionDescriptor.Builder();
    builder.setNumArguments(numChildren);
    builder.setMode(mode);

    // DECIMAL_64 decimals must have same scale.
    boolean anyDecimal64Expr = false;
    boolean isDecimal64ScaleEstablished = false;
    int decimal64ColumnScale = 0;
    boolean hasConstants = false;
    boolean scaleMismatch = false;

    for (int i = 0; i < numChildren; i++) {
      ExprNodeDesc childExpr = childExprs.get(i);

      /*
       * For columns, we check decimal columns for DECIMAL_64 DataTypePhysicalVariation.
       * For UDFs, we check for @VectorizedExpressionsSupportDecimal64 annotation, etc.
       */
      final boolean isExprDecimal64 = checkExprNodeDescForDecimal64(childExpr);
      if (isExprDecimal64) {
        anyDecimal64Expr = true;
      }

      TypeInfo typeInfo = childExpr.getTypeInfo();
      if (childExpr instanceof ExprNodeGenericFuncDesc ||
          childExpr instanceof ExprNodeColumnDesc) {
        if (isExprDecimal64) {
          DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
          if (decimalTypeInfo.getScale() != decimal64ColumnScale && i > 0) {
            scaleMismatch = true;
          }
          if (decimalTypeInfo.getScale() >= decimal64ColumnScale) {
            decimal64ColumnScale = decimalTypeInfo.getScale();
            isDecimal64ScaleEstablished = true;
          }
        }
        builder.setInputExpressionType(i, InputExpressionType.COLUMN);
      } else if (childExpr instanceof ExprNodeConstantDesc) {
        hasConstants = true;
        if (isNullConst(childExpr)) {
          // Cannot handle NULL scalar parameter.
          return null;
        }
        builder.setInputExpressionType(i, InputExpressionType.SCALAR);
      } else {
        return null;
      }

      if (isExprDecimal64) {
        builder.setArgumentType(i, ArgumentType.DECIMAL_64);
      } else {
        String undecoratedTypeName = getUndecoratedName(childExpr.getTypeString());
        if (undecoratedTypeName == null) {
          return null;
        }
        builder.setArgumentType(i, undecoratedTypeName);
      }
    }

    if (!anyDecimal64Expr) {
      return null;
    }

    final boolean isReturnDecimal64 = checkTypeInfoForDecimal64(returnTypeInfo);
    final DataTypePhysicalVariation returnDataTypePhysicalVariation;
    final boolean dontRescaleArguments = (genericUdf instanceof GenericUDFOPMultiply);
    if (isReturnDecimal64) {
      DecimalTypeInfo returnDecimalTypeInfo = (DecimalTypeInfo) returnTypeInfo;
      if (!isDecimal64ScaleEstablished) {
        decimal64ColumnScale = returnDecimalTypeInfo.getScale();
        isDecimal64ScaleEstablished = true;
      } else if (genericUdf instanceof GenericUDFOPDivide) {
        // Check possible overflow during decimal64 division for intermediate result
        // if yes then skip the optimization
        DecimalTypeInfo leftType = (DecimalTypeInfo)childExprs.get(0).getTypeInfo();
        if((leftType.precision() + returnDecimalTypeInfo.getScale()) > 18) {
          return null;
        }
      } else if (returnDecimalTypeInfo.getScale() != decimal64ColumnScale && !dontRescaleArguments) {
        return null;
      }
      returnDataTypePhysicalVariation = DataTypePhysicalVariation.DECIMAL_64;
    } else if (returnTypeInfo instanceof DecimalTypeInfo){

      // Currently, we don't have any vectorized expressions that take DECIMAL_64 inputs
      // and produce a regular decimal.  Or, currently, a way to express that in the
      // descriptor.
      return null;
    } else {
      returnDataTypePhysicalVariation = DataTypePhysicalVariation.NONE;
    }

    if (dontRescaleArguments && hasConstants) {
      builder.setUnscaled(true);
    }
    VectorExpressionDescriptor.Descriptor descriptor = builder.build();
    Class<?> vectorClass =
        this.vMap.getVectorExpressionClass(udfClass, descriptor, useCheckedVectorExpressions);
    if (vectorClass == null) {
      return null;
    }

    VectorExpressionDescriptor.Mode childrenMode = getChildrenMode(mode, udfClass);

    // Rewrite the operand with smaller scale, so we can process Decimal64 operations
    // on all the arithmetic and comparison operations.
    if (scaleMismatch && !hasConstants &&
        (genericUdf instanceof GenericUDFBaseArithmetic
        || genericUdf instanceof GenericUDFBaseCompare)) {
      ExprNodeDesc left = childExprs.get(0);
      ExprNodeDesc right = childExprs.get(1);
      DecimalTypeInfo leftTypeInfo = (DecimalTypeInfo)left.getTypeInfo();
      DecimalTypeInfo rightTypeInfo = (DecimalTypeInfo)right.getTypeInfo();
      int leftScale = leftTypeInfo.getScale();
      int rightScale = rightTypeInfo.getScale();
      int leftPrecision = leftTypeInfo.precision();
      int rightPrecision = rightTypeInfo.precision();
      ExprNodeDesc newConstant;
      List<ExprNodeDesc> children = new ArrayList<>();
      DecimalTypeInfo resultTypeInfo;
      int childIndexToRewrite = -1;
      int scaleDiff = 0;
      int resultPrecision = 0;
      int resultScale = 0;
      if (leftScale < rightScale) {
        scaleDiff = rightScale - leftScale;
        childIndexToRewrite = 0;
        resultPrecision = leftPrecision + rightScale - leftScale;
        resultScale = rightScale;
      } else {
        scaleDiff = leftScale - rightScale;
        childIndexToRewrite = 1;
        resultPrecision = rightPrecision + leftScale - rightScale;
        resultScale = leftScale;
      }
      newConstant = new ExprNodeConstantDesc(new DecimalTypeInfo(scaleDiff, 0),
          HiveDecimal.create(POWEROFTENTABLE[scaleDiff]));
      resultTypeInfo = new DecimalTypeInfo(resultPrecision, resultScale);
      children.add(childExprs.get(childIndexToRewrite));
      children.add(newConstant);
      ExprNodeGenericFuncDesc newScaledExpr = new ExprNodeGenericFuncDesc(resultTypeInfo,
          new GenericUDFOPScaleUpDecimal64(), " ScaleUp ", children);
      childExprs.remove(childIndexToRewrite);
      childExprs.add(childIndexToRewrite, newScaledExpr);
    }

    return createDecimal64VectorExpression(
        vectorClass, childExprs, childrenMode,
        isDecimal64ScaleEstablished, decimal64ColumnScale,
        returnTypeInfo, returnDataTypePhysicalVariation, dontRescaleArguments, genericUdf);
  }

  @SuppressWarnings("null")
  private VectorExpression createDecimal64VectorExpression(Class<?> vectorClass,
      List<ExprNodeDesc> childExprs, VectorExpressionDescriptor.Mode childrenMode,
      boolean isDecimal64ScaleEstablished, int decimal64ColumnScale,
      TypeInfo returnTypeInfo, DataTypePhysicalVariation returnDataTypePhysicalVariation,
      boolean dontRescaleArguments, GenericUDF genericUdf)
          throws HiveException {

    final int numChildren = childExprs.size();
    VectorExpression vectorExpression = null;
    boolean oldTryDecimal64Cast = this.tryDecimal64Cast;
    tryDecimal64Cast = true;

    /*
     * Custom build arguments.
     */

    try {
      List<VectorExpression> children = new ArrayList<>();
      Object[] arguments = new Object[numChildren];
      TypeInfo[] typeInfos = new TypeInfo[numChildren];
      DataTypePhysicalVariation[] dataTypePhysicalVariations = new DataTypePhysicalVariation[numChildren];

      for (int i = 0; i < numChildren; i++) {
        ExprNodeDesc childExpr = childExprs.get(i);
        TypeInfo typeInfo = childExpr.getTypeInfo();
        typeInfos[i] = typeInfo;
        dataTypePhysicalVariations[i] =
            (checkTypeInfoForDecimal64(typeInfo) ?
                DataTypePhysicalVariation.DECIMAL_64 : DataTypePhysicalVariation.NONE);
        if (childExpr instanceof ExprNodeGenericFuncDesc) {
          VectorExpression vChild = getVectorExpression(childExpr, childrenMode);
          if (genericUdf instanceof GenericUDFBaseBinary
            && vChild.getOutputDataTypePhysicalVariation() == DataTypePhysicalVariation.NONE) {
            return null;
          }
          children.add(vChild);
          arguments[i] = vChild.getOutputColumnNum();
        } else if (childExpr instanceof ExprNodeColumnDesc) {
          int colIndex = getInputColumnIndex((ExprNodeColumnDesc) childExpr);
          if (childrenMode == VectorExpressionDescriptor.Mode.FILTER) {

            VectorExpression filterExpr = getFilterOnBooleanColumnExpression((ExprNodeColumnDesc) childExpr, colIndex);
            if (filterExpr == null) {
              return null;
            }

            children.add(filterExpr);
          }
          arguments[i] = colIndex;
        } else if (childExpr instanceof ExprNodeConstantDesc) {
          ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) childExpr;
          if (typeInfo instanceof DecimalTypeInfo) {
            if (!isDecimal64ScaleEstablished) {
              return null;
            }
            HiveDecimal hiveDecimal = (HiveDecimal) constDesc.getValue();
            if (hiveDecimal.scale() > decimal64ColumnScale) {

              // For now, bail out on decimal constants with larger scale than column scale.
              return null;
            }
            if (dontRescaleArguments) {
              arguments[i] = new HiveDecimalWritable(hiveDecimal).serialize64(hiveDecimal.scale());
            } else {
              arguments[i] = new HiveDecimalWritable(hiveDecimal).serialize64(decimal64ColumnScale);
            }
          } else {
            Object scalarValue = getVectorTypeScalarValue(constDesc);
            arguments[i] = (scalarValue == null) ? getConstantVectorExpression(null, typeInfo, childrenMode) : scalarValue;
          }
        } else {
          return null;
        }
      }

      /*
       * Instantiate Decimal64 vector expression.
       *
       * The instantiateExpression method sets the output column and type information.
       */
      vectorExpression =
          instantiateExpression(vectorClass, returnTypeInfo, returnDataTypePhysicalVariation, arguments);
      if (vectorExpression == null) {
        handleCouldNotInstantiateVectorExpression(vectorClass, returnTypeInfo, returnDataTypePhysicalVariation,
            arguments);
      }

      Objects.requireNonNull(vectorExpression).setInputTypeInfos(typeInfos);
      vectorExpression.setInputDataTypePhysicalVariations(dataTypePhysicalVariations);

      if (!children.isEmpty()) {
        vectorExpression.setChildExpressions(children.toArray(new VectorExpression[0]));
      }
    } finally {
      tryDecimal64Cast = oldTryDecimal64Cast;
    }

    return vectorExpression;
  }

  private VectorExpression getVectorExpressionForUdf(GenericUDF genericUdf,
      Class<?> udfClass, List<ExprNodeDesc> childExpr, VectorExpressionDescriptor.Mode mode,
      TypeInfo returnType) throws HiveException {

    int numChildren = (childExpr == null) ? 0 : childExpr.size();

    if (numChildren > 2 && mode == VectorExpressionDescriptor.Mode.FILTER && ((genericUdf instanceof GenericUDFOPOr)
        || (genericUdf instanceof GenericUDFOPAnd))) {

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
      } else {
        vclass = FilterExprAndExpr.class;
      }
      VectorExpressionDescriptor.Mode childrenMode = getChildrenMode(mode, udfClass);
      return createVectorExpression(vclass, childExpr, childrenMode, returnType, DataTypePhysicalVariation.NONE);
    }
    if (numChildren > VectorExpressionDescriptor.MAX_NUM_ARGUMENTS) {
      return null;
    }

    // Intercept here for a possible Decimal64 vector expression class.
    VectorExpression result = getDecimal64VectorExpressionForUdf(genericUdf, udfClass,
        childExpr, numChildren, mode, returnType);
    if (result != null) {
      return result;
    }

    // Otherwise, fall through and proceed with non-Decimal64 vector expression classes...

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
          builder.setInputExpressionType(i, InputExpressionType.NULLSCALAR);
        }else {
          builder.setInputExpressionType(i, InputExpressionType.SCALAR);
        }
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
    return createVectorExpression(vclass, childExpr, childrenMode, returnType, DataTypePhysicalVariation.NONE);
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

    Objects.requireNonNull(vectorExpression).setInputTypeInfos(resultTypeInfo);
    vectorExpression.setInputDataTypePhysicalVariations(DataTypePhysicalVariation.DECIMAL_64);

    return vectorExpression;
  }

  public void wrapWithDecimal64ToDecimalConversions(VectorExpression[] vecExprs)
      throws HiveException{
    if (vecExprs == null) {
      return;
    }
    final int size = vecExprs.length;
    for (int i = 0; i < size; i++) {
      VectorExpression vecExpr = vecExprs[i];
      if (vecExpr.getOutputTypeInfo() instanceof DecimalTypeInfo) {
        DataTypePhysicalVariation outputDataTypePhysicalVariation =
            vecExpr.getOutputDataTypePhysicalVariation();
        if (outputDataTypePhysicalVariation == DataTypePhysicalVariation.DECIMAL_64) {
          vecExprs[i] =
              wrapWithDecimal64ToDecimalConversion(vecExpr);
        }
      }
    }
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
      List<ExprNodeDesc> childExpr, VectorExpressionDescriptor.Mode childrenMode, TypeInfo returnType,
      DataTypePhysicalVariation returnDataTypePhysicalVariation) throws HiveException {
    int numChildren = childExpr == null ? 0: childExpr.size();

    TypeInfo[] inputTypeInfos = new TypeInfo[numChildren];
    DataTypePhysicalVariation[] inputDataTypePhysicalVariations = new DataTypePhysicalVariation[numChildren];

    List<VectorExpression> children = new ArrayList<>();
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
    VectorExpression vectorExpression = instantiateExpression(vectorClass, returnType, returnDataTypePhysicalVariation,
        arguments);
    if (vectorExpression == null) {
      handleCouldNotInstantiateVectorExpression(vectorClass, returnType, DataTypePhysicalVariation.NONE, arguments);
    }

    Objects.requireNonNull(vectorExpression).setInputTypeInfos(inputTypeInfos);
    vectorExpression.setInputDataTypePhysicalVariations(inputDataTypePhysicalVariations);

    if (!children.isEmpty()) {
      vectorExpression.setChildExpressions(children.toArray(new VectorExpression[0]));
    }

    freeNonColumns(children.toArray(new VectorExpression[0]));

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
    ArrayList<String> argClasses = new ArrayList<>();
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

    return sb.toString().replaceAll("GeneratedConstructorAccessor[0-9]*", "GeneratedConstructorAccessor<omitted>");
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

        // Special handling for decimal because decimal types need scale and precision parameter.
        // This special handling should be avoided by using returnType uniformly for all cases.
        final int outputColumnNum =
            ocm.allocateOutputColumn(returnTypeInfo, returnDataTypePhysicalVariation);

        newArgs = Arrays.copyOf(Objects.requireNonNull(args), numParams);
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

  // Handle strange case of TO_DATE(date) or CAST(date to DATE)
  private VectorExpression getIdentityForDateToDate(List<ExprNodeDesc> childExprs,
      TypeInfo returnTypeInfo)
          throws HiveException {
    if (childExprs.size() != 1) {
      return null;
    }
    TypeInfo childTypeInfo = childExprs.get(0).getTypeInfo();
    if (childTypeInfo.getCategory() != Category.PRIMITIVE ||
        ((PrimitiveTypeInfo) childTypeInfo).getPrimitiveCategory() != PrimitiveCategory.DATE) {
      return null;
    }
    if (returnTypeInfo.getCategory() != Category.PRIMITIVE ||
        ((PrimitiveTypeInfo) returnTypeInfo).getPrimitiveCategory() != PrimitiveCategory.DATE) {
      return null;
    }
    return getIdentityExpression(childExprs);
  }

  private VectorExpression getGenericUdfVectorExpression(GenericUDF udf,
      List<ExprNodeDesc> childExpr, VectorExpressionDescriptor.Mode mode, TypeInfo returnType)
          throws HiveException {

    List<ExprNodeDesc> castedChildren = evaluateCastOnConstants(childExpr);
    childExpr = castedChildren;

    //First handle special cases.  If one of the special case methods cannot handle it,
    // it returns null.
    VectorExpression ve = null;
    if (udf instanceof GenericUDFBetween) {
      ve = getBetweenExpression(childExpr, mode, returnType);
    } else if (udf instanceof GenericUDFIn) {
      ve = getInExpression(childExpr, mode, returnType);
    } else if (udf instanceof GenericUDFIf) {
      ve = getIfExpression((GenericUDFIf) udf, childExpr, mode, returnType);
    } else if (udf instanceof GenericUDFWhen) {
      ve = getWhenExpression(childExpr, mode, returnType);
    } else if (udf instanceof GenericUDFOPPositive) {
      ve = getIdentityExpression(childExpr);
    } else if (udf instanceof GenericUDFCoalesce) {
      ve = getCoalesceExpression(childExpr, mode, returnType);
    } else if (udf instanceof GenericUDFElt) {

      // Elt is a special case because it can take variable number of arguments.
      ve = getEltExpression(childExpr, returnType);
    } else if (udf instanceof GenericUDFGrouping) {
      ve = getGroupingExpression((GenericUDFGrouping) udf, childExpr, returnType);
    } else if (udf instanceof GenericUDFBridge) {
      ve = getGenericUDFBridgeVectorExpression((GenericUDFBridge) udf, childExpr, mode,
          returnType);
    } else if (udf instanceof GenericUDFToString) {
      ve = getCastToString(childExpr, returnType);
    } else if (udf instanceof GenericUDFToDecimal) {
      ve = getCastToDecimal(childExpr, mode, returnType);
    } else if (udf instanceof GenericUDFToChar) {
      ve = getCastToChar(childExpr, returnType);
    } else if (udf instanceof GenericUDFToVarchar) {
      ve = getCastToVarChar(childExpr, returnType);
    } else if (udf instanceof GenericUDFToBinary) {
      ve = getCastToBinary(childExpr, returnType);
    } else if (udf instanceof GenericUDFTimestamp) {
      ve = getCastToTimestamp((GenericUDFTimestamp)udf, childExpr, mode, returnType);
    } else if (udf instanceof GenericUDFDate || udf instanceof GenericUDFToDate) {
      ve = getIdentityForDateToDate(childExpr, returnType);
    } else if (udf instanceof GenericUDFBucketNumber) {
      int outCol = ocm.allocateOutputColumn(returnType);
      ve = new BucketNumExpression(outCol);
      ve.setInputTypeInfos(returnType);
      ve.setOutputTypeInfo(returnType);
    } else if (udf instanceof GenericUDFCastFormat) {
      ve = getCastWithFormat(udf, childExpr, returnType);
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
          childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType, DataTypePhysicalVariation.NONE);
    }

    return ve;
  }

  private void freeNonColumns(VectorExpression[] vectorChildren) {
    if (vectorChildren == null) {
      return;
    }
    for (VectorExpression v : vectorChildren) {
      if (!(v instanceof IdentityExpression
            // it's not safe to reuse ConstantVectorExpression's output as a scratch column, see HIVE-26408
            || v instanceof ConstantVectorExpression)) {
        ocm.freeOutputColumn(v.getOutputColumnNum());
      }
    }
  }

  private VectorExpression getCoalesceExpression(List<ExprNodeDesc> childExpr,
      VectorExpressionDescriptor.Mode mode, TypeInfo returnType)
      throws HiveException {
    int[] inputColumns = new int[childExpr.size()];
    VectorExpression[] vectorChildren =
        getVectorExpressions(childExpr, VectorExpressionDescriptor.Mode.PROJECTION);

    final int size = vectorChildren.length;
    TypeInfo[] inputTypeInfos = new TypeInfo[size];
    DataTypePhysicalVariation[] inputDataTypePhysicalVariations = new DataTypePhysicalVariation[size];
    DataTypePhysicalVariation outputDataTypePhysicalVariation = DataTypePhysicalVariation.DECIMAL_64;
    boolean fixConstants = false;
    for (int i = 0; i < vectorChildren.length; ++i) {
      VectorExpression ve = vectorChildren[i];
      inputColumns[i] = ve.getOutputColumnNum();
      inputTypeInfos[i] = ve.getOutputTypeInfo();
      inputDataTypePhysicalVariations[i] = ve.getOutputDataTypePhysicalVariation();
      if (inputDataTypePhysicalVariations[i] == DataTypePhysicalVariation.NONE ||
          inputDataTypePhysicalVariations[i] == null) {
        if (childExpr.get(i) instanceof ExprNodeConstantDesc && inputTypeInfos[i] instanceof DecimalTypeInfo &&
            ((DecimalTypeInfo)inputTypeInfos[i]).precision() <= 18) {
          fixConstants = true;
        } else {
          outputDataTypePhysicalVariation = DataTypePhysicalVariation.NONE;
        }
      }
    }

    if (outputDataTypePhysicalVariation == DataTypePhysicalVariation.DECIMAL_64 && fixConstants) {
      for (int i = 0; i < vectorChildren.length; ++i) {
        if ((inputDataTypePhysicalVariations[i] == DataTypePhysicalVariation.NONE ||
            inputDataTypePhysicalVariations[i] == null) && vectorChildren[i] instanceof ConstantVectorExpression) {
          ConstantVectorExpression cve = ((ConstantVectorExpression)vectorChildren[i]);
          HiveDecimal hd = cve.getDecimalValue();
          Long longValue = new HiveDecimalWritable(hd).serialize64(((DecimalTypeInfo)cve.getOutputTypeInfo()).getScale());
          ((ConstantVectorExpression)vectorChildren[i]).setLongValue(longValue);
          vectorChildren[i].setOutputDataTypePhysicalVariation(DataTypePhysicalVariation.DECIMAL_64);
          int scratchColIndex = vectorChildren[i].getOutputColumnNum() - ocm.initialOutputCol;
          ocm.scratchDataTypePhysicalVariations[scratchColIndex] = DataTypePhysicalVariation.DECIMAL_64;
        }
      }
    }

    final int outputColumnNum = ocm.allocateOutputColumn(returnType, outputDataTypePhysicalVariation);
    VectorCoalesce vectorCoalesce = new VectorCoalesce(inputColumns, outputColumnNum);

    vectorCoalesce.setChildExpressions(vectorChildren);

    vectorCoalesce.setInputTypeInfos(inputTypeInfos);
    vectorCoalesce.setInputDataTypePhysicalVariations(inputDataTypePhysicalVariations);

    vectorCoalesce.setOutputTypeInfo(returnType);
    vectorCoalesce.setOutputDataTypePhysicalVariation(outputDataTypePhysicalVariation);

    freeNonColumns(vectorChildren);

    boolean isFilter = false;    // Assume.
    if (mode == VectorExpressionDescriptor.Mode.FILTER) {

      // Is output type a BOOLEAN?
      if (returnType.getCategory() == Category.PRIMITIVE &&
          ((PrimitiveTypeInfo) returnType).getPrimitiveCategory() == PrimitiveCategory.BOOLEAN) {
        isFilter = true;
      } else {
        return null;
      }
    }

    if (isFilter) {

      // Wrap the PROJECTION IF expression output with a filter.
      SelectColumnIsTrue filterVectorExpr = new SelectColumnIsTrue(vectorCoalesce.getOutputColumnNum());

      filterVectorExpr.setChildExpressions(new VectorExpression[] {vectorCoalesce});

      filterVectorExpr.setInputTypeInfos(vectorCoalesce.getOutputTypeInfo());
      filterVectorExpr.setInputDataTypePhysicalVariations(vectorCoalesce.getOutputDataTypePhysicalVariation());

      return filterVectorExpr;
    } else {
      return vectorCoalesce;
    }
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

  private VectorExpression getGroupingExpression(GenericUDFGrouping udf,
      List<ExprNodeDesc> childExprs, TypeInfo returnType)
          throws HiveException {

    ExprNodeDesc childExpr0 = childExprs.get(0);
    if (!(childExpr0 instanceof ExprNodeColumnDesc)) {
      return null;
    }
    ExprNodeColumnDesc groupingIdColDesc = (ExprNodeColumnDesc) childExpr0;
    int groupingIdColNum = getInputColumnIndex(groupingIdColDesc.getColumn());

    final int indexCount = childExprs.size() - 1;
    int[] indices = new int[indexCount];
    for (int i = 0; i < indexCount; i++) {
      ExprNodeDesc indexChildExpr = childExprs.get(i + 1);
      if (!(indexChildExpr instanceof ExprNodeConstantDesc)) {
        return null;
      }
      Object scalarObject = ((ExprNodeConstantDesc) indexChildExpr).getValue();
      final int index;
      if (scalarObject instanceof Integer) {
        index = (int) scalarObject;
      } else if (scalarObject instanceof Long) {
        index = (int) ((long) scalarObject);
      } else {
        return null;
      }
      indices[i] = index;
    }

    final int outputColumnNum = ocm.allocateOutputColumn(returnType);
    final VectorExpression ve;
    if (indices.length == 1) {
      ve = new GroupingColumn(groupingIdColNum, indices[0], outputColumnNum);
    } else {
      ve = new GroupingColumns(groupingIdColNum, indices, outputColumnNum);
    }

    ve.setInputTypeInfos(groupingIdColDesc.getTypeInfo());
    ve.setInputDataTypePhysicalVariations(DataTypePhysicalVariation.NONE);

    ve.setOutputTypeInfo(returnType);
    ve.setOutputDataTypePhysicalVariation(DataTypePhysicalVariation.NONE);

    return ve;
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

    VectorExpression expr;

    StructTypeInfo structTypeInfo = (StructTypeInfo) colTypeInfo;

    List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
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

    expr = createVectorExpression(cl, null, VectorExpressionDescriptor.Mode.PROJECTION, returnType,
        DataTypePhysicalVariation.NONE);

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
    if (colExpr instanceof ExprNodeConstantDesc) {
      return null;
    }
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
    Class<?> cl;
    // TODO: the below assumes that all the arguments to IN are of the same type;
    //       non-vectorized validates that explicitly during UDF init.
    if (isIntFamily(colType)) {
      cl = (mode == VectorExpressionDescriptor.Mode.FILTER ? FilterLongColumnInList.class : LongColumnInList.class);
      long[] inVals = new long[childrenForInList.size()];
      for (int i = 0; i != inVals.length; i++) {
        inVals[i] = getIntFamilyScalarAsLong((ExprNodeConstantDesc) childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), VectorExpressionDescriptor.Mode.PROJECTION, returnType,
          DataTypePhysicalVariation.NONE);
      ((ILongInExpr) expr).setInListValues(inVals);
    } else if (isTimestampFamily(colType)) {
      cl = (mode == VectorExpressionDescriptor.Mode.FILTER ? FilterTimestampColumnInList.class : TimestampColumnInList.class);
      Timestamp[] inVals = new Timestamp[childrenForInList.size()];
      for (int i = 0; i != inVals.length; i++) {
        inVals[i] = getTimestampScalar(childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), VectorExpressionDescriptor.Mode.PROJECTION, returnType,
          DataTypePhysicalVariation.NONE);
      ((ITimestampInExpr) expr).setInListValues(inVals);
    } else if (isStringFamily(colType)) {
      cl = (mode == VectorExpressionDescriptor.Mode.FILTER ? FilterStringColumnInList.class : StringColumnInList.class);
      byte[][] inVals = new byte[childrenForInList.size()][];
      for (int i = 0; i != inVals.length; i++) {
        inVals[i] = getStringScalarAsByteArray((ExprNodeConstantDesc) childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), VectorExpressionDescriptor.Mode.PROJECTION, returnType,
          DataTypePhysicalVariation.NONE);
      ((IStringInExpr) expr).setInListValues(inVals);
    } else if (isFloatFamily(colType)) {
      cl = (mode == VectorExpressionDescriptor.Mode.FILTER ? FilterDoubleColumnInList.class : DoubleColumnInList.class);
      double[] inValsD = new double[childrenForInList.size()];
      for (int i = 0; i != inValsD.length; i++) {
        inValsD[i] = getNumericScalarAsDouble(childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), VectorExpressionDescriptor.Mode.PROJECTION, returnType,
          DataTypePhysicalVariation.NONE);
      ((IDoubleInExpr) expr).setInListValues(inValsD);
    } else if (isDecimalFamily(colType)) {

      final boolean tryDecimal64 =
          checkExprNodeDescForDecimal64(colExpr);
      if (tryDecimal64) {
        cl = (mode == VectorExpressionDescriptor.Mode.FILTER ?
            FilterDecimal64ColumnInList.class : Decimal64ColumnInList.class);
        final int scale = ((DecimalTypeInfo) colExpr.getTypeInfo()).getScale();
        expr = createDecimal64VectorExpression(
            cl, childExpr.subList(0, 1), VectorExpressionDescriptor.Mode.PROJECTION,
            /* isDecimal64ScaleEstablished */ true,
            /* decimal64ColumnScale */ scale,
            returnType, DataTypePhysicalVariation.NONE,
            /* dontRescaleArguments */ false, new GenericUDFIn());
        if (expr != null) {
          long[] inVals = new long[childrenForInList.size()];
          for (int i = 0; i != inVals.length; i++) {
            ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc) childrenForInList.get(i);
            HiveDecimal hiveDecimal = (HiveDecimal) constDesc.getValue();
            final long decimal64Scalar =
                new HiveDecimalWritable(hiveDecimal).serialize64(scale);
            inVals[i] = decimal64Scalar;
          }
          ((ILongInExpr) expr).setInListValues(inVals);
        }
      }
      if (expr == null) {
        cl = (mode == VectorExpressionDescriptor.Mode.FILTER ?
            FilterDecimalColumnInList.class : DecimalColumnInList.class);
        expr = createVectorExpression(
            cl, childExpr.subList(0, 1), VectorExpressionDescriptor.Mode.PROJECTION, returnType,
            DataTypePhysicalVariation.NONE);
        HiveDecimal[] inValsD = new HiveDecimal[childrenForInList.size()];
        for (int i = 0; i != inValsD.length; i++) {
          inValsD[i] = (HiveDecimal) getVectorTypeScalarValue(
              (ExprNodeConstantDesc)  childrenForInList.get(i));
        }
        ((IDecimalInExpr) expr).setInListValues(inValsD);
      }
    } else if (isDateFamily(colType)) {
      cl = (mode == VectorExpressionDescriptor.Mode.FILTER ? FilterLongColumnInList.class : LongColumnInList.class);
      long[] inVals = new long[childrenForInList.size()];
      for (int i = 0; i != inVals.length; i++) {
        inVals[i] = (Long) getVectorTypeScalarValue((ExprNodeConstantDesc) childrenForInList.get(i));
      }
      expr = createVectorExpression(cl, childExpr.subList(0, 1), VectorExpressionDescriptor.Mode.PROJECTION, returnType,
          DataTypePhysicalVariation.NONE);
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
        return hiveChar.getStrippedValue().getBytes(StandardCharsets.UTF_8);
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
    } else if (o instanceof HiveVarchar) {
      HiveVarchar hiveVarchar = (HiveVarchar) o;
      try {
        return hiveVarchar.getValue().getBytes(StandardCharsets.UTF_8);
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
    } else if (isCastToBoolean(cl)) {
      ve = getCastToBooleanExpression(childExpr, mode);
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
    String typename = type.getTypeName();
    if (!(type instanceof PrimitiveTypeInfo)) {
      throw new HiveException("Unsupported type " + typename + " for cast to String");
    }
    PrimitiveTypeInfo ptinfo = (PrimitiveTypeInfo) type;
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
    case TIMESTAMP:
      return CastTimestampToString.getTimestampString((Timestamp) scalar);
    default:
      throw new HiveException("Unsupported type "+typename+" for cast to String");
    }
  }

  private Double castConstantToDouble(Object scalar, TypeInfo type) throws HiveException {
    if (null == scalar) {
      return null;
    }
    PrimitiveTypeInfo ptinfo = (PrimitiveTypeInfo) type;
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

  private VectorExpression getCastToDecimal(List<ExprNodeDesc> childExpr, VectorExpressionDescriptor.Mode mode,
      TypeInfo returnType) throws HiveException {
    ExprNodeDesc child = childExpr.get(0);
    String inputType = childExpr.get(0).getTypeString();
    if (child instanceof ExprNodeConstantDesc) {

      // Return a constant vector expression
      try {
        Object constantValue = ((ExprNodeConstantDesc) child).getValue();
        if (tryDecimal64Cast) {
          if (((DecimalTypeInfo)returnType).precision() <= 18) {
            Long longValue = castConstantToLong(constantValue, child.getTypeInfo(), PrimitiveCategory.LONG);
            return getConstantVectorExpression(longValue, TypeInfoFactory.longTypeInfo,
                VectorExpressionDescriptor.Mode.PROJECTION);
          }
          return null;
        }
        HiveDecimal decimalValue = castConstantToDecimal(constantValue, child.getTypeInfo());
        return getConstantVectorExpression(decimalValue, returnType, VectorExpressionDescriptor.Mode.PROJECTION);
      } catch (Exception e) {

        // Fall back to VectorUDFAdaptor.
        return null;
      }
    }
    if (isIntFamily(inputType)) {
      if (tryDecimal64Cast) {
        if (((DecimalTypeInfo)returnType).precision() <= 18) {
          return createVectorExpression(CastLongToDecimal64.class, childExpr,
              VectorExpressionDescriptor.Mode.PROJECTION, returnType, DataTypePhysicalVariation.DECIMAL_64);
        }
        return null;
      }
      return createVectorExpression(CastLongToDecimal.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (inputType.equals("float")) {
      return createVectorExpression(CastFloatToDecimal.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (inputType.equals("double")) {
      return createVectorExpression(CastDoubleToDecimal.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (decimalTypePattern.matcher(inputType).matches()) {
      if (child instanceof ExprNodeColumnDesc) {
        int colIndex = getInputColumnIndex((ExprNodeColumnDesc) child);
        DataTypePhysicalVariation dataTypePhysicalVariation = getDataTypePhysicalVariation(colIndex);
        if (dataTypePhysicalVariation == DataTypePhysicalVariation.DECIMAL_64) {
          // try to scale up the expression so we can match the return type scale
          if (tryDecimal64Cast && ((DecimalTypeInfo)returnType).precision() <= 18) {
            List<ExprNodeDesc> children = new ArrayList<>();
            int scaleDiff = ((DecimalTypeInfo)returnType).scale() - ((DecimalTypeInfo)childExpr.get(0).getTypeInfo()).scale();
            ExprNodeDesc newConstant = new ExprNodeConstantDesc(new DecimalTypeInfo(scaleDiff, 0),
                HiveDecimal.create(POWEROFTENTABLE[scaleDiff]));
            children.add(child);
            children.add(newConstant);
            ExprNodeGenericFuncDesc newScaledExpr = new ExprNodeGenericFuncDesc(returnType,
                new GenericUDFOPScaleUpDecimal64(), " ScaleUp ", children);
            return getVectorExpression(newScaledExpr, mode);
          }
          // Do Decimal64 conversion instead.
          return createDecimal64ToDecimalConversion(colIndex, returnType);
        } else {
          return createVectorExpression(CastDecimalToDecimal.class, childExpr,
              VectorExpressionDescriptor.Mode.PROJECTION, returnType, DataTypePhysicalVariation.NONE);
        }
      } else {
        return createVectorExpression(CastDecimalToDecimal.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
            returnType, DataTypePhysicalVariation.NONE);
      }
    } else if (isStringFamily(inputType)) {
      DataTypePhysicalVariation dataTypePhysicalVariation =
          tryDecimal64Cast && ((DecimalTypeInfo) returnType).precision() <= 18 ?
              DataTypePhysicalVariation.DECIMAL_64 : 
              DataTypePhysicalVariation.NONE;
      return createVectorExpression(CastStringToDecimal.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, dataTypePhysicalVariation);
    } else if (inputType.equals("timestamp")) {
      return createVectorExpression(CastTimestampToDecimal.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    }
    return null;
  }

  private VectorExpression getCastToString(List<ExprNodeDesc> childExpr, TypeInfo returnType)
      throws HiveException {
    ExprNodeDesc child = childExpr.get(0);
    String inputType = childExpr.get(0).getTypeString();
    if (child instanceof ExprNodeConstantDesc) {

        // Return a constant vector expression
        try {
          Object constantValue = ((ExprNodeConstantDesc) child).getValue();
          String strValue = castConstantToString(constantValue, child.getTypeInfo());
          return getConstantVectorExpression(strValue, returnType, VectorExpressionDescriptor.Mode.PROJECTION);
        } catch (Exception e) {

          // Fall back to VectorUDFAdaptor.
          return null;
        }
    }
    if (inputType.equals("boolean")) {
      // Boolean must come before the integer family. It's a special case.
      return createVectorExpression(CastBooleanToStringViaLongToString.class, childExpr,
          VectorExpressionDescriptor.Mode.PROJECTION, returnType, DataTypePhysicalVariation.NONE);
    } else if (isIntFamily(inputType)) {
      return createVectorExpression(CastLongToString.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (inputType.equals("float")) {
      return createVectorExpression(CastFloatToString.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (inputType.equals("double")) {
      return createVectorExpression(CastDoubleToString.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (isDecimalFamily(inputType)) {
      return createVectorExpression(CastDecimalToString.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (isDateFamily(inputType)) {
      return createVectorExpression(CastDateToString.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (isTimestampFamily(inputType)) {
      return createVectorExpression(CastTimestampToString.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (isStringFamily(inputType)) {

      // STRING and VARCHAR types require no conversion, so use a no-op.
      // Also, CHAR is stored in BytesColumnVector with trimmed blank padding, so it also
      // requires no conversion;
      return getIdentityExpression(childExpr);
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
      return createVectorExpression(CastBooleanToCharViaLongToChar.class, childExpr,
          VectorExpressionDescriptor.Mode.PROJECTION, returnType, DataTypePhysicalVariation.NONE);
    } else if (isIntFamily(inputType)) {
      return createVectorExpression(CastLongToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (inputType.equals("float")) {
      return createVectorExpression(CastFloatToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (inputType.equals("double")) {
      return createVectorExpression(CastDoubleToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (isDecimalFamily(inputType)) {
      return createVectorExpression(CastDecimalToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (isDateFamily(inputType)) {
      return createVectorExpression(CastDateToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (isTimestampFamily(inputType)) {
      return createVectorExpression(CastTimestampToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (isStringFamily(inputType)) {
      return createVectorExpression(CastStringGroupToChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
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
      return createVectorExpression(CastBooleanToVarCharViaLongToVarChar.class, childExpr,
          VectorExpressionDescriptor.Mode.PROJECTION, returnType, DataTypePhysicalVariation.NONE);
    } else if (isIntFamily(inputType)) {
      return createVectorExpression(CastLongToVarChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (inputType.equals("float")) {
      return createVectorExpression(CastFloatToVarChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (inputType.equals("double")) {
      return createVectorExpression(CastDoubleToVarChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (isDecimalFamily(inputType)) {
      return createVectorExpression(CastDecimalToVarChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (isDateFamily(inputType)) {
      return createVectorExpression(CastDateToVarChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (isTimestampFamily(inputType)) {
      return createVectorExpression(CastTimestampToVarChar.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (isStringFamily(inputType)) {
      return createVectorExpression(CastStringGroupToVarChar.class, childExpr,
          VectorExpressionDescriptor.Mode.PROJECTION, returnType, DataTypePhysicalVariation.NONE);
    }
    return null;
  }

  private VectorExpression getCastToBinary(List<ExprNodeDesc> childExpr, TypeInfo returnType)
      throws HiveException {
    ExprNodeDesc child = childExpr.get(0);
    String inputType = childExpr.get(0).getTypeString();
    if (child instanceof ExprNodeConstantDesc) {
      // Don't do constant folding here.  Wait until the optimizer is changed to do it.
      // Family of related JIRAs: HIVE-7421, HIVE-7422, and HIVE-7424.
      return null;
    }
    if (inputType.equalsIgnoreCase("string") || varcharTypePattern.matcher(inputType).matches()) {

      // STRING and VARCHAR types require no conversion, so use a no-op.
      return getIdentityExpression(childExpr);
    } else if (charTypePattern.matcher(inputType).matches()) {
      return createVectorExpression(CastCharToBinary.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
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
        return createVectorExpression(CastLongToFloatViaLongToDouble.class, childExpr,
            VectorExpressionDescriptor.Mode.PROJECTION, returnType, DataTypePhysicalVariation.NONE);
      } else {
        return createVectorExpression(CastLongToDouble.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
            returnType, DataTypePhysicalVariation.NONE);
      }
    } else if (inputType.equals("timestamp")) {
      return createVectorExpression(CastTimestampToDouble.class, childExpr, VectorExpressionDescriptor.Mode.PROJECTION,
          returnType, DataTypePhysicalVariation.NONE);
    } else if (isFloatFamily(inputType)) {

      // float types require no conversion, so use a no-op
      return getIdentityExpression(childExpr);
    }
    return null;
  }

  private VectorExpression getCastToBooleanExpression(List<ExprNodeDesc> childExpr, VectorExpressionDescriptor.Mode mode)
      throws HiveException {
    ExprNodeDesc child = childExpr.get(0);
    TypeInfo inputTypeInfo = child.getTypeInfo();
    String inputType = inputTypeInfo.toString();
    if (child instanceof ExprNodeConstantDesc) {
      if (null == ((ExprNodeConstantDesc)child).getValue()) {
        return getConstantVectorExpression(null, TypeInfoFactory.booleanTypeInfo, mode);
      }
      // Don't do constant folding here.  Wait until the optimizer is changed to do it.
      // Family of related JIRAs: HIVE-7421, HIVE-7422, and HIVE-7424.
      return null;
    }

    VectorExpression ve;
    // Long and double are handled using descriptors, string needs to be specially handled.
    if (isStringFamily(inputType)) {
      ve = createVectorExpression(CastStringToBoolean.class, childExpr,
          VectorExpressionDescriptor.Mode.PROJECTION, TypeInfoFactory.booleanTypeInfo, DataTypePhysicalVariation.NONE);
    } else {
      // Ok, try the UDF.
      ve = getVectorExpressionForUdf(null, UDFToBoolean.class, childExpr,
          VectorExpressionDescriptor.Mode.PROJECTION, TypeInfoFactory.booleanTypeInfo);
    }

    if (ve == null || mode == VectorExpressionDescriptor.Mode.PROJECTION) {
      return ve;
    }

    int outputColumnNum = ve.getOutputColumnNum();
    SelectColumnIsTrue filterVectorExpr = new SelectColumnIsTrue(outputColumnNum);
    filterVectorExpr.setChildExpressions(new VectorExpression[] { ve });
    filterVectorExpr.setInputTypeInfos(ve.getOutputTypeInfo());
    filterVectorExpr.setInputDataTypePhysicalVariations(DataTypePhysicalVariation.NONE);
    return filterVectorExpr;
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

  private VectorExpression getCastWithFormat(
      GenericUDF udf, List<ExprNodeDesc> childExpr, TypeInfo returnType) throws HiveException {
    String inputType = childExpr.get(1).getTypeString();
    childExpr.remove(0); // index 0 not needed since we know returnType

    Class<?> veClass = getCastFormatVectorExpressionClass(childExpr, returnType, inputType);
    return createVectorExpression(
        veClass, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType, DataTypePhysicalVariation.NONE);
  }

  private Class<?> getCastFormatVectorExpressionClass(List<ExprNodeDesc> childExpr,
      TypeInfo returnType, String inputType) throws HiveException {
    switch (inputType) {
    case serdeConstants.TIMESTAMP_TYPE_NAME:
      if (returnType.getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
        return CastTimestampToStringWithFormat.class;
      }
      if (returnType.getTypeName().startsWith(serdeConstants.VARCHAR_TYPE_NAME)) {
        return CastTimestampToVarCharWithFormat.class;
      }
      if (returnType.getTypeName().startsWith(serdeConstants.CHAR_TYPE_NAME)) {
        return CastTimestampToCharWithFormat.class;
      }
    case serdeConstants.DATE_TYPE_NAME:
      if (returnType.getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
        return CastDateToStringWithFormat.class;
      }
      if (returnType.getTypeName().startsWith(serdeConstants.VARCHAR_TYPE_NAME)) {
        return CastDateToVarCharWithFormat.class;
      }
      if (returnType.getTypeName().startsWith(serdeConstants.CHAR_TYPE_NAME)) {
        return CastDateToCharWithFormat.class;
      }
    default: //keep going
    }
    if (inputType.equals(serdeConstants.STRING_TYPE_NAME)
        || inputType.startsWith(serdeConstants.CHAR_TYPE_NAME)
        || inputType.startsWith(serdeConstants.VARCHAR_TYPE_NAME)) {
      switch (returnType.getTypeName()) {
      case serdeConstants.TIMESTAMP_TYPE_NAME:
        return CastStringToTimestampWithFormat.class;
      case serdeConstants.DATE_TYPE_NAME:
        return CastStringToDateWithFormat.class;
      default: //keep going
      }
    }
    throw new HiveException(
        "Expression cast " + inputType + " to " + returnType + " format not" + " vectorizable");
  }

  private VectorExpression tryDecimal64Between(VectorExpressionDescriptor.Mode mode, boolean isNot,
      ExprNodeDesc colExpr, List<ExprNodeDesc> childrenAfterNot, TypeInfo returnTypeInfo)
          throws HiveException {
    final Class<?> cl;
    if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
      cl = (isNot ? Decimal64ColumnNotBetween.class : Decimal64ColumnBetween.class);
    } else {
      cl = (isNot ? FilterDecimal64ColumnNotBetween.class : FilterDecimal64ColumnBetween.class);
    }
    return
        createDecimal64VectorExpression(
            cl, childrenAfterNot, VectorExpressionDescriptor.Mode.PROJECTION,
            /* isDecimal64ScaleEstablished */ true,
            /* decimal64ColumnScale */ ((DecimalTypeInfo) colExpr.getTypeInfo()).getScale(),
            returnTypeInfo, DataTypePhysicalVariation.NONE,
            /* dontRescaleArguments */ false,
            new GenericUDFBetween());
  }

  /* Get a [NOT] BETWEEN filter or projection expression. This is treated as a special case
   * because the NOT is actually specified in the expression tree as the first argument,
   * and we don't want any runtime cost for that. So creating the VectorExpression
   * needs to be done differently than the standard way where all arguments are
   * passed to the VectorExpression constructor.
   */
  private VectorExpression getBetweenExpression(List<ExprNodeDesc> childExpr,
      VectorExpressionDescriptor.Mode mode, TypeInfo returnType)
          throws HiveException {

    boolean hasDynamicValues = false;

    // We don't currently support the BETWEEN ends being columns.  They must be scalars.
    if ((childExpr.get(2) instanceof ExprNodeDynamicValueDesc) &&
        (childExpr.get(3) instanceof ExprNodeDynamicValueDesc)) {
      hasDynamicValues = true;
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {

        // Projection mode is not applicable.
        return null;
      }
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

    List<ExprNodeDesc> castChildren = new ArrayList<>();
    boolean wereCastUdfs = false;
    Category commonTypeCategory = commonType.getCategory();
    for (ExprNodeDesc desc: childExpr.subList(1, 4)) {
      TypeInfo childTypeInfo = desc.getTypeInfo();
      Category childCategory = childTypeInfo.getCategory();

      if (childCategory != commonTypeCategory) {
        return null;
      }
      final boolean isNeedsCast;
      if (commonTypeCategory == Category.PRIMITIVE) {

        // Do not to strict TypeInfo comparisons for DECIMAL -- just compare the category.
        // Otherwise, we generate unnecessary casts.
        isNeedsCast =
            ((PrimitiveTypeInfo) commonType).getPrimitiveCategory() !=
            ((PrimitiveTypeInfo) childTypeInfo).getPrimitiveCategory();
      } else {
        isNeedsCast = !commonType.equals(desc.getTypeInfo());
      }

      if (!isNeedsCast) {
        castChildren.add(desc);
      } else {
        GenericUDF castUdf = getGenericUDFForCast(commonType);
        ExprNodeGenericFuncDesc engfd = new ExprNodeGenericFuncDesc(commonType, castUdf,
            Arrays.asList(new ExprNodeDesc[] { desc }));
        castChildren.add(engfd);
        wereCastUdfs = true;
      }
    }
    String colType = commonType.getTypeName();

    // prepare arguments for createVectorExpression
    List<ExprNodeDesc> childrenAfterNot = evaluateCastOnConstants(castChildren);

    // determine class
    Class<?> cl = null;
    if (isIntFamily(colType) && !notKeywordPresent) {
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = LongColumnBetween.class;
      } else {
        cl = (hasDynamicValues ?
            FilterLongColumnBetweenDynamicValue.class :
            FilterLongColumnBetween.class);
      }
    } else if (isIntFamily(colType) && notKeywordPresent) {
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = LongColumnNotBetween.class;
      } else {
        cl = FilterLongColumnNotBetween.class;
      }
    } else if (isFloatFamily(colType) && !notKeywordPresent) {
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = DoubleColumnBetween.class;
      } else {
        cl = (hasDynamicValues ?
            FilterDoubleColumnBetweenDynamicValue.class :
            FilterDoubleColumnBetween.class);
      }
    } else if (isFloatFamily(colType) && notKeywordPresent) {
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = DoubleColumnNotBetween.class;
      } else {
        cl = FilterDoubleColumnNotBetween.class;
      }
    } else if (colType.equals("string") && !notKeywordPresent) {
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = StringColumnBetween.class;
      } else {
        cl = (hasDynamicValues ?
            FilterStringColumnBetweenDynamicValue.class :
            FilterStringColumnBetween.class);
      }
    } else if (colType.equals("string") && notKeywordPresent) {
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = StringColumnNotBetween.class;
      } else {
        cl = FilterStringColumnNotBetween.class;
      }
    } else if (varcharTypePattern.matcher(colType).matches() && !notKeywordPresent) {
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = VarCharColumnBetween.class;
      } else {
        cl = (hasDynamicValues ?
            FilterVarCharColumnBetweenDynamicValue.class :
            FilterVarCharColumnBetween.class);
      }
    } else if (varcharTypePattern.matcher(colType).matches() && notKeywordPresent) {
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = VarCharColumnNotBetween.class;
      } else {
        cl = FilterVarCharColumnNotBetween.class;
      }
    } else if (charTypePattern.matcher(colType).matches() && !notKeywordPresent) {
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = CharColumnBetween.class;
      } else {
        cl =  (hasDynamicValues ?
            FilterCharColumnBetweenDynamicValue.class :
            FilterCharColumnBetween.class);
      }
    } else if (charTypePattern.matcher(colType).matches() && notKeywordPresent) {
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = CharColumnNotBetween.class;
      } else {
        cl = FilterCharColumnNotBetween.class;
      }
    } else if (colType.equals("timestamp") && !notKeywordPresent) {
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = TimestampColumnBetween.class;
      } else {
        cl = (hasDynamicValues ?
            FilterTimestampColumnBetweenDynamicValue.class :
            FilterTimestampColumnBetween.class);
      }
    } else if (colType.equals("timestamp") && notKeywordPresent) {
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = TimestampColumnNotBetween.class;
      } else {
        cl = FilterTimestampColumnNotBetween.class;
      }
    } else if (isDecimalFamily(colType) && !notKeywordPresent) {
      final boolean tryDecimal64 =
          checkExprNodeDescForDecimal64(colExpr) && !wereCastUdfs && !hasDynamicValues;
      if (tryDecimal64) {
        VectorExpression decimal64VecExpr =
            tryDecimal64Between(
                mode, /* isNot */ false, colExpr, childrenAfterNot,
                 returnType);
        if (decimal64VecExpr != null) {
          return decimal64VecExpr;
        }
      }
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = DecimalColumnBetween.class;
      } else {
        cl = (hasDynamicValues ?
            FilterDecimalColumnBetweenDynamicValue.class :
            FilterDecimalColumnBetween.class);
      }
    } else if (isDecimalFamily(colType) && notKeywordPresent) {
      final boolean tryDecimal64 =
          checkExprNodeDescForDecimal64(colExpr) && !wereCastUdfs && !hasDynamicValues;
      if (tryDecimal64) {
        VectorExpression decimal64VecExpr =
            tryDecimal64Between(
                mode, /* isNot */ true, colExpr, childrenAfterNot, returnType);
        if (decimal64VecExpr != null) {
          return decimal64VecExpr;
        }
      }
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = DecimalColumnNotBetween.class;
      } else {
        cl = FilterDecimalColumnNotBetween.class;
      }
    } else if (isDateFamily(colType) && !notKeywordPresent) {
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = LongColumnBetween.class;
      } else {
        cl =  (hasDynamicValues ?
            FilterDateColumnBetweenDynamicValue.class :
            FilterLongColumnBetween.class);
      }
    } else if (isDateFamily(colType) && notKeywordPresent) {
      if (mode == VectorExpressionDescriptor.Mode.PROJECTION) {
        cl = LongColumnNotBetween.class;
      } else {
        cl = FilterLongColumnNotBetween.class;
      }
    }
    return createVectorExpression(
        cl, childrenAfterNot, VectorExpressionDescriptor.Mode.PROJECTION, returnType, DataTypePhysicalVariation.NONE);
  }

  private boolean isCondExpr(ExprNodeDesc exprNodeDesc) {
    return !(exprNodeDesc instanceof ExprNodeConstantDesc) && !(exprNodeDesc instanceof ExprNodeColumnDesc);
  }

  private boolean isNullConst(ExprNodeDesc exprNodeDesc) {
    //null constant could be typed so we need to check the value
    return exprNodeDesc instanceof ExprNodeConstantDesc && ((ExprNodeConstantDesc) exprNodeDesc).getValue() == null;
  }

  private VectorExpression getIfExpression(GenericUDFIf genericUDFIf, List<ExprNodeDesc> childExpr,
      VectorExpressionDescriptor.Mode mode, TypeInfo returnType) throws HiveException {

    boolean isFilter = false;    // Assume.
    if (mode == VectorExpressionDescriptor.Mode.FILTER) {

      // Is output type a BOOLEAN?
      if (returnType.getCategory() == Category.PRIMITIVE &&
          ((PrimitiveTypeInfo) returnType).getPrimitiveCategory() == PrimitiveCategory.BOOLEAN) {
        isFilter = true;
      } else {
        return null;
      }
    }

    // Get a PROJECTION IF expression.
    VectorExpression ve = doGetIfExpression(genericUDFIf, childExpr, returnType);

    if (ve == null) {
      return null;
    }

    if (isFilter) {

      // Wrap the PROJECTION IF expression output with a filter.
      SelectColumnIsTrue filterVectorExpr = new SelectColumnIsTrue(ve.getOutputColumnNum());

      filterVectorExpr.setChildExpressions(new VectorExpression[] {ve});

      filterVectorExpr.setInputTypeInfos(ve.getOutputTypeInfo());
      filterVectorExpr.setInputDataTypePhysicalVariations(ve.getOutputDataTypePhysicalVariation());

      return filterVectorExpr;
    } else {
      return ve;
    }
  }

  private VectorExpression doGetIfExpression(GenericUDFIf genericUDFIf, List<ExprNodeDesc> childExpr,
      TypeInfo returnType) throws HiveException {

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

    final ExprNodeDesc ifDesc = Objects.requireNonNull(childExpr).get(0);
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
      final VectorExpression whenExpr =
          getVectorExpression(ifDesc, VectorExpressionDescriptor.Mode.PROJECTION);
      final VectorExpression elseExpr =
          getVectorExpression(elseDesc, VectorExpressionDescriptor.Mode.PROJECTION);
      DataTypePhysicalVariation outputDataTypePhysicalVariation =
          (elseExpr.getOutputDataTypePhysicalVariation() == null)
              ? DataTypePhysicalVariation.NONE
              : elseExpr.getOutputDataTypePhysicalVariation();

      final int outputColumnNum = ocm.allocateOutputColumn(returnType, outputDataTypePhysicalVariation);

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
          outputDataTypePhysicalVariation,
          elseExpr.getOutputDataTypePhysicalVariation());

      resultExpr.setOutputTypeInfo(returnType);
      resultExpr.setOutputDataTypePhysicalVariation(outputDataTypePhysicalVariation);

      return resultExpr;
    }

    if (isElseNullConst) {
      final VectorExpression whenExpr =
          getVectorExpression(ifDesc, VectorExpressionDescriptor.Mode.PROJECTION);
      final VectorExpression thenExpr =
          getVectorExpression(thenDesc, VectorExpressionDescriptor.Mode.PROJECTION);
      DataTypePhysicalVariation outputDataTypePhysicalVariation =
          (thenExpr.getOutputDataTypePhysicalVariation() == null)
              ? DataTypePhysicalVariation.NONE
              : thenExpr.getOutputDataTypePhysicalVariation();

      final int outputColumnNum = ocm.allocateOutputColumn(returnType, outputDataTypePhysicalVariation);

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
          outputDataTypePhysicalVariation);

      resultExpr.setOutputTypeInfo(returnType);
      resultExpr.setOutputDataTypePhysicalVariation(outputDataTypePhysicalVariation);

      return resultExpr;
    }

    if ((isThenCondExpr || isElseCondExpr) && !isOnlyGood) {
      final VectorExpression whenExpr =
          getVectorExpression(ifDesc, VectorExpressionDescriptor.Mode.PROJECTION);
      final VectorExpression thenExpr =
          getVectorExpression(thenDesc, VectorExpressionDescriptor.Mode.PROJECTION);
      final VectorExpression elseExpr =
          getVectorExpression(elseDesc, VectorExpressionDescriptor.Mode.PROJECTION);

      // Only proceed if the THEN/ELSE types were aligned.
      if (thenExpr.getOutputColumnVectorType() == elseExpr.getOutputColumnVectorType()) {
        DataTypePhysicalVariation outputDataTypePhysicalVariation =
            (thenExpr.getOutputDataTypePhysicalVariation() == elseExpr.getOutputDataTypePhysicalVariation()
                && thenExpr.getOutputDataTypePhysicalVariation() != null)
                ? thenExpr.getOutputDataTypePhysicalVariation()
                : DataTypePhysicalVariation.NONE;

        final int outputColumnNum = ocm.allocateOutputColumn(returnType, outputDataTypePhysicalVariation);

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
        resultExpr.setOutputDataTypePhysicalVariation(outputDataTypePhysicalVariation);

        return resultExpr;
      }
    }

    Class<?> udfClass = genericUDFIf.getClass();
    return getVectorExpressionForUdf(
        genericUDFIf, udfClass, childExpr, VectorExpressionDescriptor.Mode.PROJECTION, returnType);
  }

  private VectorExpression getWhenExpression(List<ExprNodeDesc> childExpr,
      VectorExpressionDescriptor.Mode mode, TypeInfo returnType) throws HiveException {

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
    List<Integer> variableArgPositions = new ArrayList<>();

    // Column numbers of batch corresponding to expression result arguments
    List<Integer> exprResultColumnNums = new ArrayList<>();

    // Prepare children
    List<VectorExpression> vectorExprs = new ArrayList<>();

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
        if (child.getTypeInfo().getCategory() != Category.PRIMITIVE &&
            child.getTypeInfo().getCategory() != Category.STRUCT) {

          // Complex type constants currently not supported by VectorUDFArgDesc.prepareConstant.
          throw new HiveException(
              "Unable to vectorize custom UDF. LIST, MAP, and UNION type constants not supported: " + child);
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

  @SuppressWarnings("unused") public static boolean isIntervalYearMonthFamily(String resultType) {
    return resultType.equalsIgnoreCase("interval_year_month");
  }

  @SuppressWarnings("unused") public static boolean isIntervalDayTimeFamily(String resultType) {
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
      if (constDesc.getValue() == null) {
        return null;
      }else{
        return constDesc.getValue().equals(Boolean.TRUE) ? 1 : 0;
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
    if (o instanceof Byte) {
      return (Byte) o;
    } if (o instanceof Short) {
      return (Short) o;
    } else if (o instanceof Integer) {
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
        return (long) DateWritableV2.dateToDays((Date) scalarValue);
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
      ArrayList<ExprNodeDesc> children = new ArrayList<>();
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
    return ((org.apache.hadoop.hive.common.type.Timestamp) java).toSqlTimestamp();
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
    switch (typeName) {
    case "long":
      return "bigint";
    case "string_family":
      return "string";
    default:
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
    sb.append("Context name ").append(contextName).append(", level ").append(level).append(", ");

    Comparator<Integer> comparerInteger = Integer::compareTo;

    Map<Integer, String> sortedColumnMap = new TreeMap<>(comparerInteger);
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
