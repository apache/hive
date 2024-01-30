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

package org.apache.hadoop.hive.ql.optimizer.physical;

import static org.apache.hadoop.hive.ql.exec.FunctionRegistry.BLOOM_FILTER_FUNCTION;
import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.UNIFORM;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorCoalesce;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFComputeDsKllSketchDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFComputeDsKllSketchFinal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColDivideDecimalScalar;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinAntiJoinLongOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinAntiJoinMultiKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinAntiJoinStringOperator;
import org.apache.hadoop.hive.ql.exec.vector.reducesink.*;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFArgDesc;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKey;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.filesink.VectorFileSinkArrowOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinInnerBigOnlyLongOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinInnerBigOnlyMultiKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinInnerBigOnlyStringOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinInnerLongOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinInnerMultiKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinInnerStringOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinLeftSemiLongOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinLeftSemiMultiKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinLeftSemiStringOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinOuterLongOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinOuterMultiKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinOuterStringOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinFullOuterLongOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinFullOuterMultiKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinFullOuterStringOperator;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFOperator;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnOutputMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSourceMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOuterFilteredOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedUDAFs;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext.HiveVectorAdaptorUsageMode;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext.InConstantType;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport.Support;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.io.NullRowsInputFormat;
import org.apache.hadoop.hive.ql.io.OneNullRowInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.ZeroRowsInputFormat;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.AppMasterEventDesc;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc.ExprNodeDescEqualityWrapper;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.VectorAppMasterEventDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorFileSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorFilterDesc;
import org.apache.hadoop.hive.ql.plan.VectorPTFDesc;
import org.apache.hadoop.hive.ql.plan.VectorPTFInfo;
import org.apache.hadoop.hive.ql.plan.VectorPTFDesc.SupportedFunctionType;
import org.apache.hadoop.hive.ql.plan.VectorTableScanDesc;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc.ProcessingMode;
import org.apache.hadoop.hive.ql.plan.VectorTopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.VectorLimitDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;
import org.apache.hadoop.hive.ql.plan.VectorSMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableImplementationType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKind;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.apache.hadoop.hive.ql.plan.VectorPartitionDesc.VectorDeserializeType;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkInfo;
import org.apache.hadoop.hive.ql.plan.VectorPartitionDesc;
import org.apache.hadoop.hive.ql.plan.VectorSelectDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.plan.ptf.OrderExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.UDFAcos;
import org.apache.hadoop.hive.ql.udf.UDFAsin;
import org.apache.hadoop.hive.ql.udf.UDFAtan;
import org.apache.hadoop.hive.ql.udf.UDFBin;
import org.apache.hadoop.hive.ql.udf.UDFConv;
import org.apache.hadoop.hive.ql.udf.UDFCos;
import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFDayOfWeek;
import org.apache.hadoop.hive.ql.udf.UDFDegrees;
import org.apache.hadoop.hive.ql.udf.UDFExp;
import org.apache.hadoop.hive.ql.udf.UDFHex;
import org.apache.hadoop.hive.ql.udf.UDFHour;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.UDFLn;
import org.apache.hadoop.hive.ql.udf.UDFLog;
import org.apache.hadoop.hive.ql.udf.UDFLog10;
import org.apache.hadoop.hive.ql.udf.UDFLog2;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFRadians;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.UDFRegExpExtract;
import org.apache.hadoop.hive.ql.udf.UDFRegExpReplace;
import org.apache.hadoop.hive.ql.udf.UDFSecond;
import org.apache.hadoop.hive.ql.udf.UDFSign;
import org.apache.hadoop.hive.ql.udf.UDFSin;
import org.apache.hadoop.hive.ql.udf.UDFSqrt;
import org.apache.hadoop.hive.ql.udf.UDFSubstr;
import org.apache.hadoop.hive.ql.udf.UDFTan;
import org.apache.hadoop.hive.ql.udf.UDFToBoolean;
import org.apache.hadoop.hive.ql.udf.UDFToByte;
import org.apache.hadoop.hive.ql.udf.UDFToDouble;
import org.apache.hadoop.hive.ql.udf.UDFToFloat;
import org.apache.hadoop.hive.ql.udf.UDFToInteger;
import org.apache.hadoop.hive.ql.udf.UDFToLong;
import org.apache.hadoop.hive.ql.udf.UDFToShort;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.UDFSinh;
import org.apache.hadoop.hive.ql.udf.UDFCosh;
import org.apache.hadoop.hive.ql.udf.UDFTanh;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.NullStructSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hive.common.util.AnnotationUtils;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.base.Preconditions;

public class Vectorizer implements PhysicalPlanResolver {

  protected static final Logger LOG = LoggerFactory.getLogger(Vectorizer.class);

  private static final Pattern supportedDataTypesPattern;

  private static final TypeInfo[] EMPTY_TYPEINFO_ARRAY = new TypeInfo[0];

  static {
    StringBuilder patternBuilder = new StringBuilder();
    patternBuilder.append(serdeConstants.INT_TYPE_NAME);
    patternBuilder.append("|").append(serdeConstants.SMALLINT_TYPE_NAME);
    patternBuilder.append("|").append(serdeConstants.TINYINT_TYPE_NAME);
    patternBuilder.append("|").append(serdeConstants.BIGINT_TYPE_NAME);
    patternBuilder.append("|").append("integer");
    patternBuilder.append("|").append("long");
    patternBuilder.append("|").append("short");
    patternBuilder.append("|").append(serdeConstants.TIMESTAMP_TYPE_NAME);
    patternBuilder.append("|").append(serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME);
    patternBuilder.append("|").append(serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME);
    patternBuilder.append("|").append(serdeConstants.BOOLEAN_TYPE_NAME);
    patternBuilder.append("|").append(serdeConstants.BINARY_TYPE_NAME);
    patternBuilder.append("|").append(serdeConstants.STRING_TYPE_NAME);
    patternBuilder.append("|").append("byte");
    patternBuilder.append("|").append(serdeConstants.FLOAT_TYPE_NAME);
    patternBuilder.append("|").append(serdeConstants.DOUBLE_TYPE_NAME);
    patternBuilder.append("|").append(serdeConstants.DATE_TYPE_NAME);
    patternBuilder.append("|").append(serdeConstants.VOID_TYPE_NAME);

    /** Decimal types can be specified with different precision and scales e.g. decimal(10,5),
     * as opposed to other data types which can be represented by constant strings.
     * The regex matches only the {@link serdeConstants#DECIMAL_TYPE_NAME} prefix of the type.
     */
    patternBuilder.append("|").append(serdeConstants.DECIMAL_TYPE_NAME).append(".*");

    // CHAR and VARCHAR types can be specified with maximum length.
    patternBuilder.append("|").append(serdeConstants.CHAR_TYPE_NAME).append(".*");
    patternBuilder.append("|").append(serdeConstants.VARCHAR_TYPE_NAME).append(".*");

    supportedDataTypesPattern = Pattern.compile(patternBuilder.toString());
  }

  private final Set<Class<?>> supportedGenericUDFs = new HashSet<>();

  private final Set<String> supportedAggregationUdfs = Arrays.stream(VECTORIZABLE_UDAF.values())
      .map(e -> e.name().toLowerCase())
      .collect(Collectors.toSet());

  // The set of virtual columns that vectorized readers *MAY* support.
  public static final ImmutableSet<VirtualColumn> vectorizableVirtualColumns = 
      ImmutableSet.of(
        VirtualColumn.ROWID, 
        VirtualColumn.ROWISDELETED,
        VirtualColumn.PARTITION_SPEC_ID, 
        VirtualColumn.PARTITION_HASH, 
        VirtualColumn.FILE_PATH, 
        VirtualColumn.ROW_POSITION
      );

  private HiveConf hiveConf;

  private enum VECTORIZABLE_UDAF {
    MIN,
    MAX,
    COUNT,
    SUM,
    AVG,
    VARIANCE,
    VAR_POP,
    VAR_SAMP,
    STD,
    STDDEV,
    STDDEV_POP,
    STDDEV_SAMP,
    BLOOM_FILTER,
    COMPUTE_BIT_VECTOR_HLL,
    DS_KLL_SKETCH;

    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

  public enum EnabledOverride {
    NONE,
    DISABLE,
    ENABLE;

    public static final Map<String, EnabledOverride> NAME_MAP = new HashMap<>();
    static {
      for (EnabledOverride vectorizationEnabledOverride : values()) {
        NAME_MAP.put(
            vectorizationEnabledOverride.toString(), vectorizationEnabledOverride);
      }
    }

    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

  private boolean isVectorizationEnabled;
  private EnabledOverride vectorizationEnabledOverride;
  private boolean isTestForcedVectorizationEnable;

  private boolean useVectorizedInputFileFormat;
  private boolean useVectorDeserialize;
  private boolean useRowDeserialize;
  private boolean isReduceVectorizationEnabled;
  private boolean isPtfVectorizationEnabled;
  private boolean isVectorizationComplexTypesEnabled;

  // Now deprecated.
  private boolean isVectorizationGroupByComplexTypesEnabled;

  private boolean isVectorizedRowIdentifierEnabled;
  private String vectorizedInputFormatSupportEnabled;
  private boolean isLlapIoEnabled;
  private Set<Support> vectorizedInputFormatSupportEnabledSet;
  private Collection<Class<?>> rowDeserializeInputFormatExcludes;
  private int vectorizedPTFMaxMemoryBufferingBatchCount;
  private int vectorizedTestingReducerBatchSize;
  private boolean isTestVectorizerSuppressFatalExceptions;

  private boolean isSchemaEvolution;

  private HiveVectorAdaptorUsageMode hiveVectorAdaptorUsageMode;

  private static final Set<Support> vectorDeserializeTextSupportSet = new TreeSet<>();
  static {
    vectorDeserializeTextSupportSet.addAll(Arrays.asList(Support.values()));
  }

  private static final Set<String> supportedAcidInputFormats = new TreeSet<>();
  static {
    supportedAcidInputFormats.add(OrcInputFormat.class.getName());
    // For metadataonly or empty rows optimizations, null/onerow input format can be selected.
    supportedAcidInputFormats.add(NullRowsInputFormat.class.getName());
    supportedAcidInputFormats.add(OneNullRowInputFormat.class.getName());
    supportedAcidInputFormats.add(ZeroRowsInputFormat.class.getName());
  }

  private boolean isTestVectorizationSuppressExplainExecutionMode;

  private BaseWork currentBaseWork;
  private Operator<? extends OperatorDesc> currentOperator;
  private Collection<Class<?>> vectorizedInputFormatExcludes;
  private Map<Operator<? extends OperatorDesc>, Set<ImmutablePair<Operator<? extends OperatorDesc>, Operator<? extends OperatorDesc>>>> delayedFixups =
      new IdentityHashMap<>();

  public void testSetCurrentBaseWork(BaseWork testBaseWork) {
    currentBaseWork = testBaseWork;
  }

  private void setNodeIssue(String issue) {
    currentBaseWork.setNotVectorizedReason(
        VectorizerReason.createNodeIssue(issue));
  }

  private void setOperatorIssue(String issue) {
    currentBaseWork.setNotVectorizedReason(
        VectorizerReason.createOperatorIssue(currentOperator, issue));
  }

  private void setExpressionIssue(String expressionTitle, String issue) {
    currentBaseWork.setNotVectorizedReason(
        VectorizerReason.createExpressionIssue(currentOperator, expressionTitle, issue));
  }

  private void clearNotVectorizedReason() {
    currentBaseWork.setNotVectorizedReason(null);
  }

  private long vectorizedVertexNum = -1;

  private Set<VirtualColumn> availableVectorizedVirtualColumnSet = null;
  private Set<VirtualColumn> neededVirtualColumnSet = null;

  private PlanMapper planMapper;

  public class VectorizerCannotVectorizeException extends Exception {
  }

  public Vectorizer() {

    /*
     * We check UDFs against the supportedGenericUDFs when
     * hive.vectorized.adaptor.usage.mode=chosen or none.
     *
     * We allow all UDFs for hive.vectorized.adaptor.usage.mode=all.
     */
    supportedGenericUDFs.add(GenericUDFOPPlus.class);
    supportedGenericUDFs.add(GenericUDFOPMinus.class);
    supportedGenericUDFs.add(GenericUDFOPMultiply.class);
    supportedGenericUDFs.add(GenericUDFOPDivide.class);
    supportedGenericUDFs.add(GenericUDFOPMod.class);
    supportedGenericUDFs.add(GenericUDFOPNegative.class);
    supportedGenericUDFs.add(GenericUDFOPPositive.class);

    supportedGenericUDFs.add(GenericUDFOPEqualOrLessThan.class);
    supportedGenericUDFs.add(GenericUDFOPEqualOrGreaterThan.class);
    supportedGenericUDFs.add(GenericUDFOPGreaterThan.class);
    supportedGenericUDFs.add(GenericUDFOPLessThan.class);
    supportedGenericUDFs.add(GenericUDFOPNot.class);
    supportedGenericUDFs.add(GenericUDFOPNotEqual.class);
    supportedGenericUDFs.add(GenericUDFOPNotNull.class);
    supportedGenericUDFs.add(GenericUDFOPNull.class);
    supportedGenericUDFs.add(GenericUDFOPOr.class);
    supportedGenericUDFs.add(GenericUDFOPAnd.class);
    supportedGenericUDFs.add(GenericUDFOPEqual.class);
    supportedGenericUDFs.add(GenericUDFLength.class);
    supportedGenericUDFs.add(GenericUDFCharacterLength.class);
    supportedGenericUDFs.add(GenericUDFOctetLength.class);

    supportedGenericUDFs.add(UDFYear.class);
    supportedGenericUDFs.add(UDFMonth.class);
    supportedGenericUDFs.add(UDFDayOfMonth.class);
    supportedGenericUDFs.add(UDFDayOfWeek.class);
    supportedGenericUDFs.add(UDFHour.class);
    supportedGenericUDFs.add(UDFMinute.class);
    supportedGenericUDFs.add(UDFSecond.class);
    supportedGenericUDFs.add(UDFWeekOfYear.class);
    supportedGenericUDFs.add(GenericUDFToUnixTimeStamp.class);
    supportedGenericUDFs.add(GenericUDFFromUnixTime.class);

    supportedGenericUDFs.add(GenericUDFDateAdd.class);
    supportedGenericUDFs.add(GenericUDFDateSub.class);
    supportedGenericUDFs.add(GenericUDFDate.class);
    supportedGenericUDFs.add(GenericUDFDateDiff.class);

    supportedGenericUDFs.add(UDFLike.class);
    supportedGenericUDFs.add(GenericUDFRegExp.class);
    supportedGenericUDFs.add(UDFRegExpExtract.class);
    supportedGenericUDFs.add(UDFRegExpReplace.class);
    supportedGenericUDFs.add(UDFSubstr.class);
    supportedGenericUDFs.add(GenericUDFLTrim.class);
    supportedGenericUDFs.add(GenericUDFRTrim.class);
    supportedGenericUDFs.add(GenericUDFTrim.class);

    supportedGenericUDFs.add(UDFSin.class);
    supportedGenericUDFs.add(UDFSinh.class);
    supportedGenericUDFs.add(UDFCos.class);
    supportedGenericUDFs.add(UDFCosh.class);
    supportedGenericUDFs.add(UDFTan.class);
    supportedGenericUDFs.add(UDFTanh.class);
    supportedGenericUDFs.add(UDFAsin.class);
    supportedGenericUDFs.add(UDFAcos.class);
    supportedGenericUDFs.add(UDFAtan.class);
    supportedGenericUDFs.add(UDFDegrees.class);
    supportedGenericUDFs.add(UDFRadians.class);
    supportedGenericUDFs.add(GenericUDFFloor.class);
    supportedGenericUDFs.add(GenericUDFCeil.class);
    supportedGenericUDFs.add(UDFExp.class);
    supportedGenericUDFs.add(UDFLn.class);
    supportedGenericUDFs.add(UDFLog2.class);
    supportedGenericUDFs.add(UDFLog10.class);
    supportedGenericUDFs.add(UDFLog.class);
    supportedGenericUDFs.add(GenericUDFPower.class);
    supportedGenericUDFs.add(GenericUDFRound.class);
    supportedGenericUDFs.add(GenericUDFBRound.class);
    supportedGenericUDFs.add(GenericUDFPosMod.class);
    supportedGenericUDFs.add(UDFSqrt.class);
    supportedGenericUDFs.add(UDFSign.class);
    supportedGenericUDFs.add(UDFRand.class);
    supportedGenericUDFs.add(UDFBin.class);
    supportedGenericUDFs.add(UDFHex.class);
    supportedGenericUDFs.add(UDFConv.class);

    supportedGenericUDFs.add(GenericUDFLower.class);
    supportedGenericUDFs.add(GenericUDFUpper.class);
    supportedGenericUDFs.add(GenericUDFConcat.class);
    supportedGenericUDFs.add(GenericUDFAbs.class);
    supportedGenericUDFs.add(GenericUDFBetween.class);
    supportedGenericUDFs.add(GenericUDFIn.class);
    supportedGenericUDFs.add(GenericUDFCase.class);
    supportedGenericUDFs.add(GenericUDFWhen.class);
    supportedGenericUDFs.add(GenericUDFCoalesce.class);
    supportedGenericUDFs.add(GenericUDFElt.class);
    supportedGenericUDFs.add(GenericUDFInitCap.class);
    supportedGenericUDFs.add(GenericUDFInBloomFilter.class);
    supportedGenericUDFs.add(GenericUDFMurmurHash.class);

    // For type casts
    supportedGenericUDFs.add(UDFToLong.class);
    supportedGenericUDFs.add(UDFToInteger.class);
    supportedGenericUDFs.add(UDFToShort.class);
    supportedGenericUDFs.add(UDFToByte.class);
    supportedGenericUDFs.add(UDFToBoolean.class);
    supportedGenericUDFs.add(UDFToFloat.class);
    supportedGenericUDFs.add(UDFToDouble.class);
    supportedGenericUDFs.add(GenericUDFToString.class);
    supportedGenericUDFs.add(GenericUDFTimestamp.class);
    supportedGenericUDFs.add(GenericUDFToDecimal.class);
    supportedGenericUDFs.add(GenericUDFToDate.class);
    supportedGenericUDFs.add(GenericUDFToChar.class);
    supportedGenericUDFs.add(GenericUDFToVarchar.class);
    supportedGenericUDFs.add(GenericUDFToIntervalYearMonth.class);
    supportedGenericUDFs.add(GenericUDFToIntervalDayTime.class);

    // For conditional expressions
    supportedGenericUDFs.add(GenericUDFIf.class);
  }

  private class VectorTaskColumnInfo {
    List<String> allColumnNames;
    List<TypeInfo> allTypeInfos;
    List<Integer> dataColumnNums;

    int partitionColumnCount;
    List<VirtualColumn> availableVirtualColumnList;
    List<VirtualColumn> neededVirtualColumnList;
    //not to be confused with useVectorizedInputFileFormat at Vectorizer level
    //which represents the value of configuration hive.vectorized.use.vectorized.input.format
    private boolean useVectorizedInputFileFormat;

    Set<Support> inputFormatSupportSet;
    Set<Support> supportSetInUse;
    List<String> supportRemovedReasons;
    List<DataTypePhysicalVariation> allDataTypePhysicalVariations;

    boolean allNative;
    boolean usesVectorUDFAdaptor;

    String[] scratchTypeNameArray;
    DataTypePhysicalVariation[] scratchdataTypePhysicalVariations;

    String reduceColumnSortOrder;
    String reduceColumnNullOrder;

    VectorTaskColumnInfo() {
      partitionColumnCount = 0;
    }

    public void assume() {
      allNative = true;
      usesVectorUDFAdaptor =  false;
    }

    public void setAllColumnNames(List<String> allColumnNames) {
      this.allColumnNames = allColumnNames;
    }
    public void setAllTypeInfos(List<TypeInfo> allTypeInfos) {
      this.allTypeInfos = allTypeInfos;
    }
    public void setDataColumnNums(List<Integer> dataColumnNums) {
      this.dataColumnNums = dataColumnNums;
    }
    public void setPartitionColumnCount(int partitionColumnCount) {
      this.partitionColumnCount = partitionColumnCount;
    }
    public void setAvailableVirtualColumnList(List<VirtualColumn> availableVirtualColumnList) {
      this.availableVirtualColumnList = availableVirtualColumnList;
    }
    public void setNeededVirtualColumnList(List<VirtualColumn> neededVirtualColumnList) {
      this.neededVirtualColumnList = neededVirtualColumnList;
    }
    public void setSupportSetInUse(Set<Support> supportSetInUse) {
      this.supportSetInUse = supportSetInUse;
    }
    public void setSupportRemovedReasons(List<String> supportRemovedReasons) {
      this.supportRemovedReasons = supportRemovedReasons;
    }
    public void setAlldataTypePhysicalVariations(List<DataTypePhysicalVariation> allDataTypePhysicalVariations) {
      this.allDataTypePhysicalVariations = allDataTypePhysicalVariations;
    }
    public void setScratchTypeNameArray(String[] scratchTypeNameArray) {
      this.scratchTypeNameArray = scratchTypeNameArray;
    }
    public void setScratchdataTypePhysicalVariationsArray(DataTypePhysicalVariation[] scratchdataTypePhysicalVariations) {
      this.scratchdataTypePhysicalVariations = scratchdataTypePhysicalVariations;
    }
    public void setAllNative(boolean allNative) {
      this.allNative = allNative;
    }
    public void setUsesVectorUDFAdaptor(boolean usesVectorUDFAdaptor) {
      this.usesVectorUDFAdaptor = usesVectorUDFAdaptor;
    }
    public void setUseVectorizedInputFileFormat(boolean useVectorizedInputFileFormat) {
      this.useVectorizedInputFileFormat = useVectorizedInputFileFormat;
    }
    public void setInputFormatSupportSet(Set<Support> inputFormatSupportSet) {
      this.inputFormatSupportSet = inputFormatSupportSet;
    }

    public void setReduceColumnSortOrder(String reduceColumnSortOrder) {
      this.reduceColumnSortOrder = reduceColumnSortOrder;
    }

    public void setReduceColumnNullOrder(String reduceColumnNullOrder) {
      this.reduceColumnNullOrder = reduceColumnNullOrder;
    }

    public void transferToBaseWork(BaseWork baseWork) {

      final int virtualColumnCount =
          (availableVirtualColumnList == null ? 0 : availableVirtualColumnList.size());
      VirtualColumn[] neededVirtualColumns;
      if (neededVirtualColumnList != null && neededVirtualColumnList.size() > 0) {
        neededVirtualColumns = neededVirtualColumnList.toArray(new VirtualColumn[0]);
      } else {
        neededVirtualColumns = new VirtualColumn[0];
      }

      String[] allColumnNameArray = allColumnNames.toArray(new String[0]);
      TypeInfo[] allTypeInfoArray = allTypeInfos.toArray(new TypeInfo[0]);
      int[] dataColumnNumsArray;
      if (dataColumnNums != null) {
        dataColumnNumsArray = ArrayUtils.toPrimitive(dataColumnNums.toArray(new Integer[0]));
      } else {
        dataColumnNumsArray = null;
      }

      DataTypePhysicalVariation[] allDataTypePhysicalVariationArray;
      if (allDataTypePhysicalVariations == null) {
        allDataTypePhysicalVariationArray = new DataTypePhysicalVariation[allTypeInfoArray.length];
        Arrays.fill(allDataTypePhysicalVariationArray, DataTypePhysicalVariation.NONE);
      } else {
        allDataTypePhysicalVariationArray =
            allDataTypePhysicalVariations.toArray(new DataTypePhysicalVariation[0]);
      }

      VectorizedRowBatchCtx vectorizedRowBatchCtx =
          new VectorizedRowBatchCtx(
            allColumnNameArray,
            allTypeInfoArray,
            allDataTypePhysicalVariationArray,
            dataColumnNumsArray,
            partitionColumnCount,
            virtualColumnCount,
            neededVirtualColumns,
            scratchTypeNameArray,
            scratchdataTypePhysicalVariations);
      baseWork.setVectorizedRowBatchCtx(vectorizedRowBatchCtx);

      if (baseWork instanceof MapWork) {
        MapWork mapWork = (MapWork) baseWork;
        mapWork.setUseVectorizedInputFileFormat(useVectorizedInputFileFormat);
        mapWork.setInputFormatSupportSet(inputFormatSupportSet);
        mapWork.setSupportSetInUse(supportSetInUse);
        mapWork.setSupportRemovedReasons(supportRemovedReasons);
      }

      if (baseWork instanceof ReduceWork) {
        ReduceWork reduceWork = (ReduceWork) baseWork;
        reduceWork.setVectorReduceColumnSortOrder(reduceColumnSortOrder);
        reduceWork.setVectorReduceColumnNullOrder(reduceColumnNullOrder);
      }

      baseWork.setAllNative(allNative);
      baseWork.setUsesVectorUDFAdaptor(usesVectorUDFAdaptor);

      baseWork.setIsTestForcedVectorizationEnable(isTestForcedVectorizationEnable);
      baseWork.setIsTestVectorizationSuppressExplainExecutionMode(
          isTestVectorizationSuppressExplainExecutionMode);
    }
  }

  /*
   * Used as a dummy root operator to attach vectorized operators that will be built in parallel
   * to the current non-vectorized operator tree.
   */
  private static class DummyRootVectorDesc extends AbstractOperatorDesc {

    public DummyRootVectorDesc() {
      super();
    }
  }

  private static class DummyOperator extends Operator<DummyRootVectorDesc> {

    public DummyOperator() {
      super(new CompilationOpContext());
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
      throw new RuntimeException("Not used");
    }

    @Override
    public String getName() {
      return "DUMMY";
    }

    @Override
    public OperatorType getType() {
      return null;
    }
  }

  private static class DummyVectorOperator extends DummyOperator
      implements VectorizationOperator {

    private VectorizationContext vContext;

    public DummyVectorOperator(VectorizationContext vContext) {
      super();
      this.conf = new DummyRootVectorDesc();
      this.vContext = vContext;
    }

    @Override
    public VectorizationContext getInputVectorizationContext() {
      return vContext;
    }

    @Override
    public VectorDesc getVectorDesc() {
      return null;
    }
  }

  private static List<Operator<? extends OperatorDesc>> newOperatorList() {
    return new ArrayList<Operator<? extends OperatorDesc>>();
  }

  public static void debugDisplayJoinOperatorTree(Operator<? extends OperatorDesc> joinOperator,
      String prefix) {
    List<Operator<? extends OperatorDesc>> currentParentList = newOperatorList();
    currentParentList.add(joinOperator);

    int depth = 0;
    do {
      List<Operator<? extends OperatorDesc>> nextParentList = newOperatorList();

      final int count = currentParentList.size();
      for (int i = 0; i < count; i++) {
        Operator<? extends OperatorDesc> parent = currentParentList.get(i);
        System.out.println(prefix + " parent depth " + depth + " " +
            parent.getClass().getSimpleName() + " " + parent.toString());

        List<Operator<? extends OperatorDesc>> parentList = parent.getParentOperators();
        if (parentList == null || parentList.size() == 0) {
          continue;
        }

        nextParentList.addAll(parentList);
      }

      currentParentList = nextParentList;
      depth--;
    } while (currentParentList.size() > 0);

    List<Operator<? extends OperatorDesc>> currentChildList = newOperatorList();
    currentChildList.addAll(joinOperator.getChildOperators());

    depth = 1;
    do {
      List<Operator<? extends OperatorDesc>> nextChildList = newOperatorList();

      final int count = currentChildList.size();
      for (int i = 0; i < count; i++) {
        Operator<? extends OperatorDesc> child = currentChildList.get(i);
        System.out.println(prefix + " child depth " + depth + " " +
            child.getClass().getSimpleName() + " " + child.toString());

        List<Operator<? extends OperatorDesc>> childList = child.getChildOperators();
        if (childList == null || childList.size() == 0) {
          continue;
        }

        nextChildList.addAll(childList);
      }

      currentChildList = nextChildList;
      depth--;
    } while (currentChildList.size() > 0);
  }

  private Operator<? extends OperatorDesc> validateAndVectorizeOperatorTree(
      Operator<? extends OperatorDesc> nonVecRootOperator,
      boolean isReduce, boolean isTez,
      VectorTaskColumnInfo vectorTaskColumnInfo)
          throws VectorizerCannotVectorizeException {

    VectorizationContext taskVContext =
        new VectorizationContext(
            "Task",
            vectorTaskColumnInfo.allColumnNames,
            vectorTaskColumnInfo.allTypeInfos,
            vectorTaskColumnInfo.allDataTypePhysicalVariations,
            hiveConf);

    List<Operator<? extends OperatorDesc>> currentParentList = newOperatorList();
    currentParentList.add(nonVecRootOperator);

    // Start with dummy vector operator as the parent of the parallel vector operator tree we are
    // creating
    Operator<? extends OperatorDesc> dummyVectorOperator = new DummyVectorOperator(taskVContext);
    List<Operator<? extends OperatorDesc>> currentVectorParentList = newOperatorList();
    currentVectorParentList.add(dummyVectorOperator);

    delayedFixups.clear();

    do {
      List<Operator<? extends OperatorDesc>> nextParentList = newOperatorList();
      List<Operator<? extends OperatorDesc>> nextVectorParentList= newOperatorList();

      final int count = currentParentList.size();
      for (int i = 0; i < count; i++) {
        Operator<? extends OperatorDesc> parent = currentParentList.get(i);

        List<Operator<? extends OperatorDesc>> childrenList = parent.getChildOperators();
        if (childrenList == null || childrenList.size() == 0) {
          continue;
        }

        Operator<? extends OperatorDesc> vectorParent = currentVectorParentList.get(i);

        /*
         * Vectorize this parent's children.  Plug them into vectorParent's children list.
         *
         * Add those children / vector children to nextParentList / nextVectorParentList.
         */
        doProcessChildren(
            parent, vectorParent, nextParentList, nextVectorParentList,
            isReduce, isTez, vectorTaskColumnInfo);

      }
      currentParentList = nextParentList;
      currentVectorParentList = nextVectorParentList;
    } while (currentParentList.size() > 0);

    runDelayedFixups();

    return dummyVectorOperator;
  }

  private void doProcessChildren(
      Operator<? extends OperatorDesc> parent,
      Operator<? extends OperatorDesc> vectorParent,
      List<Operator<? extends OperatorDesc>> nextParentList,
      List<Operator<? extends OperatorDesc>> nextVectorParentList,
      boolean isReduce, boolean isTez,
      VectorTaskColumnInfo vectorTaskColumnInfo)
          throws VectorizerCannotVectorizeException {

    List<Operator<? extends OperatorDesc>> children = parent.getChildOperators();

    final int childrenCount = children.size();
    for (int i = 0; i < childrenCount; i++) {

      Operator<? extends OperatorDesc> child = children.get(i);
      Operator<? extends OperatorDesc> vectorChild =
          doProcessChild(
              child, vectorParent, isReduce, isTez, vectorTaskColumnInfo);

      fixupNewVectorChild(
          parent,
          vectorParent,
          child,
          vectorChild);

      nextParentList.add(child);
      nextVectorParentList.add(vectorChild);
    }
  }

  /*
   * Fixup the children and parents of a new vector child.
   *
   * 1) Add new vector child to the vector parent's children list.
   *
   * 2) Copy and fixup the parent list of the original child instead of just assuming a 1:1
   *    relationship.
   *
   *    a) When the child is MapJoinOperator, it will have an extra parent HashTableDummyOperator
   *       for the MapJoinOperator's small table.  It needs to be fixed up, too.
   */
  private void fixupNewVectorChild(
      Operator<? extends OperatorDesc> parent,
      Operator<? extends OperatorDesc> vectorParent,
      Operator<? extends OperatorDesc> child,
      Operator<? extends OperatorDesc> vectorChild) {

    // 1) Add new vector child to the vector parent's children list.
    vectorParent.getChildOperators().add(vectorChild);

    // 2) Copy and fixup the parent list of the original child instead of just assuming a 1:1
    //    relationship.
    List<Operator<? extends OperatorDesc>> childMultipleParents = newOperatorList();
    childMultipleParents.addAll(child.getParentOperators());
    final int childMultipleParentCount = childMultipleParents.size();
    for (int i = 0; i < childMultipleParentCount; i++) {
      Operator<? extends OperatorDesc> childMultipleParent = childMultipleParents.get(i);
      if (childMultipleParent == parent) {
        childMultipleParents.set(i, vectorParent);
      } else {
        queueDelayedFixup(childMultipleParent, child, vectorChild);
      }
    }
    vectorChild.setParentOperators(childMultipleParents);
  }

  /*
   * The fix up is delayed so that the parent operators aren't modified until the entire operator
   * tree has been vectorized.
   */
  private void queueDelayedFixup(Operator<? extends OperatorDesc> parent,
      Operator<? extends OperatorDesc> child, Operator<? extends OperatorDesc> vectorChild) {
    if (delayedFixups.get(parent) == null) {
      HashSet<ImmutablePair<Operator<? extends OperatorDesc>, Operator<? extends OperatorDesc>>> value =
          new HashSet<ImmutablePair<Operator<? extends OperatorDesc>, Operator<? extends OperatorDesc>>>(1);
      delayedFixups.put(parent, value);
    }
    delayedFixups.get(parent).add(
        new ImmutablePair<Operator<? extends OperatorDesc>, Operator<? extends OperatorDesc>>(
            child, vectorChild));
  }

  private void runDelayedFixups() {
    for (Entry<Operator<? extends OperatorDesc>, Set<ImmutablePair<Operator<? extends OperatorDesc>, Operator<? extends OperatorDesc>>>> delayed
        : delayedFixups.entrySet()) {
      Operator<? extends OperatorDesc> key = delayed.getKey();
      Set<ImmutablePair<Operator<? extends OperatorDesc>, Operator<? extends OperatorDesc>>> value =
          delayed.getValue();
      for (ImmutablePair<Operator<? extends OperatorDesc>, Operator<? extends OperatorDesc>> swap : value) {
        fixupOtherParent(key, swap.getLeft(), swap.getRight());
      }
    }
    delayedFixups.clear();
  }

  private void fixupOtherParent(
      Operator<? extends OperatorDesc> childMultipleParent,
      Operator<? extends OperatorDesc> child,
      Operator<? extends OperatorDesc> vectorChild) {

    List<Operator<? extends OperatorDesc>> children = childMultipleParent.getChildOperators();
    final int childrenCount = children.size();
    for (int i = 0; i < childrenCount; i++) {
      Operator<? extends OperatorDesc> myChild = children.get(i);
      if (myChild == child) {
        children.set(i, vectorChild);
      }
    }
  }

  private Operator<? extends OperatorDesc> doProcessChild(
      Operator<? extends OperatorDesc> child,
      Operator<? extends OperatorDesc> vectorParent,
      boolean isReduce, boolean isTez,
      VectorTaskColumnInfo vectorTaskColumnInfo)
          throws VectorizerCannotVectorizeException {

    // Use vector parent to get VectorizationContext.
    final VectorizationContext vContext;
    if (vectorParent instanceof VectorizationContextRegion) {
      vContext = ((VectorizationContextRegion) vectorParent).getOutputVectorizationContext();
    } else {
      vContext = ((VectorizationOperator) vectorParent).getInputVectorizationContext();
    }

    Operator<? extends OperatorDesc> vectorChild;

    try {
      vectorChild =
          validateAndVectorizeOperator(child, vContext, isReduce, isTez, vectorTaskColumnInfo);
    } catch (HiveException e) {
      String issue = "exception: " + VectorizationContext.getStackTraceAsSingleLine(e);
      setNodeIssue(issue);
      throw new VectorizerCannotVectorizeException();
    }

    return vectorChild;
  }

  class VectorizationDispatcher implements SemanticDispatcher {

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
        throws SemanticException {
      Task<?> currTask = (Task<?>) nd;
      if (currTask instanceof MapRedTask) {
        MapredWork mapredWork = ((MapRedTask) currTask).getWork();

        MapWork mapWork = mapredWork.getMapWork();
        setMapWorkExplainConditions(mapWork);
        convertMapWork(mapredWork.getMapWork(), /* isTez */ false);
        logMapWorkExplainVectorization(mapWork);

        ReduceWork reduceWork = mapredWork.getReduceWork();
        if (reduceWork != null) {

          // Always set the EXPLAIN conditions.
          setReduceWorkExplainConditions(reduceWork);

          // We do not vectorize MR Reduce.

          logReduceWorkExplainVectorization(reduceWork);
        }
      } else if (currTask instanceof TezTask) {
        TezWork work = ((TezTask) currTask).getWork();
        for (BaseWork baseWork: work.getAllWork()) {
          if (baseWork instanceof MapWork) {
            MapWork mapWork = (MapWork) baseWork;
            setMapWorkExplainConditions(mapWork);
            convertMapWork(mapWork, /* isTez */ true);
            logMapWorkExplainVectorization(mapWork);
          } else if (baseWork instanceof ReduceWork) {
            ReduceWork reduceWork = (ReduceWork) baseWork;

            // Always set the EXPLAIN conditions.
            setReduceWorkExplainConditions(reduceWork);

            // We are only vectorizing Reduce under Tez.
            if (isReduceVectorizationEnabled) {
              convertReduceWork(reduceWork);
            }

            logReduceWorkExplainVectorization(reduceWork);
          } else if (baseWork instanceof MergeJoinWork){
            MergeJoinWork mergeJoinWork = (MergeJoinWork) baseWork;

            // Always set the EXPLAIN conditions.
            setMergeJoinWorkExplainConditions(mergeJoinWork);

            logMergeJoinWorkExplainVectorization(mergeJoinWork);
          }
        }
      } else if (currTask instanceof FetchTask) {
        LOG.info("Vectorizing Fetch not supported");
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ignoring vectorization of " + currTask.getClass().getSimpleName());
        }
      }

      return null;
    }

    private void setExplainConditions(BaseWork baseWork) {

      // Global used when setting errors, etc.
      currentBaseWork = baseWork;

      baseWork.setVectorizedVertexNum(++vectorizedVertexNum);
      baseWork.setVectorizationExamined(true);
    }

    private void setMapWorkExplainConditions(MapWork mapWork) {
      setExplainConditions(mapWork);
    }

    private void setReduceWorkExplainConditions(ReduceWork reduceWork) {

      setExplainConditions(reduceWork);

      reduceWork.setReduceVectorizationEnabled(isReduceVectorizationEnabled);
      reduceWork.setVectorReduceEngine(
          HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE));
    }

    private void setMergeJoinWorkExplainConditions(MergeJoinWork mergeJoinWork) {

      setExplainConditions(mergeJoinWork);
    }

    private boolean logExplainVectorization(BaseWork baseWork, String name) {

      if (!baseWork.getVectorizationExamined()) {
        return false;
      }

      LOG.info(name + " vectorization enabled: " + baseWork.getVectorizationEnabled());
      boolean isVectorized = baseWork.getVectorMode();
      LOG.info(name + " vectorized: " + isVectorized);
      if (!isVectorized) {
        VectorizerReason notVectorizedReason = baseWork.getNotVectorizedReason();
        if (notVectorizedReason != null) {
          LOG.info(name + " notVectorizedReason: " + notVectorizedReason.toString());
        }
      }
      LOG.info(name + " vectorizedVertexNum: " + baseWork.getVectorizedVertexNum());

      if (LOG.isDebugEnabled() && isVectorized) {
        VectorizedRowBatchCtx batchContext = baseWork.getVectorizedRowBatchCtx();
        LOG.debug(name + " dataColumnCount: " + batchContext.getDataColumnCount());
        int[] dataColumnNums = batchContext.getDataColumnNums();
        if (dataColumnNums != null) {
          LOG.debug(name + " includeColumns: " + Arrays.toString(dataColumnNums));
        }
        LOG.debug(name + " partitionColumnCount: " + batchContext.getPartitionColumnCount());
        LOG.debug(name + " dataColumns: " +
            BaseWork.BaseExplainVectorization.getColumns(
                batchContext, 0, batchContext.getDataColumnCount()));
        LOG.debug(name + " scratchColumnTypeNames: " +
            BaseWork.BaseExplainVectorization.getScratchColumns(batchContext));
        VirtualColumn[] neededVirtualColumns = batchContext.getNeededVirtualColumns();
        if (neededVirtualColumns != null && neededVirtualColumns.length != 0) {
          LOG.debug(name + " neededVirtualColumns: " + Arrays.toString(neededVirtualColumns));
        }
      }
      return true;
    }

    private void logMapWorkExplainVectorization(MapWork mapWork) {

      if (!logExplainVectorization(mapWork, "Map")) {
        return;
      }

      // Conditions.
      List<String> enabledConditionsMet = mapWork.getVectorizationEnabledConditionsMet();
      if (enabledConditionsMet != null && !enabledConditionsMet.isEmpty()) {
        LOG.info("Map enabledConditionsMet: " + enabledConditionsMet.toString());
      }
      List<String> enabledConditionsNotMet = mapWork.getVectorizationEnabledConditionsNotMet();
      if (enabledConditionsNotMet != null && !enabledConditionsNotMet.isEmpty()) {
        LOG.info("Map enabledConditionsNotMet: " + enabledConditionsNotMet.toString());
      }
      Set<String> inputFileFormatClassNameSet =
          mapWork.getVectorizationInputFileFormatClassNameSet();
      if (inputFileFormatClassNameSet != null && !inputFileFormatClassNameSet.isEmpty()) {
        LOG.info("Map inputFileFormatClassNameSet: " + inputFileFormatClassNameSet.toString());
      }
    }

    private void logReduceWorkExplainVectorization(ReduceWork reduceWork) {

      if (!logExplainVectorization(reduceWork, "Reduce")) {
        return;
      }

      // Conditions.
      LOG.info("Reducer " + HiveConf.ConfVars.HIVE_VECTORIZATION_REDUCE_ENABLED.varname +
          ": " + reduceWork.getReduceVectorizationEnabled());
      LOG.info("Reducer engine: " + reduceWork.getVectorReduceEngine());
    }

    private void logMergeJoinWorkExplainVectorization(MergeJoinWork mergeJoinWork) {

      if (!logExplainVectorization(mergeJoinWork, "MergeJoin")) {
        return;
      }
    }

    private void convertMapWork(MapWork mapWork, boolean isTez) throws SemanticException {

      // We have to evaluate the input format to see if vectorization is enabled, so
      // we do not set it right here.

      VectorTaskColumnInfo vectorTaskColumnInfo = new VectorTaskColumnInfo();
      vectorTaskColumnInfo.assume();

      validateAndVectorizeMapWork(mapWork, vectorTaskColumnInfo, isTez);
    }

    /*
     * Determine if there is only one TableScanOperator.  Currently in Map vectorization, we do not
     * try to vectorize multiple input trees.
     */
    private ImmutablePair<String, TableScanOperator> verifyOnlyOneTableScanOperator(MapWork mapWork) {

      // Eliminate MR plans with more than one TableScanOperator.

      Map<String, Operator<? extends OperatorDesc>> aliasToWork = mapWork.getAliasToWork();
      if ((aliasToWork == null) || (aliasToWork.size() == 0)) {
        setNodeIssue("Vectorized map work requires work");
        return null;
      }
      int tableScanCount = 0;
      String alias = "";
      TableScanOperator tableScanOperator = null;
      for (Entry<String, Operator<? extends OperatorDesc>> entry : aliasToWork.entrySet()) {
        Operator<?> op = entry.getValue();
        if (op == null) {
          setNodeIssue("Vectorized map work requires a valid alias");
          return null;
        }
        if (op instanceof TableScanOperator) {
          tableScanCount++;
          alias = entry.getKey();
          tableScanOperator = (TableScanOperator) op;
        }
      }
      if (tableScanCount > 1) {
        setNodeIssue("Vectorized map work only works with 1 TableScanOperator");
        return null;
      }
      return new ImmutablePair<>(alias, tableScanOperator);
    }

    private void getTableScanOperatorSchemaInfo(TableScanOperator tableScanOperator,
        List<String> logicalColumnNameList, List<TypeInfo> logicalTypeInfoList,
        List<VirtualColumn> availableVirtualColumnList) {

      // Add all columns to make a vectorization context for
      // the TableScan operator.
      RowSchema rowSchema = tableScanOperator.getSchema();
      for (ColumnInfo c : rowSchema.getSignature()) {

        // Validation will later exclude vectorization of virtual columns usage if necessary.
        String columnName = c.getInternalName();

        // Turns out partition columns get marked as virtual in ColumnInfo, so we need to
        // check the VirtualColumn directly.
        VirtualColumn virtualColumn = VirtualColumn.VIRTUAL_COLUMN_NAME_MAP.get(columnName);
        if (virtualColumn != null) {

          // The planner gives us a subset virtual columns available for this table scan.
          //    AND
          // We only support some virtual columns in vectorization.
          //
          // So, create the intersection.  Note these are available vectorizable virtual columns.
          // Later we remember which virtual columns were *actually used* in the query so
          // just those will be included in the Map VectorizedRowBatchCtx that has the
          // information for creating the Map VectorizedRowBatch.
          //
          if (!vectorizableVirtualColumns.contains(virtualColumn)) {
            continue;
          }
          if (virtualColumn == VirtualColumn.ROWID && !isVectorizedRowIdentifierEnabled) {
            continue;
          }
          availableVirtualColumnList.add(virtualColumn);
        }

        // All columns: data, partition, and virtual are added.
        logicalColumnNameList.add(columnName);
        logicalTypeInfoList.add(TypeInfoUtils.getTypeInfoFromTypeString(c.getTypeName()));
      }
    }

    private void determineDataColumnNums(TableScanOperator tableScanOperator,
        List<String> allColumnNameList, int dataColumnCount, List<Integer> dataColumnNums) {

      /*
       * The TableScanOperator's needed columns are just the data columns.
       */
      Set<String> neededColumns = new HashSet<String>(tableScanOperator.getNeededColumns());

      for (int dataColumnNum = 0; dataColumnNum < dataColumnCount; dataColumnNum++) {
        String columnName = allColumnNameList.get(dataColumnNum);
        if (neededColumns.contains(columnName)) {
          dataColumnNums.add(dataColumnNum);
        }
      }
    }

    private Support[] getVectorizedInputFormatSupports(Class<? extends InputFormat> inputFileFormatClass,
        TableDesc properties) {

      try {
        InputFormat inputFormat = FetchOperator.getInputFormatFromCache(inputFileFormatClass, hiveConf);
        if (inputFormat instanceof VectorizedInputFormatInterface) {
          return ((VectorizedInputFormatInterface) inputFormat).getSupportedFeatures(hiveConf, properties);
        }
      } catch (IOException e) {
        LOG.error("Unable to instantiate input format class. Cannot determine vectorization support.", e);
      }
      // FUTURE: Decide how to ask an input file format what vectorization features it supports.
      return null;
    }

    /*
     * Add the support of the VectorizedInputFileFormatInterface.
     */
    private void addVectorizedInputFileFormatSupport(Set<Support> newSupportSet, boolean isInputFileFormatVectorized,
        Class<? extends InputFormat> inputFileFormatClass, TableDesc tableDesc) {

      final Support[] supports;
      if (isInputFileFormatVectorized) {
        supports = getVectorizedInputFormatSupports(inputFileFormatClass, tableDesc);
      } else {
        supports = null;
      }
      if (supports == null) {
        // No support.
      } else {
        for (Support support : supports) {
          newSupportSet.add(support);
        }
      }
    }

    private void handleSupport(
        boolean isFirstPartition, Set<Support> inputFormatSupportSet, Set<Support> newSupportSet) {
      if (isFirstPartition) {
        inputFormatSupportSet.addAll(newSupportSet);
      } else if (!newSupportSet.equals(inputFormatSupportSet)){
        // Do the intersection so only support in both is kept.
        inputFormatSupportSet.retainAll(newSupportSet);
      }
    }

    /*
     * Add a vector partition descriptor to partition descriptor, removing duplicate object.
     *
     * If the same vector partition descriptor has already been allocated, share that object.
     */
    private void addVectorPartitionDesc(PartitionDesc pd, VectorPartitionDesc vpd,
        Map<VectorPartitionDesc, VectorPartitionDesc> vectorPartitionDescMap) {

      VectorPartitionDesc existingEle = vectorPartitionDescMap.get(vpd);
      if (existingEle != null) {

        // Use the object we already have.
        vpd = existingEle;
      } else {
        vectorPartitionDescMap.put(vpd, vpd);
      }
      pd.setVectorPartitionDesc(vpd);
    }

    /*
     * There are 3 modes of reading for vectorization:
     *
     *   1) One for the Vectorized Input File Format which returns VectorizedRowBatch as the row.
     *
     *   2) One for using VectorDeserializeRow to deserialize each row into the VectorizedRowBatch.
     *      Currently, these Input File Formats:
     *        TEXTFILE
     *        SEQUENCEFILE
     *
     *   3) And one using the regular partition deserializer to get the row object and assigning
     *      the row object into the VectorizedRowBatch with VectorAssignRow.
     *      This picks up Input File Format not supported by the other two.
     */
    private boolean verifyAndSetVectorPartDesc(
        PartitionDesc pd, boolean isFullAcidTable,
        List<TypeInfo> allTypeInfoList,
        Set<String> inputFileFormatClassNameSet,
        Map<VectorPartitionDesc, VectorPartitionDesc> vectorPartitionDescMap,
        Set<String> enabledConditionsMetSet, List<String> enabledConditionsNotMetList,
        Set<Support> newSupportSet, List<TypeInfo> dataTypeInfoList) {

      Class<? extends InputFormat> inputFileFormatClass = pd.getInputFileFormatClass();
      String inputFileFormatClassName = inputFileFormatClass.getName();
      final TypeInfo[] dataTypeInfos;
      if (dataTypeInfoList == null) {
        dataTypeInfos = EMPTY_TYPEINFO_ARRAY;
      } else {
        dataTypeInfos = dataTypeInfoList.toArray(new TypeInfo[dataTypeInfoList.size()]);
      }

      // Always collect input file formats.
      inputFileFormatClassNameSet.add(inputFileFormatClassName);

      boolean isInputFileFormatVectorized = Utilities.isInputFileFormatVectorized(pd);

      if (isFullAcidTable) {

        // Today, ACID tables are only ORC and that format is vectorizable.  Verify these
        // assumptions.
        Preconditions.checkState(isInputFileFormatVectorized);
        Preconditions.checkState(supportedAcidInputFormats.contains(inputFileFormatClassName));

        if (!useVectorizedInputFileFormat) {
          enabledConditionsNotMetList.add("Vectorizing ACID tables requires "
        + HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTORIZED_INPUT_FILE_FORMAT.varname);
          return false;
        }

        addVectorizedInputFileFormatSupport(
            newSupportSet, isInputFileFormatVectorized, inputFileFormatClass, pd.getTableDesc());

        addVectorPartitionDesc(
            pd,
            VectorPartitionDesc.createVectorizedInputFileFormat(
                inputFileFormatClassName,
                Utilities.isInputFileFormatSelfDescribing(pd),
                dataTypeInfos),
            vectorPartitionDescMap);

        enabledConditionsMetSet.add(HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTORIZED_INPUT_FILE_FORMAT.varname);
        return true;
      }

      // Look for Pass-Thru case where InputFileFormat has VectorizedInputFormatInterface
      // and reads VectorizedRowBatch as a "row".

      if (useVectorizedInputFileFormat) {

        if (isInputFileFormatVectorized &&
            !isInputFormatExcluded(
                inputFileFormatClassName,
                vectorizedInputFormatExcludes) &&
            !hasUnsupportedVectorizedParquetDataType(
                inputFileFormatClass,
                allTypeInfoList)) {

          addVectorizedInputFileFormatSupport(
              newSupportSet, isInputFileFormatVectorized, inputFileFormatClass, pd.getTableDesc());

          addVectorPartitionDesc(
              pd,
              VectorPartitionDesc.createVectorizedInputFileFormat(
                  inputFileFormatClassName,
                  Utilities.isInputFileFormatSelfDescribing(pd),
                  dataTypeInfos),
              vectorPartitionDescMap);

          enabledConditionsMetSet.add(
              HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTORIZED_INPUT_FILE_FORMAT.varname);
          return true;
        }
        // Fall through and look for other options...
      }

      if (!isSchemaEvolution) {
        enabledConditionsNotMetList.add(
            "Vectorizing tables without Schema Evolution requires " + HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTORIZED_INPUT_FILE_FORMAT.varname);
        return false;
      }

      String deserializerClassName = pd.getDeserializerClassName();

      // Look for InputFileFormat / Serde combinations we can deserialize more efficiently
      // using VectorDeserializeRow and a deserialize class with the DeserializeRead interface.
      //
      // Do the "vectorized" row-by-row deserialization into a VectorizedRowBatch in the
      // VectorMapOperator.
      boolean isTextFormat = inputFileFormatClassName.equals(TextInputFormat.class.getName()) &&
          deserializerClassName.equals(LazySimpleSerDe.class.getName());
      boolean isSequenceFormat =
          inputFileFormatClassName.equals(SequenceFileInputFormat.class.getName()) &&
                  (deserializerClassName.equals(LazyBinarySerDe.class.getName()) ||
                          deserializerClassName.equals(LazyBinarySerDe2.class.getName()));
      boolean isVectorDeserializeEligable = isTextFormat || isSequenceFormat;

      if (useVectorDeserialize) {

        // Currently, we support LazySimple deserialization:
        //
        //    org.apache.hadoop.mapred.TextInputFormat
        //    org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
        //
        // AND
        //
        //    org.apache.hadoop.mapred.SequenceFileInputFormat
        //    org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe

        if (isTextFormat) {

          Properties properties = pd.getTableDesc().getProperties();
          String lastColumnTakesRestString =
              properties.getProperty(serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST);
          boolean lastColumnTakesRest =
              (lastColumnTakesRestString != null &&
              lastColumnTakesRestString.equalsIgnoreCase("true"));
          if (lastColumnTakesRest) {

            // If row mode will not catch this input file format, then not enabled.
            if (useRowDeserialize && !isInputFormatExcluded(inputFileFormatClassName,
                rowDeserializeInputFormatExcludes)) {
              enabledConditionsNotMetList.add(
                  inputFileFormatClassName + " " +
                  serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST + " must be disabled ");
              return false;
            }
          } else {

            // Add the support for read variations in Vectorized Text.
            newSupportSet.addAll(vectorDeserializeTextSupportSet);

            addVectorPartitionDesc(
                pd,
                VectorPartitionDesc.createVectorDeserialize(
                    inputFileFormatClassName, VectorDeserializeType.LAZY_SIMPLE, dataTypeInfos),
                vectorPartitionDescMap);

            enabledConditionsMetSet.add(HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTOR_DESERIALIZE.varname);
            return true;
          }
        } else if (isSequenceFormat) {

          addVectorPartitionDesc(
              pd,
              VectorPartitionDesc.createVectorDeserialize(
                  inputFileFormatClassName, VectorDeserializeType.LAZY_BINARY, dataTypeInfos),
              vectorPartitionDescMap);

          enabledConditionsMetSet.add(HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTOR_DESERIALIZE.varname);
          return true;
        }
        // Fall through and look for other options...
      }

      // Otherwise, if enabled, deserialize rows using regular Serde and add the object
      // inspect-able Object[] row to a VectorizedRowBatch in the VectorMapOperator.

      if (useRowDeserialize) {
        boolean isRowDeserializeExcluded =
            isInputFormatExcluded(inputFileFormatClassName, rowDeserializeInputFormatExcludes);
        if (!isRowDeserializeExcluded && !isInputFileFormatVectorized) {
          addVectorPartitionDesc(
              pd,
              VectorPartitionDesc.createRowDeserialize(
                  inputFileFormatClassName,
                  Utilities.isInputFileFormatSelfDescribing(pd),
                  deserializerClassName,
                  dataTypeInfos),
              vectorPartitionDescMap);

          enabledConditionsMetSet.add(HiveConf.ConfVars.HIVE_VECTORIZATION_USE_ROW_DESERIALIZE.varname);
          return true;
        } else if (isInputFileFormatVectorized) {

          /*
           * Vectorizer does not vectorize in row deserialize mode if the input format has
           * VectorizedInputFormat so input formats will be clear if the isVectorized flag
           * is on, they are doing VRB work.
           */
          enabledConditionsNotMetList.add("Row deserialization of vectorized input format not supported");
        } else {
          enabledConditionsNotMetList.add(ConfVars.HIVE_VECTORIZATION_USE_ROW_DESERIALIZE.varname
              + " IS true AND " + ConfVars.HIVE_VECTORIZATION_ROW_DESERIALIZE_INPUTFORMAT_EXCLUDES.varname
              + " NOT CONTAINS " + inputFileFormatClassName);
        }
      }
      if (isInputFileFormatVectorized) {
        if(useVectorizedInputFileFormat) {
          enabledConditionsNotMetList.add(
              ConfVars.HIVE_VECTORIZATION_USE_VECTORIZED_INPUT_FILE_FORMAT.varname + " IS true AND "
                  + ConfVars.HIVE_VECTORIZATION_VECTORIZED_INPUT_FILE_FORMAT_EXCLUDES.varname
                  + " NOT CONTAINS " + inputFileFormatClassName);
        } else {
          enabledConditionsNotMetList
              .add(HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTORIZED_INPUT_FILE_FORMAT.varname);
        }
      } else {
        // Only offer these when the input file format is not the fast vectorized formats.
        if (isVectorDeserializeEligable) {
          Preconditions.checkState(!useVectorDeserialize);
          enabledConditionsNotMetList.add(HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTOR_DESERIALIZE.varname);
        } else {
          // Since row mode takes everyone.
          enabledConditionsNotMetList.add(HiveConf.ConfVars.HIVE_VECTORIZATION_USE_ROW_DESERIALIZE.varname);
        }
      }

      return false;
    }

    private boolean shouldUseVectorizedInputFormat(Set<String> inputFileFormatClassNames) {
      if (inputFileFormatClassNames == null || inputFileFormatClassNames.isEmpty()
          || !useVectorizedInputFileFormat) {
        return useVectorizedInputFileFormat;
      }
      //Global config of vectorized input format is enabled; check if these inputformats are excluded
      for (String inputFormat : inputFileFormatClassNames) {
        if(isInputFormatExcluded(inputFormat, vectorizedInputFormatExcludes)) {
          return false;
        }
      }
      return true;
    }

    private boolean isInputFormatExcluded(String inputFileFormatClassName, Collection<Class<?>> excludes) {
      Class<?> ifClass = null;
      try {
        ifClass = Class.forName(inputFileFormatClassName);
      } catch (ClassNotFoundException e) {
        LOG.warn("Cannot verify class for " + inputFileFormatClassName, e);
        return true;
      }
      if(excludes == null || excludes.isEmpty()) {
        return false;
      }
      for (Class<?> badClass : excludes) {
        if (badClass.isAssignableFrom(ifClass)) {
          return true;
        }
      }
      return false;
    }

    private boolean hasUnsupportedVectorizedParquetDataType(
        Class<? extends InputFormat> inputFileFormatClass, List<TypeInfo> allTypeInfoList) {
      if (!inputFileFormatClass.equals(org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.class)) {
        return false;
      }

      /*
       * Currently, VectorizedParquetRecordReader cannot handle nested complex types.
       */
      for (TypeInfo typeInfo : allTypeInfoList) {
        if (!(typeInfo instanceof PrimitiveTypeInfo)) {
          switch (typeInfo.getCategory()) {
          case LIST:
            if (!(((ListTypeInfo) typeInfo).getListElementTypeInfo() instanceof PrimitiveTypeInfo)) {
              return true;
            }
            break;
          case MAP:
            {
              MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
              if (!(mapTypeInfo.getMapKeyTypeInfo() instanceof PrimitiveTypeInfo)) {
                return true;
              }
              if (!(mapTypeInfo.getMapValueTypeInfo() instanceof PrimitiveTypeInfo)) {
                return true;
              }
            }
            break;
          case STRUCT:
            {
              StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
              List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
              for (TypeInfo fieldTypeInfo : fieldTypeInfos) {
                if (!(fieldTypeInfo instanceof PrimitiveTypeInfo)) {
                  return true;
                }
              }
            }
            break;
          case UNION:
            // Not supported at all.
            return false;
          default:
            throw new RuntimeException(
                "Unsupported complex type category " + typeInfo.getCategory());
          }
        }
      }
      return false;
    }

    private void setValidateInputFormatAndSchemaEvolutionExplain(MapWork mapWork,
        Set<String> inputFileFormatClassNameSet,
        Map<VectorPartitionDesc, VectorPartitionDesc> vectorPartitionDescMap,
        Collection<String> enabledConditionsMetSet, Collection<String> enabledConditionsNotMetList) {
      mapWork.setVectorizationInputFileFormatClassNameSet(inputFileFormatClassNameSet);
      ArrayList<VectorPartitionDesc> vectorPartitionDescList = new ArrayList<VectorPartitionDesc>();
      vectorPartitionDescList.addAll(vectorPartitionDescMap.keySet());
      mapWork.setVectorPartitionDescList(vectorPartitionDescList);
      mapWork.setVectorizationEnabledConditionsMet(enabledConditionsMetSet);
      mapWork.setVectorizationEnabledConditionsNotMet(enabledConditionsNotMetList);
    }

    private ImmutablePair<Boolean, Boolean> validateInputFormatAndSchemaEvolution(MapWork mapWork, String alias,
        TableScanOperator tableScanOperator, VectorTaskColumnInfo vectorTaskColumnInfo)
            throws SemanticException {

      boolean isFullAcidTable = tableScanOperator.getConf().isFullAcidTable();

      // These names/types are the data columns plus partition columns.
      final List<String> allColumnNameList = new ArrayList<String>();
      final List<TypeInfo> allTypeInfoList = new ArrayList<TypeInfo>();

      final List<VirtualColumn> availableVirtualColumnList = new ArrayList<VirtualColumn>();

      getTableScanOperatorSchemaInfo(
          tableScanOperator,
          allColumnNameList, allTypeInfoList,
          availableVirtualColumnList);
      final int virtualColumnCount = availableVirtualColumnList.size();

      final List<Integer> dataColumnNums = new ArrayList<Integer>();

      final int dataAndPartColumnCount = allColumnNameList.size() - virtualColumnCount;

      /*
       * Validate input formats of all the partitions can be vectorized.
       */
      boolean isFirst = true;
      int dataColumnCount = 0;
      int partitionColumnCount = 0;

      List<String> tableDataColumnList = null;
      List<TypeInfo> tableDataTypeInfoList = null;

      Map<Path, List<String>> pathToAliases = mapWork.getPathToAliases();
      Map<Path, PartitionDesc> pathToPartitionInfo = mapWork.getPathToPartitionInfo();

      // Remember the input file formats we validated and why.
      Set<String> inputFileFormatClassNameSet = new HashSet<String>();
      Map<VectorPartitionDesc, VectorPartitionDesc> vectorPartitionDescMap =
          new LinkedHashMap<VectorPartitionDesc, VectorPartitionDesc>();
      Set<String> enabledConditionsMetSet = new HashSet<String>();
      List<String> enabledConditionsNotMetList = new ArrayList<String>();
      Set<Support> inputFormatSupportSet = new TreeSet<Support>();
      boolean outsideLoopIsFirstPartition = true;

      for (Entry<Path, List<String>> entry: pathToAliases.entrySet()) {
        final boolean isFirstPartition = outsideLoopIsFirstPartition;
        outsideLoopIsFirstPartition = false;

        Path path = entry.getKey();
        List<String> aliases = entry.getValue();
        boolean isPresent = (aliases != null && aliases.indexOf(alias) != -1);
        if (!isPresent) {
          setOperatorIssue("Alias " + alias + " not present in aliases " + aliases);
          return new ImmutablePair<Boolean,Boolean>(false, false);
        }
        // TODO: should this use getPartitionDescFromPathRecursively? That's what other code uses.
        PartitionDesc partDesc = pathToPartitionInfo.get(path);
        if (partDesc.getVectorPartitionDesc() != null) {
          // We've seen this already.
          continue;
        }
        Set<Support> newSupportSet = new TreeSet<Support>();

        final List<TypeInfo> nextDataTypeInfoList;
        final Deserializer deserializer;
        final StructObjectInspector partObjectInspector;
        try {
          deserializer = partDesc.getDeserializer(hiveConf);
          partObjectInspector = (StructObjectInspector) deserializer.getObjectInspector();
        } catch (Exception e) {
          throw new SemanticException(e);
        }

        if (isFirst) {

          /*
           * Determine the data and partition columns using the first partition descriptor's
           * partition count.  In other words, how to split the schema columns -- the
           * allColumnNameList and allTypeInfoList variables -- into the data and partition columns.
           */

          LinkedHashMap<String, String> partSpec = partDesc.getPartSpec();
          if (partSpec != null && partSpec.size() > 0) {
            partitionColumnCount = partSpec.size();
            dataColumnCount = dataAndPartColumnCount - partitionColumnCount;
          } else {
            partitionColumnCount = 0;
            dataColumnCount = dataAndPartColumnCount;
          }

          determineDataColumnNums(tableScanOperator, allColumnNameList, dataColumnCount,
              dataColumnNums);

          tableDataColumnList = allColumnNameList.subList(0, dataColumnCount);
          tableDataTypeInfoList = allTypeInfoList.subList(0, dataColumnCount);

          isFirst = false;
        }

        if (Utilities.isInputFileFormatSelfDescribing(partDesc)) {

          /*
           * Self-Describing Input Format will convert its data to the table schema. So, there
           * will be no VectorMapOperator conversion needed.
           */
          nextDataTypeInfoList = tableDataTypeInfoList;
        } else {
          String nextDataTypesString = ObjectInspectorUtils.getFieldTypes(partObjectInspector);

          /*
           * We convert to an array of TypeInfo using a library routine since it parses the
           * information and can handle use of different separators, etc.  We cannot use the
           * raw type string for comparison in the map because of the different separators used.
           */
          nextDataTypeInfoList =
              TypeInfoUtils.getTypeInfosFromTypeString(nextDataTypesString);
        }

        // HIVE-20419: Vectorization: Prevent mutation of VectorPartitionDesc after being used in a
        // hashmap key
        final boolean isVerifiedVectorPartDesc =
            verifyAndSetVectorPartDesc(
              partDesc, isFullAcidTable,
              allTypeInfoList,
              inputFileFormatClassNameSet,
              vectorPartitionDescMap,
              enabledConditionsMetSet, enabledConditionsNotMetList,
              newSupportSet,
              nextDataTypeInfoList);

        final VectorPartitionDesc vectorPartDesc = partDesc.getVectorPartitionDesc();

        if (!isVerifiedVectorPartDesc) {

          // Always set these so EXPLAIN can see.
          setValidateInputFormatAndSchemaEvolutionExplain(
              mapWork, inputFileFormatClassNameSet, vectorPartitionDescMap,
              enabledConditionsMetSet, enabledConditionsNotMetList);

          // We consider this an enable issue, not a not vectorized issue.
          return new ImmutablePair<Boolean,Boolean>(false, true);
        }

        handleSupport(isFirstPartition, inputFormatSupportSet, newSupportSet);

        // We need to get the partition's column names from the partition serde.
        // (e.g. Avro provides the table schema and ignores the partition schema..).
        //
        String nextDataColumnsString = ObjectInspectorUtils.getFieldNames(partObjectInspector);
        String[] nextDataColumns = nextDataColumnsString.split(",");
        List<String> nextDataColumnList = Arrays.asList(nextDataColumns);

        /*
         * Validate the column names that are present are the same.  Missing columns will be
         * implicitly defaulted to null.
         */
        if (nextDataColumnList.size() > tableDataColumnList.size()) {
          enabledConditionsNotMetList.add(
              String.format(
                  "Could not enable vectorization due to " +
                  "partition column names size %d is greater than the number of table column names size %d",
                  nextDataColumnList.size(), tableDataColumnList.size()));

          // Always set these so EXPLAIN can see.
          setValidateInputFormatAndSchemaEvolutionExplain(
              mapWork, inputFileFormatClassNameSet, vectorPartitionDescMap,
              enabledConditionsMetSet, enabledConditionsNotMetList);
          return new ImmutablePair<Boolean,Boolean>(false, true);
        }
        if (!(deserializer instanceof NullStructSerDe)) {

          // (Don't insist NullStructSerDe produce correct column names).
          for (int i = 0; i < nextDataColumnList.size(); i++) {
            String nextColumnName = nextDataColumnList.get(i);
            String tableColumnName = tableDataColumnList.get(i);
            if (!nextColumnName.equals(tableColumnName)) {
              enabledConditionsNotMetList.add(
                  String.format(
                      "Could not enable vectorization due to " +
                      "partition column name %s does not match table column name %s",
                      nextColumnName, tableColumnName));

              // Always set these so EXPLAIN can see.
              setValidateInputFormatAndSchemaEvolutionExplain(
                  mapWork, inputFileFormatClassNameSet, vectorPartitionDescMap,
                  enabledConditionsMetSet, enabledConditionsNotMetList);
              return new ImmutablePair<Boolean,Boolean>(false, true);
            }
          }
        }

        boolean isPartitionRowConversion = false;

        if (!vectorPartDesc.getIsInputFileFormatSelfDescribing()) {
          final int nextDataTypeInfoSize = nextDataTypeInfoList.size();
          if (nextDataTypeInfoSize > tableDataTypeInfoList.size()) {
            enabledConditionsNotMetList.add(
                String.format(
                    "Could not enable vectorization due to " +
                    "partition column types size %d is greater than the number of table column types size %d",
                    nextDataTypeInfoSize, tableDataTypeInfoList.size()));

            // Always set these so EXPLAIN can see.
            setValidateInputFormatAndSchemaEvolutionExplain(
                mapWork, inputFileFormatClassNameSet, vectorPartitionDescMap,
                enabledConditionsMetSet, enabledConditionsNotMetList);
            return new ImmutablePair<Boolean,Boolean>(false, true);
          }
          for (int i = 0; i < nextDataTypeInfoSize; i++) {
            TypeInfo tableDataTypeInfo = tableDataTypeInfoList.get(i);
            TypeInfo nextDataTypeInfo = nextDataTypeInfoList.get(i);

            // FUTURE: We be more sophisticated in our conversion check.
            if (!tableDataTypeInfo.equals(nextDataTypeInfo)) {
              isPartitionRowConversion = true;
              break;
            }
          }
        }
        if (isPartitionRowConversion && isLlapIoEnabled) {
          enabledConditionsNotMetList.add(
              "Could not enable vectorization. " +
              "LLAP I/O is enabled which automatically deserializes into " +
              "VECTORIZED_INPUT_FILE_FORMAT. " +
              "A partition requires data type conversion and that is not supported");

          // Always set these so EXPLAIN can see.
          setValidateInputFormatAndSchemaEvolutionExplain(
              mapWork, inputFileFormatClassNameSet, vectorPartitionDescMap,
              enabledConditionsMetSet, enabledConditionsNotMetList);
          return new ImmutablePair<Boolean,Boolean>(false, true);
        }
      }

      // For now, we don't know which virtual columns are going to be included.  We'll add them
      // later...
      vectorTaskColumnInfo.setAllColumnNames(allColumnNameList);
      vectorTaskColumnInfo.setAllTypeInfos(allTypeInfoList);

      vectorTaskColumnInfo.setDataColumnNums(dataColumnNums);
      vectorTaskColumnInfo.setPartitionColumnCount(partitionColumnCount);
      vectorTaskColumnInfo.setAvailableVirtualColumnList(availableVirtualColumnList);
      vectorTaskColumnInfo.setUseVectorizedInputFileFormat(
          shouldUseVectorizedInputFormat(inputFileFormatClassNameSet));

      vectorTaskColumnInfo.setInputFormatSupportSet(inputFormatSupportSet);

      // Always set these so EXPLAIN can see.
      mapWork.setVectorizationInputFileFormatClassNameSet(inputFileFormatClassNameSet);
      ArrayList<VectorPartitionDesc> vectorPartitionDescList = new ArrayList<VectorPartitionDesc>();
      vectorPartitionDescList.addAll(vectorPartitionDescMap.keySet());
      mapWork.setVectorPartitionDescList(vectorPartitionDescList);
      mapWork.setVectorizationEnabledConditionsMet(enabledConditionsMetSet);
      mapWork.setVectorizationEnabledConditionsNotMet(enabledConditionsNotMetList);

      return new ImmutablePair<Boolean,Boolean>(true, false);
    }

    private void validateAndVectorizeMapWork(MapWork mapWork, VectorTaskColumnInfo vectorTaskColumnInfo,
        boolean isTez) throws SemanticException {

      //--------------------------------------------------------------------------------------------

      LOG.info("Examining input format to see if vectorization is enabled.");

      ImmutablePair<String,TableScanOperator> onlyOneTableScanPair = verifyOnlyOneTableScanOperator(mapWork);
      if (onlyOneTableScanPair == null) {
        VectorizerReason notVectorizedReason = currentBaseWork.getNotVectorizedReason();
        Preconditions.checkState(notVectorizedReason != null);
        mapWork.setVectorizationEnabledConditionsNotMet(Collections.singleton(notVectorizedReason.toString()));
        return;
      }
      String alias = onlyOneTableScanPair.left;
      TableScanOperator tableScanOperator = onlyOneTableScanPair.right;

      // This call fills in the column names, types, and partition column count in
      // vectorTaskColumnInfo.
      currentOperator = tableScanOperator;
      ImmutablePair<Boolean, Boolean> validateInputFormatAndSchemaEvolutionPair =
          validateInputFormatAndSchemaEvolution(
              mapWork, alias, tableScanOperator, vectorTaskColumnInfo);
      if (!validateInputFormatAndSchemaEvolutionPair.left) {
        // Have we already set the enabled conditions not met?
        if (!validateInputFormatAndSchemaEvolutionPair.right) {
          VectorizerReason notVectorizedReason = currentBaseWork.getNotVectorizedReason();
          Preconditions.checkState(notVectorizedReason != null);
          mapWork.setVectorizationEnabledConditionsNotMet(Collections.singleton(notVectorizedReason.toString()));
        }
        return;
      }

      final int dataColumnCount =
          vectorTaskColumnInfo.allColumnNames.size() - vectorTaskColumnInfo.partitionColumnCount;

      /*
       * Take what all input formats support and eliminate any of them not enabled by
       * the Hive variable.
       */
      List<String> supportRemovedReasons = new ArrayList<String>();
      Set<Support> supportSet = new TreeSet<Support>();
      if (vectorTaskColumnInfo.inputFormatSupportSet != null) {
        supportSet.addAll(vectorTaskColumnInfo.inputFormatSupportSet);
      }
      // The retainAll method does set intersection.
      supportSet.retainAll(vectorizedInputFormatSupportEnabledSet);
      if (!supportSet.equals(vectorTaskColumnInfo.inputFormatSupportSet)) {

        Set<Support> removedSet = new TreeSet<Support>();
        removedSet.addAll(vectorizedInputFormatSupportEnabledSet);
        removedSet.removeAll(supportSet);
        String removeString =
            removedSet.toString() + " is disabled because it is not in " +
            HiveConf.ConfVars.HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_ENABLED.varname +
            " " + vectorizedInputFormatSupportEnabledSet.toString();
        supportRemovedReasons.add(removeString);
      }

      // Now remember what is supported for this query and any support that was
      // removed.
      vectorTaskColumnInfo.setSupportSetInUse(supportSet);
      vectorTaskColumnInfo.setSupportRemovedReasons(supportRemovedReasons);

      final boolean isSupportDecimal64 = supportSet.contains(Support.DECIMAL_64);
      List<DataTypePhysicalVariation> dataTypePhysicalVariations = new ArrayList<DataTypePhysicalVariation>();
      for (int i = 0; i < dataColumnCount; i++) {
        DataTypePhysicalVariation dataTypePhysicalVariation = DataTypePhysicalVariation.NONE;
        if (isSupportDecimal64) {
          TypeInfo typeInfo = vectorTaskColumnInfo.allTypeInfos.get(i);
          if (typeInfo instanceof DecimalTypeInfo) {
            DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
            if (HiveDecimalWritable.isPrecisionDecimal64(decimalTypeInfo.precision())) {
              dataTypePhysicalVariation = DataTypePhysicalVariation.DECIMAL_64;
            }
          }
        }
        dataTypePhysicalVariations.add(dataTypePhysicalVariation);
      }
      // It simplifies things to just add default ones for partitions.
      for (int i = 0; i < vectorTaskColumnInfo.partitionColumnCount; i++) {
        dataTypePhysicalVariations.add(DataTypePhysicalVariation.NONE);
      }
      vectorTaskColumnInfo.setAlldataTypePhysicalVariations(dataTypePhysicalVariations);

      // Set global member indicating which virtual columns are possible to be used by
      // the Map vertex.
      availableVectorizedVirtualColumnSet = new HashSet<VirtualColumn>();
      availableVectorizedVirtualColumnSet.addAll(vectorTaskColumnInfo.availableVirtualColumnList);

      // And, use set to remember which virtual columns were actually referenced.
      neededVirtualColumnSet = new HashSet<VirtualColumn>();

      mapWork.setVectorizationEnabled(true);
      LOG.info("Vectorization is enabled for input format(s) " + mapWork.getVectorizationInputFileFormatClassNameSet().toString());

      //--------------------------------------------------------------------------------------------

      /*
       * Validate and vectorize the Map operator tree.
       */
      if (!validateAndVectorizeMapOperators(mapWork, tableScanOperator, isTez, vectorTaskColumnInfo)) {
        return;
      }

      //--------------------------------------------------------------------------------------------

      vectorTaskColumnInfo.transferToBaseWork(mapWork);

      mapWork.setVectorMode(true);
    }

    private boolean validateAndVectorizeMapOperators(MapWork mapWork, TableScanOperator tableScanOperator,
        boolean isTez, VectorTaskColumnInfo vectorTaskColumnInfo) throws SemanticException {

      LOG.info("Validating and vectorizing MapWork... (vectorizedVertexNum " + vectorizedVertexNum + ")");

      // Set "global" member indicating where to store "not vectorized" information if necessary.
      currentBaseWork = mapWork;

      if (!validateTableScanOperator(tableScanOperator, mapWork)) {

        // The "not vectorized" information has been stored in the MapWork vertex.
        return false;
      }
      try {
        validateAndVectorizeMapOperators(tableScanOperator, isTez, vectorTaskColumnInfo);
      } catch (VectorizerCannotVectorizeException e) {

        // The "not vectorized" information has been stored in the MapWork vertex.
        return false;
      } catch (NullPointerException e) {
        if (!isTestVectorizerSuppressFatalExceptions) {
          // Re-throw without losing original stack trace.
          throw e;
        }
        setNodeIssue("exception: " + VectorizationContext.getStackTraceAsSingleLine(e));
        return false;
      } catch (ClassCastException e) {
        if (!isTestVectorizerSuppressFatalExceptions) {
          throw e;
        }
        setNodeIssue("exception: " + VectorizationContext.getStackTraceAsSingleLine(e));
        return false;
      } catch (RuntimeException e) {
        if (!isTestVectorizerSuppressFatalExceptions) {
          throw e;
        }
        setNodeIssue("exception: " + VectorizationContext.getStackTraceAsSingleLine(e));
        return false;
      }

      vectorTaskColumnInfo.setNeededVirtualColumnList(
          new ArrayList<VirtualColumn>(neededVirtualColumnSet));

      /*
       * The scratch column information was collected by the task VectorizationContext.  Go get it.
       */
      VectorizationContext vContext =
          ((VectorizationContextRegion) tableScanOperator).getOutputVectorizationContext();

      vectorTaskColumnInfo.setScratchTypeNameArray(
          vContext.getScratchColumnTypeNames());
      vectorTaskColumnInfo.setScratchdataTypePhysicalVariationsArray(
          vContext.getScratchDataTypePhysicalVariations());

      return true;
    }

    private void validateAndVectorizeMapOperators(TableScanOperator tableScanOperator,
        boolean isTez, VectorTaskColumnInfo vectorTaskColumnInfo)
            throws VectorizerCannotVectorizeException {

      Operator<? extends OperatorDesc> dummyVectorOperator =
          validateAndVectorizeOperatorTree(tableScanOperator, false, isTez, vectorTaskColumnInfo);

      // Fixup parent and child relations.
      List<Operator<? extends OperatorDesc>> vectorChildren = dummyVectorOperator.getChildOperators();
      tableScanOperator.setChildOperators(vectorChildren);

      final int vectorChildCount = vectorChildren.size();
      for (int i = 0; i < vectorChildCount; i++) {

        Operator<? extends OperatorDesc> vectorChild = vectorChildren.get(i);

        // Replace any occurrence of dummyVectorOperator with our TableScanOperator.
        List<Operator<? extends OperatorDesc>> vectorChildParents = vectorChild.getParentOperators();
        final int vectorChildParentCount = vectorChildParents.size();
        for (int p = 0; p < vectorChildParentCount; p++) {
          Operator<? extends OperatorDesc> vectorChildParent = vectorChildParents.get(p);
          if (vectorChildParent == dummyVectorOperator) {
            vectorChildParents.set(p, tableScanOperator);
          }
        }
      }

      // And, finally, save the VectorizationContext.
      tableScanOperator.setTaskVectorizationContext(
          ((VectorizationOperator) dummyVectorOperator).getInputVectorizationContext());

      // Modify TableScanOperator in-place so it knows to operate vectorized.
      vectorizeTableScanOperatorInPlace(tableScanOperator, vectorTaskColumnInfo);
    }

    /*
     * We are "committing" this vertex to be vectorized.
     */
    private void vectorizeTableScanOperatorInPlace(TableScanOperator tableScanOperator,
        VectorTaskColumnInfo vectorTaskColumnInfo) {

      TableScanDesc tableScanDesc = tableScanOperator.getConf();
      VectorTableScanDesc vectorTableScanDesc = new VectorTableScanDesc();
      tableScanDesc.setVectorDesc(vectorTableScanDesc);

      VectorizationContext vContext =
          ((VectorizationContextRegion) tableScanOperator).getOutputVectorizationContext();
      List<Integer> projectedColumns = vContext.getProjectedColumns();
      vectorTableScanDesc.setProjectedColumns(
          ArrayUtils.toPrimitive(projectedColumns.toArray(new Integer[0])));
      List<String> allColumnNameList = vectorTaskColumnInfo.allColumnNames;
      List<TypeInfo> allTypeInfoList = vectorTaskColumnInfo.allTypeInfos;
      List<DataTypePhysicalVariation> allDataTypePhysicalVariationList = vectorTaskColumnInfo.allDataTypePhysicalVariations;
      final int projectedColumnCount = projectedColumns.size();
      String[] projectedDataColumnNames = new String[projectedColumnCount];
      TypeInfo[] projectedDataColumnTypeInfos = new TypeInfo[projectedColumnCount];
      DataTypePhysicalVariation[] projectedDataColumnDataTypePhysicalVariation =
          new DataTypePhysicalVariation[projectedColumnCount];
      for (int i = 0; i < projectedColumnCount; i++) {
        final int projectedColumnNum = projectedColumns.get(i);
        projectedDataColumnNames[i] = allColumnNameList.get(projectedColumnNum);
        projectedDataColumnTypeInfos[i] = allTypeInfoList.get(projectedColumnNum);
        projectedDataColumnDataTypePhysicalVariation[i] = allDataTypePhysicalVariationList.get(projectedColumnNum);
      }
      vectorTableScanDesc.setProjectedColumnNames(projectedDataColumnNames);
      vectorTableScanDesc.setProjectedColumnTypeInfos(projectedDataColumnTypeInfos);
      vectorTableScanDesc.setProjectedColumnDataTypePhysicalVariations(projectedDataColumnDataTypePhysicalVariation);

      tableScanOperator.getConf().setVectorized(true);

      List<Operator<? extends OperatorDesc>> children = tableScanOperator.getChildOperators();
      while (children.size() > 0) {
        children = dosetVectorDesc(children);
      }
    }

    private List<Operator<? extends OperatorDesc>> dosetVectorDesc(
        List<Operator<? extends OperatorDesc>> children) {

      List<Operator<? extends OperatorDesc>> newChildren =
          new ArrayList<Operator<? extends OperatorDesc>>();

      for (Operator<? extends OperatorDesc> child : children) {

        // Get the vector description from the operator.
        VectorDesc vectorDesc = ((VectorizationOperator) child).getVectorDesc();

        // Save the vector description for the EXPLAIN.
        AbstractOperatorDesc desc = (AbstractOperatorDesc) child.getConf();
        desc.setVectorDesc(vectorDesc);

        List<Operator<? extends OperatorDesc>> childChildren = child.getChildOperators();
        if (childChildren != null) {
          newChildren.addAll(childChildren);
        }
      }

      return newChildren;
    }

    private void convertReduceWork(ReduceWork reduceWork) throws SemanticException {

      reduceWork.setVectorizationEnabled(true);

      VectorTaskColumnInfo vectorTaskColumnInfo = new VectorTaskColumnInfo();
      vectorTaskColumnInfo.assume();

      reduceWork.setVectorizedTestingReducerBatchSize(vectorizedTestingReducerBatchSize);

      validateAndVectorizeReduceWork(reduceWork, vectorTaskColumnInfo);
    }

    private void validateAndVectorizeReduceWork(ReduceWork reduceWork,
        VectorTaskColumnInfo vectorTaskColumnInfo) throws SemanticException {

      // Validate input to ReduceWork.
      if (!getOnlyStructObjectInspectors(reduceWork, vectorTaskColumnInfo)) {
        return;
      }

      //--------------------------------------------------------------------------------------------

      /*
       * Validate and vectorize the Reduce operator tree.
       */
      if (!validateAndVectorizeReduceOperators(reduceWork, vectorTaskColumnInfo)) {
        return;
      }

      //--------------------------------------------------------------------------------------------

      vectorTaskColumnInfo.transferToBaseWork(reduceWork);

      reduceWork.setVectorMode(true);

      return;
    }

    private boolean validateAndVectorizeReduceOperators(ReduceWork reduceWork,
        VectorTaskColumnInfo vectorTaskColumnInfo)
            throws SemanticException {

      LOG.info("Validating and vectorizing ReduceWork... (vectorizedVertexNum " + vectorizedVertexNum + ")");

      Operator<? extends OperatorDesc> newVectorReducer;
      try {
        newVectorReducer =
            validateAndVectorizeReduceOperators(reduceWork.getReducer(), vectorTaskColumnInfo);
      } catch (VectorizerCannotVectorizeException e) {

        // The "not vectorized" information has been stored in the MapWork vertex.
        return false;
      } catch (NullPointerException e) {
        if (!isTestVectorizerSuppressFatalExceptions) {
          // Re-throw without losing original stack trace.
          throw e;
        }
        setNodeIssue("exception: " + VectorizationContext.getStackTraceAsSingleLine(e));
        return false;
      } catch (ClassCastException e) {
        if (!isTestVectorizerSuppressFatalExceptions) {
          throw e;
        }
        setNodeIssue("exception: " + VectorizationContext.getStackTraceAsSingleLine(e));
        return false;
      } catch (RuntimeException e) {
        if (!isTestVectorizerSuppressFatalExceptions) {
          throw e;
        }
        setNodeIssue("exception: " + VectorizationContext.getStackTraceAsSingleLine(e));
        return false;
      }

      /*
       * The scratch column information was collected by the task VectorizationContext.  Go get it.
       */
      VectorizationContext vContext =
          ((VectorizationOperator) newVectorReducer).getInputVectorizationContext();

      vectorTaskColumnInfo.setScratchTypeNameArray(
          vContext.getScratchColumnTypeNames());
      vectorTaskColumnInfo.setScratchdataTypePhysicalVariationsArray(
          vContext.getScratchDataTypePhysicalVariations());

      // Replace the reducer with our fully vectorized reduce operator tree.
      reduceWork.setReducer(newVectorReducer);

      return true;
    }

    private Operator<? extends OperatorDesc> validateAndVectorizeReduceOperators(
        Operator<? extends OperatorDesc> reducerOperator,
        VectorTaskColumnInfo vectorTaskColumnInfo)
            throws VectorizerCannotVectorizeException {

      Operator<? extends OperatorDesc> dummyOperator = new DummyOperator();
      dummyOperator.getChildOperators().add(reducerOperator);

      Operator<? extends OperatorDesc> dummyVectorOperator =
          validateAndVectorizeOperatorTree(dummyOperator, true, true, vectorTaskColumnInfo);

      Operator<? extends OperatorDesc> newVectorReducer =
          dummyVectorOperator.getChildOperators().get(0);

      List<Operator<? extends OperatorDesc>> children =
          new ArrayList<Operator<? extends OperatorDesc>>();
      children.add(newVectorReducer);
      while (children.size() > 0) {
        children = dosetVectorDesc(children);
      }

      return newVectorReducer;
    }

    private boolean getOnlyStructObjectInspectors(ReduceWork reduceWork,
            VectorTaskColumnInfo vectorTaskColumnInfo) throws SemanticException {

      ArrayList<String> reduceColumnNames = new ArrayList<String>();
      ArrayList<TypeInfo> reduceTypeInfos = new ArrayList<TypeInfo>();
      ArrayList<DataTypePhysicalVariation> reduceDataTypePhysicalVariations = new ArrayList<DataTypePhysicalVariation>();

      if (reduceWork.getNeedsTagging()) {
        setNodeIssue("Tagging not supported");
        return false;
      }

      String columnSortOrder;
      String columnNullOrder;
      try {
        TableDesc keyTableDesc = reduceWork.getKeyDesc();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Using reduce tag " + reduceWork.getTag());
        }
        TableDesc valueTableDesc = reduceWork.getTagToValueDesc().get(reduceWork.getTag());

        Properties keyTableProperties = keyTableDesc.getProperties();
        AbstractSerDe keyDeserializer =
            ReflectionUtils.newInstance(
                keyTableDesc.getSerDeClass(), null);
        keyDeserializer.initialize(null, keyTableProperties, null);
        ObjectInspector keyObjectInspector = keyDeserializer.getObjectInspector();
        if (keyObjectInspector == null) {
          setNodeIssue("Key object inspector null");
          return false;
        }
        if (!(keyObjectInspector instanceof StructObjectInspector)) {
          setNodeIssue("Key object inspector not StructObjectInspector");
          return false;
        }
        StructObjectInspector keyStructObjectInspector = (StructObjectInspector) keyObjectInspector;
        List<? extends StructField> keyFields = keyStructObjectInspector.getAllStructFieldRefs();

        for (StructField field: keyFields) {
          reduceColumnNames.add(Utilities.ReduceField.KEY.toString() + "." + field.getFieldName());
          reduceTypeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(field.getFieldObjectInspector().getTypeName()));
          reduceDataTypePhysicalVariations.add(DataTypePhysicalVariation.NONE);
        }

        columnSortOrder = keyTableProperties.getProperty(serdeConstants.SERIALIZATION_SORT_ORDER);
        columnNullOrder = keyTableProperties.getProperty(serdeConstants.SERIALIZATION_NULL_SORT_ORDER);

        AbstractSerDe valueDeserializer =
            ReflectionUtils.newInstance(
                valueTableDesc.getSerDeClass(), null);
        valueDeserializer.initialize(null, valueTableDesc.getProperties(), null);
        ObjectInspector valueObjectInspector = valueDeserializer.getObjectInspector();
        if (valueObjectInspector != null) {
          if (!(valueObjectInspector instanceof StructObjectInspector)) {
            setNodeIssue("Value object inspector not StructObjectInspector");
            return false;
          }
          StructObjectInspector valueStructObjectInspector = (StructObjectInspector) valueObjectInspector;
          List<? extends StructField> valueFields = valueStructObjectInspector.getAllStructFieldRefs();

          for (StructField field: valueFields) {
            reduceColumnNames.add(Utilities.ReduceField.VALUE.toString() + "." + field.getFieldName());
            TypeInfo reduceTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(
                field.getFieldObjectInspector().getTypeName());
            reduceTypeInfos.add(reduceTypeInfo);
            if (reduceTypeInfo instanceof DecimalTypeInfo &&
                HiveDecimalWritable.isPrecisionDecimal64(((DecimalTypeInfo)reduceTypeInfo).getPrecision())) {
              reduceDataTypePhysicalVariations.add(DataTypePhysicalVariation.DECIMAL_64);
            }
            else {
              reduceDataTypePhysicalVariations.add(DataTypePhysicalVariation.NONE);
            }
          }
        }
      } catch (Exception e) {
        throw new SemanticException(e);
      }

      vectorTaskColumnInfo.setAllColumnNames(reduceColumnNames);
      vectorTaskColumnInfo.setAllTypeInfos(reduceTypeInfos);
      vectorTaskColumnInfo.setAlldataTypePhysicalVariations(reduceDataTypePhysicalVariations);

      vectorTaskColumnInfo.setReduceColumnSortOrder(columnSortOrder);
      vectorTaskColumnInfo.setReduceColumnNullOrder(columnNullOrder);

      return true;
    }
  }

  @Override
  public PhysicalContext resolve(PhysicalContext physicalContext) throws SemanticException {

    hiveConf = physicalContext.getConf();
    planMapper = physicalContext.getContext().getPlanMapper();

    String vectorizationEnabledOverrideString =
        HiveConf.getVar(hiveConf,
            HiveConf.ConfVars.HIVE_TEST_VECTORIZATION_ENABLED_OVERRIDE);
    vectorizationEnabledOverride =
        EnabledOverride.NAME_MAP.get(vectorizationEnabledOverrideString);

    isVectorizationEnabled = HiveConf.getBoolVar(hiveConf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED);

    final boolean weCanAttemptVectorization;
    isTestForcedVectorizationEnable = false;
    switch (vectorizationEnabledOverride) {
    case NONE:
      weCanAttemptVectorization = isVectorizationEnabled;
      break;
    case DISABLE:
      weCanAttemptVectorization = false;
      break;
    case ENABLE:
      weCanAttemptVectorization = true;
      isTestForcedVectorizationEnable = !isVectorizationEnabled;

      // Different parts of the code rely on this being set...
      HiveConf.setBoolVar(hiveConf,
          HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
      isVectorizationEnabled = true;
      break;
    default:
      throw new RuntimeException("Unexpected vectorization enabled override " +
          vectorizationEnabledOverride);
    }
    if (!weCanAttemptVectorization) {
      LOG.info("Vectorization is disabled");
      return physicalContext;
    }

    useVectorizedInputFileFormat =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTORIZED_INPUT_FILE_FORMAT);
    if(useVectorizedInputFileFormat) {
      initVectorizedInputFormatExcludeClasses();
    }
    useVectorDeserialize =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTOR_DESERIALIZE);
    useRowDeserialize =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_USE_ROW_DESERIALIZE);
    if (useRowDeserialize) {
      initRowDeserializeExcludeClasses();
    }

    // TODO: we could also vectorize some formats based on hive.llap.io.encode.formats if LLAP IO
    //       is enabled and we are going to run in LLAP. However, we don't know if we end up in
    //       LLAP or not at this stage, so don't do this now. We may need to add a 'force' option.

    isReduceVectorizationEnabled =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_REDUCE_ENABLED);
    isPtfVectorizationEnabled =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_PTF_ENABLED);

    isVectorizationComplexTypesEnabled =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_COMPLEX_TYPES_ENABLED);
    isVectorizationGroupByComplexTypesEnabled =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_COMPLEX_TYPES_ENABLED);

    isVectorizedRowIdentifierEnabled =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_ROW_IDENTIFIER_ENABLED);

    vectorizedPTFMaxMemoryBufferingBatchCount =
        HiveConf.getIntVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_PTF_MAX_MEMORY_BUFFERING_BATCH_COUNT);
    vectorizedTestingReducerBatchSize =
        HiveConf.getIntVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_TESTING_REDUCER_BATCH_SIZE);

    isTestVectorizerSuppressFatalExceptions =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_TEST_VECTORIZER_SUPPRESS_FATAL_EXCEPTIONS);

    vectorizedInputFormatSupportEnabled =
        HiveConf.getVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_ENABLED);
    String[] supportEnabledStrings = vectorizedInputFormatSupportEnabled.toLowerCase().split(",");
    vectorizedInputFormatSupportEnabledSet = new TreeSet<Support>();
    for (String supportEnabledString : supportEnabledStrings) {
      Support support = Support.nameToSupportMap.get(supportEnabledString);

      // Known?
      if (support != null) {
        vectorizedInputFormatSupportEnabledSet.add(support);
      }
    }

    /*
     * Notice the default value for LLAP_IO_ENABLED is overridden to be whether we are
     * executing under LLAP.
     */
    isLlapIoEnabled =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.LLAP_IO_ENABLED,
            LlapProxy.isDaemon());

    isSchemaEvolution =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_SCHEMA_EVOLUTION);

    hiveVectorAdaptorUsageMode = HiveVectorAdaptorUsageMode.getHiveConfValue(hiveConf);

    isTestVectorizationSuppressExplainExecutionMode =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_TEST_VECTORIZATION_SUPPRESS_EXPLAIN_EXECUTION_MODE);

    // create dispatcher and graph walker
    SemanticDispatcher disp = new VectorizationDispatcher();
    TaskGraphWalker ogw = new TaskGraphWalker(disp);

    // get all the tasks nodes from root task
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(physicalContext.getRootTasks());

    // begin to walk through the task tree.
    ogw.startWalking(topNodes, null);
    return physicalContext;
  }

  private void initVectorizedInputFormatExcludeClasses() {
    vectorizedInputFormatExcludes = Utilities.getClassNamesFromConfig(hiveConf,
        ConfVars.HIVE_VECTORIZATION_VECTORIZED_INPUT_FILE_FORMAT_EXCLUDES);
  }

  private void initRowDeserializeExcludeClasses() {
    rowDeserializeInputFormatExcludes = Utilities.getClassNamesFromConfig(hiveConf,
        ConfVars.HIVE_VECTORIZATION_ROW_DESERIALIZE_INPUTFORMAT_EXCLUDES);
  }

  private void setOperatorNotSupported(Operator<? extends OperatorDesc> op) {
    OperatorDesc desc = op.getConf();
    Annotation note = AnnotationUtils.getAnnotation(desc.getClass(), Explain.class);
    if (note != null) {
      Explain explainNote = (Explain) note;
      setNodeIssue(explainNote.displayName() + " (" + op.getType() + ") not supported");
    } else {
      setNodeIssue("Operator " + op.getType() + " not supported");
    }
  }


  private boolean validateSMBMapJoinOperator(SMBMapJoinOperator op) {
    SMBJoinDesc desc = op.getConf();
    // Validation is the same as for map join, since the 'small' tables are not vectorized
    return validateMapJoinDesc(desc);
  }

  private boolean validateTableScanOperator(TableScanOperator op, MapWork mWork) {
    TableScanDesc desc = op.getConf();
    if (desc.isGatherStats()) {
      setOperatorIssue("gather stats not supported");
      return false;
    }

    return true;
  }

  private boolean validateMapJoinOperator(MapJoinOperator op) {
    MapJoinDesc desc = op.getConf();
    return validateMapJoinDesc(desc);
  }

  private boolean validateMapJoinDesc(MapJoinDesc desc) {
    byte posBigTable = (byte) desc.getPosBigTable();
    List<ExprNodeDesc> filterExprs = desc.getFilters().get(posBigTable);
    if (!validateExprNodeDesc(
        filterExprs, "Filter", VectorExpressionDescriptor.Mode.FILTER, /* allowComplex */ true)) {
      return false;
    }
    List<ExprNodeDesc> keyExprs = desc.getKeys().get(posBigTable);
    if (!validateExprNodeDescNoComplex(keyExprs, "Key")) {
      // Vectorization for join keys of complex type is not supported.
      // https://issues.apache.org/jira/browse/HIVE-24989
      return false;
    }
    List<ExprNodeDesc> valueExprs = desc.getExprs().get(posBigTable);
    if (!validateExprNodeDesc(valueExprs, "Value")) {
      return false;
    }
    Byte[] order = desc.getTagOrder();
    Byte posSingleVectorMapJoinSmallTable = (order[0] == posBigTable ? order[1] : order[0]);
    List<ExprNodeDesc> smallTableExprs = desc.getExprs().get(posSingleVectorMapJoinSmallTable);
    if (!validateExprNodeDesc(smallTableExprs, "Small Table")) {
      return false;
    }
    if (desc.getResidualFilterExprs() != null && !desc.getResidualFilterExprs().isEmpty()) {
      setOperatorIssue("Non-equi joins not supported");
      return false;
    }
    return true;
  }

  private boolean validateReduceSinkOperator(ReduceSinkOperator op) {
    List<ExprNodeDesc> keyDescs = op.getConf().getKeyCols();
    List<ExprNodeDesc> partitionDescs = op.getConf().getPartitionCols();
    List<ExprNodeDesc> valueDesc = op.getConf().getValueCols();
    return validateExprNodeDesc(keyDescs, "Key") && validateExprNodeDesc(partitionDescs, "Partition") &&
        validateExprNodeDesc(valueDesc, "Value");
  }

  private boolean validateSelectOperator(SelectOperator op) {
    List<ExprNodeDesc> descList = op.getConf().getColList();
    for (ExprNodeDesc desc : descList) {
      boolean ret =
          validateExprNodeDesc(
              desc, "Select",
              VectorExpressionDescriptor.Mode.PROJECTION,
              /* allowComplex */ true, /* allowVoidProjection */ true);
      if (!ret) {
        return false;
      }
    }
    return true;
  }

  private boolean validateFilterOperator(FilterOperator op) {
    ExprNodeDesc desc = op.getConf().getPredicate();
    return validateExprNodeDesc(
        desc, "Predicate", VectorExpressionDescriptor.Mode.FILTER, /* allowComplex */ true);
  }

  private boolean validateTopNKeyOperator(TopNKeyOperator op) {
    List<ExprNodeDesc> keyColumns = op.getConf().getKeyColumns();
    return validateExprNodeDesc(keyColumns, "Key columns");
  }

  private boolean validateGroupByOperator(GroupByOperator op, boolean isReduce,
      boolean isTez, VectorGroupByDesc vectorGroupByDesc) {

    GroupByDesc desc = op.getConf();

    if (desc.getMode() != GroupByDesc.Mode.HASH && desc.isDistinct()) {
      setOperatorIssue("DISTINCT not supported");
      return false;
    }
    boolean ret = validateExprNodeDescNoComplex(desc.getKeys(), "Key");
    if (!ret) {
      return false;
    }

    /**
     *
     * GROUP BY DEFINITIONS:
     *
     * GroupByDesc.Mode enumeration:
     *
     *    The different modes of a GROUP BY operator.
     *
     *    These descriptions are hopefully less cryptic than the comments for GroupByDesc.Mode.
     *
     *        COMPLETE       Aggregates original rows into full aggregation row(s).
     *
     *                       If the key length is 0, this is also called Global aggregation and
     *                       1 output row is produced.
     *
     *                       When the key length is > 0, the original rows come in ALREADY GROUPED.
     *
     *                       An example for key length > 0 is a GROUP BY being applied to the
     *                       ALREADY GROUPED rows coming from an upstream JOIN operator.  Or,
     *                       ALREADY GROUPED rows coming from upstream MERGEPARTIAL GROUP BY
     *                       operator.
     *
     *        PARTIAL1       The first of 2 (or more) phases that aggregates ALREADY GROUPED
     *                       original rows into partial aggregations.
     *
     *                       Subsequent phases PARTIAL2 (optional) and MERGEPARTIAL will merge
     *                       the partial aggregations and output full aggregations.
     *
     *        PARTIAL2       Accept ALREADY GROUPED partial aggregations and merge them into another
     *                       partial aggregation.  Output the merged partial aggregations.
     *
     *                       (Haven't seen this one used)
     *
     *        PARTIALS       (Behaves for non-distinct the same as PARTIAL2; and behaves for
     *                       distinct the same as PARTIAL1.)
     *
     *        FINAL          Accept ALREADY GROUPED original rows and aggregate them into
     *                       full aggregations.
     *
     *                       Example is a GROUP BY being applied to rows from a sorted table, where
     *                       the group key is the table sort key (or a prefix).
     *
     *        HASH           Accept UNORDERED original rows and aggregate them into a memory table.
     *                       Output the partial aggregations on closeOp (or low memory).
     *
     *                       Similar to PARTIAL1 except original rows are UNORDERED.
     *
     *                       Commonly used in both Mapper and Reducer nodes.  Always followed by
     *                       a Reducer with MERGEPARTIAL GROUP BY.
     *
     *        MERGEPARTIAL   Always first operator of a Reducer.  Data is grouped by reduce-shuffle.
     *
     *                       (Behaves for non-distinct aggregations the same as FINAL; and behaves
     *                       for distinct aggregations the same as COMPLETE.)
     *
     *                       The output is full aggregation(s).
     *
     *                       Used in Reducers after a stage with a HASH GROUP BY operator.
     *
     *
     *  VectorGroupByDesc.ProcessingMode for VectorGroupByOperator:
     *
     *     GLOBAL         No key.  All rows --> 1 full aggregation on end of input
     *
     *     HASH           Rows aggregated in to hash table on group key -->
     *                        1 partial aggregation per key (normally, unless there is spilling)
     *
     *     MERGE_PARTIAL  As first operator in a REDUCER, partial aggregations come grouped from
     *                    reduce-shuffle -->
     *                        aggregate the partial aggregations and emit full aggregation on
     *                        endGroup / closeOp
     *
     *     STREAMING      Rows come from PARENT operator ALREADY GROUPED -->
     *                        aggregate the rows and emit full aggregation on key change / closeOp
     *
     *     NOTE: Hash can spill partial result rows prematurely if it runs low on memory.
     *     NOTE: Streaming has to compare keys where MergePartial gets an endGroup call.
     *
     *
     *  DECIDER: Which VectorGroupByDesc.ProcessingMode for VectorGroupByOperator?
     *
     *     Decides using GroupByDesc.Mode and whether there are keys with the
     *     VectorGroupByDesc.groupByDescModeToVectorProcessingMode method.
     *
     *         Mode.COMPLETE      --> (numKeys == 0 ? ProcessingMode.GLOBAL : ProcessingMode.STREAMING)
     *
     *         Mode.HASH          --> ProcessingMode.HASH
     *
     *         Mode.MERGEPARTIAL  --> (numKeys == 0 ? ProcessingMode.GLOBAL : ProcessingMode.MERGE_PARTIAL)
     *
     *         Mode.PARTIAL1,
     *         Mode.PARTIAL2,
     *         Mode.PARTIALS,
     *         Mode.FINAL        --> ProcessingMode.STREAMING
     *
     */
    boolean hasKeys = (desc.getKeys().size() > 0);

    ProcessingMode processingMode =
        VectorGroupByDesc.groupByDescModeToVectorProcessingMode(desc.getMode(), hasKeys);
    if (desc.isGroupingSetsPresent() &&
        (processingMode != ProcessingMode.HASH && processingMode != ProcessingMode.STREAMING)) {
      setOperatorIssue("Vectorized GROUPING SETS only expected for HASH and STREAMING processing modes");
      return false;
    }

    //TODO: isGroupingSetsPresent() is returning false, even though
    // ListGroupingSets is present. Need to check if there is hidden bug.
    boolean isGroupingSetsPresent = (desc.getListGroupingSets() != null && !desc.getListGroupingSets().isEmpty());
    if (!validateAggregationDescs(desc.getAggregators(), desc.getMode(),
        isGroupingSetsPresent, hasKeys)) {
      return false;
    }

    vectorGroupByDesc.setProcessingMode(processingMode);

    vectorGroupByDesc.setIsVectorizationComplexTypesEnabled(isVectorizationComplexTypesEnabled);
    vectorGroupByDesc.setIsVectorizationGroupByComplexTypesEnabled(isVectorizationGroupByComplexTypesEnabled);

    LOG.info("Vector GROUP BY operator will use processing mode " + processingMode.name());

    return true;
  }

  private boolean validateFileSinkOperator(FileSinkOperator op) {
   return true;
  }

  /*
   * Determine recursively if the PTF LEAD or LAG function is being used in an expression.
   */
  private boolean containsLeadLag(ExprNodeDesc exprNodeDesc) {
    if (exprNodeDesc instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc genericFuncDesc = (ExprNodeGenericFuncDesc) exprNodeDesc;
      GenericUDF genFuncClass = genericFuncDesc.getGenericUDF();
      if (genFuncClass instanceof GenericUDFLag ||
          genFuncClass instanceof GenericUDFLead) {
        return true;
      }
      return containsLeadLag(genericFuncDesc.getChildren());
    } else {
      // ExprNodeColumnDesc, ExprNodeConstantDesc, ExprNodeDynamicValueDesc, etc do not have
      // LEAD/LAG inside.
      return false;
    }
  }

  private boolean containsLeadLag(List<ExprNodeDesc> exprNodeDescList) {
    for (ExprNodeDesc exprNodeDesc : exprNodeDescList) {
      if (containsLeadLag(exprNodeDesc)) {
        return true;
      }
    }
    return false;
  }

  private boolean validatePTFOperator(PTFOperator op, VectorizationContext vContext,
      VectorPTFDesc vectorPTFDesc)
      throws HiveException {

    if (!isPtfVectorizationEnabled) {
      setNodeIssue("Vectorization of PTF is not enabled (" +
          HiveConf.ConfVars.HIVE_VECTORIZATION_PTF_ENABLED.varname + " IS false)");
      return false;
    }
    PTFDesc ptfDesc = op.getConf();
    boolean isMapSide = ptfDesc.isMapSide();
    if (isMapSide) {
      setOperatorIssue("PTF Mapper not supported");
      return false;
    }
    List<Operator<? extends OperatorDesc>> ptfParents = op.getParentOperators();
    if (ptfParents != null && ptfParents.size() > 0) {
      Operator<? extends OperatorDesc> ptfParent = op.getParentOperators().get(0);
      if (!(ptfParent instanceof ReduceSinkOperator)) {
        boolean isReduceShufflePtf = false;
        if (ptfParent instanceof SelectOperator) {
          ptfParents = ptfParent.getParentOperators();
          if (ptfParents == null || ptfParents.size() == 0) {
            isReduceShufflePtf = true;
          } else {
            ptfParent = ptfParent.getParentOperators().get(0);
            isReduceShufflePtf = (ptfParent instanceof ReduceSinkOperator);
          }
        }
        if (!isReduceShufflePtf) {
          setOperatorIssue("Only PTF directly under reduce-shuffle is supported");
          return false;
        }
      }
    }
    boolean forNoop = ptfDesc.forNoop();
    if (forNoop) {
      setOperatorIssue("NOOP not supported");
      return false;
    }
    boolean forWindowing = ptfDesc.forWindowing();
    if (!forWindowing) {
      setOperatorIssue("Windowing required");
      return false;
    }
    PartitionedTableFunctionDef funcDef = ptfDesc.getFuncDef();
    boolean isWindowTableFunctionDef = (funcDef instanceof WindowTableFunctionDef);
    if (!isWindowTableFunctionDef) {
      setOperatorIssue("Must be a WindowTableFunctionDef");
      return false;
    }

    // We collect information in VectorPTFDesc that doesn't need the VectorizationContext.
    // We use this information for validation.  Later when creating the vector operator
    // we create an additional object VectorPTFInfo.

    try {
      createVectorPTFDesc(
          op, ptfDesc, vContext, vectorPTFDesc, vectorizedPTFMaxMemoryBufferingBatchCount);
    } catch (HiveException e) {
      setOperatorIssue("exception: " + VectorizationContext.getStackTraceAsSingleLine(e));
      return false;
    }

    // Output columns ok?
    String[] outputColumnNames = vectorPTFDesc.getOutputColumnNames();
    TypeInfo[] outputTypeInfos = vectorPTFDesc.getOutputTypeInfos();
    final int outputCount = outputColumnNames.length;
    for (int i = 0; i < outputCount; i++) {
      String typeName = outputTypeInfos[i].getTypeName();
      boolean ret = validateDataType(typeName, VectorExpressionDescriptor.Mode.PROJECTION, /* allowComplex */ false);
      if (!ret) {
        setExpressionIssue("PTF Output Columns", "Data type " + typeName + " of column " + outputColumnNames[i] + " not supported");
        return false;
      }
    }

    boolean[] distinctEvaluator = vectorPTFDesc.getEvaluatorsAreDistinct();
    String[] evaluatorFunctionNames = vectorPTFDesc.getEvaluatorFunctionNames();
    final int count = evaluatorFunctionNames.length;
    WindowFrameDef[] evaluatorWindowFrameDefs = vectorPTFDesc.getEvaluatorWindowFrameDefs();
    List<ExprNodeDesc>[] evaluatorInputExprNodeDescLists = vectorPTFDesc.getEvaluatorInputExprNodeDescLists();

    for (int i = 0; i < count; i++) {
      String functionName = evaluatorFunctionNames[i].toLowerCase();
      SupportedFunctionType supportedFunctionType = VectorPTFDesc.supportedFunctionsMap.get(functionName);
      if (supportedFunctionType == null) {
        setOperatorIssue(functionName + " not in supported functions " + VectorPTFDesc.supportedFunctionNames);
        return false;
      }

      if (distinctEvaluator[i] && !supportedFunctionType.isSupportDistinct()) {
        setOperatorIssue(functionName + " distinct is not supported ");
        return false;
      }

      WindowFrameDef windowFrameDef = evaluatorWindowFrameDefs[i];

      List<ExprNodeDesc> exprNodeDescList = evaluatorInputExprNodeDescLists[i];
      final boolean isSingleParameter =
          (exprNodeDescList != null &&
          exprNodeDescList.size() == 1);
      final ExprNodeDesc singleExprNodeDesc =
          (isSingleParameter ? exprNodeDescList.get(0) : null);
      final TypeInfo singleTypeInfo =
          (isSingleParameter ? singleExprNodeDesc.getTypeInfo() : null);
      final PrimitiveCategory singlePrimitiveCategory =
          (singleTypeInfo instanceof PrimitiveTypeInfo ?
              ((PrimitiveTypeInfo) singleTypeInfo).getPrimitiveCategory() : null);

      switch (windowFrameDef.getWindowType()) {
      case RANGE:
        if (!windowFrameDef.getEnd().isCurrentRow()) {
          setOperatorIssue(functionName + " only CURRENT ROW end frame is supported for RANGE");
          return false;
        }
        break;
      case ROWS:
        {
          boolean isRowEndCurrent =
              (windowFrameDef.getEnd().isCurrentRow() &&
              (supportedFunctionType == SupportedFunctionType.AVG ||
               supportedFunctionType == SupportedFunctionType.MAX ||
               supportedFunctionType == SupportedFunctionType.MIN ||
               supportedFunctionType == SupportedFunctionType.SUM) &&
              isSingleParameter &&
              singlePrimitiveCategory != null);
          if (!isRowEndCurrent && !windowFrameDef.isEndUnbounded()) {
            setOperatorIssue(
                functionName + " UNBOUNDED end frame is required for ROWS window type");
            return false;
          }
        }
        break;
      default:
        throw new RuntimeException("Unexpected window type " + windowFrameDef.getWindowType());
      }

      // RANK/DENSE_RANK don't care about columns.
      if (supportedFunctionType != SupportedFunctionType.RANK &&
          supportedFunctionType != SupportedFunctionType.DENSE_RANK) {

        if (exprNodeDescList != null) {
          // LEAD and LAG now supports multiple arguments in vectorized mode
          if (exprNodeDescList.size() > 1 && supportedFunctionType != SupportedFunctionType.LAG
              && supportedFunctionType != SupportedFunctionType.LEAD) {
            setOperatorIssue(
                "More than 1 argument expression of aggregation function " + functionName);
            return false;
          }

          ExprNodeDesc exprNodeDesc = exprNodeDescList.get(0);

          if (containsLeadLag(exprNodeDesc)) {
            setOperatorIssue("lead and lag function not supported in argument expression of aggregation function " + functionName);
            return false;
          }

          if (supportedFunctionType != SupportedFunctionType.COUNT) {

            // COUNT does not care about column types.  The rest do.
            TypeInfo typeInfo = exprNodeDesc.getTypeInfo();
            Category category = typeInfo.getCategory();
            boolean isSupportedType;
            if (category != Category.PRIMITIVE) {
              isSupportedType = false;
            } else {
              ColumnVector.Type colVecType =
                  VectorizationContext.getColumnVectorTypeFromTypeInfo(typeInfo);
              switch (colVecType) {
              case LONG:
              case DOUBLE:
              case DECIMAL:
                isSupportedType = true;
                break;
              default:
                isSupportedType = false;
                break;
              }
            }
            if (!isSupportedType) {
              setOperatorIssue(typeInfo.getTypeName() + " data type not supported in argument expression of aggregation function " + functionName);
              return false;
            }
          }
        }
      }
      if (vectorPTFDesc.getOrderExprNodeDescs().length > 1) {
        /*
         * Currently, we need to rule out here all cases where a range boundary scanner can run,
         * basically: 1. bounded start 2. bounded end which is not current row
         */
        if (windowFrameDef.getWindowType() == WindowType.RANGE
            && (!windowFrameDef.isStartUnbounded()
                || !(windowFrameDef.getEnd().isCurrentRow() || windowFrameDef.isEndUnbounded()))) {
          setOperatorIssue(
              "Multi-column ordered RANGE boundary scanner is not supported in vectorized mode (window: "
                  + windowFrameDef + ")");
          return false;
        }
      }
    }
    return true;
  }

  private boolean validateExprNodeDesc(List<ExprNodeDesc> descs, String expressionTitle) {
    return validateExprNodeDesc(
        descs, expressionTitle, VectorExpressionDescriptor.Mode.PROJECTION, /* allowComplex */ true);
  }

  private boolean validateExprNodeDescNoComplex(List<ExprNodeDesc> descs, String expressionTitle) {
    return validateExprNodeDesc(
        descs, expressionTitle, VectorExpressionDescriptor.Mode.PROJECTION, /* allowComplex */ false);
  }

  private boolean validateExprNodeDesc(List<ExprNodeDesc> descs,
          String expressionTitle,
          VectorExpressionDescriptor.Mode mode,
          boolean allowComplex) {
    for (ExprNodeDesc d : descs) {
      boolean ret = validateExprNodeDesc(d, expressionTitle, mode, allowComplex);
      if (!ret) {
        return false;
      }
    }
    return true;
  }

  private boolean validateAggregationDescs(List<AggregationDesc> descs,
      GroupByDesc.Mode groupByMode, boolean isGroupingSetsPresent,
      boolean hasKeys) {

    for (AggregationDesc d : descs) {
      if (!validateAggregationDesc(d, groupByMode, isGroupingSetsPresent,
          hasKeys)) {
        return false;
      }
    }
    return true;
  }

  private boolean validateExprNodeDescRecursive(ExprNodeDesc desc, String expressionTitle,
      VectorExpressionDescriptor.Mode mode, boolean allowComplex) {
    return validateExprNodeDescRecursive(desc, expressionTitle, mode,
        allowComplex, /* allowVoidProjection */ false);
  }

  private boolean validateExprNodeDescRecursive(ExprNodeDesc desc, String expressionTitle,
      VectorExpressionDescriptor.Mode mode, boolean allowComplex, boolean allowVoidProjection) {
    if (desc instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc c = (ExprNodeColumnDesc) desc;
      String columnName = c.getColumn();

      if (availableVectorizedVirtualColumnSet != null) {

        // For Map, check for virtual columns.
        VirtualColumn virtualColumn = VirtualColumn.VIRTUAL_COLUMN_NAME_MAP.get(columnName);
        if (virtualColumn != null) {

          // We support some virtual columns in vectorization for this table scan.

          if (!availableVectorizedVirtualColumnSet.contains(virtualColumn)) {
            setExpressionIssue(expressionTitle, "Virtual column " + columnName + " is not supported");
            return false;
          }

          // Remember we used this one in the query.
          neededVirtualColumnSet.add(virtualColumn);
        }
      }
    }
    String typeName = desc.getTypeInfo().getTypeName();
    boolean ret = validateDataType(
        typeName, mode, allowComplex && isVectorizationComplexTypesEnabled, allowVoidProjection);
    if (!ret) {
      setExpressionIssue(expressionTitle,
          getValidateDataTypeErrorMsg(
              typeName, mode, allowComplex, isVectorizationComplexTypesEnabled));
      return false;
    }
    boolean isInExpression = false;
    if (desc instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc d = (ExprNodeGenericFuncDesc) desc;
      boolean r = validateGenericUdf(d);
      if (!r) {
        setExpressionIssue(expressionTitle, "UDF " + d + " not supported");
        return false;
      }
      GenericUDF genericUDF = d.getGenericUDF();
      isInExpression = (genericUDF instanceof GenericUDFIn);
    }
    if (desc.getChildren() != null) {
      if (isInExpression
          && desc.getChildren().get(0).getTypeInfo().getCategory() == Category.STRUCT) {
        // Don't restrict child expressions for projection.
        // Always use loose FILTER mode.
        if (!validateStructInExpression(
            desc, expressionTitle, VectorExpressionDescriptor.Mode.FILTER)) {
          return false;
        }
      } else {
        for (ExprNodeDesc d : desc.getChildren()) {
          // Don't restrict child expressions for projection.
          // Always use loose FILTER mode.
          if (!validateExprNodeDescRecursive(
              d, expressionTitle, VectorExpressionDescriptor.Mode.FILTER,
              /* allowComplex */ true, allowVoidProjection)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  private boolean validateStructInExpression(ExprNodeDesc desc,
      String expressionTitle, VectorExpressionDescriptor.Mode mode) {
    for (ExprNodeDesc d : desc.getChildren()) {
      TypeInfo typeInfo = d.getTypeInfo();
      if (typeInfo.getCategory() != Category.STRUCT) {
        return false;
      }
      StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;

      List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
      List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
      final int fieldCount = fieldTypeInfos.size();
      for (int f = 0; f < fieldCount; f++) {
        TypeInfo fieldTypeInfo = fieldTypeInfos.get(f);
        Category category = fieldTypeInfo.getCategory();
        if (category != Category.PRIMITIVE) {
          setExpressionIssue(expressionTitle,
              "Cannot vectorize struct field " + fieldNames.get(f)
              + " of type " + fieldTypeInfo.getTypeName());
          return false;
        }
        PrimitiveTypeInfo fieldPrimitiveTypeInfo = (PrimitiveTypeInfo) fieldTypeInfo;
        InConstantType inConstantType = VectorizationContext
            .getInConstantTypeFromPrimitiveCategory(fieldPrimitiveTypeInfo
                .getPrimitiveCategory());

        // For now, limit the data types we support for Vectorized Struct IN().
        if (inConstantType != InConstantType.INT_FAMILY
            && inConstantType != InConstantType.FLOAT_FAMILY
            && inConstantType != InConstantType.STRING_FAMILY) {
          setExpressionIssue(expressionTitle,
              "Cannot vectorize struct field " + fieldNames.get(f)
              + " of type " + fieldTypeInfo.getTypeName());
          return false;
        }
      }
    }
    return true;
  }

  private boolean validateExprNodeDesc(ExprNodeDesc desc, String expressionTitle) {
    return validateExprNodeDesc(
        desc, expressionTitle, VectorExpressionDescriptor.Mode.PROJECTION,
        /* allowComplex */ true, /* allowVoidProjection */ false);
  }

  boolean validateExprNodeDesc(ExprNodeDesc desc, String expressionTitle,
      VectorExpressionDescriptor.Mode mode, boolean allowComplex) {
    return validateExprNodeDescRecursive(desc, expressionTitle, mode, allowComplex);
  }

  boolean validateExprNodeDesc(ExprNodeDesc desc, String expressionTitle,
      VectorExpressionDescriptor.Mode mode, boolean allowComplex, boolean allowVoidProjection) {
    return validateExprNodeDescRecursive(
        desc, expressionTitle, mode, allowComplex, allowVoidProjection);
  }

  private boolean validateGenericUdf(ExprNodeGenericFuncDesc genericUDFExpr) {
    if (VectorizationContext.isCustomUDF(genericUDFExpr)) {
      return true;
    }
    if (hiveVectorAdaptorUsageMode == HiveVectorAdaptorUsageMode.NONE ||
        hiveVectorAdaptorUsageMode == HiveVectorAdaptorUsageMode.CHOSEN) {
      GenericUDF genericUDF = genericUDFExpr.getGenericUDF();
      if (genericUDF instanceof GenericUDFBridge) {
        Class<? extends UDF> udf = ((GenericUDFBridge) genericUDF).getUdfClass();
        return supportedGenericUDFs.contains(udf);
      } else {
        return supportedGenericUDFs.contains(genericUDF.getClass());
      }
    }
    return true;
  }

  private boolean validateAggregationDesc(AggregationDesc aggDesc, GroupByDesc.Mode groupByMode,
      boolean isGroupingSetsPresent, boolean hasKeys) {

    String udfName = aggDesc.getGenericUDAFName().toLowerCase();
    if (!supportedAggregationUdfs.contains(udfName)) {
      setExpressionIssue("Aggregation Function", "UDF " + udfName + " not supported");
      return false;
    }

    // The planner seems to pull this one out.
    if (groupByMode != GroupByDesc.Mode.HASH && aggDesc.getDistinct()) {
      setExpressionIssue("Aggregation Function", "DISTINCT not supported");
      return false;
    }

    if (isGroupingSetsPresent && aggDesc.getDistinct()) {
      setExpressionIssue("Aggregation Function", "DISTINCT with Groupingsets not supported");
      return false;
    }


    List<ExprNodeDesc> parameters = aggDesc.getParameters();

    if (parameters != null && !validateExprNodeDesc(parameters, "Aggregation Function UDF " + udfName + " parameter")) {
      return false;
    }

    return true;
  }

  public static boolean validateDataType(String type, VectorExpressionDescriptor.Mode mode,
      boolean allowComplex) {
    return validateDataType(type, mode, allowComplex, /* allowVoidProjection */ false);
  }

  public static boolean validateDataType(String type, VectorExpressionDescriptor.Mode mode,
      boolean allowComplex, boolean allowVoidProjection) {

    type = type.toLowerCase();
    boolean result = supportedDataTypesPattern.matcher(type).matches();
    if (result && !allowVoidProjection &&
        mode == VectorExpressionDescriptor.Mode.PROJECTION && type.equals(serdeConstants.VOID_TYPE_NAME)) {
      return false;
    }

    if (!result) {
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);
      if (typeInfo.getCategory() != Category.PRIMITIVE) {
        if (allowComplex) {
          return true;
        }
      }
    }
    return result;
  }

  public static String getValidateDataTypeErrorMsg(String type, VectorExpressionDescriptor.Mode mode,
      boolean allowComplex, boolean isVectorizationComplexTypesEnabled) {
    return getValidateDataTypeErrorMsg(
        type, mode, allowComplex, isVectorizationComplexTypesEnabled, false);
  }

  public static String getValidateDataTypeErrorMsg(String type, VectorExpressionDescriptor.Mode mode,
      boolean allowComplex, boolean isVectorizationComplexTypesEnabled,
      boolean allowVoidProjection) {

    type = type.toLowerCase();
    boolean result = supportedDataTypesPattern.matcher(type).matches();
    if (result && !allowVoidProjection &&
        mode == VectorExpressionDescriptor.Mode.PROJECTION && type.equals(serdeConstants.VOID_TYPE_NAME)) {
      return "Vectorizing data type void not supported when mode = PROJECTION";
    }

    if (!result) {
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);
      if (typeInfo.getCategory() != Category.PRIMITIVE) {
        if (allowComplex && isVectorizationComplexTypesEnabled) {
          return null;
        } else if (!allowComplex) {
          return "Vectorizing complex type " + typeInfo.getCategory() + " not supported";
        } else {
          return "Vectorizing complex type " + typeInfo.getCategory() + " not enabled (" +
              type +  ") since " +
              GroupByDesc.getComplexTypeEnabledCondition(
                  isVectorizationComplexTypesEnabled);
        }
      }
    }
    return (result ? null : "Vectorizing data type " + type + " not supported");
  }

  private void fixupParentChildOperators(Operator<? extends OperatorDesc> op,
          Operator<? extends OperatorDesc> vectorOp) {
    if (op.getParentOperators() != null) {
      vectorOp.setParentOperators(op.getParentOperators());
      for (Operator<? extends OperatorDesc> p : op.getParentOperators()) {
        p.replaceChild(op, vectorOp);
      }
    }
    if (op.getChildOperators() != null) {
      vectorOp.setChildOperators(op.getChildOperators());
      for (Operator<? extends OperatorDesc> c : op.getChildOperators()) {
        c.replaceParent(op, vectorOp);
      }
    }
  }

  private boolean isBigTableOnlyResults(MapJoinDesc desc) {
    Byte[] order = desc.getTagOrder();
    byte posBigTable = (byte) desc.getPosBigTable();
    Byte posSingleVectorMapJoinSmallTable = (order[0] == posBigTable ? order[1] : order[0]);

    int[] smallTableIndices;
    int smallTableIndicesSize;
    if (desc.getValueIndices() != null && desc.getValueIndices().get(posSingleVectorMapJoinSmallTable) != null) {
      smallTableIndices = desc.getValueIndices().get(posSingleVectorMapJoinSmallTable);
      LOG.info("Vectorizer isBigTableOnlyResults smallTableIndices " + Arrays.toString(smallTableIndices));
      smallTableIndicesSize = smallTableIndices.length;
    } else {
      smallTableIndices = null;
      LOG.info("Vectorizer isBigTableOnlyResults smallTableIndices EMPTY");
      smallTableIndicesSize = 0;
    }

    List<Integer> smallTableRetainList = desc.getRetainList().get(posSingleVectorMapJoinSmallTable);
    LOG.info("Vectorizer isBigTableOnlyResults smallTableRetainList " + smallTableRetainList);
    int smallTableRetainSize = smallTableRetainList.size();

    if (smallTableIndicesSize > 0) {
      // Small table indices has priority over retain.
      for (int i = 0; i < smallTableIndicesSize; i++) {
        if (smallTableIndices[i] < 0) {
          // Negative numbers indicate a column to be (deserialize) read from the small table's
          // LazyBinary value row.
          setOperatorIssue("Vectorizer isBigTableOnlyResults smallTableIndices[i] < 0 returning false");
          return false;
        }
      }
    } else if (smallTableRetainSize > 0) {
      setOperatorIssue("Vectorizer isBigTableOnlyResults smallTableRetainSize > 0 returning false");
      return false;
    }

    LOG.info("Vectorizer isBigTableOnlyResults returning true");
    return true;
  }

  Operator<? extends OperatorDesc> specializeMapJoinOperator(Operator<? extends OperatorDesc> op,
        VectorizationContext vContext, MapJoinDesc desc, VectorMapJoinDesc vectorDesc)
            throws HiveException {
    Operator<? extends OperatorDesc> vectorOp = null;
    Class<? extends Operator<?>> opClass = null;

    VectorMapJoinInfo vectorMapJoinInfo = vectorDesc.getVectorMapJoinInfo();

    HashTableImplementationType hashTableImplementationType = HashTableImplementationType.NONE;
    HashTableKind hashTableKind = HashTableKind.NONE;
    HashTableKeyType hashTableKeyType = HashTableKeyType.NONE;
    VectorMapJoinVariation vectorMapJoinVariation = null;

    if (vectorDesc.getIsFastHashTableEnabled()) {
      hashTableImplementationType = HashTableImplementationType.FAST;
    } else {
      hashTableImplementationType = HashTableImplementationType.OPTIMIZED;
    }

    int joinType = desc.getConds()[0].getType();

    boolean isInnerBigOnly = false;
    if (joinType == JoinDesc.INNER_JOIN && isBigTableOnlyResults(desc)) {
      isInnerBigOnly = true;
    }

    // By default, we can always use the multi-key class.
    hashTableKeyType = HashTableKeyType.MULTI_KEY;

    if (!HiveConf.getBoolVar(hiveConf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_MULTIKEY_ONLY_ENABLED)) {

      // Look for single column optimization.
      byte posBigTable = (byte) desc.getPosBigTable();
      Map<Byte, List<ExprNodeDesc>> keyExprs = desc.getKeys();
      List<ExprNodeDesc> bigTableKeyExprs = keyExprs.get(posBigTable);
      if (bigTableKeyExprs.size() == 1) {
        TypeInfo typeInfo = bigTableKeyExprs.get(0).getTypeInfo();
        LOG.info("Vectorizer vectorizeOperator map join typeName " + typeInfo.getTypeName());
        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
        case BOOLEAN:
          hashTableKeyType = HashTableKeyType.BOOLEAN;
          break;
        case BYTE:
          hashTableKeyType = HashTableKeyType.BYTE;
          break;
        case SHORT:
          hashTableKeyType = HashTableKeyType.SHORT;
          break;
        case INT:
          hashTableKeyType = HashTableKeyType.INT;
          break;
        case DATE:
          hashTableKeyType = HashTableKeyType.DATE;
          break;
        case LONG:
          hashTableKeyType = HashTableKeyType.LONG;
          break;
        case STRING:
        case CHAR:
        case VARCHAR:
        case BINARY:
          hashTableKeyType = HashTableKeyType.STRING;
        default:
          // Stay with multi-key.
        }
      }
    }

    switch (joinType) {
    case JoinDesc.INNER_JOIN:
      if (!isInnerBigOnly) {
        vectorMapJoinVariation = VectorMapJoinVariation.INNER;
        hashTableKind = HashTableKind.HASH_MAP;
      } else {
        vectorMapJoinVariation = VectorMapJoinVariation.INNER_BIG_ONLY;
        hashTableKind = HashTableKind.HASH_MULTISET;
      }
      break;
    case JoinDesc.LEFT_OUTER_JOIN:
    case JoinDesc.RIGHT_OUTER_JOIN:
      vectorMapJoinVariation = VectorMapJoinVariation.OUTER;
      hashTableKind = HashTableKind.HASH_MAP;
      break;
    case JoinDesc.FULL_OUTER_JOIN:
      vectorMapJoinVariation = VectorMapJoinVariation.FULL_OUTER;
      hashTableKind = HashTableKind.HASH_MAP;
      break;
    case JoinDesc.LEFT_SEMI_JOIN:
      vectorMapJoinVariation = VectorMapJoinVariation.LEFT_SEMI;
      hashTableKind = HashTableKind.HASH_SET;
      break;
    case JoinDesc.ANTI_JOIN:
        vectorMapJoinVariation = VectorMapJoinVariation.LEFT_ANTI;
        hashTableKind = HashTableKind.HASH_SET;
        break;
    default:
      throw new HiveException("Unknown join type " + joinType);
    }

    LOG.info("Vectorizer vectorizeOperator map join hashTableKind " + hashTableKind.name() + " hashTableKeyType " + hashTableKeyType.name());

    switch (hashTableKeyType) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case DATE:
    case LONG:
      switch (vectorMapJoinVariation) {
      case INNER:
        opClass = VectorMapJoinInnerLongOperator.class;
        break;
      case INNER_BIG_ONLY:
        opClass = VectorMapJoinInnerBigOnlyLongOperator.class;
        break;
      case LEFT_SEMI:
        opClass = VectorMapJoinLeftSemiLongOperator.class;
        break;
      case LEFT_ANTI:
        opClass = VectorMapJoinAntiJoinLongOperator.class;
        break;
      case OUTER:
        opClass = VectorMapJoinOuterLongOperator.class;
        break;
      case FULL_OUTER:
        opClass = VectorMapJoinFullOuterLongOperator.class;
        break;
      default:
        throw new HiveException("Unknown operator variation " + vectorMapJoinVariation);
      }
      break;
    case STRING:
      switch (vectorMapJoinVariation) {
      case INNER:
        opClass = VectorMapJoinInnerStringOperator.class;
        break;
      case INNER_BIG_ONLY:
        opClass = VectorMapJoinInnerBigOnlyStringOperator.class;
        break;
      case LEFT_SEMI:
        opClass = VectorMapJoinLeftSemiStringOperator.class;
        break;
      case LEFT_ANTI:
        opClass = VectorMapJoinAntiJoinStringOperator.class;
        break;
      case OUTER:
        opClass = VectorMapJoinOuterStringOperator.class;
        break;
      case FULL_OUTER:
        opClass = VectorMapJoinFullOuterStringOperator.class;
        break;
      default:
        throw new HiveException("Unknown operator variation " + vectorMapJoinVariation);
      }
      break;
    case MULTI_KEY:
      switch (vectorMapJoinVariation) {
      case INNER:
        opClass = VectorMapJoinInnerMultiKeyOperator.class;
        break;
      case INNER_BIG_ONLY:
        opClass = VectorMapJoinInnerBigOnlyMultiKeyOperator.class;
        break;
      case LEFT_SEMI:
        opClass = VectorMapJoinLeftSemiMultiKeyOperator.class;
        break;
      case LEFT_ANTI:
        opClass = VectorMapJoinAntiJoinMultiKeyOperator.class;
        break;
      case OUTER:
        opClass = VectorMapJoinOuterMultiKeyOperator.class;
        break;
      case FULL_OUTER:
        opClass = VectorMapJoinFullOuterMultiKeyOperator.class;
        break;
      default:
        throw new HiveException("Unknown operator variation " + vectorMapJoinVariation);
      }
      break;
    default:
      throw new RuntimeException("Unexpected hash table key type " + hashTableKeyType.name());
    }

    boolean minMaxEnabled = HiveConf.getBoolVar(hiveConf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_MINMAX_ENABLED);

    vectorDesc.setHashTableImplementationType(hashTableImplementationType);
    vectorDesc.setHashTableKind(hashTableKind);
    vectorDesc.setHashTableKeyType(hashTableKeyType);
    vectorDesc.setVectorMapJoinVariation(vectorMapJoinVariation);
    if (vectorMapJoinVariation == VectorMapJoinVariation.FULL_OUTER) {

      vectorDesc.setIsFullOuter(true);
    }
    vectorDesc.setMinMaxEnabled(minMaxEnabled);
    vectorDesc.setVectorMapJoinInfo(vectorMapJoinInfo);

    vectorOp = OperatorFactory.getVectorOperator(
        opClass, op.getCompilationOpContext(), op.getConf(), vContext, vectorDesc);
    LOG.info("Vectorizer vectorizeOperator map join class " + vectorOp.getClass().getSimpleName());

    return vectorOp;
  }

  public static boolean onExpressionHasNullSafes(MapJoinDesc desc) {
    boolean[] nullSafes = desc.getNullSafes();
    if (nullSafes == null) {
      return false;
    }
    for (boolean nullSafe : nullSafes) {
      if (nullSafe) {
        return true;
      }
    }
    return false;
  }

  private boolean canSpecializeMapJoin(Operator<? extends OperatorDesc> op, MapJoinDesc desc,
      boolean isTez, VectorizationContext vContext, VectorMapJoinDesc vectorDesc)
          throws HiveException {

    Preconditions.checkState(op instanceof MapJoinOperator);

    VectorMapJoinInfo vectorMapJoinInfo = new VectorMapJoinInfo();

    boolean isVectorizationMapJoinNativeEnabled = HiveConf.getBoolVar(hiveConf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_ENABLED);

    String engine = HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);

    boolean oneMapJoinCondition = (desc.getConds().length == 1);

    boolean hasNullSafes = onExpressionHasNullSafes(desc);

    byte posBigTable = (byte) desc.getPosBigTable();

    // Since we want to display all the met and not met conditions in EXPLAIN, we determine all
    // information first....

    List<ExprNodeDesc> keyDesc = desc.getKeys().get(posBigTable);

    boolean outerJoinHasNoKeys = (!desc.isNoOuterJoin() && keyDesc.size() == 0);

    // For now, we don't support joins on or using DECIMAL_64.
    VectorExpression[] allBigTableKeyExpressions =
        vContext.getVectorExpressionsUpConvertDecimal64(keyDesc);
    final int allBigTableKeyExpressionsLength = allBigTableKeyExpressions.length;
    boolean supportsKeyTypes = true;  // Assume.
    HashSet<String> notSupportedKeyTypes = new HashSet<String>();

    // Since a key expression can be a calculation and the key will go into a scratch column,
    // we need the mapping and type information.
    int[] bigTableKeyColumnMap = new int[allBigTableKeyExpressionsLength];
    String[] bigTableKeyColumnNames = new String[allBigTableKeyExpressionsLength];
    TypeInfo[] bigTableKeyTypeInfos = new TypeInfo[allBigTableKeyExpressionsLength];
    ArrayList<VectorExpression> bigTableKeyExpressionsList = new ArrayList<VectorExpression>();
    VectorExpression[] slimmedBigTableKeyExpressions;
    for (int i = 0; i < allBigTableKeyExpressionsLength; i++) {
      VectorExpression ve = allBigTableKeyExpressions[i];
      if (!IdentityExpression.isColumnOnly(ve)) {
        bigTableKeyExpressionsList.add(ve);
      }
      bigTableKeyColumnMap[i] = ve.getOutputColumnNum();

      ExprNodeDesc exprNode = keyDesc.get(i);
      bigTableKeyColumnNames[i] = exprNode.toString();

      TypeInfo typeInfo = exprNode.getTypeInfo();
      // Verify we handle the key column types for an optimized table.  This is the effectively the
      // same check used in HashTableLoader.
      if (!MapJoinKey.isSupportedField(typeInfo)) {
        supportsKeyTypes = false;
        Category category = typeInfo.getCategory();
        notSupportedKeyTypes.add(
            (category != Category.PRIMITIVE ? category.toString() :
              ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory().toString()));
      }
      bigTableKeyTypeInfos[i] = typeInfo;
    }
    if (bigTableKeyExpressionsList.size() == 0) {
      slimmedBigTableKeyExpressions = null;
    } else {
      slimmedBigTableKeyExpressions = bigTableKeyExpressionsList.toArray(new VectorExpression[0]);
    }

    List<ExprNodeDesc> bigTableExprs = desc.getExprs().get(posBigTable);

    // For now, we don't support joins on or using DECIMAL_64.
    VectorExpression[] allBigTableValueExpressions =
        vContext.getVectorExpressions(bigTableExprs);

    boolean isFastHashTableEnabled =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_FAST_HASHTABLE_ENABLED);

    // Especially since LLAP is prone to turn it off in the MapJoinDesc in later
    // physical optimizer stages...
    boolean isHybridHashJoin = desc.isHybridHashJoin();

    /*
     * Populate vectorMapJoininfo.
     */

    /*
     * Similarly, we need a mapping since a value expression can be a calculation and the value
     * will go into a scratch column.
     *
     * Value expressions include keys? YES.
     */
    boolean supportsValueTypes = true;  // Assume.
    HashSet<String> notSupportedValueTypes = new HashSet<String>();

    int[] bigTableValueColumnMap = new int[allBigTableValueExpressions.length];
    String[] bigTableValueColumnNames = new String[allBigTableValueExpressions.length];
    TypeInfo[] bigTableValueTypeInfos = new TypeInfo[allBigTableValueExpressions.length];
    ArrayList<VectorExpression> bigTableValueExpressionsList = new ArrayList<VectorExpression>();
    VectorExpression[] slimmedBigTableValueExpressions;
    for (int i = 0; i < bigTableValueColumnMap.length; i++) {
      VectorExpression ve = allBigTableValueExpressions[i];
      if (!IdentityExpression.isColumnOnly(ve)) {
        bigTableValueExpressionsList.add(ve);
      }
      bigTableValueColumnMap[i] = ve.getOutputColumnNum();

      ExprNodeDesc exprNode = bigTableExprs.get(i);
      bigTableValueColumnNames[i] = exprNode.toString();
      TypeInfo typeInfo = exprNode.getTypeInfo();
      if (!(typeInfo instanceof PrimitiveTypeInfo)) {
        supportsValueTypes = false;
        Category category = typeInfo.getCategory();
        notSupportedValueTypes.add(category.toString());
      }
      bigTableValueTypeInfos[i] = typeInfo;
    }
    if (bigTableValueExpressionsList.size() == 0) {
      slimmedBigTableValueExpressions = null;
    } else {
      slimmedBigTableValueExpressions =
          bigTableValueExpressionsList.toArray(new VectorExpression[0]);
    }

    vectorMapJoinInfo.setBigTableKeyColumnMap(bigTableKeyColumnMap);
    vectorMapJoinInfo.setBigTableKeyColumnNames(bigTableKeyColumnNames);
    vectorMapJoinInfo.setBigTableKeyTypeInfos(bigTableKeyTypeInfos);
    vectorMapJoinInfo.setSlimmedBigTableKeyExpressions(slimmedBigTableKeyExpressions);

    vectorDesc.setAllBigTableKeyExpressions(allBigTableKeyExpressions);

    vectorMapJoinInfo.setBigTableValueColumnMap(bigTableValueColumnMap);
    vectorMapJoinInfo.setBigTableValueColumnNames(bigTableValueColumnNames);
    vectorMapJoinInfo.setBigTableValueTypeInfos(bigTableValueTypeInfos);
    vectorMapJoinInfo.setSlimmedBigTableValueExpressions(slimmedBigTableValueExpressions);

    vectorDesc.setAllBigTableValueExpressions(allBigTableValueExpressions);

    /*
     * Column mapping.
     */
    VectorColumnOutputMapping bigTableRetainMapping =
        new VectorColumnOutputMapping("Big Table Retain Mapping");

    VectorColumnOutputMapping nonOuterSmallTableKeyMapping =
        new VectorColumnOutputMapping("Non Outer Small Table Key Key Mapping");

    VectorColumnOutputMapping outerSmallTableKeyMapping =
        new VectorColumnOutputMapping("Outer Small Table Key Mapping");

    VectorColumnSourceMapping fullOuterSmallTableKeyMapping =
        new VectorColumnSourceMapping("Full Outer Small Table Key Mapping");

    // The order of the fields in the LazyBinary small table value must be used, so
    // we use the source ordering flavor for the mapping.
    VectorColumnSourceMapping smallTableValueMapping =
        new VectorColumnSourceMapping("Small Table Value Mapping");

    Byte[] order = desc.getTagOrder();
    Byte posSingleVectorMapJoinSmallTable = (order[0] == posBigTable ? order[1] : order[0]);
    boolean isOuterJoin = !desc.getNoOuterJoin();

    /*
     * Gather up big and small table output result information from the MapJoinDesc.
     */
    List<Integer> bigTableRetainList = desc.getRetainList().get(posBigTable);

    int[] smallTableIndices;
    int smallTableIndicesSize;
    List<ExprNodeDesc> smallTableExprs = desc.getExprs().get(posSingleVectorMapJoinSmallTable);
    if (desc.getValueIndices() != null && desc.getValueIndices().get(posSingleVectorMapJoinSmallTable) != null) {
      smallTableIndices = desc.getValueIndices().get(posSingleVectorMapJoinSmallTable);
      smallTableIndicesSize = smallTableIndices.length;
    } else {
      smallTableIndices = null;
      smallTableIndicesSize = 0;
    }

    List<Integer> smallTableRetainList = desc.getRetainList().get(posSingleVectorMapJoinSmallTable);
    int smallTableRetainSize = smallTableRetainList.size();

    int smallTableResultSize = 0;
    if (smallTableIndicesSize > 0) {
      smallTableResultSize = smallTableIndicesSize;
    } else if (smallTableRetainSize > 0) {
      smallTableResultSize = smallTableRetainSize;
    }

    /*
     * Determine the big table retained mapping first so we can optimize out (with
     * projection) copying inner join big table keys in the subsequent small table results section.
     */

    // We use a mapping object here so we can build the projection in any order and
    // get the ordered by 0 to n-1 output columns at the end.
    //
    // Also, to avoid copying a big table key into the small table result area for inner joins,
    // we reference it with the projection so there can be duplicate output columns
    // in the projection.
    VectorColumnSourceMapping projectionMapping = new VectorColumnSourceMapping("Projection Mapping");

    int nextOutputColumn = (order[0] == posBigTable ? 0 : smallTableResultSize);

    final int bigTableRetainSize = bigTableRetainList.size();
    for (int i = 0; i < bigTableRetainSize; i++) {

      // Since bigTableValueExpressions may do a calculation and produce a scratch column, we
      // need to map to the right batch column.

      int retainColumn = bigTableRetainList.get(i);
      int batchColumnIndex = bigTableValueColumnMap[retainColumn];
      TypeInfo typeInfo = bigTableValueTypeInfos[i];

      // With this map we project the big table batch to make it look like an output batch.
      projectionMapping.add(nextOutputColumn, batchColumnIndex, typeInfo);

      // Collect columns we copy from the big table batch to the overflow batch.
      if (!bigTableRetainMapping.containsOutputColumn(batchColumnIndex)) {

        // Tolerate repeated use of a big table column.
        bigTableRetainMapping.add(batchColumnIndex, batchColumnIndex, typeInfo);
      }

      nextOutputColumn++;
    }

    /*
     * Now determine the small table results.
     */
    boolean smallTableExprVectorizes = true;

    int firstSmallTableOutputColumn;
    firstSmallTableOutputColumn = (order[0] == posBigTable ? bigTableRetainSize : 0);
    nextOutputColumn = firstSmallTableOutputColumn;

    // Small table indices has more information (i.e. keys) than retain, so use it if it exists...
    if (smallTableIndicesSize > 0) {

      for (int i = 0; i < smallTableIndicesSize; i++) {
        if (smallTableIndices[i] >= 0) {
          // Zero and above numbers indicate a big table key is needed for
          // small table result "area".

          int keyIndex = smallTableIndices[i];

          // Since bigTableKeyExpressions may do a calculation and produce a scratch column, we
          // need to map the right column.
          int bigTableKeyColumn = bigTableKeyColumnMap[keyIndex];
          TypeInfo typeInfo = bigTableKeyTypeInfos[keyIndex];

          if (!isOuterJoin) {

            // Optimize inner join keys of small table results.

            // Project the big table key into the small table result "area".
            projectionMapping.add(nextOutputColumn, bigTableKeyColumn, typeInfo);

            if (!bigTableRetainMapping.containsOutputColumn(bigTableKeyColumn)) {

              // When the Big Key is not retained in the output result, we do need to copy the
              // Big Table key into the overflow batch so the projection of it (Big Table key) to
              // the Small Table key will work properly...
              //
              nonOuterSmallTableKeyMapping.add(bigTableKeyColumn, bigTableKeyColumn, typeInfo);
            }
          } else {

            // For outer joins, since the small table key can be null when there for NOMATCH,
            // we must have a physical (scratch) column for those keys.  We cannot use the
            // projection optimization used by non-[FULL} OUTER joins above.

            int scratchColumn = vContext.allocateScratchColumn(typeInfo);
            projectionMapping.add(nextOutputColumn, scratchColumn, typeInfo);

            outerSmallTableKeyMapping.add(bigTableKeyColumn, scratchColumn, typeInfo);

            // For FULL OUTER MapJoin, we need to be able to deserialize a Small Table key
            // into the output result.
            fullOuterSmallTableKeyMapping.add(keyIndex, scratchColumn, typeInfo);
          }
        } else {

          // Negative numbers indicate a column to be (deserialize) read from the small table's
          // LazyBinary value row.
          int smallTableValueIndex = -smallTableIndices[i] - 1;

          ExprNodeDesc smallTableExprNode = smallTableExprs.get(i);
          if (!validateExprNodeDesc(smallTableExprNode, "Small Table")) {
            clearNotVectorizedReason();
            smallTableExprVectorizes = false;
          }

          TypeInfo typeInfo = smallTableExprNode.getTypeInfo();

          // Make a new big table scratch column for the small table value.
          int scratchColumn = vContext.allocateScratchColumn(typeInfo);
          projectionMapping.add(nextOutputColumn, scratchColumn, typeInfo);

          smallTableValueMapping.add(smallTableValueIndex, scratchColumn, typeInfo);
        }
        nextOutputColumn++;
      }
    } else if (smallTableRetainSize > 0) {
      // Only small table values appear in join output result.

      for (int i = 0; i < smallTableRetainSize; i++) {
        int smallTableValueIndex = smallTableRetainList.get(i);

        ExprNodeDesc smallTableExprNode = smallTableExprs.get(i);
        if (!validateExprNodeDesc(smallTableExprNode, "Small Table")) {
          clearNotVectorizedReason();
          smallTableExprVectorizes = false;
        }

        // Make a new big table scratch column for the small table value.
        TypeInfo typeInfo = smallTableExprNode.getTypeInfo();
        int scratchColumn = vContext.allocateScratchColumn(typeInfo);

        projectionMapping.add(nextOutputColumn, scratchColumn, typeInfo);

        smallTableValueMapping.add(smallTableValueIndex, scratchColumn, typeInfo);
        nextOutputColumn++;
      }
    }

    Map<Byte, List<ExprNodeDesc>> filterExpressions = desc.getFilters();
    VectorExpression[] bigTableFilterExpressions =
        vContext.getVectorExpressions(
            filterExpressions.get(posBigTable),
            VectorExpressionDescriptor.Mode.FILTER);
    vectorMapJoinInfo.setBigTableFilterExpressions(bigTableFilterExpressions);

    boolean useOptimizedTable =
        HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_MAPJOIN_USE_OPTIMIZED_TABLE);

    // Remember the condition variables for EXPLAIN regardless of whether we specialize or not.
    vectorDesc.setVectorMapJoinInfo(vectorMapJoinInfo);

    vectorDesc.setUseOptimizedTable(useOptimizedTable);
    vectorDesc.setIsVectorizationMapJoinNativeEnabled(isVectorizationMapJoinNativeEnabled);
    vectorDesc.setEngine(engine);
    vectorDesc.setOneMapJoinCondition(oneMapJoinCondition);
    vectorDesc.setHasNullSafes(hasNullSafes);
    vectorDesc.setSmallTableExprVectorizes(smallTableExprVectorizes);
    vectorDesc.setOuterJoinHasNoKeys(outerJoinHasNoKeys);

    vectorDesc.setIsFastHashTableEnabled(isFastHashTableEnabled);
    vectorDesc.setIsHybridHashJoin(isHybridHashJoin);

    vectorDesc.setSupportsKeyTypes(supportsKeyTypes);
    if (!supportsKeyTypes) {
      vectorDesc.setNotSupportedKeyTypes(new ArrayList<>(notSupportedKeyTypes));
    }
    vectorDesc.setSupportsValueTypes(supportsValueTypes);
    if (!supportsValueTypes) {
      vectorDesc.setNotSupportedValueTypes(new ArrayList<>(notSupportedValueTypes));
    }

    // Check common conditions for both Optimized and Fast Hash Tables.
    boolean result = true;    // Assume.
    if (!useOptimizedTable ||
        !isVectorizationMapJoinNativeEnabled ||
        !isTez ||
        !oneMapJoinCondition ||
        hasNullSafes ||
        !smallTableExprVectorizes ||
        outerJoinHasNoKeys ||
        !supportsValueTypes) {
      result = false;
    }

    // supportsKeyTypes

    if (!isFastHashTableEnabled) {

      // Check optimized-only hash table restrictions.
      if (!supportsKeyTypes) {
        result = false;
      }

    } else {

      // With the fast hash table implementation, we currently do not support
      // Hybrid Grace Hash Join.

      if (isHybridHashJoin) {
        result = false;
      }
    }

    // Convert dynamic arrays and maps to simple arrays.

    bigTableRetainMapping.finalize();
    vectorMapJoinInfo.setBigTableRetainColumnMap(bigTableRetainMapping.getOutputColumns());
    vectorMapJoinInfo.setBigTableRetainTypeInfos(bigTableRetainMapping.getTypeInfos());

    nonOuterSmallTableKeyMapping.finalize();
    vectorMapJoinInfo.setNonOuterSmallTableKeyColumnMap(nonOuterSmallTableKeyMapping.getOutputColumns());
    vectorMapJoinInfo.setNonOuterSmallTableKeyTypeInfos(nonOuterSmallTableKeyMapping.getTypeInfos());

    outerSmallTableKeyMapping.finalize();
    fullOuterSmallTableKeyMapping.finalize();

    vectorMapJoinInfo.setOuterSmallTableKeyMapping(outerSmallTableKeyMapping);
    vectorMapJoinInfo.setFullOuterSmallTableKeyMapping(fullOuterSmallTableKeyMapping);

    smallTableValueMapping.finalize();

    vectorMapJoinInfo.setSmallTableValueMapping(smallTableValueMapping);

    projectionMapping.finalize();

    // Verify we added an entry for each output.
    assert projectionMapping.isSourceSequenceGood();

    vectorMapJoinInfo.setProjectionMapping(projectionMapping);

    return result;
  }

  private Operator<? extends OperatorDesc> specializeReduceSinkOperator(
      Operator<? extends OperatorDesc> op, VectorizationContext vContext, ReduceSinkDesc desc,
      VectorReduceSinkDesc vectorDesc) throws HiveException {

    VectorReduceSinkInfo vectorReduceSinkInfo = vectorDesc.getVectorReduceSinkInfo();

    Type[] reduceSinkKeyColumnVectorTypes = vectorReduceSinkInfo.getReduceSinkKeyColumnVectorTypes();

    // By default, we can always use the multi-key class.
    VectorReduceSinkDesc.ReduceSinkKeyType reduceSinkKeyType = VectorReduceSinkDesc.ReduceSinkKeyType.MULTI_KEY;

    // Look for single column optimization.
    if (reduceSinkKeyColumnVectorTypes != null && reduceSinkKeyColumnVectorTypes.length == 1) {
      LOG.info("Vectorizer vectorizeOperator groupby typeName " + vectorReduceSinkInfo.getReduceSinkKeyTypeInfos()[0]);
      Type columnVectorType = reduceSinkKeyColumnVectorTypes[0];
      switch (columnVectorType) {
      case LONG:
        {
          PrimitiveCategory primitiveCategory =
              ((PrimitiveTypeInfo) vectorReduceSinkInfo.getReduceSinkKeyTypeInfos()[0]).getPrimitiveCategory();
          switch (primitiveCategory) {
          case BOOLEAN:
          case BYTE:
          case SHORT:
          case INT:
          case DATE:
          case LONG:
            reduceSinkKeyType = VectorReduceSinkDesc.ReduceSinkKeyType.LONG;
            break;
          default:
            // For any remaining Long CV types use default multi-key
            LOG.warn("Unsupported Long-CV key type {} defaulted to multi-key ReduceSink", primitiveCategory);
            break;
          }
        }
        break;
      case BYTES:
        reduceSinkKeyType = VectorReduceSinkDesc.ReduceSinkKeyType.STRING;
      default:
        // Stay with multi-key.
        break;
      }
    }

    Class<? extends Operator<?>> opClass = null;
    if (vectorReduceSinkInfo.getUseUniformHash()) {
      if (vectorDesc.getIsEmptyKey()) {
        opClass = VectorReduceSinkEmptyKeyOperator.class;
      } else {
        switch (reduceSinkKeyType) {
        case LONG:
          opClass = VectorReduceSinkLongOperator.class;
          break;
        case STRING:
          opClass = VectorReduceSinkStringOperator.class;
          break;
        case MULTI_KEY:
          opClass = VectorReduceSinkMultiKeyOperator.class;
          break;
        default:
          throw new HiveException("Unknown reduce sink key type " + reduceSinkKeyType);
        }
      }
    } else {
      if (vectorDesc.getIsEmptyKey() && vectorDesc.getIsEmptyBuckets() && vectorDesc.getIsEmptyPartitions()) {
        opClass = VectorReduceSinkEmptyKeyOperator.class;
      } else {
        opClass = VectorReduceSinkObjectHashOperator.class;
      }
    }

    vectorDesc.setReduceSinkKeyType(reduceSinkKeyType);
    vectorDesc.setVectorReduceSinkInfo(vectorReduceSinkInfo);

    LOG.info("Vectorizer vectorizeOperator reduce sink class " + opClass.getSimpleName());

    Operator<? extends OperatorDesc> vectorOp = null;
    try {
      vectorOp = OperatorFactory.getVectorOperator(
          opClass, op.getCompilationOpContext(), op.getConf(),
          vContext, vectorDesc);
    } catch (Exception e) {
      LOG.info("Vectorizer vectorizeOperator reduce sink class exception " + opClass.getSimpleName() +
          " exception " + e);
      throw new HiveException(e);
    }

    Preconditions.checkArgument(vectorOp instanceof VectorReduceSinkCommonOperator);

    return vectorOp;
  }

  private boolean canSpecializeReduceSink(ReduceSinkDesc desc,
      boolean isTez, VectorizationContext vContext,
      VectorReduceSinkDesc vectorDesc) throws HiveException {

    VectorReduceSinkInfo vectorReduceSinkInfo = new VectorReduceSinkInfo();

    // Various restrictions.

    // Set this if we encounter a condition we were not expecting.
    boolean isUnexpectedCondition = false;

    boolean isVectorizationReduceSinkNativeEnabled =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_REDUCESINK_NEW_ENABLED);

    String engine = HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);

    int limit = desc.getTopN();
    float memUsage = desc.getTopNMemoryUsage();

    boolean hasPTFTopN = (limit >= 0 && memUsage > 0 && desc.isPTFReduceSink());

    boolean hasDistinctColumns = (desc.getDistinctColumnIndices().size() > 0);

    TableDesc keyTableDesc = desc.getKeySerializeInfo();
    Class<? extends Deserializer> keySerializerClass = keyTableDesc.getSerDeClass();
    boolean isKeyBinarySortable = (keySerializerClass == org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe.class);

    TableDesc valueTableDesc = desc.getValueSerializeInfo();
    Class<? extends Deserializer> valueDeserializerClass = valueTableDesc.getSerDeClass();
    boolean isValueLazyBinary =
            (valueDeserializerClass == org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe.class) ||
            (valueDeserializerClass == org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe2.class);

    // We are doing work here we'd normally do in VectorGroupByCommonOperator's constructor.
    // So if we later decide not to specialize, we'll just waste any scratch columns allocated...

    List<ExprNodeDesc> keysDescs = desc.getKeyCols();
    final boolean isEmptyKey = (keysDescs.size() == 0);
    if (!isEmptyKey) {

      VectorExpression[] allKeyExpressions = vContext.getVectorExpressions(keysDescs);

      final int[] reduceSinkKeyColumnMap = new int[allKeyExpressions.length];
      final TypeInfo[] reduceSinkKeyTypeInfos = new TypeInfo[allKeyExpressions.length];
      final Type[] reduceSinkKeyColumnVectorTypes = new Type[allKeyExpressions.length];
      final VectorExpression[] reduceSinkKeyExpressions;

      // Since a key expression can be a calculation and the key will go into a scratch column,
      // we need the mapping and type information.
      ArrayList<VectorExpression> groupByKeyExpressionsList = new ArrayList<VectorExpression>();
      for (int i = 0; i < reduceSinkKeyColumnMap.length; i++) {
        VectorExpression ve = allKeyExpressions[i];
        reduceSinkKeyColumnMap[i] = ve.getOutputColumnNum();
        reduceSinkKeyTypeInfos[i] = keysDescs.get(i).getTypeInfo();
        reduceSinkKeyColumnVectorTypes[i] =
            VectorizationContext.getColumnVectorTypeFromTypeInfo(reduceSinkKeyTypeInfos[i]);
        if (!IdentityExpression.isColumnOnly(ve)) {
          groupByKeyExpressionsList.add(ve);
        }
      }
      if (groupByKeyExpressionsList.size() == 0) {
        reduceSinkKeyExpressions = null;
      } else {
        reduceSinkKeyExpressions = groupByKeyExpressionsList.toArray(new VectorExpression[0]);
      }

      vectorReduceSinkInfo.setReduceSinkKeyColumnMap(reduceSinkKeyColumnMap);
      vectorReduceSinkInfo.setReduceSinkKeyTypeInfos(reduceSinkKeyTypeInfos);
      vectorReduceSinkInfo.setReduceSinkKeyColumnVectorTypes(reduceSinkKeyColumnVectorTypes);
      vectorReduceSinkInfo.setReduceSinkKeyExpressions(reduceSinkKeyExpressions);
    }

    List<ExprNodeDesc> valueDescs = desc.getValueCols();
    final boolean isEmptyValue = (valueDescs.size() == 0);
    if (!isEmptyValue) {
      VectorExpression[] allValueExpressions = vContext.getVectorExpressions(valueDescs);

      final int[] reduceSinkValueColumnMap = new int[allValueExpressions.length];
      final TypeInfo[] reduceSinkValueTypeInfos = new TypeInfo[allValueExpressions.length];
      final Type[] reduceSinkValueColumnVectorTypes = new Type[allValueExpressions.length];
      VectorExpression[] reduceSinkValueExpressions;

      ArrayList<VectorExpression> reduceSinkValueExpressionsList = new ArrayList<VectorExpression>();
      for (int i = 0; i < valueDescs.size(); ++i) {
        VectorExpression ve = allValueExpressions[i];
        reduceSinkValueColumnMap[i] = ve.getOutputColumnNum();
        reduceSinkValueTypeInfos[i] = valueDescs.get(i).getTypeInfo();
        reduceSinkValueColumnVectorTypes[i] =
            VectorizationContext.getColumnVectorTypeFromTypeInfo(reduceSinkValueTypeInfos[i]);
        if (!IdentityExpression.isColumnOnly(ve)) {
          reduceSinkValueExpressionsList.add(ve);
        }
      }
      if (reduceSinkValueExpressionsList.size() == 0) {
        reduceSinkValueExpressions = null;
      } else {
        reduceSinkValueExpressions = reduceSinkValueExpressionsList.toArray(new VectorExpression[0]);
      }

      vectorReduceSinkInfo.setReduceSinkValueColumnMap(reduceSinkValueColumnMap);
      vectorReduceSinkInfo.setReduceSinkValueTypeInfos(reduceSinkValueTypeInfos);
      vectorReduceSinkInfo.setReduceSinkValueColumnVectorTypes(reduceSinkValueColumnVectorTypes);
      vectorReduceSinkInfo.setReduceSinkValueExpressions(reduceSinkValueExpressions);

    }

    boolean useUniformHash = desc.getReducerTraits().contains(UNIFORM);
    vectorReduceSinkInfo.setUseUniformHash(useUniformHash);

    List<ExprNodeDesc> bucketDescs = desc.getBucketCols();
    final boolean isEmptyBuckets = (bucketDescs == null || bucketDescs.size() == 0);
    List<ExprNodeDesc> partitionDescs = desc.getPartitionCols();
    final boolean isEmptyPartitions = (partitionDescs == null || partitionDescs.size() == 0);

    if (useUniformHash || (isEmptyKey && isEmptyBuckets && isEmptyPartitions)) {

      // NOTE: For Uniform Hash or no buckets/partitions, when the key is empty, we will use the VectorReduceSinkEmptyKeyOperator instead.

    } else {

      // Collect bucket and/or partition information for object hashing.

      int[] reduceSinkBucketColumnMap = null;
      TypeInfo[] reduceSinkBucketTypeInfos = null;
      Type[] reduceSinkBucketColumnVectorTypes = null;
      VectorExpression[] reduceSinkBucketExpressions = null;

      if (!isEmptyBuckets) {
        VectorExpression[] allBucketExpressions = vContext.getVectorExpressions(bucketDescs);

        reduceSinkBucketColumnMap = new int[bucketDescs.size()];
        reduceSinkBucketTypeInfos = new TypeInfo[bucketDescs.size()];
        reduceSinkBucketColumnVectorTypes = new Type[bucketDescs.size()];
        ArrayList<VectorExpression> reduceSinkBucketExpressionsList = new ArrayList<VectorExpression>();
        for (int i = 0; i < bucketDescs.size(); ++i) {
          VectorExpression ve = allBucketExpressions[i];
          reduceSinkBucketColumnMap[i] = ve.getOutputColumnNum();
          reduceSinkBucketTypeInfos[i] = bucketDescs.get(i).getTypeInfo();
          reduceSinkBucketColumnVectorTypes[i] =
              VectorizationContext.getColumnVectorTypeFromTypeInfo(reduceSinkBucketTypeInfos[i]);
          if (!IdentityExpression.isColumnOnly(ve)) {
            reduceSinkBucketExpressionsList.add(ve);
          }
        }
        if (reduceSinkBucketExpressionsList.size() == 0) {
          reduceSinkBucketExpressions = null;
        } else {
          reduceSinkBucketExpressions = reduceSinkBucketExpressionsList.toArray(new VectorExpression[0]);
        }
      }

      int[] reduceSinkPartitionColumnMap = null;
      TypeInfo[] reduceSinkPartitionTypeInfos = null;
      Type[] reduceSinkPartitionColumnVectorTypes = null;
      VectorExpression[] reduceSinkPartitionExpressions = null;

      if (!isEmptyPartitions) {
        VectorExpression[] allPartitionExpressions = vContext.getVectorExpressions(partitionDescs);

        reduceSinkPartitionColumnMap = new int[partitionDescs.size()];
        reduceSinkPartitionTypeInfos = new TypeInfo[partitionDescs.size()];
        reduceSinkPartitionColumnVectorTypes = new Type[partitionDescs.size()];
        ArrayList<VectorExpression> reduceSinkPartitionExpressionsList = new ArrayList<VectorExpression>();
        for (int i = 0; i < partitionDescs.size(); ++i) {
          VectorExpression ve = allPartitionExpressions[i];
          reduceSinkPartitionColumnMap[i] = ve.getOutputColumnNum();
          reduceSinkPartitionTypeInfos[i] = partitionDescs.get(i).getTypeInfo();
          reduceSinkPartitionColumnVectorTypes[i] =
              VectorizationContext.getColumnVectorTypeFromTypeInfo(reduceSinkPartitionTypeInfos[i]);
          if (!IdentityExpression.isColumnOnly(ve)) {
            reduceSinkPartitionExpressionsList.add(ve);
          }
        }
        if (reduceSinkPartitionExpressionsList.size() == 0) {
          reduceSinkPartitionExpressions = null;
        } else {
          reduceSinkPartitionExpressions = reduceSinkPartitionExpressionsList.toArray(new VectorExpression[0]);
        }
      }

      vectorReduceSinkInfo.setReduceSinkBucketColumnMap(reduceSinkBucketColumnMap);
      vectorReduceSinkInfo.setReduceSinkBucketTypeInfos(reduceSinkBucketTypeInfos);
      vectorReduceSinkInfo.setReduceSinkBucketColumnVectorTypes(reduceSinkBucketColumnVectorTypes);
      vectorReduceSinkInfo.setReduceSinkBucketExpressions(reduceSinkBucketExpressions);

      vectorReduceSinkInfo.setReduceSinkPartitionColumnMap(reduceSinkPartitionColumnMap);
      vectorReduceSinkInfo.setReduceSinkPartitionTypeInfos(reduceSinkPartitionTypeInfos);
      vectorReduceSinkInfo.setReduceSinkPartitionColumnVectorTypes(reduceSinkPartitionColumnVectorTypes);
      vectorReduceSinkInfo.setReduceSinkPartitionExpressions(reduceSinkPartitionExpressions);
    }

    // Remember the condition variables for EXPLAIN regardless.

    vectorDesc.setVectorReduceSinkInfo(vectorReduceSinkInfo);

    vectorDesc.setIsVectorizationReduceSinkNativeEnabled(isVectorizationReduceSinkNativeEnabled);
    vectorDesc.setEngine(engine);
    vectorDesc.setIsEmptyKey(isEmptyKey);
    vectorDesc.setIsEmptyValue(isEmptyValue);
    vectorDesc.setIsEmptyBuckets(isEmptyBuckets);
    vectorDesc.setIsEmptyPartitions(isEmptyPartitions);
    vectorDesc.setHasPTFTopN(hasPTFTopN);
    vectorDesc.setHasDistinctColumns(hasDistinctColumns);
    vectorDesc.setIsKeyBinarySortable(isKeyBinarySortable);
    vectorDesc.setIsValueLazyBinary(isValueLazyBinary);
    vectorDesc.setIsAcidChange(desc.getWriteType() == AcidUtils.Operation.DELETE ||
                               desc.getWriteType() == AcidUtils.Operation.UPDATE);

    // This indicates we logged an inconsistency (from our point-of-view) and will not make this
    // operator native...
    vectorDesc.setIsUnexpectedCondition(isUnexpectedCondition);

    // Many restrictions.
    if (!isVectorizationReduceSinkNativeEnabled ||
        !isTez ||
        hasPTFTopN ||
        hasDistinctColumns ||
        !isKeyBinarySortable ||
        !isValueLazyBinary ||
        isUnexpectedCondition) {
      return false;
    }

    return true;
  }

  private boolean checkForArrowFileSink(FileSinkDesc fileSinkDesc,
      boolean isTez, VectorizationContext vContext,
      VectorFileSinkDesc vectorDesc) throws HiveException {

    // Various restrictions.

    boolean isVectorizationFileSinkArrowNativeEnabled =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_FILESINK_ARROW_NATIVE_ENABLED);

    String engine = HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);

    String serdeClassName = fileSinkDesc.getTableInfo().getSerdeClassName();

    boolean isOkArrowFileSink =
        serdeClassName.equals("org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe") &&
        isVectorizationFileSinkArrowNativeEnabled &&
        engine.equalsIgnoreCase("tez");

    return isOkArrowFileSink;
  }

  private Operator<? extends OperatorDesc> specializeArrowFileSinkOperator(
      Operator<? extends OperatorDesc> op, VectorizationContext vContext, FileSinkDesc desc,
      VectorFileSinkDesc vectorDesc) throws HiveException {

    Class<? extends Operator<?>> opClass = VectorFileSinkArrowOperator.class;

    Operator<? extends OperatorDesc> vectorOp = null;
    try {
      vectorOp = OperatorFactory.getVectorOperator(
          opClass, op.getCompilationOpContext(), op.getConf(),
          vContext, vectorDesc);
    } catch (Exception e) {
      LOG.info("Vectorizer vectorizeOperator file sink class exception " + opClass.getSimpleName() +
          " exception " + e);
      throw new HiveException(e);
    }

    return vectorOp;
  }

  private boolean usesVectorUDFAdaptor(VectorExpression vecExpr) {
    if (vecExpr == null) {
      return false;
    }
    if (vecExpr instanceof VectorUDFAdaptor) {
      return true;
    }
    if (usesVectorUDFAdaptor(vecExpr.getChildExpressions())) {
      return true;
    }
    return false;
  }

  private boolean usesVectorUDFAdaptor(VectorExpression[] vecExprs) {
    if (vecExprs == null) {
      return false;
    }
    for (VectorExpression vecExpr : vecExprs) {
      if (usesVectorUDFAdaptor(vecExpr)) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  public static Operator<? extends OperatorDesc> vectorizeFilterOperator(
      Operator<? extends OperatorDesc> filterOp, VectorizationContext vContext,
      VectorFilterDesc vectorFilterDesc)
          throws HiveException {

    FilterDesc filterDesc = (FilterDesc) filterOp.getConf();

    ExprNodeDesc predicateExpr = filterDesc.getPredicate();
    VectorExpression vectorPredicateExpr =
        vContext.getVectorExpression(predicateExpr, VectorExpressionDescriptor.Mode.FILTER);
    if (vectorPredicateExpr != null) {
      vectorPredicateExpr = fixDecimalDataTypePhysicalVariations(vectorPredicateExpr,
          vectorPredicateExpr.getChildExpressions(), vContext);
    }
    vectorFilterDesc.setPredicateExpression(vectorPredicateExpr);
    return OperatorFactory.getVectorOperator(
        filterOp.getCompilationOpContext(), filterDesc,
        vContext, vectorFilterDesc);
  }

  @VisibleForTesting
  public static Operator<? extends OperatorDesc> vectorizeTopNKeyOperator(
          Operator<? extends OperatorDesc> topNKeyOperator, VectorizationContext vContext,
          VectorTopNKeyDesc vectorTopNKeyDesc) throws HiveException {

    TopNKeyDesc topNKeyDesc = (TopNKeyDesc) topNKeyOperator.getConf();
    VectorExpression[] keyExpressions = getVectorExpressions(vContext, topNKeyDesc.getKeyColumns());
    VectorExpression[] partitionKeyExpressions = getVectorExpressions(vContext, topNKeyDesc.getPartitionKeyColumns());

    vectorTopNKeyDesc.setKeyExpressions(keyExpressions);
    vectorTopNKeyDesc.setPartitionKeyColumns(partitionKeyExpressions);
    return OperatorFactory.getVectorOperator(
        topNKeyOperator.getCompilationOpContext(), topNKeyDesc,
        vContext, vectorTopNKeyDesc);
  }

  private static VectorExpression[] getVectorExpressions(VectorizationContext vContext, List<ExprNodeDesc> keyColumns) throws HiveException {
    VectorExpression[] keyExpressions;
    vContext.markActualScratchColumns();
    try {
      keyExpressions = vContext.getVectorExpressionsUpConvertDecimal64(keyColumns);
      fixDecimalDataTypePhysicalVariations(vContext, keyExpressions);
    } finally {
      vContext.freeMarkedScratchColumns();
    }
    return keyExpressions;
  }

  private static Class<? extends VectorAggregateExpression> findVecAggrClass(
      Class<? extends VectorAggregateExpression>[] vecAggrClasses,
      String aggregateName, ColumnVector.Type inputColVectorType,
      ColumnVector.Type outputColumnVecType, GenericUDAFEvaluator.Mode udafEvaluatorMode)
          throws HiveException {

    for (Class<? extends VectorAggregateExpression> vecAggrClass : vecAggrClasses) {

      VectorAggregateExpression vecAggrExprCheck;
      try {
        vecAggrExprCheck = vecAggrClass.newInstance();
      } catch (Exception e) {
        throw new HiveException(
            vecAggrClass.getSimpleName() + "() failed to initialize", e);
      }

      if (vecAggrExprCheck.matches(
          aggregateName, inputColVectorType, outputColumnVecType, udafEvaluatorMode)) {
        return vecAggrClass;
      }
    }
    return null;
  }

  private static ImmutablePair<VectorAggregationDesc,String> getVectorAggregationDesc(
      AggregationDesc aggrDesc, VectorizationContext vContext) throws HiveException {

    String aggregateName = aggrDesc.getGenericUDAFName();
    List<ExprNodeDesc> parameterList = aggrDesc.getParameters();
    final int parameterCount = parameterList.size();
    final GenericUDAFEvaluator.Mode udafEvaluatorMode = aggrDesc.getMode();

    /*
     * Look at evaluator to get output type info.
     */
    GenericUDAFEvaluator evaluator = aggrDesc.getGenericUDAFEvaluator();

    ObjectInspector[] parameterObjectInspectors = new ObjectInspector[parameterCount];
    for (int i = 0; i < parameterCount; i++) {
      TypeInfo typeInfo = parameterList.get(i).getTypeInfo();
      parameterObjectInspectors[i] = TypeInfoUtils
          .getStandardWritableObjectInspectorFromTypeInfo(typeInfo);
    }

    // The only way to get the return object inspector (and its return type) is to
    // initialize it...
    ObjectInspector returnOI =
        evaluator.init(
            aggrDesc.getMode(),
            parameterObjectInspectors);

    final TypeInfo outputTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(returnOI.getTypeName());

    return getVectorAggregationDesc(
        aggregateName, parameterList, evaluator, outputTypeInfo, udafEvaluatorMode, vContext);
  }

  public static ImmutablePair<VectorAggregationDesc,String> getVectorAggregationDesc(
      String aggregationName, List<ExprNodeDesc> parameterList,
      GenericUDAFEvaluator evaluator, TypeInfo outputTypeInfo,
      GenericUDAFEvaluator.Mode udafEvaluatorMode,
      VectorizationContext vContext)
          throws HiveException {

    Class<? extends VectorAggregateExpression>[] vecAggrClasses;
    // "ds_kll_sketch" needs special treatment because the UDAF is coming from data
    // sketches library, we cannot add annotations there
    if (aggregationName.equals(VECTORIZABLE_UDAF.DS_KLL_SKETCH.toString())) {
      vecAggrClasses = new Class[] {
          VectorUDAFComputeDsKllSketchDouble.class, VectorUDAFComputeDsKllSketchFinal.class
      };
    } else {
      VectorizedUDAFs annotation =
          AnnotationUtils.getAnnotation(evaluator.getClass(), VectorizedUDAFs.class);
      if (annotation == null) {
        String issue = "Evaluator " + evaluator.getClass().getSimpleName() + " does not have a "
            + "vectorized UDAF annotation (aggregation: \"" + aggregationName + "\"). " + "Vectorization not supported";
        return new ImmutablePair<>(null, issue);
      }
      vecAggrClasses = annotation.value();
    }

    // Not final since it may change later due to DECIMAL_64.
    ColumnVector.Type outputColVectorType =
        VectorizationContext.getColumnVectorTypeFromTypeInfo(outputTypeInfo);

    /*
     * Determine input type info.
     */
    final TypeInfo inputTypeInfo;

    // Not final since it may change later due to DECIMAL_64.
    VectorExpression inputExpression;
    ColumnVector.Type inputColVectorType;

    final int parameterCount = parameterList.size();
    final List<ConstantVectorExpression> constants = new ArrayList<>();

    if (parameterCount == 0) {
      // COUNT(*)
      inputTypeInfo = null;
      inputColVectorType = null;
      inputExpression = null;
    } else {
      ExprNodeDesc exprNodeDesc = parameterList.get(0);
      inputTypeInfo = exprNodeDesc.getTypeInfo();
      if (inputTypeInfo == null) {
        String issue ="Aggregations with null parameter type not supported " +
            aggregationName + "(" + parameterList + ")";
        return new ImmutablePair<>(null, issue);
      }

      /*
       * Determine an *initial* input vector expression.
       *
       * Note: we may have to convert it later from DECIMAL_64 to regular decimal.
       */
      inputExpression =
          vContext.getVectorExpression(
              exprNodeDesc, VectorExpressionDescriptor.Mode.PROJECTION);
      if (inputExpression == null) {
        String issue ="Parameter expression " + exprNodeDesc + " not supported " +
            aggregationName + "(" + parameterList + ")";
        return new ImmutablePair<>(null, issue);
      }
      if (inputExpression.getOutputTypeInfo() == null) {
        String issue ="Parameter expression " + exprNodeDesc + " with null type not supported " +
            aggregationName + "(" + parameterList + ")";
        return new ImmutablePair<>(null, issue);
      }
      inputColVectorType = inputExpression.getOutputColumnVectorType();

      // ignore the first param as it's supposed to be a column expression
      constants.addAll(getConstantParams(vContext, parameterList));

      if (constants.size() != parameterCount - 1) {
        // No multi-parameter aggregations supported unless all the extra parameters are constants.
        String issue = "Aggregations with > 1 parameter are not supported unless all the extra "
            + "parameters are constants " + aggregationName + "(" + parameterList + ")";
        return new ImmutablePair<>(null, issue);
      }
    }

    /*
     * When we have DECIMAL_64 as the input parameter then we have to see if there is a special
     * vector UDAF for it.  If not we will need to convert the input parameter.
     */
    if (inputTypeInfo != null && inputColVectorType == ColumnVector.Type.DECIMAL_64) {

      if (outputColVectorType == ColumnVector.Type.DECIMAL) {
        DecimalTypeInfo outputDecimalTypeInfo = (DecimalTypeInfo) outputTypeInfo;
        if (HiveDecimalWritable.isPrecisionDecimal64(outputDecimalTypeInfo.getPrecision())) {

          // Try with DECIMAL_64 input and DECIMAL_64 output.
          final Class<? extends VectorAggregateExpression> vecAggrClass =
              findVecAggrClass(
                  vecAggrClasses, aggregationName, inputColVectorType,
                  ColumnVector.Type.DECIMAL_64, udafEvaluatorMode);
          if (vecAggrClass != null) {
            final VectorAggregationDesc vecAggrDesc =
                new VectorAggregationDesc.VectorAggregationDescBuilder()
                    .aggregationName(aggregationName)
                    .evaluator(evaluator)
                    .udafEvaluatorMode(udafEvaluatorMode)
                    .inputTypeInfo(inputTypeInfo)
                    .inputColVectorType(inputColVectorType)
                    .inputExpression(inputExpression)
                    .outputTypeInfo(outputTypeInfo)
                    .outputColVectorType(ColumnVector.Type.DECIMAL_64)
                    .vectorAggregationClass(vecAggrClass)
                    .constants(constants)
                    .build();
            return new ImmutablePair<>(vecAggrDesc, null);
          }
        }

        // Try with regular DECIMAL output type.
        final Class<? extends VectorAggregateExpression> vecAggrClass =
            findVecAggrClass(
                vecAggrClasses, aggregationName, inputColVectorType,
                outputColVectorType, udafEvaluatorMode);
        if (vecAggrClass != null) {
          final VectorAggregationDesc vecAggrDesc =
              new VectorAggregationDesc.VectorAggregationDescBuilder()
                  .aggregationName(aggregationName)
                  .evaluator(evaluator)
                  .udafEvaluatorMode(udafEvaluatorMode)
                  .inputTypeInfo(inputTypeInfo)
                  .inputColVectorType(inputColVectorType)
                  .inputExpression(inputExpression)
                  .outputTypeInfo(outputTypeInfo)
                  .outputColVectorType(outputColVectorType)
                  .vectorAggregationClass(vecAggrClass)
                  .constants(constants)
                  .build();
          return new ImmutablePair<>(vecAggrDesc, null);
        }

        // No support for DECIMAL_64 input.  We must convert.
        inputExpression = vContext.wrapWithDecimal64ToDecimalConversion(inputExpression);
        inputColVectorType = ColumnVector.Type.DECIMAL;

        // Fall through...
      } else {

        // Try with DECIMAL_64 input and desired output type.
        final Class<? extends VectorAggregateExpression> vecAggrClass =
            findVecAggrClass(
                vecAggrClasses, aggregationName, inputColVectorType,
                outputColVectorType, udafEvaluatorMode);
        if (vecAggrClass != null) {
          // for now, disable operating on decimal64 column vectors for semijoin reduction as
          // we have to make sure same decimal type should be used during bloom filter creation
          // and bloom filter probing
          if (aggregationName.equals(BLOOM_FILTER_FUNCTION)) {
            inputExpression = vContext.wrapWithDecimal64ToDecimalConversion(inputExpression);
            inputColVectorType = ColumnVector.Type.DECIMAL;
          }
          final VectorAggregationDesc vecAggrDesc =
              new VectorAggregationDesc.VectorAggregationDescBuilder()
                  .aggregationName(aggregationName)
                  .evaluator(evaluator)
                  .udafEvaluatorMode(udafEvaluatorMode)
                  .inputTypeInfo(inputTypeInfo)
                  .inputColVectorType(inputColVectorType)
                  .inputExpression(inputExpression)
                  .outputTypeInfo(outputTypeInfo)
                  .outputColVectorType(outputColVectorType)
                  .vectorAggregationClass(vecAggrClass)
                  .constants(constants)
                  .build();
          return new ImmutablePair<>(vecAggrDesc, null);
        }

        // No support for DECIMAL_64 input.  We must convert.
        inputExpression = vContext.wrapWithDecimal64ToDecimalConversion(inputExpression);
        inputColVectorType = ColumnVector.Type.DECIMAL;

        // Fall through...
      }
    }

    /*
     * Look for normal match.
     */
    Class<? extends VectorAggregateExpression> vecAggrClass =
        findVecAggrClass(
            vecAggrClasses, aggregationName, inputColVectorType,
            outputColVectorType, udafEvaluatorMode);
    if (vecAggrClass != null) {
      final VectorAggregationDesc vecAggrDesc =
          new VectorAggregationDesc.VectorAggregationDescBuilder()
              .aggregationName(aggregationName)
              .evaluator(evaluator)
              .udafEvaluatorMode(udafEvaluatorMode)
              .inputTypeInfo(inputTypeInfo)
              .inputColVectorType(inputColVectorType)
              .inputExpression(inputExpression)
              .outputTypeInfo(outputTypeInfo)
              .outputColVectorType(outputColVectorType)
              .vectorAggregationClass(vecAggrClass)
              .constants(constants)
              .build();
      return new ImmutablePair<>(vecAggrDesc, null);
    }

    // No match?
    String issue =
        "Vector aggregation : \"" + aggregationName + "\" " +
            "for input type: " +
                 (inputColVectorType == null ? "any" : "\"" + inputColVectorType) + "\" " +
            "and output type: \"" + outputColVectorType + "\" " +
            "and mode: " + udafEvaluatorMode + " not supported for " +
            "evaluator " + evaluator.getClass().getSimpleName();
    return new ImmutablePair<>(null, issue);
  }

  private static List<ConstantVectorExpression> getConstantParams(
      VectorizationContext vContext, List<ExprNodeDesc> parameterList) throws HiveException {
    if (parameterList.isEmpty()) {
      return Collections.emptyList();
    }

    final List<ConstantVectorExpression> res = new ArrayList<>(parameterList.size() - 1);

    // skip the first parameter as it is not a constant
    for (ExprNodeDesc exprNodeDesc : parameterList.subList(1, parameterList.size())) {
      VectorExpression vectorExpression =
          vContext.getVectorExpression(exprNodeDesc, VectorExpressionDescriptor.Mode.PROJECTION);
      if (vectorExpression instanceof ConstantVectorExpression) {
        res.add((ConstantVectorExpression) vectorExpression);
      }
    }
    return res;
  }

  public static Operator<? extends OperatorDesc> vectorizeGroupByOperator(
      Operator<? extends OperatorDesc> groupByOp, VectorizationContext vContext,
      VectorGroupByDesc vectorGroupByDesc)
          throws HiveException {
    ImmutablePair<Operator<? extends OperatorDesc>,String> pair =
        doVectorizeGroupByOperator(
            groupByOp, vContext, vectorGroupByDesc);
    return pair.left;
  }

  /*
   * NOTE: The VectorGroupByDesc has already been allocated and will be updated here.
   */
  private static ImmutablePair<Operator<? extends OperatorDesc>,String> doVectorizeGroupByOperator(
      Operator<? extends OperatorDesc> groupByOp, VectorizationContext vContext,
      VectorGroupByDesc vectorGroupByDesc)
          throws HiveException {

    GroupByDesc groupByDesc = (GroupByDesc) groupByOp.getConf();

    List<ExprNodeDesc> keysDesc = groupByDesc.getKeys();

    // For now, we don't support group by on DECIMAL_64 keys.
    VectorExpression[] vecKeyExpressions =
        vContext.getVectorExpressionsUpConvertDecimal64(keysDesc);
    List<AggregationDesc> aggrDesc = groupByDesc.getAggregators();
    final int size = aggrDesc.size();

    VectorAggregationDesc[] vecAggrDescs = new VectorAggregationDesc[size];
    int[] projectedOutputColumns = new int[size];
    for (int i = 0; i < size; ++i) {
      AggregationDesc aggDesc = aggrDesc.get(i);
      ImmutablePair<VectorAggregationDesc,String> pair =
          getVectorAggregationDesc(aggDesc, vContext);
      if (pair.left == null) {
        return new ImmutablePair<Operator<? extends OperatorDesc>, String>(null, pair.right);
      }
      vecAggrDescs[i] = pair.left;

      // GroupBy generates a new vectorized row batch...
      projectedOutputColumns[i] = i;
    }

    vectorGroupByDesc.setKeyExpressions(vecKeyExpressions);
    vectorGroupByDesc.setVecAggrDescs(vecAggrDescs);
    vectorGroupByDesc.setProjectedOutputColumns(projectedOutputColumns);
    Operator<GroupByDesc> vectorOp =
        OperatorFactory.getVectorOperator(
            groupByOp.getCompilationOpContext(), groupByDesc,
            vContext, vectorGroupByDesc);
    return new ImmutablePair<Operator<? extends OperatorDesc>, String>(vectorOp, null);
  }

  public static Operator<? extends OperatorDesc> vectorizeSelectOperator(
      Operator<? extends OperatorDesc> selectOp, VectorizationContext vContext,
      VectorSelectDesc vectorSelectDesc)
          throws HiveException {

    SelectDesc selectDesc = (SelectDesc) selectOp.getConf();

    List<ExprNodeDesc> colList = selectDesc.getColList();
    int index = 0;
    final int size = colList.size();

    // this will mark all actual computed columns
    vContext.markActualScratchColumns();

    VectorExpression[] vectorSelectExprs = new VectorExpression[size];
    int[] projectedOutputColumns = new int[size];
    for (int i = 0; i < size; i++) {
      ExprNodeDesc expr = colList.get(i);
      VectorExpression ve = vContext.getVectorExpression(expr);
      projectedOutputColumns[i] = ve.getOutputColumnNum();
      if (ve instanceof IdentityExpression) {
        // Suppress useless evaluation.
        continue;
      }
      vectorSelectExprs[index++] = ve;
    }
    if (index < size) {
      vectorSelectExprs = Arrays.copyOf(vectorSelectExprs, index);
    }

    // Fix up the case where parent expression's output data type physical variations is DECIMAL whereas
    // at least one of its children is DECIMAL_64. Some expressions like x % y for example only accepts DECIMAL
    // for x and y (at this time there is only DecimalColModuloDecimalColumn so both x and y has to be DECIMAL).
    // The following method introduces a cast if x or y is DECIMAL_64 and parent expression (x % y) is DECIMAL.
    try {
      fixDecimalDataTypePhysicalVariations(vContext, vectorSelectExprs);
    } finally {
      vContext.freeMarkedScratchColumns();
    }


    vectorSelectDesc.setSelectExpressions(vectorSelectExprs);
    vectorSelectDesc.setProjectedOutputColumns(projectedOutputColumns);

    return OperatorFactory.getVectorOperator(
        selectOp.getCompilationOpContext(), selectDesc,
        vContext, vectorSelectDesc);
  }

  private static void fixDecimalDataTypePhysicalVariations(final VectorizationContext vContext,
    final VectorExpression[] vectorSelectExprs) throws HiveException {
    for (int i = 0; i < vectorSelectExprs.length; i++) {
      VectorExpression parent = vectorSelectExprs[i];
      VectorExpression newParent = fixDecimalDataTypePhysicalVariations(parent, parent.getChildExpressions(),
        vContext);
      if (parent.getClass() == newParent.getClass() && parent != newParent) {
        vectorSelectExprs[i] = newParent;
      }
    }
  }

  private static VectorExpression fixDecimalDataTypePhysicalVariations(final VectorExpression parent,
    final VectorExpression[] children, final VectorizationContext vContext) throws HiveException {
    if (children == null || children.length == 0) {
      return parent;
    }

    for (int i = 0; i < children.length; i++) {
      VectorExpression child = children[i];
      VectorExpression newChild = fixDecimalDataTypePhysicalVariations(child, child.getChildExpressions(), vContext);
      if (child.getClass() == newChild.getClass() && child != newChild) {
        children[i] = newChild;
      }
    }
    if (parent.shouldConvertDecimal64ToDecimal()) {
      boolean inputArgsChanged = false;
      DataTypePhysicalVariation[] dataTypePhysicalVariations = parent.getInputDataTypePhysicalVariations();
      for (int i = 0; i < children.length; i++) {
        // we found at least one children with mismatch
        if (children[i].getOutputDataTypePhysicalVariation() == DataTypePhysicalVariation.DECIMAL_64) {
          children[i] = vContext.wrapWithDecimal64ToDecimalConversion(children[i]);
          inputArgsChanged = true;
          dataTypePhysicalVariations[i] = DataTypePhysicalVariation.NONE;
        }
      }
      // fix up the input column numbers and output column numbers
      if (inputArgsChanged) {
        if (parent instanceof VectorUDFAdaptor) {
          VectorUDFAdaptor parentAdaptor = (VectorUDFAdaptor) parent;
          VectorUDFArgDesc[] argDescs = parentAdaptor.getArgDescs();
          for (int i = 0; i < argDescs.length; ++i) {
            if (argDescs[i].getColumnNum() != children[i].getOutputColumnNum()) {
              argDescs[i].setColumnNum(children[i].getOutputColumnNum());
              break;
            }
          }
        } else {
          Object[] arguments;
          int argumentCount = children.length + (parent.getOutputColumnNum() == -1 ? 0 : 1);
          // VectorCoalesce receives arguments as an array.
          // Need to handle it as a special case to avoid instantiation failure.
          if (parent instanceof VectorCoalesce) {
            arguments = new Object[2];
            arguments[0] = new int[children.length];
            for (int i = 0; i < children.length; i++) {
              VectorExpression vce = children[i];
              ((int[]) arguments[0])[i] = vce.getOutputColumnNum();
            }
            arguments[1] = parent.getOutputColumnNum();
          } else {
            if (parent instanceof DecimalColDivideDecimalScalar) {
              arguments = new Object[argumentCount + 1];
              arguments[children.length] = ((DecimalColDivideDecimalScalar) parent).getValue();
            } else {
              arguments = new Object[argumentCount];
            }
            for (int i = 0; i < children.length; i++) {
              VectorExpression vce = children[i];
              arguments[i] = vce.getOutputColumnNum();
            }
          }
          // retain output column number from parent
          if (parent.getOutputColumnNum() != -1) {
            arguments[arguments.length - 1] = parent.getOutputColumnNum();
          }
          // re-instantiate the parent expression with new arguments
          VectorExpression newParent = vContext.instantiateExpression(parent.getClass(), parent.getOutputTypeInfo(),
            parent.getOutputDataTypePhysicalVariation(), arguments);
          newParent.setOutputTypeInfo(parent.getOutputTypeInfo());
          newParent.setOutputDataTypePhysicalVariation(parent.getOutputDataTypePhysicalVariation());
          newParent.setInputTypeInfos(parent.getInputTypeInfos());
          newParent.setInputDataTypePhysicalVariations(dataTypePhysicalVariations);
          newParent.setChildExpressions(parent.getChildExpressions());
          return newParent;
        }
      }
    }
    return parent;
  }

  private static void fillInPTFEvaluators(List<WindowFunctionDef> windowsFunctions,
      String[] evaluatorFunctionNames, boolean[] evaluatorsAreDistinct,
      WindowFrameDef[] evaluatorWindowFrameDefs,
      List<ExprNodeDesc>[] evaluatorInputExprNodeDescLists) throws HiveException {
    final int functionCount = windowsFunctions.size();
    for (int i = 0; i < functionCount; i++) {
      WindowFunctionDef winFunc = windowsFunctions.get(i);
      evaluatorFunctionNames[i] = winFunc.getName();
      evaluatorsAreDistinct[i] = winFunc.isDistinct();
      evaluatorWindowFrameDefs[i] = winFunc.getWindowFrame();

      List<PTFExpressionDef> args = winFunc.getArgs();
      if (args != null) {

        List<ExprNodeDesc> exprNodeDescList = new ArrayList<ExprNodeDesc>();
        for (PTFExpressionDef arg : args) {
          exprNodeDescList.add(arg.getExprNode());
        }

        evaluatorInputExprNodeDescLists[i] = exprNodeDescList;
      }
    }
  }

  private static ExprNodeDesc[] getPartitionExprNodeDescs(List<PTFExpressionDef> partitionExpressions) {
    final int size = partitionExpressions.size();
    ExprNodeDesc[] exprNodeDescs = new ExprNodeDesc[size];
    for (int i = 0; i < size; i++) {
      exprNodeDescs[i] = partitionExpressions.get(i).getExprNode();
    }
    return exprNodeDescs;
  }

  private static ExprNodeDesc[] getOrderExprNodeDescs(List<OrderExpressionDef> orderExpressions) {
    final int size = orderExpressions.size();
    ExprNodeDesc[] exprNodeDescs = new ExprNodeDesc[size];
    for (int i = 0; i < size; i++) {
      exprNodeDescs[i] = orderExpressions.get(i).getExprNode();
    }
    return exprNodeDescs;
  }

  /*
   * Update the VectorPTFDesc with data that is used during validation and that doesn't rely on
   * VectorizationContext to lookup column names, etc.
   */
  private static void createVectorPTFDesc(Operator<? extends OperatorDesc> ptfOp,
      PTFDesc ptfDesc, VectorizationContext vContext, VectorPTFDesc vectorPTFDesc,
      int vectorizedPTFMaxMemoryBufferingBatchCount)
          throws HiveException {

    PartitionedTableFunctionDef funcDef = ptfDesc.getFuncDef();

    WindowTableFunctionDef windowTableFunctionDef = (WindowTableFunctionDef) funcDef;
    List<WindowFunctionDef> windowsFunctions = windowTableFunctionDef.getWindowFunctions();
    final int functionCount = windowsFunctions.size();

    List<ColumnInfo> outputSignature = ptfOp.getSchema().getSignature();
    final int outputSize = outputSignature.size();

    /*
     * Output columns.
     */


    TypeInfo[] reducerBatchTypeInfos = vContext.getAllTypeInfos();
    DataTypePhysicalVariation[] reducerBatchDataTypePhysicalVariations = vContext.getAllDataTypePhysicalVariations();

    // Evaluator results are first.
    String[] outputColumnNames = new String[outputSize];
    TypeInfo[] outputTypeInfos = new TypeInfo[outputSize];
    DataTypePhysicalVariation[] outputDataTypePhysicalVariations = new DataTypePhysicalVariation[outputSize];
    for (int i = 0; i < functionCount; i++) {
      ColumnInfo colInfo = outputSignature.get(i);
      TypeInfo typeInfo = colInfo.getType();
      outputColumnNames[i] = colInfo.getInternalName();
      outputTypeInfos[i] = typeInfo;
      outputDataTypePhysicalVariations[i] = DataTypePhysicalVariation.NONE;
    }

    // Followed by key and non-key input columns (some may be missing).
    for (int i = functionCount; i < outputSize; i++) {
      ColumnInfo colInfo = outputSignature.get(i);
      outputColumnNames[i] = colInfo.getInternalName();
      outputTypeInfos[i] = colInfo.getType();
      outputDataTypePhysicalVariations[i] = reducerBatchDataTypePhysicalVariations[i-functionCount];
    }

    List<PTFExpressionDef> partitionExpressions = funcDef.getPartition().getExpressions();
    final int partitionKeyCount = partitionExpressions.size();
    ExprNodeDesc[] partitionExprNodeDescs = getPartitionExprNodeDescs(partitionExpressions);

    List<OrderExpressionDef> orderExpressions = funcDef.getOrder().getExpressions();
    final int orderKeyCount = orderExpressions.size();
    ExprNodeDesc[] orderExprNodeDescs = getOrderExprNodeDescs(orderExpressions);

    // When there are PARTITION and ORDER BY clauses, will have different partition expressions.
    // Otherwise, only order by expressions.
    boolean isPartitionOrderBy = false;

    if (partitionKeyCount != orderKeyCount) {
      // Obviously different expressions.
      isPartitionOrderBy = true;
    } else {
      // Check each ExprNodeDesc.
      for (int i = 0; i < partitionKeyCount; i++) {
        final ExprNodeDescEqualityWrapper partitionExprEqualityWrapper =
            new ExprNodeDesc.ExprNodeDescEqualityWrapper(partitionExprNodeDescs[i]);
        final ExprNodeDescEqualityWrapper orderExprEqualityWrapper =
            new ExprNodeDesc.ExprNodeDescEqualityWrapper(orderExprNodeDescs[i]);
        if (!partitionExprEqualityWrapper.equals(orderExprEqualityWrapper)) {
          isPartitionOrderBy = true;
          break;
        }
      }
    }

    String[] evaluatorFunctionNames = new String[functionCount];
    boolean[] evaluatorsAreDistinct = new boolean[functionCount];
    WindowFrameDef[] evaluatorWindowFrameDefs = new WindowFrameDef[functionCount];
    List<ExprNodeDesc>[] evaluatorInputExprNodeDescLists = (List<ExprNodeDesc>[]) new List<?>[functionCount];

    fillInPTFEvaluators(
        windowsFunctions,
        evaluatorFunctionNames,
        evaluatorsAreDistinct,
        evaluatorWindowFrameDefs,
        evaluatorInputExprNodeDescLists);

    vectorPTFDesc.setReducerBatchTypeInfos(reducerBatchTypeInfos, reducerBatchDataTypePhysicalVariations);

    vectorPTFDesc.setIsPartitionOrderBy(isPartitionOrderBy);

    vectorPTFDesc.setOrderExprNodeDescs(orderExprNodeDescs);
    vectorPTFDesc.setPartitionExprNodeDescs(partitionExprNodeDescs);

    vectorPTFDesc.setEvaluatorFunctionNames(evaluatorFunctionNames);
    vectorPTFDesc.setEvaluatorsAreDistinct(evaluatorsAreDistinct);
    vectorPTFDesc.setEvaluatorWindowFrameDefs(evaluatorWindowFrameDefs);
    vectorPTFDesc.setEvaluatorInputExprNodeDescLists(evaluatorInputExprNodeDescLists);

    vectorPTFDesc.setOutputColumnNames(outputColumnNames);
    vectorPTFDesc.setOutputTypeInfos(outputTypeInfos, outputDataTypePhysicalVariations);

    vectorPTFDesc.setVectorizedPTFMaxMemoryBufferingBatchCount(
        vectorizedPTFMaxMemoryBufferingBatchCount);
  }

  private static void determineKeyAndNonKeyInputColumnMap(int[] outputColumnProjectionMap,
      boolean isPartitionOrderBy, int[] orderColumnMap, int[] partitionColumnMap,
      int evaluatorCount, ArrayList<Integer> keyInputColumns,
      ArrayList<Integer> nonKeyInputColumns) {

    final int outputSize = outputColumnProjectionMap.length;
    final int orderKeyCount = orderColumnMap.length;
    final int partitionKeyCount = (isPartitionOrderBy ? partitionColumnMap.length : 0);
    for (int i = evaluatorCount; i < outputSize; i++) {
      final int nonEvalColumnNum = outputColumnProjectionMap[i];
      boolean isKey = false;
      for (int o = 0; o < orderKeyCount; o++) {
        if (nonEvalColumnNum == orderColumnMap[o]) {
          isKey = true;
          break;
        }
      }
      if (!isKey && isPartitionOrderBy) {
        for (int p = 0; p < partitionKeyCount; p++) {
          if (nonEvalColumnNum == partitionColumnMap[p]) {
            isKey = true;
            break;
          }
        }
      }
      if (isKey) {
        keyInputColumns.add(nonEvalColumnNum);
      } else {
        nonKeyInputColumns.add(nonEvalColumnNum);
      }
    }
  }

  /*
   * Create the additional vectorization PTF information needed by the VectorPTFOperator during
   * execution.
   */
  private static VectorPTFInfo createVectorPTFInfo(Operator<? extends OperatorDesc> ptfOp,
      PTFDesc ptfDesc, VectorizationContext vContext, VectorPTFDesc vectorPTFDesc)
          throws HiveException {

    List<ColumnInfo> outputSignature = ptfOp.getSchema().getSignature();
    final int outputSize = outputSignature.size();

    boolean isPartitionOrderBy = vectorPTFDesc.getIsPartitionOrderBy();
    ExprNodeDesc[] orderExprNodeDescs = vectorPTFDesc.getOrderExprNodeDescs();
    ExprNodeDesc[] partitionExprNodeDescs = vectorPTFDesc.getPartitionExprNodeDescs();
    String[] evaluatorFunctionNames = vectorPTFDesc.getEvaluatorFunctionNames();

    final int evaluatorCount = evaluatorFunctionNames.length;
    List<ExprNodeDesc>[] evaluatorInputExprNodeDescLists = vectorPTFDesc.getEvaluatorInputExprNodeDescLists();

    /*
     * Output columns.
     */
    int[] outputColumnProjectionMap = new int[outputSize];

    // Evaluator results are first.
    for (int i = 0; i < evaluatorCount; i++) {
      ColumnInfo colInfo = outputSignature.get(i);
      TypeInfo typeInfo = colInfo.getType();
      final int outputColumnNum;
        outputColumnNum = vContext.allocateScratchColumn(typeInfo);
      outputColumnProjectionMap[i] = outputColumnNum;
    }

    // Followed by key and non-key input columns (some may be missing).
    for (int i = evaluatorCount; i < outputSize; i++) {
      ColumnInfo colInfo = outputSignature.get(i);
      outputColumnProjectionMap[i] = vContext.getInputColumnIndex(colInfo.getInternalName());
    }

    /*
     * Partition and order by.
     */

    int[] partitionColumnMap;
    Type[] partitionColumnVectorTypes;
    VectorExpression[] partitionExpressions;

    final int partitionKeyCount = partitionExprNodeDescs.length;
    partitionColumnMap = new int[partitionKeyCount];
    partitionColumnVectorTypes = new Type[partitionKeyCount];
    partitionExpressions = new VectorExpression[partitionKeyCount];

    for (int i = 0; i < partitionKeyCount; i++) {
      VectorExpression partitionExpression = vContext.getVectorExpression(partitionExprNodeDescs[i]);
      TypeInfo typeInfo = partitionExpression.getOutputTypeInfo();
      Type columnVectorType = VectorizationContext.getColumnVectorTypeFromTypeInfo(typeInfo);
      partitionColumnVectorTypes[i] = columnVectorType;
      partitionColumnMap[i] = partitionExpression.getOutputColumnNum();
      partitionExpressions[i] = partitionExpression;
    }

    final int orderKeyCount = orderExprNodeDescs.length;
    int[] orderColumnMap = new int[orderKeyCount];
    Type[] orderColumnVectorTypes = new Type[orderKeyCount];
    VectorExpression[] orderExpressions = new VectorExpression[orderKeyCount];
    for (int i = 0; i < orderKeyCount; i++) {
      VectorExpression orderExpression = vContext.getVectorExpression(orderExprNodeDescs[i]);
      TypeInfo typeInfo = orderExpression.getOutputTypeInfo();
      Type columnVectorType = VectorizationContext.getColumnVectorTypeFromTypeInfo(typeInfo);
      orderColumnVectorTypes[i] = columnVectorType;
      orderColumnMap[i] = orderExpression.getOutputColumnNum();
      orderExpressions[i] = orderExpression;
    }

    ArrayList<Integer> keyInputColumns = new ArrayList<Integer>();
    ArrayList<Integer> nonKeyInputColumns = new ArrayList<Integer>();
    determineKeyAndNonKeyInputColumnMap(outputColumnProjectionMap, isPartitionOrderBy, orderColumnMap,
        partitionColumnMap, evaluatorCount, keyInputColumns, nonKeyInputColumns);
    int[] keyInputColumnMap = ArrayUtils.toPrimitive(keyInputColumns.toArray(new Integer[0]));
    int[] nonKeyInputColumnMap = ArrayUtils.toPrimitive(nonKeyInputColumns.toArray(new Integer[0]));

    VectorExpression[][] evaluatorInputExpressions = new VectorExpression[evaluatorCount][];
    Type[][] evaluatorInputColumnVectorTypes = new Type[evaluatorCount][];
    for (int i = 0; i < evaluatorCount; i++) {
      List<ExprNodeDesc> exprNodeDescList = evaluatorInputExprNodeDescLists[i];
      VectorExpression inputVectorExpression;
      Type columnVectorType;
      if (exprNodeDescList != null) {
        evaluatorInputExpressions[i] = new VectorExpression[exprNodeDescList.size()];
        evaluatorInputColumnVectorTypes[i] = new Type[exprNodeDescList.size()];
        for (int j = 0; j < exprNodeDescList.size(); j++){
          ExprNodeDesc exprNodeDesc = exprNodeDescList.get(j);

          // Determine input vector expression using the VectorizationContext.
          inputVectorExpression = vContext.getVectorExpression(exprNodeDesc);

          if (inputVectorExpression.getOutputColumnVectorType() == ColumnVector.Type.DECIMAL_64) {
            inputVectorExpression = vContext.wrapWithDecimal64ToDecimalConversion(inputVectorExpression);
          }

          TypeInfo typeInfo = exprNodeDesc.getTypeInfo();
          columnVectorType = VectorizationContext.getColumnVectorTypeFromTypeInfo(typeInfo);
          evaluatorInputExpressions[i][j]= inputVectorExpression;
          evaluatorInputColumnVectorTypes[i][j] = columnVectorType;
        }
      } else {
        evaluatorInputExpressions[i] = new VectorExpression[] { null };
        evaluatorInputColumnVectorTypes[i] = new Type[] { ColumnVector.Type.NONE };
      }
    }

    VectorPTFInfo vectorPTFInfo = new VectorPTFInfo();

    vectorPTFInfo.setOutputColumnMap(outputColumnProjectionMap);

    vectorPTFInfo.setPartitionColumnMap(partitionColumnMap);
    vectorPTFInfo.setPartitionColumnVectorTypes(partitionColumnVectorTypes);
    vectorPTFInfo.setPartitionExpressions(partitionExpressions);

    vectorPTFInfo.setOrderColumnMap(orderColumnMap);
    vectorPTFInfo.setOrderColumnVectorTypes(orderColumnVectorTypes);
    vectorPTFInfo.setOrderExpressions(orderExpressions);

    vectorPTFInfo.setEvaluatorInputExpressions(evaluatorInputExpressions);
    vectorPTFInfo.setEvaluatorInputColumnVectorTypes(evaluatorInputColumnVectorTypes);

    vectorPTFInfo.setKeyInputColumnMap(keyInputColumnMap);
    vectorPTFInfo.setNonKeyInputColumnMap(nonKeyInputColumnMap);

    return vectorPTFInfo;
  }

  /*
   * NOTE: The VectorPTFDesc has already been allocated and populated.
   */
  public static Operator<? extends OperatorDesc> vectorizePTFOperator(
      Operator<? extends OperatorDesc> ptfOp, VectorizationContext vContext,
      VectorPTFDesc vectorPTFDesc)
          throws HiveException {

    PTFDesc ptfDesc = (PTFDesc) ptfOp.getConf();

    VectorPTFInfo vectorPTFInfo = createVectorPTFInfo(ptfOp, ptfDesc, vContext, vectorPTFDesc);

    vectorPTFDesc.setVectorPTFInfo(vectorPTFInfo);

    Class<? extends Operator<?>> opClass = VectorPTFOperator.class;
    return OperatorFactory.getVectorOperator(
        opClass, ptfOp.getCompilationOpContext(), ptfOp.getConf(),
        vContext, vectorPTFDesc);
  }

  // UNDONE: Used by tests...
  public Operator<? extends OperatorDesc> vectorizeOperator(Operator<? extends OperatorDesc> op,
      VectorizationContext vContext, boolean isReduce, boolean isTez, VectorTaskColumnInfo vectorTaskColumnInfo)
          throws HiveException, VectorizerCannotVectorizeException {
    Operator<? extends OperatorDesc> vectorOp =
        validateAndVectorizeOperator(op, vContext, isReduce, isTez, vectorTaskColumnInfo);
    if (vectorOp != op) {
      fixupParentChildOperators(op, vectorOp);
    }
    return vectorOp;
  }

  public Operator<? extends OperatorDesc> validateAndVectorizeOperator(Operator<? extends OperatorDesc> op,
      VectorizationContext vContext, boolean isReduce, boolean isTez,
      VectorTaskColumnInfo vectorTaskColumnInfo)
          throws HiveException, VectorizerCannotVectorizeException {
    Operator<? extends OperatorDesc> vectorOp = null;

    // This "global" allows various validation methods to set the "not vectorized" reason.
    currentOperator = op;

    boolean isNative;
    try {
      switch (op.getType()) {
        case MAPJOIN:
          {
            if (op instanceof MapJoinOperator) {
              if (!validateMapJoinOperator((MapJoinOperator) op)) {
                throw new VectorizerCannotVectorizeException();
              }
            } else if (op instanceof SMBMapJoinOperator) {
              if (!validateSMBMapJoinOperator((SMBMapJoinOperator) op)) {
                throw new VectorizerCannotVectorizeException();
              }
            } else {
              setOperatorNotSupported(op);
              throw new VectorizerCannotVectorizeException();
            }

            if (op instanceof MapJoinOperator) {

              MapJoinDesc desc = (MapJoinDesc) op.getConf();
              int joinType = desc.getConds()[0].getType();

              VectorMapJoinDesc vectorMapJoinDesc = new VectorMapJoinDesc();
              boolean specialize =
                  canSpecializeMapJoin(op, desc, isTez, vContext, vectorMapJoinDesc);

              if (!specialize) {

                Class<? extends Operator<?>> opClass = null;

                // *NON-NATIVE* vector map differences for LEFT OUTER JOIN and Filtered...

                List<ExprNodeDesc> bigTableFilters = desc.getFilters().get((byte) desc.getPosBigTable());
                boolean isOuterAndFiltered = (!desc.isNoOuterJoin() && bigTableFilters.size() > 0);
                if (!isOuterAndFiltered) {
                  opClass = VectorMapJoinOperator.class;
                } else {
                  if (joinType == JoinDesc.FULL_OUTER_JOIN) {
                    setOperatorIssue("Vectorized & filtered full-outer joins not supported");
                    throw new VectorizerCannotVectorizeException();
                  }
                  opClass = VectorMapJoinOuterFilteredOperator.class;
                }

                vectorOp = OperatorFactory.getVectorOperator(
                    opClass, op.getCompilationOpContext(), desc,
                    vContext, vectorMapJoinDesc);
                isNative = false;
              } else {

                // TEMPORARY Until Native Vector Map Join with Hybrid passes tests...
                // HiveConf.setBoolVar(physicalContext.getConf(),
                //    HiveConf.ConfVars.HIVE_USE_HYBRIDGRACE_HASHJOIN, false);

                vectorOp = specializeMapJoinOperator(op, vContext, desc, vectorMapJoinDesc);
                isNative = true;

                if (vectorTaskColumnInfo != null) {
                  if (usesVectorUDFAdaptor(vectorMapJoinDesc.getAllBigTableKeyExpressions())) {
                    vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
                  }
                  if (usesVectorUDFAdaptor(vectorMapJoinDesc.getAllBigTableValueExpressions())) {
                    vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
                  }
                }
              }
            } else {
              Preconditions.checkState(op instanceof SMBMapJoinOperator);

              SMBJoinDesc smbJoinSinkDesc = (SMBJoinDesc) op.getConf();

              // Check additional constraint.
              if (smbJoinSinkDesc.getFilterMap() != null) {
                setOperatorIssue("FilterMaps not supported for Vector Pass-Thru SMB MapJoin");
                throw new VectorizerCannotVectorizeException();
              }

              VectorSMBJoinDesc vectorSMBJoinDesc = new VectorSMBJoinDesc();
              vectorOp = OperatorFactory.getVectorOperator(
                  op.getCompilationOpContext(), smbJoinSinkDesc, vContext, vectorSMBJoinDesc);
              isNative = false;
            }
          }
          break;

        case REDUCESINK:
          {
            if (!validateReduceSinkOperator((ReduceSinkOperator) op)) {
              throw new VectorizerCannotVectorizeException();
            }

            ReduceSinkDesc reduceDesc = (ReduceSinkDesc) op.getConf();

            VectorReduceSinkDesc vectorReduceSinkDesc = new VectorReduceSinkDesc();
            boolean specialize =
                canSpecializeReduceSink(reduceDesc, isTez, vContext, vectorReduceSinkDesc);

            if (!specialize) {

              vectorOp = OperatorFactory.getVectorOperator(
                  op.getCompilationOpContext(), reduceDesc, vContext, vectorReduceSinkDesc);
              isNative = false;
            } else {

              vectorOp = specializeReduceSinkOperator(op, vContext, reduceDesc, vectorReduceSinkDesc);
              isNative = true;

              if (vectorTaskColumnInfo != null) {
                VectorReduceSinkInfo vectorReduceSinkInfo = vectorReduceSinkDesc.getVectorReduceSinkInfo();
                if (usesVectorUDFAdaptor(vectorReduceSinkInfo.getReduceSinkKeyExpressions())) {
                  vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
                }
                if (usesVectorUDFAdaptor(vectorReduceSinkInfo.getReduceSinkValueExpressions())) {
                  vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
                }
              }
            }
          }
          break;
        case FILTER:
          {
            if (!validateFilterOperator((FilterOperator) op)) {
              throw new VectorizerCannotVectorizeException();
            }

            VectorFilterDesc vectorFilterDesc = new VectorFilterDesc();
            vectorOp = vectorizeFilterOperator(op, vContext, vectorFilterDesc);
            isNative = true;
            if (vectorTaskColumnInfo != null) {
              VectorExpression vectorPredicateExpr = vectorFilterDesc.getPredicateExpression();
              if (usesVectorUDFAdaptor(vectorPredicateExpr)) {
                vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
              }
            }
          }
          break;
        case TOPNKEY:
          {
            if (!validateTopNKeyOperator((TopNKeyOperator) op)) {
              throw new VectorizerCannotVectorizeException();
            }

            VectorTopNKeyDesc vectorTopNKeyDesc = new VectorTopNKeyDesc();
            vectorOp = vectorizeTopNKeyOperator(op, vContext, vectorTopNKeyDesc);
            isNative = true;
            if (vectorTaskColumnInfo != null) {
              VectorExpression[] keyExpressions = vectorTopNKeyDesc.getKeyExpressions();
              if (usesVectorUDFAdaptor(keyExpressions)) {
                vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
              }
            }
          }
          break;
        case SELECT:
          {
            if (!validateSelectOperator((SelectOperator) op)) {
              throw new VectorizerCannotVectorizeException();
            }

            VectorSelectDesc vectorSelectDesc = new VectorSelectDesc();
            vectorOp = vectorizeSelectOperator(op, vContext, vectorSelectDesc);
            isNative = true;
            if (vectorTaskColumnInfo != null) {
              VectorExpression[] vectorSelectExprs = vectorSelectDesc.getSelectExpressions();
              if (usesVectorUDFAdaptor(vectorSelectExprs)) {
                vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
              }
            }
          }
          break;
        case GROUPBY:
          {
            // The validateGroupByOperator method will update vectorGroupByDesc.
            VectorGroupByDesc vectorGroupByDesc = new VectorGroupByDesc();
            if (!validateGroupByOperator((GroupByOperator) op, isReduce, isTez,
                vectorGroupByDesc)) {
              throw new VectorizerCannotVectorizeException();
            }

            ImmutablePair<Operator<? extends OperatorDesc>,String> pair =
                doVectorizeGroupByOperator(op, vContext, vectorGroupByDesc);
            if (pair.left == null) {
              setOperatorIssue(pair.right);
              throw new VectorizerCannotVectorizeException();
            }
            vectorOp = pair.left;
            isNative = false;
            if (vectorTaskColumnInfo != null) {
              VectorExpression[] vecKeyExpressions = vectorGroupByDesc.getKeyExpressions();
              if (usesVectorUDFAdaptor(vecKeyExpressions)) {
                vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
              }
              VectorAggregationDesc[] vecAggrDescs = vectorGroupByDesc.getVecAggrDescs();
              for (VectorAggregationDesc vecAggrDesc : vecAggrDescs) {
                if (usesVectorUDFAdaptor(vecAggrDesc.getInputExpression())) {
                  vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
                }
              }
            }

          }
          break;
        case FILESINK:
          {
            if (!validateFileSinkOperator((FileSinkOperator) op)) {
              throw new VectorizerCannotVectorizeException();
            }

            FileSinkDesc fileSinkDesc = (FileSinkDesc) op.getConf();

            VectorFileSinkDesc vectorFileSinkDesc = new VectorFileSinkDesc();
            boolean isArrowSpecialization =
                checkForArrowFileSink(fileSinkDesc, isTez, vContext, vectorFileSinkDesc);

            if (isArrowSpecialization) {
              vectorOp =
                  specializeArrowFileSinkOperator(
                      op, vContext, fileSinkDesc, vectorFileSinkDesc);
              isNative = true;
            } else {
              vectorOp =
                  OperatorFactory.getVectorOperator(
                      op.getCompilationOpContext(), fileSinkDesc, vContext, vectorFileSinkDesc);
              isNative = false;
            }
          }
          break;
        case LIMIT:
          {
            // No validation.

            LimitDesc limitDesc = (LimitDesc) op.getConf();

            VectorLimitDesc vectorLimitDesc = new VectorLimitDesc();
            vectorOp = OperatorFactory.getVectorOperator(
                op.getCompilationOpContext(), limitDesc, vContext, vectorLimitDesc);
            isNative = true;
          }
          break;
        case EVENT:
          {
            // No validation.

            AppMasterEventDesc eventDesc = (AppMasterEventDesc) op.getConf();

            VectorAppMasterEventDesc vectorEventDesc = new VectorAppMasterEventDesc();
            vectorOp = OperatorFactory.getVectorOperator(
                op.getCompilationOpContext(), eventDesc, vContext, vectorEventDesc);
            isNative = true;
          }
          break;
        case PTF:
          {
            // The validatePTFOperator method will update vectorPTFDesc.
            VectorPTFDesc vectorPTFDesc = new VectorPTFDesc();
            if (!validatePTFOperator((PTFOperator) op, vContext, vectorPTFDesc)) {
              throw new VectorizerCannotVectorizeException();
            }

            vectorOp = vectorizePTFOperator(op, vContext, vectorPTFDesc);
            isNative = true;
          }
          break;
        default:
          setOperatorNotSupported(op);
          throw new VectorizerCannotVectorizeException();
      }
    } catch (HiveException e) {
      setOperatorIssue(e.getMessage());
      throw new VectorizerCannotVectorizeException();
    }

    Preconditions.checkState(vectorOp != null);
    if (vectorTaskColumnInfo != null && !isNative) {
      vectorTaskColumnInfo.setAllNative(false);
    }

    LOG.debug("vectorizeOperator " + vectorOp.getClass().getName());
    LOG.debug("vectorizeOperator " + vectorOp.getConf().getClass().getName());
    // These operators need to be linked to enable runtime statistics to be gathered/used correctly
    planMapper.link(op, vectorOp);

    return vectorOp;
  }
}
