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

package org.apache.hadoop.hive.ql.optimizer.physical;

import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.UNIFORM;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Pattern;
import org.apache.commons.lang.ArrayUtils;

import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKey;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
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
import org.apache.hadoop.hive.ql.exec.vector.reducesink.VectorReduceSinkLongOperator;
import org.apache.hadoop.hive.ql.exec.vector.reducesink.VectorReduceSinkMultiKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.reducesink.VectorReduceSinkObjectHashOperator;
import org.apache.hadoop.hive.ql.exec.vector.reducesink.VectorReduceSinkStringOperator;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnOutputMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSourceMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOuterFilteredOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext.HiveVectorAdaptorUsageMode;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext.InConstantType;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderOnceWalker;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.AppMasterEventDesc;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.VectorAppMasterEventDesc;
import org.apache.hadoop.hive.ql.plan.VectorFileSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorFilterDesc;
import org.apache.hadoop.hive.ql.plan.VectorTableScanDesc;
import org.apache.hadoop.hive.ql.plan.VectorizationCondition;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc.ProcessingMode;
import org.apache.hadoop.hive.ql.plan.VectorSparkHashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorSparkPartitionPruningSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorLimitDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;
import org.apache.hadoop.hive.ql.plan.VectorSMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.SparkHashTableSinkDesc;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkPartitionPruningSinkDesc;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableImplementationType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKind;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.OperatorVariation;
import org.apache.hadoop.hive.ql.plan.VectorPartitionDesc.VectorDeserializeType;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkInfo;
import org.apache.hadoop.hive.ql.plan.VectorPartitionDesc;
import org.apache.hadoop.hive.ql.plan.VectorSelectDesc;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkInfo;
import org.apache.hadoop.hive.ql.plan.VectorPartitionDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
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
import org.apache.hadoop.hive.ql.udf.UDFFromUnixTime;
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
import org.apache.hadoop.hive.ql.udf.UDFToString;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.NullStructSerDe;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hive.common.util.AnnotationUtils;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Preconditions;

public class Vectorizer implements PhysicalPlanResolver {

  protected static transient final Logger LOG = LoggerFactory.getLogger(Vectorizer.class);

  static Pattern supportedDataTypesPattern;

  static {
    StringBuilder patternBuilder = new StringBuilder();
    patternBuilder.append("int");
    patternBuilder.append("|smallint");
    patternBuilder.append("|tinyint");
    patternBuilder.append("|bigint");
    patternBuilder.append("|integer");
    patternBuilder.append("|long");
    patternBuilder.append("|short");
    patternBuilder.append("|timestamp");
    patternBuilder.append("|" + serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME);
    patternBuilder.append("|" + serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME);
    patternBuilder.append("|boolean");
    patternBuilder.append("|binary");
    patternBuilder.append("|string");
    patternBuilder.append("|byte");
    patternBuilder.append("|float");
    patternBuilder.append("|double");
    patternBuilder.append("|date");
    patternBuilder.append("|void");

    // Decimal types can be specified with different precision and scales e.g. decimal(10,5),
    // as opposed to other data types which can be represented by constant strings.
    // The regex matches only the "decimal" prefix of the type.
    patternBuilder.append("|decimal.*");

    // CHAR and VARCHAR types can be specified with maximum length.
    patternBuilder.append("|char.*");
    patternBuilder.append("|varchar.*");

    supportedDataTypesPattern = Pattern.compile(patternBuilder.toString());
  }

  private Set<Class<?>> supportedGenericUDFs = new HashSet<Class<?>>();

  private Set<String> supportedAggregationUdfs = new HashSet<String>();

  private HiveConf hiveConf;

  private boolean useVectorizedInputFileFormat;
  private boolean useVectorDeserialize;
  private boolean useRowDeserialize;
  private boolean isReduceVectorizationEnabled;

  private boolean isSchemaEvolution;

  private HiveVectorAdaptorUsageMode hiveVectorAdaptorUsageMode;

  private BaseWork currentBaseWork;
  private Operator<? extends OperatorDesc> currentOperator;

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
    supportedGenericUDFs.add(UDFFromUnixTime.class);

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
    supportedGenericUDFs.add(UDFCos.class);
    supportedGenericUDFs.add(UDFTan.class);
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
    supportedGenericUDFs.add(GenericUDFNvl.class);
    supportedGenericUDFs.add(GenericUDFElt.class);
    supportedGenericUDFs.add(GenericUDFInitCap.class);
    supportedGenericUDFs.add(GenericUDFInBloomFilter.class);

    // For type casts
    supportedGenericUDFs.add(UDFToLong.class);
    supportedGenericUDFs.add(UDFToInteger.class);
    supportedGenericUDFs.add(UDFToShort.class);
    supportedGenericUDFs.add(UDFToByte.class);
    supportedGenericUDFs.add(UDFToBoolean.class);
    supportedGenericUDFs.add(UDFToFloat.class);
    supportedGenericUDFs.add(UDFToDouble.class);
    supportedGenericUDFs.add(UDFToString.class);
    supportedGenericUDFs.add(GenericUDFTimestamp.class);
    supportedGenericUDFs.add(GenericUDFToDecimal.class);
    supportedGenericUDFs.add(GenericUDFToDate.class);
    supportedGenericUDFs.add(GenericUDFToChar.class);
    supportedGenericUDFs.add(GenericUDFToVarchar.class);
    supportedGenericUDFs.add(GenericUDFToIntervalYearMonth.class);
    supportedGenericUDFs.add(GenericUDFToIntervalDayTime.class);

    // For conditional expressions
    supportedGenericUDFs.add(GenericUDFIf.class);

    supportedAggregationUdfs.add("min");
    supportedAggregationUdfs.add("max");
    supportedAggregationUdfs.add("count");
    supportedAggregationUdfs.add("sum");
    supportedAggregationUdfs.add("avg");
    supportedAggregationUdfs.add("variance");
    supportedAggregationUdfs.add("var_pop");
    supportedAggregationUdfs.add("var_samp");
    supportedAggregationUdfs.add("std");
    supportedAggregationUdfs.add("stddev");
    supportedAggregationUdfs.add("stddev_pop");
    supportedAggregationUdfs.add("stddev_samp");
    supportedAggregationUdfs.add("bloom_filter");
  }

  private class VectorTaskColumnInfo {
    List<String> allColumnNames;
    List<TypeInfo> allTypeInfos;
    List<Integer> dataColumnNums;

    int partitionColumnCount;
    boolean useVectorizedInputFileFormat;

    boolean groupByVectorOutput;
    boolean allNative;
    boolean usesVectorUDFAdaptor;

    String[] scratchTypeNameArray;

    Set<Operator<? extends OperatorDesc>> nonVectorizedOps;

    VectorTaskColumnInfo() {
      partitionColumnCount = 0;
    }

    public void assume() {
      groupByVectorOutput = true;
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
    public void setScratchTypeNameArray(String[] scratchTypeNameArray) {
      this.scratchTypeNameArray = scratchTypeNameArray;
    }
    public void setGroupByVectorOutput(boolean groupByVectorOutput) {
      this.groupByVectorOutput = groupByVectorOutput;
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

    public void setNonVectorizedOps(Set<Operator<? extends OperatorDesc>> nonVectorizedOps) {
      this.nonVectorizedOps = nonVectorizedOps;
    }

    public Set<Operator<? extends OperatorDesc>> getNonVectorizedOps() {
      return nonVectorizedOps;
    }

    public void transferToBaseWork(BaseWork baseWork) {

      String[] allColumnNameArray = allColumnNames.toArray(new String[0]);
      TypeInfo[] allTypeInfoArray = allTypeInfos.toArray(new TypeInfo[0]);
      int[] dataColumnNumsArray;
      if (dataColumnNums != null) {
        dataColumnNumsArray = ArrayUtils.toPrimitive(dataColumnNums.toArray(new Integer[0]));
      } else {
        dataColumnNumsArray = null;
      }

      VectorizedRowBatchCtx vectorizedRowBatchCtx =
          new VectorizedRowBatchCtx(
            allColumnNameArray,
            allTypeInfoArray,
            dataColumnNumsArray,
            partitionColumnCount,
            scratchTypeNameArray);
      baseWork.setVectorizedRowBatchCtx(vectorizedRowBatchCtx);

      if (baseWork instanceof MapWork) {
        MapWork mapWork = (MapWork) baseWork;
        mapWork.setUseVectorizedInputFileFormat(useVectorizedInputFileFormat);
      }

      baseWork.setAllNative(allNative);
      baseWork.setGroupByVectorOutput(groupByVectorOutput);
      baseWork.setUsesVectorUDFAdaptor(usesVectorUDFAdaptor);
    }
  }

  class VectorizationDispatcher implements Dispatcher {

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
        throws SemanticException {
      Task<? extends Serializable> currTask = (Task<? extends Serializable>) nd;
      if (currTask instanceof MapRedTask) {
        MapredWork mapredWork = ((MapRedTask) currTask).getWork();
        convertMapWork(mapredWork.getMapWork(), false);
        ReduceWork reduceWork = mapredWork.getReduceWork();
        if (reduceWork != null) {
          // Always set the EXPLAIN conditions.
          setReduceWorkExplainConditions(reduceWork);

          // We do not vectorize MR Reduce.
        }
      } else if (currTask instanceof TezTask) {
        TezWork work = ((TezTask) currTask).getWork();
        for (BaseWork baseWork: work.getAllWork()) {
          if (baseWork instanceof MapWork) {
            convertMapWork((MapWork) baseWork, true);
          } else if (baseWork instanceof ReduceWork) {
            ReduceWork reduceWork = (ReduceWork) baseWork;

            // Always set the EXPLAIN conditions.
            setReduceWorkExplainConditions(reduceWork);

            // We are only vectorizing Reduce under Tez/Spark.
            if (isReduceVectorizationEnabled) {
              convertReduceWork(reduceWork);
            }
          }
        }
      } else if (currTask instanceof SparkTask) {
        SparkWork sparkWork = (SparkWork) currTask.getWork();
        for (BaseWork baseWork : sparkWork.getAllWork()) {
          if (baseWork instanceof MapWork) {
            convertMapWork((MapWork) baseWork, true);
          } else if (baseWork instanceof ReduceWork) {
            ReduceWork reduceWork = (ReduceWork) baseWork;

            // Always set the EXPLAIN conditions.
            setReduceWorkExplainConditions(reduceWork);

            if (isReduceVectorizationEnabled) {
              convertReduceWork(reduceWork);
            }
          }
        }
      }

      return null;
    }

    private void convertMapWork(MapWork mapWork, boolean isTezOrSpark) throws SemanticException {

      mapWork.setVectorizationExamined(true);

      // Global used when setting errors, etc.
      currentBaseWork = mapWork;

      VectorTaskColumnInfo vectorTaskColumnInfo = new VectorTaskColumnInfo();
      vectorTaskColumnInfo.assume();

      mapWork.setVectorizedVertexNum(++vectorizedVertexNum);

      boolean ret;
      try {
        ret = validateMapWork(mapWork, vectorTaskColumnInfo, isTezOrSpark);
      } catch (Exception e) {
        String issue = "exception: " + VectorizationContext.getStackTraceAsSingleLine(e);
        setNodeIssue(issue);
        ret = false;
      }
      if (ret) {
        vectorizeMapWork(mapWork, vectorTaskColumnInfo, isTezOrSpark);
      } else if (currentBaseWork.getVectorizationEnabled()) {
        VectorizerReason notVectorizedReason  = currentBaseWork.getNotVectorizedReason();
        if (notVectorizedReason == null) {
          LOG.info("Cannot vectorize: unknown");
        } else {
          LOG.info("Cannot vectorize: " + notVectorizedReason.toString());
        }
        clearMapWorkVectorDescs(mapWork);
      }
    }

    private void addMapWorkRules(Map<Rule, NodeProcessor> opRules, NodeProcessor np) {
      opRules.put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + ".*"
          + FileSinkOperator.getOperatorName()), np);
      opRules.put(new RuleRegExp("R2", TableScanOperator.getOperatorName() + ".*"
          + ReduceSinkOperator.getOperatorName()), np);
    }

    /*
     * Determine if there is only one TableScanOperator.  Currently in Map vectorization, we do not
     * try to vectorize multiple input trees.
     */
    private ImmutablePair<String, TableScanOperator> verifyOnlyOneTableScanOperator(MapWork mapWork) {

      // Eliminate MR plans with more than one TableScanOperator.

      LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork = mapWork.getAliasToWork();
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
      return new ImmutablePair(alias, tableScanOperator);
    }

    private void getTableScanOperatorSchemaInfo(TableScanOperator tableScanOperator,
        List<String> logicalColumnNameList, List<TypeInfo> logicalTypeInfoList) {

      // Add all non-virtual columns to make a vectorization context for
      // the TableScan operator.
      RowSchema rowSchema = tableScanOperator.getSchema();
      for (ColumnInfo c : rowSchema.getSignature()) {
        // Validation will later exclude vectorization of virtual columns usage (HIVE-5560).
        if (!isVirtualColumn(c)) {
          String columnName = c.getInternalName();
          String typeName = c.getTypeName();
          TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);

          logicalColumnNameList.add(columnName);
          logicalTypeInfoList.add(typeInfo);
        }
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
    private boolean verifyAndSetVectorPartDesc(PartitionDesc pd, boolean isAcidTable,
        HashSet<String> inputFileFormatClassNameSet, HashSet<String> enabledConditionsMetSet,
        ArrayList<String> enabledConditionsNotMetList) {

      String inputFileFormatClassName = pd.getInputFileFormatClassName();

      // Always collect input file formats.
      inputFileFormatClassNameSet.add(inputFileFormatClassName);

      boolean isInputFileFormatVectorized = Utilities.isInputFileFormatVectorized(pd);

      if (isAcidTable) {

        // Today, ACID tables are only ORC and that format is vectorizable.  Verify these
        // assumptions.
        Preconditions.checkState(isInputFileFormatVectorized);
        Preconditions.checkState(inputFileFormatClassName.equals(OrcInputFormat.class.getName()));

        if (!useVectorizedInputFileFormat) {
          enabledConditionsNotMetList.add(
              "Vectorizing ACID tables requires " + HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTORIZED_INPUT_FILE_FORMAT.varname);
          return false;
        }

        pd.setVectorPartitionDesc(
            VectorPartitionDesc.createVectorizedInputFileFormat(
                inputFileFormatClassName, Utilities.isInputFileFormatSelfDescribing(pd)));

        enabledConditionsMetSet.add(HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTORIZED_INPUT_FILE_FORMAT.varname);
        return true;
      }

      // Look for Pass-Thru case where InputFileFormat has VectorizedInputFormatInterface
      // and reads VectorizedRowBatch as a "row".

      if (useVectorizedInputFileFormat) {

        if (isInputFileFormatVectorized) {

          pd.setVectorPartitionDesc(
              VectorPartitionDesc.createVectorizedInputFileFormat(
                  inputFileFormatClassName, Utilities.isInputFileFormatSelfDescribing(pd)));

          enabledConditionsMetSet.add(HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTORIZED_INPUT_FILE_FORMAT.varname);
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
          deserializerClassName.equals(LazyBinarySerDe.class.getName());
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
            if (useRowDeserialize) {
              enabledConditionsNotMetList.add(
                  inputFileFormatClassName + " " +
                  serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST + " must be disabled ");
              return false;
            }
          } else {
            pd.setVectorPartitionDesc(
                VectorPartitionDesc.createVectorDeserialize(
                    inputFileFormatClassName, VectorDeserializeType.LAZY_SIMPLE));

            enabledConditionsMetSet.add(HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTOR_DESERIALIZE.varname);
            return true;
          }
        } else if (isSequenceFormat) {

          pd.setVectorPartitionDesc(
              VectorPartitionDesc.createVectorDeserialize(
                  inputFileFormatClassName, VectorDeserializeType.LAZY_BINARY));

          enabledConditionsMetSet.add(HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTOR_DESERIALIZE.varname);
          return true;
        }
        // Fall through and look for other options...
      }

      // Otherwise, if enabled, deserialize rows using regular Serde and add the object
      // inspect-able Object[] row to a VectorizedRowBatch in the VectorMapOperator.

      if (useRowDeserialize) {

        pd.setVectorPartitionDesc(
            VectorPartitionDesc.createRowDeserialize(
                inputFileFormatClassName,
                Utilities.isInputFileFormatSelfDescribing(pd),
                deserializerClassName));

        enabledConditionsMetSet.add(HiveConf.ConfVars.HIVE_VECTORIZATION_USE_ROW_DESERIALIZE.varname);
        return true;

      }

      if (isInputFileFormatVectorized) {
        Preconditions.checkState(!useVectorizedInputFileFormat);
        enabledConditionsNotMetList.add(HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTORIZED_INPUT_FILE_FORMAT.varname);
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

    private ImmutablePair<Boolean, Boolean> validateInputFormatAndSchemaEvolution(MapWork mapWork, String alias,
        TableScanOperator tableScanOperator, VectorTaskColumnInfo vectorTaskColumnInfo)
            throws SemanticException {

      boolean isAcidTable = tableScanOperator.getConf().isAcidTable();

      // These names/types are the data columns plus partition columns.
      final List<String> allColumnNameList = new ArrayList<String>();
      final List<TypeInfo> allTypeInfoList = new ArrayList<TypeInfo>();

      getTableScanOperatorSchemaInfo(tableScanOperator, allColumnNameList, allTypeInfoList);

      final List<Integer> dataColumnNums = new ArrayList<Integer>();

      final int allColumnCount = allColumnNameList.size();

      /*
       * Validate input formats of all the partitions can be vectorized.
       */
      boolean isFirst = true;
      int dataColumnCount = 0;
      int partitionColumnCount = 0;

      List<String> tableDataColumnList = null;
      List<TypeInfo> tableDataTypeInfoList = null;

      LinkedHashMap<Path, ArrayList<String>> pathToAliases = mapWork.getPathToAliases();
      LinkedHashMap<Path, PartitionDesc> pathToPartitionInfo = mapWork.getPathToPartitionInfo();

      // Remember the input file formats we validated and why.
      HashSet<String> inputFileFormatClassNameSet = new HashSet<String>();
      HashSet<String> enabledConditionsMetSet = new HashSet<String>();
      ArrayList<String> enabledConditionsNotMetList = new ArrayList<String>();

      for (Entry<Path, ArrayList<String>> entry: pathToAliases.entrySet()) {
        Path path = entry.getKey();
        List<String> aliases = entry.getValue();
        boolean isPresent = (aliases != null && aliases.indexOf(alias) != -1);
        if (!isPresent) {
          setOperatorIssue("Alias " + alias + " not present in aliases " + aliases);
          return new ImmutablePair<Boolean,Boolean>(false, false);
        }
        PartitionDesc partDesc = pathToPartitionInfo.get(path);
        if (partDesc.getVectorPartitionDesc() != null) {
          // We've seen this already.
          continue;
        }
        if (!verifyAndSetVectorPartDesc(partDesc, isAcidTable, inputFileFormatClassNameSet,
            enabledConditionsMetSet, enabledConditionsNotMetList)) {

          // Always set these so EXPLAIN can see.
          mapWork.setVectorizationInputFileFormatClassNameSet(inputFileFormatClassNameSet);
          mapWork.setVectorizationEnabledConditionsMet(new ArrayList(enabledConditionsMetSet));
          mapWork.setVectorizationEnabledConditionsNotMet(enabledConditionsNotMetList);

          // We consider this an enable issue, not a not vectorized issue.
          LOG.info("Cannot enable vectorization because input file format(s) " + inputFileFormatClassNameSet +
              " do not met conditions " + VectorizationCondition.addBooleans(enabledConditionsNotMetList, false));
          return new ImmutablePair<Boolean,Boolean>(false, true);
        }

        VectorPartitionDesc vectorPartDesc = partDesc.getVectorPartitionDesc();

        if (isFirst) {

          // Determine the data and partition columns using the first partition descriptor.

          LinkedHashMap<String, String> partSpec = partDesc.getPartSpec();
          if (partSpec != null && partSpec.size() > 0) {
            partitionColumnCount = partSpec.size();
            dataColumnCount = allColumnCount - partitionColumnCount;
          } else {
            partitionColumnCount = 0;
            dataColumnCount = allColumnCount;
          }

          determineDataColumnNums(tableScanOperator, allColumnNameList, dataColumnCount,
              dataColumnNums);

          tableDataColumnList = allColumnNameList.subList(0, dataColumnCount);
          tableDataTypeInfoList = allTypeInfoList.subList(0, dataColumnCount);

          isFirst = false;
        }

        // We need to get the partition's column names from the partition serde.
        // (e.g. Avro provides the table schema and ignores the partition schema..).
        //
        Deserializer deserializer;
        StructObjectInspector partObjectInspector;
        try {
          deserializer = partDesc.getDeserializer(hiveConf);
          partObjectInspector = (StructObjectInspector) deserializer.getObjectInspector();
        } catch (Exception e) {
          throw new SemanticException(e);
        }
        String nextDataColumnsString = ObjectInspectorUtils.getFieldNames(partObjectInspector);
        String[] nextDataColumns = nextDataColumnsString.split(",");
        List<String> nextDataColumnList = Arrays.asList(nextDataColumns);

        /*
         * Validate the column names that are present are the same.  Missing columns will be
         * implicitly defaulted to null.
         */
        if (nextDataColumnList.size() > tableDataColumnList.size()) {
          setOperatorIssue(
              String.format(
                  "Could not vectorize partition %s " +
                  "(deserializer " + deserializer.getClass().getName() + ")" +
                  "The partition column names %d is greater than the number of table columns %d",
                  path, nextDataColumnList.size(), tableDataColumnList.size()));
          return new ImmutablePair<Boolean,Boolean>(false, false);
        }
        if (!(deserializer instanceof NullStructSerDe)) {

          // (Don't insist NullStructSerDe produce correct column names).
          for (int i = 0; i < nextDataColumnList.size(); i++) {
            String nextColumnName = nextDataColumnList.get(i);
            String tableColumnName = tableDataColumnList.get(i);
            if (!nextColumnName.equals(tableColumnName)) {
              setOperatorIssue(
                  String.format(
                      "Could not vectorize partition %s " +
                      "(deserializer " + deserializer.getClass().getName() + ")" +
                      "The partition column name %s is does not match table column name %s",
                      path, nextColumnName, tableColumnName));
              return new ImmutablePair<Boolean,Boolean>(false, false);
            }
          }
        }

        List<TypeInfo> nextDataTypeInfoList;
        if (vectorPartDesc.getIsInputFileFormatSelfDescribing()) {

          /*
           * Self-Describing Input Format will convert its data to the table schema.
           */
          nextDataTypeInfoList = tableDataTypeInfoList;

        } else {
          String nextDataTypesString = ObjectInspectorUtils.getFieldTypes(partObjectInspector);

          // We convert to an array of TypeInfo using a library routine since it parses the information
          // and can handle use of different separators, etc.  We cannot use the raw type string
          // for comparison in the map because of the different separators used.
          nextDataTypeInfoList =
              TypeInfoUtils.getTypeInfosFromTypeString(nextDataTypesString);
        }

        vectorPartDesc.setDataTypeInfos(nextDataTypeInfoList);
      }

      vectorTaskColumnInfo.setAllColumnNames(allColumnNameList);
      vectorTaskColumnInfo.setAllTypeInfos(allTypeInfoList);
      vectorTaskColumnInfo.setDataColumnNums(dataColumnNums);
      vectorTaskColumnInfo.setPartitionColumnCount(partitionColumnCount);
      vectorTaskColumnInfo.setUseVectorizedInputFileFormat(useVectorizedInputFileFormat);

      // Always set these so EXPLAIN can see.
      mapWork.setVectorizationInputFileFormatClassNameSet(inputFileFormatClassNameSet);
      mapWork.setVectorizationEnabledConditionsMet(new ArrayList(enabledConditionsMetSet));
      mapWork.setVectorizationEnabledConditionsNotMet(enabledConditionsNotMetList);

      return new ImmutablePair<Boolean,Boolean>(true, false);
    }

    private boolean validateMapWork(MapWork mapWork, VectorTaskColumnInfo vectorTaskColumnInfo, boolean isTezOrSpark)
            throws SemanticException {

      LOG.info("Validating MapWork...");

      ImmutablePair<String,TableScanOperator> onlyOneTableScanPair = verifyOnlyOneTableScanOperator(mapWork);
      if (onlyOneTableScanPair ==  null) {
        VectorizerReason notVectorizedReason = currentBaseWork.getNotVectorizedReason();
        Preconditions.checkState(notVectorizedReason != null);
        mapWork.setVectorizationEnabledConditionsNotMet(Arrays.asList(new String[] {notVectorizedReason.toString()}));
        return false;
      }
      String alias = onlyOneTableScanPair.left;
      TableScanOperator tableScanOperator = onlyOneTableScanPair.right;

      // This call fills in the column names, types, and partition column count in
      // vectorTaskColumnInfo.
      currentOperator = tableScanOperator;
      ImmutablePair<Boolean, Boolean> validateInputFormatAndSchemaEvolutionPair =
          validateInputFormatAndSchemaEvolution(mapWork, alias, tableScanOperator, vectorTaskColumnInfo);
      if (!validateInputFormatAndSchemaEvolutionPair.left) {
        // Have we already set the enabled conditions not met?
        if (!validateInputFormatAndSchemaEvolutionPair.right) {
          VectorizerReason notVectorizedReason = currentBaseWork.getNotVectorizedReason();
          Preconditions.checkState(notVectorizedReason != null);
          mapWork.setVectorizationEnabledConditionsNotMet(Arrays.asList(new String[] {notVectorizedReason.toString()}));
        }
        return false;
      }

      // Now we are enabled and any issues found from here on out are considered
      // not vectorized issues.
      mapWork.setVectorizationEnabled(true);

      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      MapWorkValidationNodeProcessor vnp = new MapWorkValidationNodeProcessor(mapWork, isTezOrSpark);
      addMapWorkRules(opRules, vnp);
      Dispatcher disp = new DefaultRuleDispatcher(vnp, opRules, null);
      GraphWalker ogw = new DefaultGraphWalker(disp);

      // iterator the mapper operator tree
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(mapWork.getAliasToWork().values());
      HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();
      ogw.startWalking(topNodes, nodeOutput);
      for (Node n : nodeOutput.keySet()) {
        if (nodeOutput.get(n) != null) {
          if (!((Boolean)nodeOutput.get(n)).booleanValue()) {
            return false;
          }
        }
      }
      vectorTaskColumnInfo.setNonVectorizedOps(vnp.getNonVectorizedOps());
      return true;
    }

    private void vectorizeMapWork(MapWork mapWork, VectorTaskColumnInfo vectorTaskColumnInfo,
            boolean isTezOrSpark) throws SemanticException {

      LOG.info("Vectorizing MapWork...");
      mapWork.setVectorMode(true);
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      MapWorkVectorizationNodeProcessor vnp =
          new MapWorkVectorizationNodeProcessor(mapWork, isTezOrSpark, vectorTaskColumnInfo);
      addMapWorkRules(opRules, vnp);
      Dispatcher disp = new DefaultRuleDispatcher(vnp, opRules, null);
      GraphWalker ogw = new PreOrderOnceWalker(disp);
      // iterator the mapper operator tree
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(mapWork.getAliasToWork().values());
      HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();
      ogw.startWalking(topNodes, nodeOutput);

      vectorTaskColumnInfo.setScratchTypeNameArray(vnp.getVectorScratchColumnTypeNames());

      vectorTaskColumnInfo.transferToBaseWork(mapWork);

      if (LOG.isDebugEnabled()) {
        debugDisplayAllMaps(mapWork);
      }

      return;
    }

    private void setReduceWorkExplainConditions(ReduceWork reduceWork) {

      reduceWork.setVectorizationExamined(true);

      reduceWork.setReduceVectorizationEnabled(isReduceVectorizationEnabled);
      reduceWork.setVectorReduceEngine(
          HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE));
    }

    private void convertReduceWork(ReduceWork reduceWork) throws SemanticException {

      // Global used when setting errors, etc.
      currentBaseWork = reduceWork;
      currentBaseWork.setVectorizationEnabled(true);

      VectorTaskColumnInfo vectorTaskColumnInfo = new VectorTaskColumnInfo();
      vectorTaskColumnInfo.assume();

      reduceWork.setVectorizedVertexNum(++vectorizedVertexNum);

      boolean ret;
      try {
        ret = validateReduceWork(reduceWork, vectorTaskColumnInfo);
      } catch (Exception e) {
        String issue = "exception: " + VectorizationContext.getStackTraceAsSingleLine(e);
        setNodeIssue(issue);
        ret = false;
      }
      if (ret) {
        vectorizeReduceWork(reduceWork, vectorTaskColumnInfo);
      } else if (currentBaseWork.getVectorizationEnabled()) {
        VectorizerReason notVectorizedReason  = currentBaseWork.getNotVectorizedReason();
        if (notVectorizedReason == null) {
          LOG.info("Cannot vectorize: unknown");
        } else {
          LOG.info("Cannot vectorize: " + notVectorizedReason.toString());
        }
        clearReduceWorkVectorDescs(reduceWork);
      }
    }

    private boolean getOnlyStructObjectInspectors(ReduceWork reduceWork,
            VectorTaskColumnInfo vectorTaskColumnInfo) throws SemanticException {

      ArrayList<String> reduceColumnNames = new ArrayList<String>();
      ArrayList<TypeInfo> reduceTypeInfos = new ArrayList<TypeInfo>();

      if (reduceWork.getNeedsTagging()) {
        setNodeIssue("Tagging not supported");
        return false;
      }

      try {
        TableDesc keyTableDesc = reduceWork.getKeyDesc();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Using reduce tag " + reduceWork.getTag());
        }
        TableDesc valueTableDesc = reduceWork.getTagToValueDesc().get(reduceWork.getTag());

        Deserializer keyDeserializer =
            ReflectionUtils.newInstance(
                keyTableDesc.getDeserializerClass(), null);
        SerDeUtils.initializeSerDe(keyDeserializer, null, keyTableDesc.getProperties(), null);
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
        }

        Deserializer valueDeserializer =
            ReflectionUtils.newInstance(
                valueTableDesc.getDeserializerClass(), null);
        SerDeUtils.initializeSerDe(valueDeserializer, null, valueTableDesc.getProperties(), null);
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
            reduceTypeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(field.getFieldObjectInspector().getTypeName()));
          }
        }
      } catch (Exception e) {
        throw new SemanticException(e);
      }

      vectorTaskColumnInfo.setAllColumnNames(reduceColumnNames);
      vectorTaskColumnInfo.setAllTypeInfos(reduceTypeInfos);

      return true;
    }

    private void addReduceWorkRules(Map<Rule, NodeProcessor> opRules, NodeProcessor np) {
      opRules.put(new RuleRegExp("R1", GroupByOperator.getOperatorName() + ".*"), np);
      opRules.put(new RuleRegExp("R2", SelectOperator.getOperatorName() + ".*"), np);
    }

    private boolean validateReduceWork(ReduceWork reduceWork,
        VectorTaskColumnInfo vectorTaskColumnInfo) throws SemanticException {

      LOG.info("Validating ReduceWork...");

      // Validate input to ReduceWork.
      if (!getOnlyStructObjectInspectors(reduceWork, vectorTaskColumnInfo)) {
        return false;
      }
      // Now check the reduce operator tree.
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      ReduceWorkValidationNodeProcessor vnp = new ReduceWorkValidationNodeProcessor();
      addReduceWorkRules(opRules, vnp);
      Dispatcher disp = new DefaultRuleDispatcher(vnp, opRules, null);
      GraphWalker ogw = new DefaultGraphWalker(disp);
      // iterator the reduce operator tree
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.add(reduceWork.getReducer());
      HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();
      ogw.startWalking(topNodes, nodeOutput);
      for (Node n : nodeOutput.keySet()) {
        if (nodeOutput.get(n) != null) {
          if (!((Boolean)nodeOutput.get(n)).booleanValue()) {
            return false;
          }
        }
      }
      vectorTaskColumnInfo.setNonVectorizedOps(vnp.getNonVectorizedOps());
      return true;
    }

    private void vectorizeReduceWork(ReduceWork reduceWork,
        VectorTaskColumnInfo vectorTaskColumnInfo) throws SemanticException {

      LOG.info("Vectorizing ReduceWork...");
      reduceWork.setVectorMode(true);

      // For some reason, the DefaultGraphWalker does not descend down from the reducer Operator as
      // expected.  We need to descend down, otherwise it breaks our algorithm that determines
      // VectorizationContext...  Do we use PreOrderWalker instead of DefaultGraphWalker.
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      ReduceWorkVectorizationNodeProcessor vnp =
              new ReduceWorkVectorizationNodeProcessor(vectorTaskColumnInfo);
      addReduceWorkRules(opRules, vnp);
      Dispatcher disp = new DefaultRuleDispatcher(vnp, opRules, null);
      GraphWalker ogw = new PreOrderWalker(disp);
      // iterator the reduce operator tree
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.add(reduceWork.getReducer());
      LOG.info("vectorizeReduceWork reducer Operator: " +
              reduceWork.getReducer().getName() + "...");
      HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();
      ogw.startWalking(topNodes, nodeOutput);

      // Necessary since we are vectorizing the root operator in reduce.
      reduceWork.setReducer(vnp.getRootVectorOp());

      vectorTaskColumnInfo.setScratchTypeNameArray(vnp.getVectorScratchColumnTypeNames());

      vectorTaskColumnInfo.transferToBaseWork(reduceWork);

      if (LOG.isDebugEnabled()) {
        debugDisplayAllMaps(reduceWork);
      }
    }

    class ClearVectorDescsNodeProcessor implements NodeProcessor {

      public ClearVectorDescsNodeProcessor() {
      }

      @Override
      public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
          Object... nodeOutputs) throws SemanticException {
        for (Node n : stack) {
          Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) n;

          OperatorDesc desc = op.getConf();
          if (desc instanceof AbstractOperatorDesc) {
            AbstractOperatorDesc abstractDesc = (AbstractOperatorDesc) desc;
            abstractDesc.setVectorDesc(null);
          }
        }
        return null;
      }
    }

    private void clearMapWorkVectorDescs(MapWork mapWork) throws SemanticException {
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      ClearVectorDescsNodeProcessor vnp = new ClearVectorDescsNodeProcessor();
      addMapWorkRules(opRules, vnp);
      Dispatcher disp = new DefaultRuleDispatcher(vnp, opRules, null);
      GraphWalker ogw = new DefaultGraphWalker(disp);
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(mapWork.getAliasToWork().values());
      ogw.startWalking(topNodes, null);
    }

    private void clearReduceWorkVectorDescs(ReduceWork reduceWork) throws SemanticException {
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      ClearVectorDescsNodeProcessor vnp = new ClearVectorDescsNodeProcessor();
      addReduceWorkRules(opRules, vnp);
      Dispatcher disp = new DefaultRuleDispatcher(vnp, opRules, null);
      GraphWalker ogw = new DefaultGraphWalker(disp);
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.add(reduceWork.getReducer());
      ogw.startWalking(topNodes, null);
    }
  }

  class MapWorkValidationNodeProcessor implements NodeProcessor {

    private final MapWork mapWork;
    private final boolean isTezOrSpark;

    // Children of Vectorized GROUPBY that outputs rows instead of vectorized row batchs.
    protected final Set<Operator<? extends OperatorDesc>> nonVectorizedOps =
        new HashSet<Operator<? extends OperatorDesc>>();

    public Set<Operator<? extends OperatorDesc>> getNonVectorizedOps() {
      return nonVectorizedOps;
    }

    public MapWorkValidationNodeProcessor(MapWork mapWork, boolean isTezOrSpark) {
      this.mapWork = mapWork;
      this.isTezOrSpark = isTezOrSpark;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      for (Node n : stack) {
        Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) n;
        if (nonVectorizedOps.contains(op)) {
          return new Boolean(true);
        }
        boolean ret;
        currentOperator = op;
        try {
          ret = validateMapWorkOperator(op, mapWork, isTezOrSpark);
        } catch (Exception e) {
          throw new SemanticException(e);
        }
        if (!ret) {
          return new Boolean(false);
        }
        // When Vectorized GROUPBY outputs rows instead of vectorized row batches, we don't
        // vectorize the operators below it.
        if (isVectorizedGroupByThatOutputsRows(op)) {
          addOperatorChildrenToSet(op, nonVectorizedOps);
          return new Boolean(true);
        }
      }
      return new Boolean(true);
    }
  }

  class ReduceWorkValidationNodeProcessor implements NodeProcessor {

    // Children of Vectorized GROUPBY that outputs rows instead of vectorized row batchs.
    protected final Set<Operator<? extends OperatorDesc>> nonVectorizedOps =
        new HashSet<Operator<? extends OperatorDesc>>();

    public Set<Operator<? extends OperatorDesc>> getNonVectorizeOps() {
      return nonVectorizedOps;
    }

    public Set<Operator<? extends OperatorDesc>> getNonVectorizedOps() {
      return nonVectorizedOps;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      for (Node n : stack) {
        Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) n;
        if (nonVectorizedOps.contains(op)) {
          return new Boolean(true);
        }
        currentOperator = op;
        boolean ret = validateReduceWorkOperator(op);
        if (!ret) {
          return new Boolean(false);
        }
        // When Vectorized GROUPBY outputs rows instead of vectorized row batches, we don't
        // vectorize the operators below it.
        if (isVectorizedGroupByThatOutputsRows(op)) {
          addOperatorChildrenToSet(op, nonVectorizedOps);
          return new Boolean(true);
        }
      }
      return new Boolean(true);
    }
  }

  // This class has common code used by both MapWorkVectorizationNodeProcessor and
  // ReduceWorkVectorizationNodeProcessor.
  class VectorizationNodeProcessor implements NodeProcessor {

    // The vectorization context for the Map or Reduce task.
    protected VectorizationContext taskVectorizationContext;

    protected final VectorTaskColumnInfo vectorTaskColumnInfo;
    protected final Set<Operator<? extends OperatorDesc>> nonVectorizedOps;

    VectorizationNodeProcessor(VectorTaskColumnInfo vectorTaskColumnInfo,
        Set<Operator<? extends OperatorDesc>> nonVectorizedOps) {
      this.vectorTaskColumnInfo = vectorTaskColumnInfo;
      this.nonVectorizedOps = nonVectorizedOps;
    }

    public String[] getVectorScratchColumnTypeNames() {
      return taskVectorizationContext.getScratchColumnTypeNames();
    }

    protected final Set<Operator<? extends OperatorDesc>> opsDone =
        new HashSet<Operator<? extends OperatorDesc>>();

    protected final Map<Operator<? extends OperatorDesc>, Operator<? extends OperatorDesc>> opToVectorOpMap =
        new HashMap<Operator<? extends OperatorDesc>, Operator<? extends OperatorDesc>>();

    public VectorizationContext walkStackToFindVectorizationContext(Stack<Node> stack,
            Operator<? extends OperatorDesc> op) throws SemanticException {
      VectorizationContext vContext = null;
      if (stack.size() <= 1) {
        throw new SemanticException(
            String.format("Expected operator stack for operator %s to have at least 2 operators",
                  op.getName()));
      }
      // Walk down the stack of operators until we found one willing to give us a context.
      // At the bottom will be the root operator, guaranteed to have a context
      int i= stack.size()-2;
      while (vContext == null) {
        if (i < 0) {
          return null;
        }
        Operator<? extends OperatorDesc> opParent = (Operator<? extends OperatorDesc>) stack.get(i);
        Operator<? extends OperatorDesc> vectorOpParent = opToVectorOpMap.get(opParent);
        if (vectorOpParent != null) {
          if (vectorOpParent instanceof VectorizationContextRegion) {
            VectorizationContextRegion vcRegion = (VectorizationContextRegion) vectorOpParent;
            vContext = vcRegion.getOuputVectorizationContext();
            LOG.info("walkStackToFindVectorizationContext " + vectorOpParent.getName() + " has new vectorization context " + vContext.toString());
          } else {
            LOG.info("walkStackToFindVectorizationContext " + vectorOpParent.getName() + " does not have new vectorization context");
          }
        } else {
          LOG.info("walkStackToFindVectorizationContext " + opParent.getName() + " is not vectorized");
        }
        --i;
      }
      return vContext;
    }

    public Operator<? extends OperatorDesc> doVectorize(Operator<? extends OperatorDesc> op,
            VectorizationContext vContext, boolean isTezOrSpark) throws SemanticException {
      Operator<? extends OperatorDesc> vectorOp = op;
      try {
        if (!opsDone.contains(op)) {
          vectorOp = vectorizeOperator(op, vContext, isTezOrSpark, vectorTaskColumnInfo);
          opsDone.add(op);
          if (vectorOp != op) {
            opToVectorOpMap.put(op, vectorOp);
            opsDone.add(vectorOp);
          }
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }
      return vectorOp;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      throw new SemanticException("Must be overridden");
    }
  }

  class MapWorkVectorizationNodeProcessor extends VectorizationNodeProcessor {

    private final VectorTaskColumnInfo vectorTaskColumnInfo;
    private final boolean isTezOrSpark;

    public MapWorkVectorizationNodeProcessor(MapWork mWork, boolean isTezOrSpark,
        VectorTaskColumnInfo vectorTaskColumnInfo) {
      super(vectorTaskColumnInfo, vectorTaskColumnInfo.getNonVectorizedOps());
      this.vectorTaskColumnInfo = vectorTaskColumnInfo;
      this.isTezOrSpark = isTezOrSpark;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      if (nonVectorizedOps.contains(op)) {
        return null;
      }

      VectorizationContext vContext = null;

      currentOperator = op;
      if (op instanceof TableScanOperator) {
        if (taskVectorizationContext == null) {
          taskVectorizationContext = getVectorizationContext(op.getName(), vectorTaskColumnInfo);
          if (LOG.isInfoEnabled()) {
            LOG.info("MapWorkVectorizationNodeProcessor process vectorizedVertexNum " + vectorizedVertexNum + " mapColumnNames " + vectorTaskColumnInfo.allColumnNames.toString());
            LOG.info("MapWorkVectorizationNodeProcessor process vectorizedVertexNum " + vectorizedVertexNum + " mapTypeInfos " + vectorTaskColumnInfo.allTypeInfos.toString());
          }
        }
        vContext = taskVectorizationContext;
      } else {
        LOG.debug("MapWorkVectorizationNodeProcessor process going to walk the operator stack to get vectorization context for " + op.getName());
        vContext = walkStackToFindVectorizationContext(stack, op);
        if (vContext == null) {
          // No operator has "pushed" a new context -- so use the task vectorization context.
          vContext = taskVectorizationContext;
        }
      }

      assert vContext != null;
      if (LOG.isDebugEnabled()) {
        LOG.debug("MapWorkVectorizationNodeProcessor process operator " + op.getName()
            + " using vectorization context" + vContext.toString());
      }

      Operator<? extends OperatorDesc> vectorOp = doVectorize(op, vContext, isTezOrSpark);

      if (LOG.isDebugEnabled()) {
        if (vectorOp instanceof VectorizationContextRegion) {
          VectorizationContextRegion vcRegion = (VectorizationContextRegion) vectorOp;
          VectorizationContext vNewContext = vcRegion.getOuputVectorizationContext();
          LOG.debug("Vectorized MapWork operator " + vectorOp.getName() + " added vectorization context " + vNewContext.toString());
        }
      }

      return null;
    }
  }

  class ReduceWorkVectorizationNodeProcessor extends VectorizationNodeProcessor {

    private final VectorTaskColumnInfo vectorTaskColumnInfo;


    private Operator<? extends OperatorDesc> rootVectorOp;

    public Operator<? extends OperatorDesc> getRootVectorOp() {
      return rootVectorOp;
    }

    public ReduceWorkVectorizationNodeProcessor(VectorTaskColumnInfo vectorTaskColumnInfo) {

      super(vectorTaskColumnInfo, vectorTaskColumnInfo.getNonVectorizedOps());
      this.vectorTaskColumnInfo =  vectorTaskColumnInfo;
      rootVectorOp = null;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      if (nonVectorizedOps.contains(op)) {
        return null;
      }

      VectorizationContext vContext = null;

      boolean saveRootVectorOp = false;

      currentOperator = op;
      if (op.getParentOperators().size() == 0) {
        if (LOG.isInfoEnabled()) {
          LOG.info("ReduceWorkVectorizationNodeProcessor process vectorizedVertexNum " + vectorizedVertexNum + " reduceColumnNames " + vectorTaskColumnInfo.allColumnNames.toString());
          LOG.info("ReduceWorkVectorizationNodeProcessor process vectorizedVertexNum " + vectorizedVertexNum + " reduceTypeInfos " + vectorTaskColumnInfo.allTypeInfos.toString());
        }
        vContext = new VectorizationContext("__Reduce_Shuffle__", vectorTaskColumnInfo.allColumnNames, hiveConf);
        taskVectorizationContext = vContext;

        saveRootVectorOp = true;

        if (LOG.isDebugEnabled()) {
          LOG.debug("Vectorized ReduceWork reduce shuffle vectorization context " + vContext.toString());
        }
      } else {
        LOG.info("ReduceWorkVectorizationNodeProcessor process going to walk the operator stack to get vectorization context for " + op.getName());
        vContext = walkStackToFindVectorizationContext(stack, op);
        if (vContext == null) {
          // If we didn't find a context among the operators, assume the top -- reduce shuffle's
          // vectorization context.
          vContext = taskVectorizationContext;
        }
      }

      assert vContext != null;
      LOG.info("ReduceWorkVectorizationNodeProcessor process operator " + op.getName() + " using vectorization context" + vContext.toString());

      Operator<? extends OperatorDesc> vectorOp = doVectorize(op, vContext, true);

      if (LOG.isDebugEnabled()) {
        if (vectorOp instanceof VectorizationContextRegion) {
          VectorizationContextRegion vcRegion = (VectorizationContextRegion) vectorOp;
          VectorizationContext vNewContext = vcRegion.getOuputVectorizationContext();
          LOG.debug("Vectorized ReduceWork operator " + vectorOp.getName() + " added vectorization context " + vNewContext.toString());
        }
      }
      if (saveRootVectorOp && op != vectorOp) {
        rootVectorOp = vectorOp;
      }

      return null;
    }
  }

  private static class ValidatorVectorizationContext extends VectorizationContext {
    private ValidatorVectorizationContext(HiveConf hiveConf) {
      super("No Name", hiveConf);
    }

    @Override
    protected int getInputColumnIndex(String name) {
      return 0;
    }

    @Override
    protected int getInputColumnIndex(ExprNodeColumnDesc colExpr) {
      return 0;
    }
  }

  @Override
  public PhysicalContext resolve(PhysicalContext physicalContext) throws SemanticException {

    hiveConf = physicalContext.getConf();

    boolean vectorPath = HiveConf.getBoolVar(hiveConf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED);
    if (!vectorPath) {
      LOG.info("Vectorization is disabled");
      return physicalContext;
    }

    useVectorizedInputFileFormat =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTORIZED_INPUT_FILE_FORMAT);
    useVectorDeserialize =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTOR_DESERIALIZE);
    useRowDeserialize =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_USE_ROW_DESERIALIZE);
    // TODO: we could also vectorize some formats based on hive.llap.io.encode.formats if LLAP IO
    //       is enabled and we are going to run in LLAP. However, we don't know if we end up in
    //       LLAP or not at this stage, so don't do this now. We may need to add a 'force' option.

    isReduceVectorizationEnabled =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_REDUCE_ENABLED);

    isSchemaEvolution =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_SCHEMA_EVOLUTION);

    hiveVectorAdaptorUsageMode = HiveVectorAdaptorUsageMode.getHiveConfValue(hiveConf);

    // create dispatcher and graph walker
    Dispatcher disp = new VectorizationDispatcher();
    TaskGraphWalker ogw = new TaskGraphWalker(disp);

    // get all the tasks nodes from root task
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(physicalContext.getRootTasks());

    // begin to walk through the task tree.
    ogw.startWalking(topNodes, null);
    return physicalContext;
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

  boolean validateMapWorkOperator(Operator<? extends OperatorDesc> op, MapWork mWork, boolean isTezOrSpark) {
    boolean ret;
    switch (op.getType()) {
      case MAPJOIN:
        if (op instanceof MapJoinOperator) {
          ret = validateMapJoinOperator((MapJoinOperator) op);
        } else if (op instanceof SMBMapJoinOperator) {
          ret = validateSMBMapJoinOperator((SMBMapJoinOperator) op);
        } else {
          setOperatorNotSupported(op);
          ret = false;
        }
        break;
      case GROUPBY:
        ret = validateGroupByOperator((GroupByOperator) op, false, isTezOrSpark);
        break;
      case FILTER:
        ret = validateFilterOperator((FilterOperator) op);
        break;
      case SELECT:
        ret = validateSelectOperator((SelectOperator) op);
        break;
      case REDUCESINK:
        ret = validateReduceSinkOperator((ReduceSinkOperator) op);
        break;
      case TABLESCAN:
        ret = validateTableScanOperator((TableScanOperator) op, mWork);
        break;
      case FILESINK:
      case LIMIT:
      case EVENT:
      case SPARKPRUNINGSINK:
        ret = true;
        break;
      case HASHTABLESINK:
        ret = op instanceof SparkHashTableSinkOperator &&
            validateSparkHashTableSinkOperator((SparkHashTableSinkOperator) op);
        break;
      default:
        setOperatorNotSupported(op);
        ret = false;
        break;
    }
    return ret;
  }

  boolean validateReduceWorkOperator(Operator<? extends OperatorDesc> op) {
    boolean ret;
    switch (op.getType()) {
      case MAPJOIN:
        // Does MAPJOIN actually get planned in Reduce?
        if (op instanceof MapJoinOperator) {
          ret = validateMapJoinOperator((MapJoinOperator) op);
        } else if (op instanceof SMBMapJoinOperator) {
          ret = validateSMBMapJoinOperator((SMBMapJoinOperator) op);
        } else {
          setOperatorNotSupported(op);
          ret = false;
        }
        break;
      case GROUPBY:
        if (HiveConf.getBoolVar(hiveConf,
                    HiveConf.ConfVars.HIVE_VECTORIZATION_REDUCE_GROUPBY_ENABLED)) {
          ret = validateGroupByOperator((GroupByOperator) op, true, true);
        } else {
          setNodeIssue("Operator " + op.getType() + " not enabled (" + HiveConf.ConfVars.HIVE_VECTORIZATION_REDUCE_GROUPBY_ENABLED.name() + "=true IS false)");
          ret = false;
        }
        break;
      case FILTER:
        ret = validateFilterOperator((FilterOperator) op);
        break;
      case SELECT:
        ret = validateSelectOperator((SelectOperator) op);
        break;
      case REDUCESINK:
        ret = validateReduceSinkOperator((ReduceSinkOperator) op);
        break;
      case FILESINK:
        ret = validateFileSinkOperator((FileSinkOperator) op);
        break;
      case LIMIT:
      case EVENT:
      case SPARKPRUNINGSINK:
        ret = true;
        break;
      case HASHTABLESINK:
        ret = op instanceof SparkHashTableSinkOperator &&
            validateSparkHashTableSinkOperator((SparkHashTableSinkOperator) op);
        break;
      default:
        setOperatorNotSupported(op);
        ret = false;
        break;
    }
    return ret;
  }

  private void addOperatorChildrenToSet(Operator<? extends OperatorDesc> op,
      Set<Operator<? extends OperatorDesc>> nonVectorizedOps) {
    for (Operator<? extends OperatorDesc> childOp : op.getChildOperators()) {
      if (!nonVectorizedOps.contains(childOp)) {
        nonVectorizedOps.add(childOp);
        addOperatorChildrenToSet(childOp, nonVectorizedOps);
      }
    }
  }

  // When Vectorized GROUPBY outputs rows instead of vectorized row batchs, we don't
  // vectorize the operators below it.
   private Boolean isVectorizedGroupByThatOutputsRows(Operator<? extends OperatorDesc> op)
      throws SemanticException {
    if (op.getType().equals(OperatorType.GROUPBY)) {
      GroupByDesc desc = (GroupByDesc) op.getConf();
      return !((VectorGroupByDesc) desc.getVectorDesc()).isVectorOutput();
    }
    return false;
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
    if (!validateExprNodeDesc(filterExprs, "Filter", VectorExpressionDescriptor.Mode.FILTER)) {
      return false;
    }
    List<ExprNodeDesc> keyExprs = desc.getKeys().get(posBigTable);
    if (!validateExprNodeDesc(keyExprs, "Key")) {
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
      LOG.info("Cannot vectorize outer join with complex ON clause");
      return false;
    }
    return true;
  }

  private boolean validateSparkHashTableSinkOperator(SparkHashTableSinkOperator op) {
    SparkHashTableSinkDesc desc = op.getConf();
    byte tag = desc.getTag();
    // it's essentially a MapJoinDesc
    List<ExprNodeDesc> filterExprs = desc.getFilters().get(tag);
    List<ExprNodeDesc> keyExprs = desc.getKeys().get(tag);
    List<ExprNodeDesc> valueExprs = desc.getExprs().get(tag);
    return validateExprNodeDesc(filterExprs, "Filter", VectorExpressionDescriptor.Mode.FILTER) &&
        validateExprNodeDesc(keyExprs, "Key") && validateExprNodeDesc(valueExprs, "Value");
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
      boolean ret = validateExprNodeDesc(desc, "Select");
      if (!ret) {
        return false;
      }
    }
    return true;
  }

  private boolean validateFilterOperator(FilterOperator op) {
    ExprNodeDesc desc = op.getConf().getPredicate();
    return validateExprNodeDesc(desc, "Predicate", VectorExpressionDescriptor.Mode.FILTER);
  }

  private boolean validateGroupByOperator(GroupByOperator op, boolean isReduce, boolean isTezOrSpark) {
    GroupByDesc desc = op.getConf();

    if (desc.getMode() != GroupByDesc.Mode.HASH && desc.isDistinct()) {
      setOperatorIssue("DISTINCT not supported");
      return false;
    }
    boolean ret = validateExprNodeDesc(desc.getKeys(), "Key");
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
      LOG.info("Vectorized GROUPING SETS only expected for HASH and STREAMING processing modes");
      return false;
    }

    if (processingMode == ProcessingMode.MERGE_PARTIAL) {
      // For now, VectorGroupByOperator ProcessingModeReduceMergePartial cannot handle key
      // expressions.
      for (ExprNodeDesc keyExpr : desc.getKeys()) {
        if (!(keyExpr instanceof ExprNodeColumnDesc)) {
          setExpressionIssue("Key", "Non-column key expressions not supported for MERGEPARTIAL");
          return false;
        }
      }
    }

    Pair<Boolean,Boolean> retPair =
        validateAggregationDescs(desc.getAggregators(), processingMode, hasKeys);
    if (!retPair.left) {
      return false;
    }

    // If all the aggregation outputs are primitive, we can output VectorizedRowBatch.
    // Otherwise, we the rest of the operator tree will be row mode.
    VectorGroupByDesc vectorDesc = new VectorGroupByDesc();
    desc.setVectorDesc(vectorDesc);

    vectorDesc.setVectorOutput(retPair.right);

    vectorDesc.setProcessingMode(processingMode);

    LOG.info("Vector GROUP BY operator will use processing mode " + processingMode.name() +
        ", isVectorOutput " + vectorDesc.isVectorOutput());

    return true;
  }

  private boolean validateFileSinkOperator(FileSinkOperator op) {
   return true;
  }

  private boolean validateExprNodeDesc(List<ExprNodeDesc> descs, String expressionTitle) {
    return validateExprNodeDesc(descs, expressionTitle, VectorExpressionDescriptor.Mode.PROJECTION);
  }

  private boolean validateExprNodeDesc(List<ExprNodeDesc> descs,
          String expressionTitle,
          VectorExpressionDescriptor.Mode mode) {
    for (ExprNodeDesc d : descs) {
      boolean ret = validateExprNodeDesc(d, expressionTitle, mode);
      if (!ret) {
        return false;
      }
    }
    return true;
  }

  private Pair<Boolean,Boolean> validateAggregationDescs(List<AggregationDesc> descs,
      ProcessingMode processingMode, boolean hasKeys) {
    boolean outputIsPrimitive = true;
    for (AggregationDesc d : descs) {
      Pair<Boolean,Boolean>  retPair = validateAggregationDesc(d, processingMode, hasKeys);
      if (!retPair.left) {
        return retPair;
      }
      if (!retPair.right) {
        outputIsPrimitive = false;
      }
    }
    return new Pair<Boolean, Boolean>(true, outputIsPrimitive);
  }

  private boolean validateExprNodeDescRecursive(ExprNodeDesc desc, String expressionTitle,
      VectorExpressionDescriptor.Mode mode) {
    if (desc instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc c = (ExprNodeColumnDesc) desc;
      // Currently, we do not support vectorized virtual columns (see HIVE-5570).
      if (VirtualColumn.VIRTUAL_COLUMN_NAMES.contains(c.getColumn())) {
        setExpressionIssue(expressionTitle, "Virtual columns not supported (" + c.getColumn() + ")");
        return false;
      }
    }
    String typeName = desc.getTypeInfo().getTypeName();
    boolean ret = validateDataType(typeName, mode);
    if (!ret) {
      setExpressionIssue(expressionTitle, "Data type " + typeName + " of " + desc.toString() + " not supported");
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
        if (!validateStructInExpression(desc, expressionTitle, VectorExpressionDescriptor.Mode.FILTER)) {
          return false;
        }
      } else {
        for (ExprNodeDesc d : desc.getChildren()) {
          // Don't restrict child expressions for projection.
          // Always use loose FILTER mode.
          if (!validateExprNodeDescRecursive(d, expressionTitle, VectorExpressionDescriptor.Mode.FILTER)) {
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

      ArrayList<TypeInfo> fieldTypeInfos = structTypeInfo
          .getAllStructFieldTypeInfos();
      ArrayList<String> fieldNames = structTypeInfo.getAllStructFieldNames();
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
    return validateExprNodeDesc(desc, expressionTitle, VectorExpressionDescriptor.Mode.PROJECTION);
  }

  boolean validateExprNodeDesc(ExprNodeDesc desc, String expressionTitle,
      VectorExpressionDescriptor.Mode mode) {
    if (!validateExprNodeDescRecursive(desc, expressionTitle, mode)) {
      return false;
    }
    try {
      VectorizationContext vc = new ValidatorVectorizationContext(hiveConf);
      if (vc.getVectorExpression(desc, mode) == null) {
        // TODO: this cannot happen - VectorizationContext throws in such cases.
        setExpressionIssue(expressionTitle, "getVectorExpression returned null");
        return false;
      }
    } catch (Exception e) {
      if (e instanceof HiveException) {
        setExpressionIssue(expressionTitle, e.getMessage());
      } else {
        String issue = "exception: " + VectorizationContext.getStackTraceAsSingleLine(e);
        setExpressionIssue(expressionTitle, issue);
      }
      return false;
    }
    return true;
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

  public static ObjectInspector.Category aggregationOutputCategory(VectorAggregateExpression vectorAggrExpr) {
    ObjectInspector outputObjInspector = vectorAggrExpr.getOutputObjectInspector();
    return outputObjInspector.getCategory();
  }

  private Pair<Boolean,Boolean> validateAggregationDesc(AggregationDesc aggDesc, ProcessingMode processingMode,
      boolean hasKeys) {

    String udfName = aggDesc.getGenericUDAFName().toLowerCase();
    if (!supportedAggregationUdfs.contains(udfName)) {
      setExpressionIssue("Aggregation Function", "UDF " + udfName + " not supported");
      return new Pair<Boolean,Boolean>(false, false);
    }
    /*
    if (aggDesc.getDistinct()) {
      setExpressionIssue("Aggregation Function", "DISTINCT not supported");
      return new Pair<Boolean,Boolean>(false, false);
    }
    */
    if (aggDesc.getParameters() != null && !validateExprNodeDesc(aggDesc.getParameters(), "Aggregation Function UDF " + udfName + " parameter")) {
      return new Pair<Boolean,Boolean>(false, false);
    }

    // See if we can vectorize the aggregation.
    VectorizationContext vc = new ValidatorVectorizationContext(hiveConf);
    VectorAggregateExpression vectorAggrExpr;
    try {
        vectorAggrExpr = vc.getAggregatorExpression(aggDesc);
    } catch (Exception e) {
      // We should have already attempted to vectorize in validateAggregationDesc.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Vectorization of aggregation should have succeeded ", e);
      }
      setExpressionIssue("Aggregation Function", "Vectorization of aggreation should have succeeded " + e);
      return new Pair<Boolean,Boolean>(false, false);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Aggregation " + aggDesc.getExprString() + " --> " +
          " vector expression " + vectorAggrExpr.toString());
    }

    ObjectInspector.Category outputCategory = aggregationOutputCategory(vectorAggrExpr);
    boolean outputIsPrimitive = (outputCategory == ObjectInspector.Category.PRIMITIVE);
    if (processingMode == ProcessingMode.MERGE_PARTIAL &&
        hasKeys &&
        !outputIsPrimitive) {
      setOperatorIssue("Vectorized Reduce MergePartial GROUP BY keys can only handle aggregate outputs that are primitive types");
      return new Pair<Boolean,Boolean>(false, false);
    }

    return new Pair<Boolean,Boolean>(true, outputIsPrimitive);
  }

  public static boolean validateDataType(String type, VectorExpressionDescriptor.Mode mode) {
    type = type.toLowerCase();
    boolean result = supportedDataTypesPattern.matcher(type).matches();
    if (result && mode == VectorExpressionDescriptor.Mode.PROJECTION && type.equals("void")) {
      return false;
    }
    return result;
  }

  private VectorizationContext getVectorizationContext(String contextName,
      VectorTaskColumnInfo vectorTaskColumnInfo) {

    VectorizationContext vContext =
        new VectorizationContext(contextName, vectorTaskColumnInfo.allColumnNames, hiveConf);

    return vContext;
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
        VectorizationContext vContext, MapJoinDesc desc, VectorMapJoinInfo vectorMapJoinInfo)
            throws HiveException {
    Operator<? extends OperatorDesc> vectorOp = null;
    Class<? extends Operator<?>> opClass = null;

    VectorMapJoinDesc vectorDesc = (VectorMapJoinDesc) desc.getVectorDesc();

    HashTableImplementationType hashTableImplementationType = HashTableImplementationType.NONE;
    HashTableKind hashTableKind = HashTableKind.NONE;
    HashTableKeyType hashTableKeyType = HashTableKeyType.NONE;
    OperatorVariation operatorVariation = OperatorVariation.NONE;

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
        operatorVariation = OperatorVariation.INNER;
        hashTableKind = HashTableKind.HASH_MAP;
      } else {
        operatorVariation = OperatorVariation.INNER_BIG_ONLY;
        hashTableKind = HashTableKind.HASH_MULTISET;
      }
      break;
    case JoinDesc.LEFT_OUTER_JOIN:
    case JoinDesc.RIGHT_OUTER_JOIN:
      operatorVariation = OperatorVariation.OUTER;
      hashTableKind = HashTableKind.HASH_MAP;
      break;
    case JoinDesc.LEFT_SEMI_JOIN:
      operatorVariation = OperatorVariation.LEFT_SEMI;
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
    case LONG:
      switch (operatorVariation) {
      case INNER:
        opClass = VectorMapJoinInnerLongOperator.class;
        break;
      case INNER_BIG_ONLY:
        opClass = VectorMapJoinInnerBigOnlyLongOperator.class;
        break;
      case LEFT_SEMI:
        opClass = VectorMapJoinLeftSemiLongOperator.class;
        break;
      case OUTER:
        opClass = VectorMapJoinOuterLongOperator.class;
        break;
      default:
        throw new HiveException("Unknown operator variation " + operatorVariation);
      }
      break;
    case STRING:
      switch (operatorVariation) {
      case INNER:
        opClass = VectorMapJoinInnerStringOperator.class;
        break;
      case INNER_BIG_ONLY:
        opClass = VectorMapJoinInnerBigOnlyStringOperator.class;
        break;
      case LEFT_SEMI:
        opClass = VectorMapJoinLeftSemiStringOperator.class;
        break;
      case OUTER:
        opClass = VectorMapJoinOuterStringOperator.class;
        break;
      default:
        throw new HiveException("Unknown operator variation " + operatorVariation);
      }
      break;
    case MULTI_KEY:
      switch (operatorVariation) {
      case INNER:
        opClass = VectorMapJoinInnerMultiKeyOperator.class;
        break;
      case INNER_BIG_ONLY:
        opClass = VectorMapJoinInnerBigOnlyMultiKeyOperator.class;
        break;
      case LEFT_SEMI:
        opClass = VectorMapJoinLeftSemiMultiKeyOperator.class;
        break;
      case OUTER:
        opClass = VectorMapJoinOuterMultiKeyOperator.class;
        break;
      default:
        throw new HiveException("Unknown operator variation " + operatorVariation);
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
    vectorDesc.setOperatorVariation(operatorVariation);
    vectorDesc.setMinMaxEnabled(minMaxEnabled);
    vectorDesc.setVectorMapJoinInfo(vectorMapJoinInfo);

    vectorOp = OperatorFactory.getVectorOperator(
        opClass, op.getCompilationOpContext(), op.getConf(), vContext, op);
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
      boolean isTezOrSpark, VectorizationContext vContext, VectorMapJoinInfo vectorMapJoinInfo)
          throws HiveException {

    Preconditions.checkState(op instanceof MapJoinOperator);

    // Allocate a VectorReduceSinkDesc initially with implementation type NONE so EXPLAIN
    // can report this operator was vectorized, but not native.  And, the conditions.
    VectorMapJoinDesc vectorDesc = new VectorMapJoinDesc();
    desc.setVectorDesc(vectorDesc);

    boolean isVectorizationMapJoinNativeEnabled = HiveConf.getBoolVar(hiveConf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_ENABLED);

    String engine = HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);

    boolean oneMapJoinCondition = (desc.getConds().length == 1);

    boolean hasNullSafes = onExpressionHasNullSafes(desc);

    byte posBigTable = (byte) desc.getPosBigTable();

    // Since we want to display all the met and not met conditions in EXPLAIN, we determine all
    // information first....

    List<ExprNodeDesc> keyDesc = desc.getKeys().get(posBigTable);
    VectorExpression[] allBigTableKeyExpressions = vContext.getVectorExpressions(keyDesc);
    final int allBigTableKeyExpressionsLength = allBigTableKeyExpressions.length;
    boolean supportsKeyTypes = true;  // Assume.
    HashSet<String> notSupportedKeyTypes = new HashSet<String>();

    // Since a key expression can be a calculation and the key will go into a scratch column,
    // we need the mapping and type information.
    int[] bigTableKeyColumnMap = new int[allBigTableKeyExpressionsLength];
    String[] bigTableKeyColumnNames = new String[allBigTableKeyExpressionsLength];
    TypeInfo[] bigTableKeyTypeInfos = new TypeInfo[allBigTableKeyExpressionsLength];
    ArrayList<VectorExpression> bigTableKeyExpressionsList = new ArrayList<VectorExpression>();
    VectorExpression[] bigTableKeyExpressions;
    for (int i = 0; i < allBigTableKeyExpressionsLength; i++) {
      VectorExpression ve = allBigTableKeyExpressions[i];
      if (!IdentityExpression.isColumnOnly(ve)) {
        bigTableKeyExpressionsList.add(ve);
      }
      bigTableKeyColumnMap[i] = ve.getOutputColumn();

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
      bigTableKeyExpressions = null;
    } else {
      bigTableKeyExpressions = bigTableKeyExpressionsList.toArray(new VectorExpression[0]);
    }

    List<ExprNodeDesc> bigTableExprs = desc.getExprs().get(posBigTable);
    VectorExpression[] allBigTableValueExpressions = vContext.getVectorExpressions(bigTableExprs);

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
     */
    int[] bigTableValueColumnMap = new int[allBigTableValueExpressions.length];
    String[] bigTableValueColumnNames = new String[allBigTableValueExpressions.length];
    TypeInfo[] bigTableValueTypeInfos = new TypeInfo[allBigTableValueExpressions.length];
    ArrayList<VectorExpression> bigTableValueExpressionsList = new ArrayList<VectorExpression>();
    VectorExpression[] bigTableValueExpressions;
    for (int i = 0; i < bigTableValueColumnMap.length; i++) {
      VectorExpression ve = allBigTableValueExpressions[i];
      if (!IdentityExpression.isColumnOnly(ve)) {
        bigTableValueExpressionsList.add(ve);
      }
      bigTableValueColumnMap[i] = ve.getOutputColumn();

      ExprNodeDesc exprNode = bigTableExprs.get(i);
      bigTableValueColumnNames[i] = exprNode.toString();
      bigTableValueTypeInfos[i] = exprNode.getTypeInfo();
    }
    if (bigTableValueExpressionsList.size() == 0) {
      bigTableValueExpressions = null;
    } else {
      bigTableValueExpressions = bigTableValueExpressionsList.toArray(new VectorExpression[0]);
    }

    vectorMapJoinInfo.setBigTableKeyColumnMap(bigTableKeyColumnMap);
    vectorMapJoinInfo.setBigTableKeyColumnNames(bigTableKeyColumnNames);
    vectorMapJoinInfo.setBigTableKeyTypeInfos(bigTableKeyTypeInfos);
    vectorMapJoinInfo.setBigTableKeyExpressions(bigTableKeyExpressions);

    vectorMapJoinInfo.setBigTableValueColumnMap(bigTableValueColumnMap);
    vectorMapJoinInfo.setBigTableValueColumnNames(bigTableValueColumnNames);
    vectorMapJoinInfo.setBigTableValueTypeInfos(bigTableValueTypeInfos);
    vectorMapJoinInfo.setBigTableValueExpressions(bigTableValueExpressions);

    /*
     * Small table information.
     */
    VectorColumnOutputMapping bigTableRetainedMapping =
        new VectorColumnOutputMapping("Big Table Retained Mapping");

    VectorColumnOutputMapping bigTableOuterKeyMapping =
        new VectorColumnOutputMapping("Big Table Outer Key Mapping");

    // The order of the fields in the LazyBinary small table value must be used, so
    // we use the source ordering flavor for the mapping.
    VectorColumnSourceMapping smallTableMapping =
        new VectorColumnSourceMapping("Small Table Mapping");

    Byte[] order = desc.getTagOrder();
    Byte posSingleVectorMapJoinSmallTable = (order[0] == posBigTable ? order[1] : order[0]);
    boolean isOuterJoin = !desc.getNoOuterJoin();

    /*
     * Gather up big and small table output result information from the MapJoinDesc.
     */
    List<Integer> bigTableRetainList = desc.getRetainList().get(posBigTable);
    int bigTableRetainSize = bigTableRetainList.size();

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
    for (int i = 0; i < bigTableRetainSize; i++) {

      // Since bigTableValueExpressions may do a calculation and produce a scratch column, we
      // need to map to the right batch column.

      int retainColumn = bigTableRetainList.get(i);
      int batchColumnIndex = bigTableValueColumnMap[retainColumn];
      TypeInfo typeInfo = bigTableValueTypeInfos[i];

      // With this map we project the big table batch to make it look like an output batch.
      projectionMapping.add(nextOutputColumn, batchColumnIndex, typeInfo);

      // Collect columns we copy from the big table batch to the overflow batch.
      if (!bigTableRetainedMapping.containsOutputColumn(batchColumnIndex)) {
        // Tolerate repeated use of a big table column.
        bigTableRetainedMapping.add(batchColumnIndex, batchColumnIndex, typeInfo);
      }

      nextOutputColumn++;
    }

    /*
     * Now determine the small table results.
     */
    boolean smallTableExprVectorizes = true;

    int firstSmallTableOutputColumn;
    firstSmallTableOutputColumn = (order[0] == posBigTable ? bigTableRetainSize : 0);
    int smallTableOutputCount = 0;
    nextOutputColumn = firstSmallTableOutputColumn;

    // Small table indices has more information (i.e. keys) than retain, so use it if it exists...
    String[] bigTableRetainedNames;
    if (smallTableIndicesSize > 0) {
      smallTableOutputCount = smallTableIndicesSize;
      bigTableRetainedNames = new String[smallTableOutputCount];

      for (int i = 0; i < smallTableIndicesSize; i++) {
        if (smallTableIndices[i] >= 0) {

          // Zero and above numbers indicate a big table key is needed for
          // small table result "area".

          int keyIndex = smallTableIndices[i];

          // Since bigTableKeyExpressions may do a calculation and produce a scratch column, we
          // need to map the right column.
          int batchKeyColumn = bigTableKeyColumnMap[keyIndex];
          bigTableRetainedNames[i] = bigTableKeyColumnNames[keyIndex];
          TypeInfo typeInfo = bigTableKeyTypeInfos[keyIndex];

          if (!isOuterJoin) {

            // Optimize inner join keys of small table results.

            // Project the big table key into the small table result "area".
            projectionMapping.add(nextOutputColumn, batchKeyColumn, typeInfo);

            if (!bigTableRetainedMapping.containsOutputColumn(batchKeyColumn)) {
              // If necessary, copy the big table key into the overflow batch's small table
              // result "area".
              bigTableRetainedMapping.add(batchKeyColumn, batchKeyColumn, typeInfo);
            }
          } else {

            // For outer joins, since the small table key can be null when there is no match,
            // we must have a physical (scratch) column for those keys.  We cannot use the
            // projection optimization used by inner joins above.

            int scratchColumn = vContext.allocateScratchColumn(typeInfo);
            projectionMapping.add(nextOutputColumn, scratchColumn, typeInfo);

            bigTableRetainedMapping.add(batchKeyColumn, scratchColumn, typeInfo);

            bigTableOuterKeyMapping.add(batchKeyColumn, scratchColumn, typeInfo);
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

          bigTableRetainedNames[i] = smallTableExprNode.toString();

          TypeInfo typeInfo = smallTableExprNode.getTypeInfo();

          // Make a new big table scratch column for the small table value.
          int scratchColumn = vContext.allocateScratchColumn(typeInfo);
          projectionMapping.add(nextOutputColumn, scratchColumn, typeInfo);

          smallTableMapping.add(smallTableValueIndex, scratchColumn, typeInfo);
        }
        nextOutputColumn++;
      }
    } else if (smallTableRetainSize > 0) {
      smallTableOutputCount = smallTableRetainSize;
      bigTableRetainedNames = new String[smallTableOutputCount];

      // Only small table values appear in join output result.

      for (int i = 0; i < smallTableRetainSize; i++) {
        int smallTableValueIndex = smallTableRetainList.get(i);

        ExprNodeDesc smallTableExprNode = smallTableExprs.get(i);
        if (!validateExprNodeDesc(smallTableExprNode, "Small Table")) {
          clearNotVectorizedReason();
          smallTableExprVectorizes = false;
        }

        bigTableRetainedNames[i] = smallTableExprNode.toString();

        // Make a new big table scratch column for the small table value.
        TypeInfo typeInfo = smallTableExprNode.getTypeInfo();
        int scratchColumn = vContext.allocateScratchColumn(typeInfo);

        projectionMapping.add(nextOutputColumn, scratchColumn, typeInfo);

        smallTableMapping.add(smallTableValueIndex, scratchColumn, typeInfo);
        nextOutputColumn++;
      }
    } else {
      bigTableRetainedNames = new String[0];
    }

    boolean useOptimizedTable =
        HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEMAPJOINUSEOPTIMIZEDTABLE);

    // Remember the condition variables for EXPLAIN regardless of whether we specialize or not.
    vectorDesc.setUseOptimizedTable(useOptimizedTable);
    vectorDesc.setIsVectorizationMapJoinNativeEnabled(isVectorizationMapJoinNativeEnabled);
    vectorDesc.setEngine(engine);
    vectorDesc.setOneMapJoinCondition(oneMapJoinCondition);
    vectorDesc.setHasNullSafes(hasNullSafes);
    vectorDesc.setSmallTableExprVectorizes(smallTableExprVectorizes);

    vectorDesc.setIsFastHashTableEnabled(isFastHashTableEnabled);
    vectorDesc.setIsHybridHashJoin(isHybridHashJoin);

    vectorDesc.setSupportsKeyTypes(supportsKeyTypes);
    if (!supportsKeyTypes) {
      vectorDesc.setNotSupportedKeyTypes(new ArrayList(notSupportedKeyTypes));
    }

    // Check common conditions for both Optimized and Fast Hash Tables.
    boolean result = true;    // Assume.
    if (!useOptimizedTable ||
        !isVectorizationMapJoinNativeEnabled ||
        !isTezOrSpark ||
        !oneMapJoinCondition ||
        hasNullSafes ||
        !smallTableExprVectorizes) {
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

    bigTableRetainedMapping.finalize();

    bigTableOuterKeyMapping.finalize();

    smallTableMapping.finalize();

    vectorMapJoinInfo.setBigTableRetainedMapping(bigTableRetainedMapping);
    vectorMapJoinInfo.setBigTableOuterKeyMapping(bigTableOuterKeyMapping);
    vectorMapJoinInfo.setSmallTableMapping(smallTableMapping);

    projectionMapping.finalize();

    // Verify we added an entry for each output.
    assert projectionMapping.isSourceSequenceGood();

    vectorMapJoinInfo.setProjectionMapping(projectionMapping);

    return result;
  }

  private Operator<? extends OperatorDesc> specializeReduceSinkOperator(
      Operator<? extends OperatorDesc> op, VectorizationContext vContext, ReduceSinkDesc desc,
      VectorReduceSinkInfo vectorReduceSinkInfo) throws HiveException {

    Type[] reduceSinkKeyColumnVectorTypes = vectorReduceSinkInfo.getReduceSinkKeyColumnVectorTypes();

    // By default, we can always use the multi-key class.
    VectorReduceSinkDesc.ReduceSinkKeyType reduceSinkKeyType = VectorReduceSinkDesc.ReduceSinkKeyType.MULTI_KEY;

    // Look for single column optimization.
    if (reduceSinkKeyColumnVectorTypes.length == 1) {
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
          case LONG:
            reduceSinkKeyType = VectorReduceSinkDesc.ReduceSinkKeyType.LONG;
            break;
          default:
            // Other integer types not supported yet.
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
    } else {
      opClass = VectorReduceSinkObjectHashOperator.class;
    }

    VectorReduceSinkDesc vectorDesc = (VectorReduceSinkDesc) desc.getVectorDesc();

    vectorDesc.setReduceSinkKeyType(reduceSinkKeyType);
    vectorDesc.setVectorReduceSinkInfo(vectorReduceSinkInfo);

    LOG.info("Vectorizer vectorizeOperator reduce sink class " + opClass.getSimpleName());

    Operator<? extends OperatorDesc> vectorOp = null;
    try {
      vectorOp = OperatorFactory.getVectorOperator(
          opClass, op.getCompilationOpContext(), op.getConf(), vContext, op);
    } catch (Exception e) {
      LOG.info("Vectorizer vectorizeOperator reduce sink class exception " + opClass.getSimpleName() +
          " exception " + e);
      throw new HiveException(e);
    }

    return vectorOp;
  }

  private boolean canSpecializeReduceSink(ReduceSinkDesc desc,
      boolean isTezOrSpark, VectorizationContext vContext,
      VectorReduceSinkInfo vectorReduceSinkInfo) throws HiveException {

    // Allocate a VectorReduceSinkDesc initially with key type NONE so EXPLAIN can report this
    // operator was vectorized, but not native.  And, the conditions.
    VectorReduceSinkDesc vectorDesc = new VectorReduceSinkDesc();
    desc.setVectorDesc(vectorDesc);

    // Various restrictions.

    // Set this if we encounter a condition we were not expecting.
    boolean isUnexpectedCondition = false;

    boolean isVectorizationReduceSinkNativeEnabled =
        HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_REDUCESINK_NEW_ENABLED);

    String engine = HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);

    boolean hasTopN = (desc.getTopN() >= 0);

    boolean hasDistinctColumns = (desc.getDistinctColumnIndices().size() > 0);

    TableDesc keyTableDesc = desc.getKeySerializeInfo();
    Class<? extends Deserializer> keySerializerClass = keyTableDesc.getDeserializerClass();
    boolean isKeyBinarySortable = (keySerializerClass == org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe.class);

    TableDesc valueTableDesc = desc.getValueSerializeInfo();
    Class<? extends Deserializer> valueDeserializerClass = valueTableDesc.getDeserializerClass();
    boolean isValueLazyBinary = (valueDeserializerClass == org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe.class);

    // We are doing work here we'd normally do in VectorGroupByCommonOperator's constructor.
    // So if we later decide not to specialize, we'll just waste any scratch columns allocated...

    List<ExprNodeDesc> keysDescs = desc.getKeyCols();
    VectorExpression[] allKeyExpressions = vContext.getVectorExpressions(keysDescs);

    // Since a key expression can be a calculation and the key will go into a scratch column,
    // we need the mapping and type information.
    int[] reduceSinkKeyColumnMap = new int[allKeyExpressions.length];
    TypeInfo[] reduceSinkKeyTypeInfos = new TypeInfo[allKeyExpressions.length];
    Type[] reduceSinkKeyColumnVectorTypes = new Type[allKeyExpressions.length];
    ArrayList<VectorExpression> groupByKeyExpressionsList = new ArrayList<VectorExpression>();
    VectorExpression[] reduceSinkKeyExpressions;
    for (int i = 0; i < reduceSinkKeyColumnMap.length; i++) {
      VectorExpression ve = allKeyExpressions[i];
      reduceSinkKeyColumnMap[i] = ve.getOutputColumn();
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

    ArrayList<ExprNodeDesc> valueDescs = desc.getValueCols();
    VectorExpression[] allValueExpressions = vContext.getVectorExpressions(valueDescs);

    int[] reduceSinkValueColumnMap = new int[valueDescs.size()];
    TypeInfo[] reduceSinkValueTypeInfos = new TypeInfo[valueDescs.size()];
    Type[] reduceSinkValueColumnVectorTypes = new Type[valueDescs.size()];
    ArrayList<VectorExpression> reduceSinkValueExpressionsList = new ArrayList<VectorExpression>();
    VectorExpression[] reduceSinkValueExpressions;
    for (int i = 0; i < valueDescs.size(); ++i) {
      VectorExpression ve = allValueExpressions[i];
      reduceSinkValueColumnMap[i] = ve.getOutputColumn();
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

    vectorReduceSinkInfo.setReduceSinkKeyColumnMap(reduceSinkKeyColumnMap);
    vectorReduceSinkInfo.setReduceSinkKeyTypeInfos(reduceSinkKeyTypeInfos);
    vectorReduceSinkInfo.setReduceSinkKeyColumnVectorTypes(reduceSinkKeyColumnVectorTypes);
    vectorReduceSinkInfo.setReduceSinkKeyExpressions(reduceSinkKeyExpressions);

    vectorReduceSinkInfo.setReduceSinkValueColumnMap(reduceSinkValueColumnMap);
    vectorReduceSinkInfo.setReduceSinkValueTypeInfos(reduceSinkValueTypeInfos);
    vectorReduceSinkInfo.setReduceSinkValueColumnVectorTypes(reduceSinkValueColumnVectorTypes);
    vectorReduceSinkInfo.setReduceSinkValueExpressions(reduceSinkValueExpressions);

    boolean useUniformHash = desc.getReducerTraits().contains(UNIFORM);
    vectorReduceSinkInfo.setUseUniformHash(useUniformHash);

    boolean hasEmptyBuckets = false;
    boolean hasNoPartitions = false;
    if (useUniformHash) {

      // Check for unexpected conditions...
      hasEmptyBuckets =
          (desc.getBucketCols() != null && !desc.getBucketCols().isEmpty()) ||
          (desc.getPartitionCols().size() == 0);

      if (hasEmptyBuckets) {
        LOG.info("Unexpected condition: UNIFORM hash and empty buckets");
        isUnexpectedCondition = true;
      }

      hasNoPartitions = (desc.getPartitionCols() == null);

      if (hasNoPartitions) {
        LOG.info("Unexpected condition: UNIFORM hash and no partitions");
        isUnexpectedCondition = true;
      }

    } else {

      // Collect bucket and/or partition information for object hashing.

      int[] reduceSinkBucketColumnMap = null;
      TypeInfo[] reduceSinkBucketTypeInfos = null;
      Type[] reduceSinkBucketColumnVectorTypes = null;
      VectorExpression[] reduceSinkBucketExpressions = null;

      List<ExprNodeDesc> bucketDescs = desc.getBucketCols();
      if (bucketDescs != null) {
        VectorExpression[] allBucketExpressions = vContext.getVectorExpressions(bucketDescs);
    
        reduceSinkBucketColumnMap = new int[bucketDescs.size()];
        reduceSinkBucketTypeInfos = new TypeInfo[bucketDescs.size()];
        reduceSinkBucketColumnVectorTypes = new Type[bucketDescs.size()];
        ArrayList<VectorExpression> reduceSinkBucketExpressionsList = new ArrayList<VectorExpression>();
        for (int i = 0; i < bucketDescs.size(); ++i) {
          VectorExpression ve = allBucketExpressions[i];
          reduceSinkBucketColumnMap[i] = ve.getOutputColumn();
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

      List<ExprNodeDesc> partitionDescs = desc.getPartitionCols();
      if (partitionDescs != null) {
        VectorExpression[] allPartitionExpressions = vContext.getVectorExpressions(partitionDescs);
    
        reduceSinkPartitionColumnMap = new int[partitionDescs.size()];
        reduceSinkPartitionTypeInfos = new TypeInfo[partitionDescs.size()];
        reduceSinkPartitionColumnVectorTypes = new Type[partitionDescs.size()];
        ArrayList<VectorExpression> reduceSinkPartitionExpressionsList = new ArrayList<VectorExpression>();
        for (int i = 0; i < partitionDescs.size(); ++i) {
          VectorExpression ve = allPartitionExpressions[i];
          reduceSinkPartitionColumnMap[i] = ve.getOutputColumn();
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
    vectorDesc.setIsVectorizationReduceSinkNativeEnabled(isVectorizationReduceSinkNativeEnabled);
    vectorDesc.setEngine(engine);
    vectorDesc.setHasTopN(hasTopN);
    vectorDesc.setHasDistinctColumns(hasDistinctColumns);
    vectorDesc.setIsKeyBinarySortable(isKeyBinarySortable);
    vectorDesc.setIsValueLazyBinary(isValueLazyBinary);

    // This indicates we logged an inconsistency (from our point-of-view) and will not make this
    // operator native...
    vectorDesc.setIsUnexpectedCondition(isUnexpectedCondition);

    // Many restrictions.
    if (!isVectorizationReduceSinkNativeEnabled ||
        !isTezOrSpark ||
        (useUniformHash && (hasEmptyBuckets || hasNoPartitions)) ||
        hasTopN ||
        hasDistinctColumns ||
        !isKeyBinarySortable ||
        !isValueLazyBinary ||
        isUnexpectedCondition) {
      return false;
    }

    return true;
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

  public static Operator<? extends OperatorDesc> vectorizeTableScanOperator(
      Operator<? extends OperatorDesc> tableScanOp, VectorizationContext vContext)
          throws HiveException {
    TableScanDesc tableScanDesc = (TableScanDesc) tableScanOp.getConf();
    VectorTableScanDesc vectorTableScanDesc = new VectorTableScanDesc();
    tableScanDesc.setVectorDesc(vectorTableScanDesc);
    vectorTableScanDesc.setProjectedOutputColumns(
        ArrayUtils.toPrimitive(vContext.getProjectedColumns().toArray(new Integer[0])));
    return tableScanOp;
  }

  public static Operator<? extends OperatorDesc> vectorizeFilterOperator(
      Operator<? extends OperatorDesc> filterOp, VectorizationContext vContext)
          throws HiveException {
    FilterDesc filterDesc = (FilterDesc) filterOp.getConf();
    VectorFilterDesc vectorFilterDesc = new VectorFilterDesc();
    filterDesc.setVectorDesc(vectorFilterDesc);
    ExprNodeDesc predicateExpr = filterDesc.getPredicate();
    VectorExpression vectorPredicateExpr =
        vContext.getVectorExpression(predicateExpr, VectorExpressionDescriptor.Mode.FILTER);
    vectorFilterDesc.setPredicateExpression(vectorPredicateExpr);
    return OperatorFactory.getVectorOperator(
        filterOp.getCompilationOpContext(), filterDesc, vContext, filterOp);
  }

  /*
   * NOTE: The VectorGroupByDesc has already been allocated and partially populated.
   */
  public static Operator<? extends OperatorDesc> vectorizeGroupByOperator(
      Operator<? extends OperatorDesc> groupByOp, VectorizationContext vContext)
          throws HiveException {
    GroupByDesc groupByDesc = (GroupByDesc) groupByOp.getConf();
    List<ExprNodeDesc> keysDesc = groupByDesc.getKeys();
    VectorExpression[] vecKeyExpressions = vContext.getVectorExpressions(keysDesc);
    ArrayList<AggregationDesc> aggrDesc = groupByDesc.getAggregators();
    final int size = aggrDesc.size();
    VectorAggregateExpression[] vecAggregators = new VectorAggregateExpression[size];
    int[] projectedOutputColumns = new int[size];
    for (int i = 0; i < size; ++i) {
      AggregationDesc aggDesc = aggrDesc.get(i);
      vecAggregators[i] = vContext.getAggregatorExpression(aggDesc);

      // GroupBy generates a new vectorized row batch...
      projectedOutputColumns[i] = i;
    }
    VectorGroupByDesc vectorGroupByDesc = (VectorGroupByDesc) groupByDesc.getVectorDesc();
    vectorGroupByDesc.setKeyExpressions(vecKeyExpressions);
    vectorGroupByDesc.setAggregators(vecAggregators);
    vectorGroupByDesc.setProjectedOutputColumns(projectedOutputColumns);
    return OperatorFactory.getVectorOperator(
        groupByOp.getCompilationOpContext(), groupByDesc, vContext, groupByOp);
  }

  public static Operator<? extends OperatorDesc> vectorizeSelectOperator(
      Operator<? extends OperatorDesc> selectOp, VectorizationContext vContext)
          throws HiveException {
    SelectDesc selectDesc = (SelectDesc) selectOp.getConf();
    VectorSelectDesc vectorSelectDesc = new VectorSelectDesc();
    selectDesc.setVectorDesc(vectorSelectDesc);
    List<ExprNodeDesc> colList = selectDesc.getColList();
    int index = 0;
    final int size = colList.size();
    VectorExpression[] vectorSelectExprs = new VectorExpression[size];
    int[] projectedOutputColumns = new int[size];
    for (int i = 0; i < size; i++) {
      ExprNodeDesc expr = colList.get(i);
      VectorExpression ve = vContext.getVectorExpression(expr);
      projectedOutputColumns[i] = ve.getOutputColumn();
      if (ve instanceof IdentityExpression) {
        // Suppress useless evaluation.
        continue;
      }
      vectorSelectExprs[index++] = ve;
    }
    if (index < size) {
      vectorSelectExprs = Arrays.copyOf(vectorSelectExprs, index);
    }
    vectorSelectDesc.setSelectExpressions(vectorSelectExprs);
    vectorSelectDesc.setProjectedOutputColumns(projectedOutputColumns);
    return OperatorFactory.getVectorOperator(
        selectOp.getCompilationOpContext(), selectDesc, vContext, selectOp);
  }

  public Operator<? extends OperatorDesc> vectorizeOperator(Operator<? extends OperatorDesc> op,
      VectorizationContext vContext, boolean isTezOrSpark, VectorTaskColumnInfo vectorTaskColumnInfo)
          throws HiveException {
    Operator<? extends OperatorDesc> vectorOp = null;

    boolean isNative;
    switch (op.getType()) {
      case TABLESCAN:
        vectorOp = vectorizeTableScanOperator(op, vContext);
        isNative = true;
        break;
      case MAPJOIN:
        {
          if (op instanceof MapJoinOperator) {
            VectorMapJoinInfo vectorMapJoinInfo = new VectorMapJoinInfo();
            MapJoinDesc desc = (MapJoinDesc) op.getConf();
            boolean specialize = canSpecializeMapJoin(op, desc, isTezOrSpark, vContext, vectorMapJoinInfo);
  
            if (!specialize) {
  
              Class<? extends Operator<?>> opClass = null;
  
              // *NON-NATIVE* vector map differences for LEFT OUTER JOIN and Filtered...

              List<ExprNodeDesc> bigTableFilters = desc.getFilters().get((byte) desc.getPosBigTable());
              boolean isOuterAndFiltered = (!desc.isNoOuterJoin() && bigTableFilters.size() > 0);
              if (!isOuterAndFiltered) {
                opClass = VectorMapJoinOperator.class;
              } else {
                opClass = VectorMapJoinOuterFilteredOperator.class;
              }
  
              vectorOp = OperatorFactory.getVectorOperator(
                  opClass, op.getCompilationOpContext(), op.getConf(), vContext, op);
              isNative = false;
            } else {
  
              // TEMPORARY Until Native Vector Map Join with Hybrid passes tests...
              // HiveConf.setBoolVar(physicalContext.getConf(),
              //    HiveConf.ConfVars.HIVEUSEHYBRIDGRACEHASHJOIN, false);
  
              vectorOp = specializeMapJoinOperator(op, vContext, desc, vectorMapJoinInfo);
              isNative = true;
  
              if (vectorTaskColumnInfo != null) {
                if (usesVectorUDFAdaptor(vectorMapJoinInfo.getBigTableKeyExpressions())) {
                  vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
                }
                if (usesVectorUDFAdaptor(vectorMapJoinInfo.getBigTableValueExpressions())) {
                  vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
                }
              }
            }
          } else {
            Preconditions.checkState(op instanceof SMBMapJoinOperator);
            SMBJoinDesc smbJoinSinkDesc = (SMBJoinDesc) op.getConf();
            VectorSMBJoinDesc vectorSMBJoinDesc = new VectorSMBJoinDesc();
            smbJoinSinkDesc.setVectorDesc(vectorSMBJoinDesc);
            vectorOp = OperatorFactory.getVectorOperator(
                op.getCompilationOpContext(), smbJoinSinkDesc, vContext, op);
            isNative = false;
          }
        }
        break;

      case REDUCESINK:
        {
          VectorReduceSinkInfo vectorReduceSinkInfo = new VectorReduceSinkInfo();
          ReduceSinkDesc desc = (ReduceSinkDesc) op.getConf();
          boolean specialize = canSpecializeReduceSink(desc, isTezOrSpark, vContext, vectorReduceSinkInfo);

          if (!specialize) {

            vectorOp = OperatorFactory.getVectorOperator(
                op.getCompilationOpContext(), op.getConf(), vContext, op);
            isNative = false;
          } else {

            vectorOp = specializeReduceSinkOperator(op, vContext, desc, vectorReduceSinkInfo);
            isNative = true;

            if (vectorTaskColumnInfo != null) {
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
          vectorOp = vectorizeFilterOperator(op, vContext);
          isNative = true;
          if (vectorTaskColumnInfo != null) {
            VectorFilterDesc vectorFilterDesc =
                (VectorFilterDesc) ((AbstractOperatorDesc) vectorOp.getConf()).getVectorDesc();
            VectorExpression vectorPredicateExpr = vectorFilterDesc.getPredicateExpression();
            if (usesVectorUDFAdaptor(vectorPredicateExpr)) {
              vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
            }
          }
        }
        break;
      case SELECT:
        {
          vectorOp = vectorizeSelectOperator(op, vContext);
          isNative = true;
          if (vectorTaskColumnInfo != null) {
            VectorSelectDesc vectorSelectDesc =
                (VectorSelectDesc) ((AbstractOperatorDesc) vectorOp.getConf()).getVectorDesc();
            VectorExpression[] vectorSelectExprs = vectorSelectDesc.getSelectExpressions();
            if (usesVectorUDFAdaptor(vectorSelectExprs)) {
              vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
            }
          }
        }
        break;
      case GROUPBY:
        {
          vectorOp = vectorizeGroupByOperator(op, vContext);
          isNative = false;
          if (vectorTaskColumnInfo != null) {
            VectorGroupByDesc vectorGroupByDesc =
                (VectorGroupByDesc) ((AbstractOperatorDesc) vectorOp.getConf()).getVectorDesc();
            if (!vectorGroupByDesc.isVectorOutput()) {
              vectorTaskColumnInfo.setGroupByVectorOutput(false);
            }
            VectorExpression[] vecKeyExpressions = vectorGroupByDesc.getKeyExpressions();
            if (usesVectorUDFAdaptor(vecKeyExpressions)) {
              vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
            }
            VectorAggregateExpression[] vecAggregators = vectorGroupByDesc.getAggregators();
            for (VectorAggregateExpression vecAggr : vecAggregators) {
              if (usesVectorUDFAdaptor(vecAggr.inputExpression())) {
                vectorTaskColumnInfo.setUsesVectorUDFAdaptor(true);
              }
            }
          }

        }
        break;
      case FILESINK:
        {
          FileSinkDesc fileSinkDesc = (FileSinkDesc) op.getConf();
          VectorFileSinkDesc vectorFileSinkDesc = new VectorFileSinkDesc();
          fileSinkDesc.setVectorDesc(vectorFileSinkDesc);
          vectorOp = OperatorFactory.getVectorOperator(
              op.getCompilationOpContext(), fileSinkDesc, vContext, op);
          isNative = false;
        }
        break;
      case LIMIT:
        {
          LimitDesc limitDesc = (LimitDesc) op.getConf();
          VectorLimitDesc vectorLimitDesc = new VectorLimitDesc();
          limitDesc.setVectorDesc(vectorLimitDesc);
          vectorOp = OperatorFactory.getVectorOperator(
              op.getCompilationOpContext(), limitDesc, vContext, op);
          isNative = true;
        }
        break;
      case EVENT:
        {
          AppMasterEventDesc eventDesc = (AppMasterEventDesc) op.getConf();
          VectorAppMasterEventDesc vectorEventDesc = new VectorAppMasterEventDesc();
          eventDesc.setVectorDesc(vectorEventDesc);
          vectorOp = OperatorFactory.getVectorOperator(
              op.getCompilationOpContext(), eventDesc, vContext, op);
          isNative = true;
        }
        break;
      case HASHTABLESINK:
        {
          SparkHashTableSinkDesc sparkHashTableSinkDesc = (SparkHashTableSinkDesc) op.getConf();
          VectorSparkHashTableSinkDesc vectorSparkHashTableSinkDesc = new VectorSparkHashTableSinkDesc();
          sparkHashTableSinkDesc.setVectorDesc(vectorSparkHashTableSinkDesc);
          vectorOp = OperatorFactory.getVectorOperator(
              op.getCompilationOpContext(), sparkHashTableSinkDesc, vContext, op);
          isNative = true;
        }
        break;
      case SPARKPRUNINGSINK:
        {
          SparkPartitionPruningSinkDesc sparkPartitionPruningSinkDesc = (SparkPartitionPruningSinkDesc) op.getConf();
          VectorSparkPartitionPruningSinkDesc vectorSparkPartitionPruningSinkDesc = new VectorSparkPartitionPruningSinkDesc();
          sparkPartitionPruningSinkDesc.setVectorDesc(vectorSparkPartitionPruningSinkDesc);
          vectorOp = OperatorFactory.getVectorOperator(
              op.getCompilationOpContext(), sparkPartitionPruningSinkDesc, vContext, op);
          isNative = true;
        }
        break;
      default:
        // These are children of GROUP BY operators with non-vector outputs.
        isNative = false;
        vectorOp = op;
        break;
    }
    Preconditions.checkState(vectorOp != null);
    if (vectorTaskColumnInfo != null && !isNative) {
      vectorTaskColumnInfo.setAllNative(false);
    }

    LOG.debug("vectorizeOperator " + vectorOp.getClass().getName());
    LOG.debug("vectorizeOperator " + vectorOp.getConf().getClass().getName());

    if (vectorOp != op) {
      fixupParentChildOperators(op, vectorOp);
      ((AbstractOperatorDesc) vectorOp.getConf()).setVectorMode(true);
    }
    return vectorOp;
  }

  private boolean isVirtualColumn(ColumnInfo column) {

    // Not using method column.getIsVirtualCol() because partitioning columns are also
    // treated as virtual columns in ColumnInfo.
    if (VirtualColumn.VIRTUAL_COLUMN_NAMES.contains(column.getInternalName())) {
        return true;
    }
    return false;
  }

  public void debugDisplayAllMaps(BaseWork work) {

    VectorizedRowBatchCtx vectorizedRowBatchCtx = work.getVectorizedRowBatchCtx();

    String[] allColumnNames = vectorizedRowBatchCtx.getRowColumnNames();
    Object columnTypeInfos = vectorizedRowBatchCtx.getRowColumnTypeInfos();
    int partitionColumnCount = vectorizedRowBatchCtx.getPartitionColumnCount();
    String[] scratchColumnTypeNames =vectorizedRowBatchCtx.getScratchColumnTypeNames();

    LOG.debug("debugDisplayAllMaps allColumnNames " + Arrays.toString(allColumnNames));
    LOG.debug("debugDisplayAllMaps columnTypeInfos " + Arrays.deepToString((Object[]) columnTypeInfos));
    LOG.debug("debugDisplayAllMaps partitionColumnCount " + partitionColumnCount);
    LOG.debug("debugDisplayAllMaps scratchColumnTypeNames " + Arrays.toString(scratchColumnTypeNames));
  }
}
