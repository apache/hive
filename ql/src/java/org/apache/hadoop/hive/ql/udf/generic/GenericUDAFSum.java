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
package org.apache.hadoop.hive.ql.udf.generic;

import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedUDAFs;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.*;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.ptf.BasePartitionEvaluator;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorObject;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GenericUDAFSum.
 *
 */
@Description(name = "sum", value = "_FUNC_(x) - Returns the sum of a set of numbers")
public class GenericUDAFSum extends AbstractGenericUDAFResolver {

  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFSum.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
              + parameters[0].getTypeName() + " is passed.");
    }
    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return new GenericUDAFSumLong();
    case TIMESTAMP:
    case FLOAT:
    case DOUBLE:
    case STRING:
    case VARCHAR:
    case CHAR:
      return new GenericUDAFSumDouble();
    case DECIMAL:
      return new GenericUDAFSumHiveDecimal();
    case BOOLEAN:
    case DATE:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string type arguments are accepted but "
              + parameters[0].getTypeName() + " is passed.");
    }
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
      throws SemanticException {
    TypeInfo[] parameters = info.getParameters();

    GenericUDAFSumEvaluator eval = (GenericUDAFSumEvaluator) getEvaluator(parameters);
    eval.setWindowing(info.isWindowing());
    eval.setSumDistinct(info.isDistinct());

    return eval;
  }

  public static PrimitiveObjectInspector.PrimitiveCategory getReturnType(TypeInfo type) {
    if (type.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      return null;
    }
    switch (((PrimitiveTypeInfo) type).getPrimitiveCategory()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return PrimitiveObjectInspector.PrimitiveCategory.LONG;
      case TIMESTAMP:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case VARCHAR:
      case CHAR:
        return PrimitiveObjectInspector.PrimitiveCategory.DOUBLE;
      case DECIMAL:
        return PrimitiveObjectInspector.PrimitiveCategory.DECIMAL;
    }
    return null;
  }

  /**
   * The base type for sum operator evaluator
   *
   */
  public static abstract class GenericUDAFSumEvaluator<ResultType extends Writable> extends GenericUDAFEvaluator {
    static abstract class SumAgg<T> extends AbstractAggregationBuffer {
      boolean empty;
      T sum;
      HashSet<ObjectInspectorObject> uniqueObjects; // Unique rows.
    }

    protected PrimitiveObjectInspector inputOI;
    protected PrimitiveObjectInspector outputOI;
    protected ResultType result;
    protected boolean isWindowing;
    protected boolean sumDistinct;

    public void setWindowing(boolean isWindowing) {
      this.isWindowing = isWindowing;
    }

    public void setSumDistinct(boolean sumDistinct) {
      this.sumDistinct = sumDistinct;
    }

    public boolean isWindowingDistinct() {
      return isWindowing && sumDistinct;
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      if (isWindowingDistinct()) {
        throw new HiveException("Distinct windowing UDAF doesn't support merge and terminatePartial");
      } else {
        return terminate(agg);
      }
    }

    /**
     * Check if the input object is eligible to contribute to the sum. If it's null
     * or the same value as the previous one for the case of SUM(DISTINCT). Then
     * skip it.
     * @param input the input object
     * @return True if sumDistinct is false or the non-null input is different from the previous object
     */
    protected boolean isEligibleValue(SumAgg agg, Object input) {
      if (input == null) {
        return false;
      }

      if (isWindowingDistinct()) {
        if (agg.uniqueObjects == null) {
          agg.uniqueObjects = new HashSet<ObjectInspectorObject>();
        }
        HashSet<ObjectInspectorObject> uniqueObjs = agg.uniqueObjects;
        ObjectInspectorObject obj = input instanceof ObjectInspectorObject ?
            (ObjectInspectorObject)input :
            new ObjectInspectorObject(
            ObjectInspectorUtils.copyToStandardObject(input, inputOI, ObjectInspectorCopyOption.JAVA),
            outputOI);
        if (!uniqueObjs.contains(obj)) {
          uniqueObjs.add(obj);
          return true;
        }

        return false;
      }

      return true;
    }
  }

  /**
   * GenericUDAFSumHiveDecimal.
   *
   */
  @VectorizedUDAFs({
      VectorUDAFSumDecimal.class,
      VectorUDAFSumDecimal64.class,
      VectorUDAFSumDecimal64ToDecimal.class})
  public static class GenericUDAFSumHiveDecimal extends GenericUDAFSumEvaluator<HiveDecimalWritable> {

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);
      result = new HiveDecimalWritable(0);
      inputOI = (PrimitiveObjectInspector) parameters[0];

      final DecimalTypeInfo outputTypeInfo;
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        outputTypeInfo = getOutputDecimalTypeInfoForSum(inputOI.precision(), inputOI.scale(), mode);
      } else {
        // No change.
        outputTypeInfo = (DecimalTypeInfo) inputOI.getTypeInfo();
      }

      ObjectInspector oi = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(outputTypeInfo);
      outputOI = (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(
          oi, ObjectInspectorCopyOption.JAVA);

      return oi;
    }

    public static DecimalTypeInfo getOutputDecimalTypeInfoForSum(final int inputPrecision,
        int inputScale, Mode mode) {

      // The output precision is 10 greater than the input which should cover at least
      // 10b rows. The scale is the same as the input.
      DecimalTypeInfo outputTypeInfo = null;
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        int precision = Math.min(HiveDecimal.MAX_PRECISION, inputPrecision + 10);
        outputTypeInfo = TypeInfoFactory.getDecimalTypeInfo(precision, inputScale);
      } else {
        outputTypeInfo = TypeInfoFactory.getDecimalTypeInfo(inputPrecision, inputScale);
      }
      return outputTypeInfo;
    }

    /** class for storing decimal sum value. */
    @AggregationType(estimable = false) // hard to know exactly for decimals
    static class SumHiveDecimalWritableAgg extends SumAgg<HiveDecimalWritable> {
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      SumHiveDecimalWritableAgg agg = new SumHiveDecimalWritableAgg();
      reset(agg);
      return agg;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      SumAgg<HiveDecimalWritable> bdAgg = (SumAgg<HiveDecimalWritable>) agg;
      bdAgg.empty = true;
      bdAgg.sum = new HiveDecimalWritable(0);
      bdAgg.uniqueObjects = null;
    }

    boolean warned = false;

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 1);
      try {
        if (isEligibleValue((SumHiveDecimalWritableAgg) agg, parameters[0])) {
          ((SumHiveDecimalWritableAgg)agg).empty = false;
          ((SumHiveDecimalWritableAgg)agg).sum.mutateAdd(
              PrimitiveObjectInspectorUtils.getHiveDecimal(parameters[0], inputOI));
        }
      } catch (NumberFormatException e) {
        if (!warned) {
          warned = true;
          LOG.warn(getClass().getSimpleName() + " "
              + StringUtils.stringifyException(e));
          LOG
          .warn(getClass().getSimpleName()
              + " ignoring similar exceptions.");
        }
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        SumHiveDecimalWritableAgg myagg = (SumHiveDecimalWritableAgg) agg;
        if (myagg.sum == null || !myagg.sum.isSet()) {
          return;
        }

        myagg.empty = false;
        if (isWindowingDistinct()) {
          throw new HiveException("Distinct windowing UDAF doesn't support merge and terminatePartial");
        } else {
          // If partial is NULL, then there was an overflow and myagg.sum will be marked as not set.
          myagg.sum.mutateAdd(PrimitiveObjectInspectorUtils.getHiveDecimal(partial, inputOI));
        }
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      SumHiveDecimalWritableAgg myagg = (SumHiveDecimalWritableAgg) agg;
      if (myagg.empty || myagg.sum == null || !myagg.sum.isSet()) {
        return null;
      }
      DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo)outputOI.getTypeInfo();
      myagg.sum.mutateEnforcePrecisionScale(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
      if (!myagg.sum.isSet()) {
        LOG.warn("The sum of a column with data type HiveDecimal is out of range");
        return null;
      }

      result.set(myagg.sum);
      return result;
    }

    @Override
    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrameDef) {
      // Don't use streaming for distinct cases
      if (sumDistinct) {
        return null;
      }

      return new GenericUDAFStreamingEvaluator.SumAvgEnhancer<HiveDecimalWritable, HiveDecimal>(
          this, wFrameDef) {

        @Override
        protected HiveDecimalWritable getNextResult(
            org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStreamingEvaluator.SumAvgEnhancer<HiveDecimalWritable, HiveDecimal>.SumAvgStreamingState ss)
            throws HiveException {
          SumHiveDecimalWritableAgg myagg = (SumHiveDecimalWritableAgg) ss.wrappedBuf;
          HiveDecimal r = myagg.empty ? null : myagg.sum.getHiveDecimal();
          HiveDecimal d = ss.retrieveNextIntermediateValue();
          if (d != null ) {
            r = r == null ? null : r.subtract(d);
          }

          return r == null ? null : new HiveDecimalWritable(r);
        }

        @Override
        protected HiveDecimal getCurrentIntermediateResult(
            org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStreamingEvaluator.SumAvgEnhancer<HiveDecimalWritable, HiveDecimal>.SumAvgStreamingState ss)
            throws HiveException {
          SumHiveDecimalWritableAgg myagg = (SumHiveDecimalWritableAgg) ss.wrappedBuf;
          return myagg.empty ? null : myagg.sum.getHiveDecimal();
        }

      };
    }

    @Override
    protected BasePartitionEvaluator createPartitionEvaluator(
        WindowFrameDef winFrame,
        PTFPartition partition,
        List<PTFExpressionDef> parameters,
        ObjectInspector outputOI,
        boolean nullsLast) {
      return new BasePartitionEvaluator.SumPartitionHiveDecimalEvaluator(this, winFrame,
          partition, parameters, outputOI, nullsLast);
    }
  }

  /**
   * GenericUDAFSumDouble.
   *
   */
  @VectorizedUDAFs({
    VectorUDAFSumDouble.class,
    VectorUDAFSumTimestamp.class})
  public static class GenericUDAFSumDouble extends GenericUDAFSumEvaluator<DoubleWritable> {
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);
      result = new DoubleWritable(0);
      inputOI = (PrimitiveObjectInspector) parameters[0];
      outputOI = (PrimitiveObjectInspector)ObjectInspectorUtils.getStandardObjectInspector(inputOI,
          ObjectInspectorCopyOption.JAVA);
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    /** class for storing double sum value. */
    @AggregationType(estimable = true)
    static class SumDoubleAgg extends SumAgg<Double> {
      @Override
      public int estimate() { return JavaDataModel.PRIMITIVES1 + JavaDataModel.PRIMITIVES2; }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      SumDoubleAgg result = new SumDoubleAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      SumDoubleAgg myagg = (SumDoubleAgg) agg;
      myagg.empty = true;
      myagg.sum = 0.0;
      myagg.uniqueObjects = null;
    }

    boolean warned = false;

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 1);
      try {
        if (isEligibleValue((SumDoubleAgg) agg, parameters[0])) {
          ((SumDoubleAgg)agg).empty = false;
          ((SumDoubleAgg)agg).sum += PrimitiveObjectInspectorUtils.getDouble(parameters[0], inputOI);
        }
      } catch (NumberFormatException e) {
        if (!warned) {
          warned = true;
          LOG.warn(getClass().getSimpleName() + " "
              + StringUtils.stringifyException(e));
          LOG
          .warn(getClass().getSimpleName()
              + " ignoring similar exceptions.");
        }
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        SumDoubleAgg myagg = (SumDoubleAgg) agg;
        myagg.empty = false;
        if (isWindowingDistinct()) {
          throw new HiveException("Distinct windowing UDAF doesn't support merge and terminatePartial");
        } else {
          myagg.sum += PrimitiveObjectInspectorUtils.getDouble(partial, inputOI);
        }
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      SumDoubleAgg myagg = (SumDoubleAgg) agg;
      if (myagg.empty) {
        return null;
      }
      result.set(myagg.sum);
      return result;
    }

    @Override
    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrameDef) {
      // Don't use streaming for distinct cases
      if (sumDistinct) {
        return null;
      }

      return new GenericUDAFStreamingEvaluator.SumAvgEnhancer<DoubleWritable, Double>(this,
          wFrameDef) {

        @Override
        protected DoubleWritable getNextResult(
            org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStreamingEvaluator.SumAvgEnhancer<DoubleWritable, Double>.SumAvgStreamingState ss)
            throws HiveException {
          SumDoubleAgg myagg = (SumDoubleAgg) ss.wrappedBuf;
          Double r = myagg.empty ? null : myagg.sum;
          Double d = ss.retrieveNextIntermediateValue();
          if (d != null) {
            r = r == null ? null : r - d;
          }

          return r == null ? null : new DoubleWritable(r);
        }

        @Override
        protected Double getCurrentIntermediateResult(
            org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStreamingEvaluator.SumAvgEnhancer<DoubleWritable, Double>.SumAvgStreamingState ss)
            throws HiveException {
          SumDoubleAgg myagg = (SumDoubleAgg) ss.wrappedBuf;
          return myagg.empty ? null : myagg.sum;
        }

      };
    }

    @Override
    protected BasePartitionEvaluator createPartitionEvaluator(
        WindowFrameDef winFrame,
        PTFPartition partition,
        List<PTFExpressionDef> parameters,
        ObjectInspector outputOI,
        boolean nullsLast) {
      return new BasePartitionEvaluator.SumPartitionDoubleEvaluator(this, winFrame, partition,
          parameters, outputOI, nullsLast);
    }
  }

  /**
   * GenericUDAFSumLong.
   *
   */
  @VectorizedUDAFs({
    VectorUDAFSumLong.class})
  public static class GenericUDAFSumLong extends GenericUDAFSumEvaluator<LongWritable> {
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);
      result = new LongWritable(0);
      inputOI = (PrimitiveObjectInspector) parameters[0];
      outputOI = (PrimitiveObjectInspector)ObjectInspectorUtils.getStandardObjectInspector(inputOI,
          ObjectInspectorCopyOption.JAVA);
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    /** class for storing double sum value. */
    @AggregationType(estimable = true)
    static class SumLongAgg extends SumAgg<Long> {
      @Override
      public int estimate() { return JavaDataModel.PRIMITIVES1 + JavaDataModel.PRIMITIVES2; }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      SumLongAgg result = new SumLongAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      SumLongAgg myagg = (SumLongAgg) agg;
      myagg.empty = true;
      myagg.sum = 0L;
      myagg.uniqueObjects = null;
    }

    private boolean warned = false;

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 1);
      try {
        if (isEligibleValue((SumLongAgg) agg, parameters[0])) {
          ((SumLongAgg)agg).empty = false;
          ((SumLongAgg)agg).sum += PrimitiveObjectInspectorUtils.getLong(parameters[0], inputOI);
        }
      } catch (NumberFormatException e) {
        if (!warned) {
          warned = true;
          LOG.warn(getClass().getSimpleName() + " "
              + StringUtils.stringifyException(e));
        }
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        SumLongAgg myagg = (SumLongAgg) agg;
        myagg.empty = false;
        if (isWindowingDistinct()) {
          throw new HiveException("Distinct windowing UDAF doesn't support merge and terminatePartial");
        } else {
            myagg.sum += PrimitiveObjectInspectorUtils.getLong(partial, inputOI);
        }
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      SumLongAgg myagg = (SumLongAgg) agg;
      if (myagg.empty) {
        return null;
      }
      result.set(myagg.sum);
      return result;
    }

    @Override
    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrameDef) {
      // Don't use streaming for distinct cases
      if (isWindowingDistinct()) {
        return null;
      }

      return new GenericUDAFStreamingEvaluator.SumAvgEnhancer<LongWritable, Long>(this,
          wFrameDef) {

        @Override
        protected LongWritable getNextResult(
            org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStreamingEvaluator.SumAvgEnhancer<LongWritable, Long>.SumAvgStreamingState ss)
            throws HiveException {
          SumLongAgg myagg = (SumLongAgg) ss.wrappedBuf;
          Long r = myagg.empty ? null : myagg.sum;
          Long d = ss.retrieveNextIntermediateValue();
          if (d != null) {
            r = r == null ? null : r - d;
          }

          return r == null ? null : new LongWritable(r);
        }

        @Override
        protected Long getCurrentIntermediateResult(
            org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStreamingEvaluator.SumAvgEnhancer<LongWritable, Long>.SumAvgStreamingState ss)
            throws HiveException {
          SumLongAgg myagg = (SumLongAgg) ss.wrappedBuf;
          return myagg.empty ? null : myagg.sum;
        }
      };
    }

    @Override
    protected BasePartitionEvaluator createPartitionEvaluator(
        WindowFrameDef winFrame,
        PTFPartition partition,
        List<PTFExpressionDef> parameters,
        ObjectInspector outputOI,
        boolean nullsLast) {
      return new BasePartitionEvaluator.SumPartitionLongEvaluator(this, winFrame, partition,
          parameters, outputOI, nullsLast);
    }
  }
}
