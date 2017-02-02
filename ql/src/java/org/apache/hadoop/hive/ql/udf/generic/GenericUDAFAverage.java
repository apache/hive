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
package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationType;
import org.apache.hadoop.hive.ql.udf.ptf.BasePartitionEvaluator;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorObject;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * GenericUDAFAverage.
 *
 */
@Description(name = "avg", value = "_FUNC_(x) - Returns the mean of a set of numbers")
public class GenericUDAFAverage extends AbstractGenericUDAFResolver {

  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFAverage.class.getName());

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
    case FLOAT:
    case DOUBLE:
    case STRING:
    case VARCHAR:
    case CHAR:
    case TIMESTAMP:
      return new GenericUDAFAverageEvaluatorDouble();
    case DECIMAL:
      return new GenericUDAFAverageEvaluatorDecimal();
    case BOOLEAN:
    case DATE:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string type arguments are accepted but "
              + parameters[0].getTypeName() + " is passed.");
    }
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo)
  throws SemanticException {
    if (paramInfo.isAllColumns()) {
      throw new SemanticException(
          "The specified syntax for UDAF invocation is invalid.");
    }

    AbstractGenericUDAFAverageEvaluator eval =
        (AbstractGenericUDAFAverageEvaluator) getEvaluator(paramInfo.getParameters());
    eval.avgDistinct = paramInfo.isDistinct();
    eval.isWindowing = paramInfo.isWindowing();
    return eval;
  }

  public static class GenericUDAFAverageEvaluatorDouble extends AbstractGenericUDAFAverageEvaluator<Double> {

    @Override
    public void doReset(AverageAggregationBuffer<Double> aggregation) throws HiveException {
      aggregation.count = 0;
      aggregation.sum = new Double(0);
      aggregation.uniqueObjects = new HashSet<ObjectInspectorObject>();
    }

    @Override
    protected ObjectInspector getSumFieldJavaObjectInspector() {
      return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    }
    @Override
    protected ObjectInspector getSumFieldWritableObjectInspector() {
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    @Override
    protected void doIterate(AverageAggregationBuffer<Double> aggregation,
        PrimitiveObjectInspector oi, Object parameter) {
      double value = PrimitiveObjectInspectorUtils.getDouble(parameter, oi);
      aggregation.count++;
      aggregation.sum += value;

    }

    @Override
    protected void doMerge(AverageAggregationBuffer<Double> aggregation, Long partialCount,
        ObjectInspector sumFieldOI, Object partialSum) {
      double value = ((DoubleObjectInspector)sumFieldOI).get(partialSum);
      aggregation.count += partialCount;
      aggregation.sum += value;
    }

    @Override
    protected void doMergeAdd(Double sum,
        ObjectInspectorObject obj) {
      sum += PrimitiveObjectInspectorUtils.getDouble(obj.getValues()[0], copiedOI);
    }

    @Override
    protected void doTerminatePartial(AverageAggregationBuffer<Double> aggregation) {
      if(partialResult[1] == null) {
        partialResult[1] = new DoubleWritable(0);
      }
      ((LongWritable) partialResult[0]).set(aggregation.count);
      ((DoubleWritable) partialResult[1]).set(aggregation.sum);
    }

    @Override
    protected Object doTerminate(AverageAggregationBuffer<Double> aggregation) {
      if(aggregation.count == 0) {
        return null;
      } else {
        DoubleWritable result = new DoubleWritable(0);
        result.set(aggregation.sum / aggregation.count);
        return result;
      }
    }
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AverageAggregationBuffer<Double> result = new AverageAggregationBuffer<Double>();
      reset(result);
      return result;
    }

    @Override
    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrameDef) {
      // Don't use streaming for distinct cases
      if (isWindowingDistinct()) {
        return null;
      }

      return new GenericUDAFStreamingEvaluator.SumAvgEnhancer<DoubleWritable, Object[]>(this, wFrameDef) {

        @Override
        protected DoubleWritable getNextResult(
            org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStreamingEvaluator.SumAvgEnhancer<DoubleWritable, Object[]>.SumAvgStreamingState ss)
            throws HiveException {
          AverageAggregationBuffer<Double> myagg = (AverageAggregationBuffer<Double>) ss.wrappedBuf;
          Double r = myagg.count == 0 ? null : myagg.sum;
          long cnt = myagg.count;

          Object[] o = ss.retrieveNextIntermediateValue();
          if (o != null) {
            Double d = (Double) o[0];
            r = r == null ? null : r - d;
            cnt = cnt - ((Long) o[1]);
          }

          return r == null ? null : new DoubleWritable(r / cnt);
        }

        @Override
        protected Object[] getCurrentIntermediateResult(
            org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStreamingEvaluator.SumAvgEnhancer<DoubleWritable, Object[]>.SumAvgStreamingState ss)
            throws HiveException {
          AverageAggregationBuffer<Double> myagg = (AverageAggregationBuffer<Double>) ss.wrappedBuf;
          return myagg.count == 0 ? null : new Object[] {
              new Double(myagg.sum), myagg.count };
        }

      };
    }

    @Override
    protected BasePartitionEvaluator createPartitionEvaluator(
        WindowFrameDef winFrame,
        PTFPartition partition,
        List<PTFExpressionDef> parameters,
        ObjectInspector outputOI) {
      try {
        return new BasePartitionEvaluator.AvgPartitionDoubleEvaluator(this, winFrame, partition, parameters, inputOI, outputOI);
      } catch(HiveException e) {
        return super.createPartitionEvaluator(winFrame, partition, parameters, outputOI);
      }
    }
  }

  public static class GenericUDAFAverageEvaluatorDecimal extends AbstractGenericUDAFAverageEvaluator<HiveDecimal> {

    @Override
    public void doReset(AverageAggregationBuffer<HiveDecimal> aggregation) throws HiveException {
      aggregation.count = 0;
      aggregation.sum = HiveDecimal.ZERO;
      aggregation.uniqueObjects = new HashSet<ObjectInspectorObject>();
    }

    @Override
    protected ObjectInspector getSumFieldJavaObjectInspector() {
      DecimalTypeInfo typeInfo = deriveResultDecimalTypeInfo();
      return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(typeInfo);
    }

    @Override
    protected ObjectInspector getSumFieldWritableObjectInspector() {
      DecimalTypeInfo typeInfo = deriveResultDecimalTypeInfo();
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo);
    }

    private DecimalTypeInfo deriveResultDecimalTypeInfo() {
      int prec = inputOI.precision();
      int scale = inputOI.scale();
      if (mode == Mode.FINAL || mode == Mode.COMPLETE) {
        int intPart = prec - scale;
        // The avg() result type has the same number of integer digits and 4 more decimal digits.
        scale = Math.min(scale + 4, HiveDecimal.MAX_SCALE - intPart);
        return TypeInfoFactory.getDecimalTypeInfo(intPart + scale, scale);
      } else {
        // For intermediate sum field
        return GenericUDAFAverage.deriveSumFieldTypeInfo(prec, scale);
      }
    }

    @Override
    protected void doIterate(AverageAggregationBuffer<HiveDecimal> aggregation,
        PrimitiveObjectInspector oi, Object parameter) {
      HiveDecimal value = PrimitiveObjectInspectorUtils.getHiveDecimal(parameter, oi);
      aggregation.count++;
      if (aggregation.sum != null) {
        aggregation.sum = aggregation.sum.add(value);
      }
    }

    @Override
    protected void doMerge(AverageAggregationBuffer<HiveDecimal> aggregation, Long partialCount,
        ObjectInspector sumFieldOI, Object partialSum) {
      HiveDecimal value = ((HiveDecimalObjectInspector)sumFieldOI).getPrimitiveJavaObject(partialSum);
      if (value == null) {
        aggregation.sum = null;
      }
      aggregation.count += partialCount;
      if (aggregation.sum != null) {
        aggregation.sum = aggregation.sum.add(value);
      }
    }


    @Override
    protected void doMergeAdd(
        HiveDecimal sum,
        ObjectInspectorObject obj) {
      sum.add(PrimitiveObjectInspectorUtils.getHiveDecimal(obj.getValues()[0], copiedOI));
    }

    @Override
    protected void doTerminatePartial(AverageAggregationBuffer<HiveDecimal> aggregation) {
      if(partialResult[1] == null && aggregation.sum != null) {
        partialResult[1] = new HiveDecimalWritable(HiveDecimal.ZERO);
      }
      ((LongWritable) partialResult[0]).set(aggregation.count);
      if (aggregation.sum != null) {
        ((HiveDecimalWritable) partialResult[1]).set(aggregation.sum);
      } else {
        partialResult[1] = null;
      }
    }

    @Override
    protected Object doTerminate(AverageAggregationBuffer<HiveDecimal> aggregation) {
      if(aggregation.count == 0 || aggregation.sum == null) {
        return null;
      } else {
        HiveDecimalWritable result = new HiveDecimalWritable(HiveDecimal.ZERO);
        result.set(aggregation.sum.divide(HiveDecimal.create(aggregation.count)));
        return result;
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AverageAggregationBuffer<HiveDecimal> result = new AverageAggregationBuffer<HiveDecimal>();
      reset(result);
      return result;
    }

    @Override
    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrameDef) {
      // Don't use streaming for distinct cases
      if (isWindowingDistinct()) {
        return null;
      }

      return new GenericUDAFStreamingEvaluator.SumAvgEnhancer<HiveDecimalWritable, Object[]>(
          this, wFrameDef) {

        @Override
        protected HiveDecimalWritable getNextResult(
            org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStreamingEvaluator.SumAvgEnhancer<HiveDecimalWritable, Object[]>.SumAvgStreamingState ss)
            throws HiveException {
          AverageAggregationBuffer<HiveDecimal> myagg = (AverageAggregationBuffer<HiveDecimal>) ss.wrappedBuf;
          HiveDecimal r = myagg.count == 0 ? null : myagg.sum;
          long cnt = myagg.count;

          Object[] o = ss.retrieveNextIntermediateValue();
          if (o != null) {
            HiveDecimal d = (HiveDecimal) o[0];
            r = r == null ? null : r.subtract(d);
            cnt = cnt - ((Long) o[1]);
          }

          return r == null ? null : new HiveDecimalWritable(
              r.divide(HiveDecimal.create(cnt)));
        }

        @Override
        protected Object[] getCurrentIntermediateResult(
            org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStreamingEvaluator.SumAvgEnhancer<HiveDecimalWritable, Object[]>.SumAvgStreamingState ss)
            throws HiveException {
          AverageAggregationBuffer<HiveDecimal> myagg = (AverageAggregationBuffer<HiveDecimal>) ss.wrappedBuf;
          return myagg.count == 0 ? null : new Object[] { myagg.sum,
              myagg.count };
        }

      };
    }

    @Override
    protected BasePartitionEvaluator createPartitionEvaluator(
        WindowFrameDef winFrame,
        PTFPartition partition,
        List<PTFExpressionDef> parameters,
        ObjectInspector outputOI) {
      try {
        return new BasePartitionEvaluator.AvgPartitionHiveDecimalEvaluator(this, winFrame, partition, parameters, inputOI, outputOI);
      } catch(HiveException e) {
        return super.createPartitionEvaluator(winFrame, partition, parameters, outputOI);
      }
    }
  }

  @AggregationType(estimable = true)
  private static class AverageAggregationBuffer<TYPE> extends AbstractAggregationBuffer {
    private HashSet<ObjectInspectorObject> uniqueObjects; // Unique rows.
    private long count;
    private TYPE sum;

    @Override
    public int estimate() {
      return 2*JavaDataModel.PRIMITIVES2;
    }
  };

  @SuppressWarnings("unchecked")
  public static abstract class AbstractGenericUDAFAverageEvaluator<TYPE> extends GenericUDAFEvaluator {
    protected boolean isWindowing;
    protected boolean avgDistinct;
    // For PARTIAL1 and COMPLETE
    protected transient PrimitiveObjectInspector inputOI;
    protected transient PrimitiveObjectInspector copiedOI;
    // For PARTIAL2 and FINAL
    private transient StructObjectInspector soi;
    private transient StructField countField;
    private transient StructField sumField;
    private LongObjectInspector countFieldOI;
    protected ObjectInspector sumFieldOI;
    // For PARTIAL1 and PARTIAL2
    protected transient Object[] partialResult;

    private boolean warned = false;


    protected abstract ObjectInspector getSumFieldJavaObjectInspector();
    protected abstract ObjectInspector getSumFieldWritableObjectInspector();
    protected abstract void doIterate(AverageAggregationBuffer<TYPE> aggregation,
        PrimitiveObjectInspector inputOI, Object parameter);
    protected abstract void doMerge(AverageAggregationBuffer<TYPE> aggregation, Long partialCount,
        ObjectInspector sumFieldOI, Object partialSum);
    protected abstract void doMergeAdd(TYPE sum, ObjectInspectorObject obj);
    protected abstract void doTerminatePartial(AverageAggregationBuffer<TYPE> aggregation);
    protected abstract Object doTerminate(AverageAggregationBuffer<TYPE> aggregation);
    protected abstract void doReset(AverageAggregationBuffer<TYPE> aggregation) throws HiveException;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);

      // init input
      partialResult = new Object[2];
      partialResult[0] = new LongWritable(0);
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        copiedOI = (PrimitiveObjectInspector)ObjectInspectorUtils.getStandardObjectInspector(inputOI,
            ObjectInspectorCopyOption.JAVA);
      } else {
        soi = (StructObjectInspector) parameters[0];
        countField = soi.getStructFieldRef("count");
        sumField = soi.getStructFieldRef("sum");
        countFieldOI = (LongObjectInspector) countField.getFieldObjectInspector();
        sumFieldOI = sumField.getFieldObjectInspector();
        inputOI = (PrimitiveObjectInspector) soi.getStructFieldRef("input").getFieldObjectInspector();
      }

      // init output
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        // The output of a partial aggregation is a struct containing
        // a "long" count and a "double" sum.
        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(getSumFieldWritableObjectInspector());
        // We need to "remember" the input object inspector so that we need to know the input type
        // in order to determine the sum field type (precision/scale) for Mode.PARTIAL2 and Mode.FINAL.
        foi.add(inputOI);
        ArrayList<String> fname = new ArrayList<String>();
        fname.add("count");
        fname.add("sum");
        fname.add("input");
        // index 1 set by child
        return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
      } else {
        return getSumFieldWritableObjectInspector();
      }
    }

    public boolean isWindowingDistinct() {
      return isWindowing && avgDistinct;
    }

    @AggregationType(estimable = true)
    static class AverageAgg extends AbstractAggregationBuffer {
      long count;
      double sum;
      @Override
      public int estimate() { return JavaDataModel.PRIMITIVES2 * 2; }
    };

    @Override
    public void reset(AggregationBuffer aggregation) throws HiveException {
      doReset((AverageAggregationBuffer<TYPE>)aggregation);
    }

    @Override
    public void iterate(AggregationBuffer aggregation, Object[] parameters)
        throws HiveException {
      assert (parameters.length == 1);
      Object parameter = parameters[0];
      if (parameter != null) {
        AverageAggregationBuffer<TYPE> averageAggregation = (AverageAggregationBuffer<TYPE>) aggregation;
        try {
          // Skip the same value if avgDistinct is true
          if (isWindowingDistinct()) {
            ObjectInspectorObject obj = new ObjectInspectorObject(
                ObjectInspectorUtils.copyToStandardObject(parameter, inputOI, ObjectInspectorCopyOption.JAVA),
                copiedOI);
            if (averageAggregation.uniqueObjects.contains(obj)) {
              return;
            }
            averageAggregation.uniqueObjects.add(obj);
          }

          doIterate(averageAggregation, inputOI, parameter);
        } catch (NumberFormatException e) {
          if (!warned) {
            warned = true;
            LOG.warn("Ignoring similar exceptions: " + StringUtils.stringifyException(e));
          }
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer aggregation) throws HiveException {
      if (isWindowingDistinct()) {
        throw new HiveException("Distinct windowing UDAF doesn't support merge and terminatePartial");
      }

      doTerminatePartial((AverageAggregationBuffer<TYPE>) aggregation);
      return partialResult;
    }

    @Override
    public void merge(AggregationBuffer aggregation, Object partial)
        throws HiveException {
      if (partial != null) {
        if (isWindowingDistinct()) {
          throw new HiveException("Distinct windowing UDAF doesn't support merge and terminatePartial");
        } else {
          doMerge((AverageAggregationBuffer<TYPE>)aggregation,
              countFieldOI.get(soi.getStructFieldData(partial, countField)),
              sumFieldOI, soi.getStructFieldData(partial, sumField));
        }
      }
    }

    @Override
    public Object terminate(AggregationBuffer aggregation) throws HiveException {
      return doTerminate((AverageAggregationBuffer<TYPE>)aggregation);
    }
  }

  /**
   * The intermediate sum field has 10 more integer digits with the same scale.
   * This is exposed as static so that the vectorized AVG operator use the same logic
   * @param precision
   * @param scale
   * @return
   */
  public static DecimalTypeInfo deriveSumFieldTypeInfo(int precision, int scale) {
    int intPart = precision - scale;
    intPart = Math.min(intPart + 10, HiveDecimal.MAX_PRECISION - scale);
    return TypeInfoFactory.getDecimalTypeInfo(intPart + scale, scale);
  }

}
