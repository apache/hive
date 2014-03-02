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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
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

  static final Log LOG = LogFactory.getLog(GenericUDAFAverage.class.getName());

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


  public static class GenericUDAFAverageEvaluatorDouble extends AbstractGenericUDAFAverageEvaluator<Double> {

    @Override
    public void doReset(AverageAggregationBuffer<Double> aggregation) throws HiveException {
      aggregation.count = 0;
      aggregation.sum = new Double(0);
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
  }

  public static class GenericUDAFAverageEvaluatorDecimal extends AbstractGenericUDAFAverageEvaluator<HiveDecimal> {

    @Override
    public void doReset(AverageAggregationBuffer<HiveDecimal> aggregation) throws HiveException {
      aggregation.count = 0;
      aggregation.sum = HiveDecimal.ZERO;
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
  }

  private static class AverageAggregationBuffer<TYPE> implements AggregationBuffer {
    private long count;
    private TYPE sum;
  };

  @SuppressWarnings("unchecked")
  public static abstract class AbstractGenericUDAFAverageEvaluator<TYPE> extends GenericUDAFEvaluator {

    // For PARTIAL1 and COMPLETE
    protected transient PrimitiveObjectInspector inputOI;
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
    protected abstract void doTerminatePartial(AverageAggregationBuffer<TYPE> aggregation);
    protected abstract Object doTerminate(AverageAggregationBuffer<TYPE> aggregation);
    protected abstract void doReset(AverageAggregationBuffer<TYPE> aggregation) throws HiveException;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);

      // init input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
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
        partialResult = new Object[2];
        partialResult[0] = new LongWritable(0);
        // index 1 set by child
        return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
      } else {
        return getSumFieldWritableObjectInspector();
      }
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
      doTerminatePartial((AverageAggregationBuffer<TYPE>) aggregation);
      return partialResult;
    }

    @Override
    public void merge(AggregationBuffer aggregation, Object partial)
        throws HiveException {
      if (partial != null) {
        doMerge((AverageAggregationBuffer<TYPE>)aggregation,
            countFieldOI.get(soi.getStructFieldData(partial, countField)),
            sumFieldOI, soi.getStructFieldData(partial, sumField));
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
