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

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.stats.ColStatsProcessor.ColumnStatsType;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;

import static org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory.getEmptyNumDistinctValueEstimator;
import static org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory.getNumDistinctValueEstimator;

/**
 * GenericUDAFComputeBitVector. This UDAF will compute a bit vector using the
 * algorithm provided as a parameter. The ndv_compute_bit_vector function can
 * be used on top of it to extract an estimate of the ndv from it.
 */
@Description(name = "compute_bit_vector",
      value = "_FUNC_(x) - Computes bit vector for NDV computation.")
public class GenericUDAFComputeBitVector extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length < 2 ) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly 2 (col + hll) or 3 (col + fm + #bitvectors) arguments are expected.");
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " is passed.");
    }

    ColumnStatsType cst = ColumnStatsType.getColumnStatsType(((PrimitiveTypeInfo) parameters[0]));
    switch (cst) {
    case LONG:
      return new GenericUDAFLongStatsEvaluator();
    case DOUBLE:
      return new GenericUDAFDoubleStatsEvaluator();
    case STRING:
      return new GenericUDAFStringStatsEvaluator();
    case DECIMAL:
      return new GenericUDAFDecimalStatsEvaluator();
    case DATE:
      return new GenericUDAFDateStatsEvaluator();
    case TIMESTAMP:
      return new GenericUDAFTimestampStatsEvaluator();
    default:
      throw new UDFArgumentTypeException(0,
          "Type argument " + parameters[0].getTypeName() + " not valid");
    }
  }

  public static abstract class GenericUDAFNumericStatsEvaluator<V, OI extends PrimitiveObjectInspector>
      extends GenericUDAFEvaluator {

    protected final static int MAX_BIT_VECTORS = 1024;

    /* Object Inspector corresponding to the input parameter.
     */
    protected transient PrimitiveObjectInspector inputOI;
    protected transient PrimitiveObjectInspector funcOI;
    protected transient PrimitiveObjectInspector numVectorsOI;

    /* Object Inspector corresponding to the bitvector.
     */
    protected transient BinaryObjectInspector ndvFieldOI;

    /* Partial aggregation result returned by TerminatePartial.
     */
    protected transient BytesWritable partialResult;

    /* Output of final result of the aggregation.
     */
    protected transient BytesWritable result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // initialize input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        funcOI = (PrimitiveObjectInspector) parameters[1];
        if (parameters.length > 2) {
          numVectorsOI = (PrimitiveObjectInspector) parameters[2];
        }
      } else {
        ndvFieldOI = (BinaryObjectInspector) parameters[0];
      }

      // initialize output
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        partialResult = new BytesWritable();
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
      } else {
        result = new BytesWritable();
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
      }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      NumericStatsAgg myagg = (NumericStatsAgg) agg;

      if (myagg.numDV == null) {
        int numVectors = 0;
        // func may be null when GBY op is closing.
        // see mvn test -Dtest=TestMiniTezCliDriver -Dqfile=explainuser_3.q
        // original behavior is to create FMSketch
        String func = parameters[1] == null ? "fm" : PrimitiveObjectInspectorUtils.getString(
            parameters[1], funcOI);
        if (parameters.length == 3) {
          numVectors = parameters[2] == null ? 0 : PrimitiveObjectInspectorUtils.getInt(
              parameters[2], numVectorsOI);
          if (numVectors > MAX_BIT_VECTORS) {
            throw new HiveException("The maximum allowed value for number of bit vectors " + " is "
                + MAX_BIT_VECTORS + ", but was passed " + numVectors + " bit vectors");
          }
        }
        myagg.initNDVEstimator(func, numVectors);
      }

      if (parameters[0] != null) {
        myagg.update(parameters[0], inputOI);
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        NumericStatsAgg myagg = (NumericStatsAgg) agg;
        // Merge numDistinctValue Estimators
        byte[] buf = ndvFieldOI.getPrimitiveJavaObject(partial);
        if (buf != null && buf.length != 0) {
          if (myagg.numDV == null) {
            myagg.numDV = getNumDistinctValueEstimator(buf);
          } else {
            myagg.numDV.mergeEstimators(getNumDistinctValueEstimator(buf));
          }
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return ((NumericStatsAgg) agg).serializePartial(partialResult);
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return ((NumericStatsAgg) agg).serialize(result);
    }

    public abstract class NumericStatsAgg extends AbstractAggregationBuffer {

      public NumDistinctValueEstimator numDV;    /* Distinct value estimator */

      @Override
      public int estimate() {
        JavaDataModel model = JavaDataModel.get();
        return (numDV == null) ?
            lengthFor(model) : numDV.lengthFor(model);
      }

      protected void initNDVEstimator(String func, int numBitVectors) {
        numDV = getEmptyNumDistinctValueEstimator(func, numBitVectors);
      }

      protected abstract void update(Object p, PrimitiveObjectInspector inputOI);

      protected Object serialize(BytesWritable result) {
        if (numDV != null) {
          byte[] buf = numDV.serialize();
          result.set(buf, 0, buf.length);
        }
        return result;
      }

      protected Object serializePartial(BytesWritable result) {
        if (numDV != null) {
          // Serialize numDistinctValue Estimator
          byte[] buf = numDV.serialize();
          result.set(buf, 0, buf.length);
        }
        return result;
      }

      public void reset() throws HiveException {
        numDV = null;
      }
    };
  }

  /**
   * GenericUDAFLongStatsEvaluator.
   *
   */
  public static class GenericUDAFLongStatsEvaluator
      extends GenericUDAFNumericStatsEvaluator<Long, LongObjectInspector> {

    @AggregationType(estimable = true)
    public class LongStatsAgg extends NumericStatsAgg {
      @Override
      public int estimate() {
        JavaDataModel model = JavaDataModel.get();
        return super.estimate() + model.primitive2() * 2;
      }

      @Override
      protected void update(Object p, PrimitiveObjectInspector inputOI) {
        long v = PrimitiveObjectInspectorUtils.getLong(p, inputOI);
        numDV.addToEstimator(v);
      }
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new LongStatsAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((NumericStatsAgg)agg).reset();
    }
  }

  /**
   * GenericUDAFDoubleStatsEvaluator.
   */
  public static class GenericUDAFDoubleStatsEvaluator
      extends GenericUDAFNumericStatsEvaluator<Double, DoubleObjectInspector> {

    @AggregationType(estimable = true)
    public class DoubleStatsAgg extends NumericStatsAgg {
      @Override
      public int estimate() {
        JavaDataModel model = JavaDataModel.get();
        return super.estimate() + model.primitive2() * 2;
      }

      @Override
      protected void update(Object p, PrimitiveObjectInspector inputOI) {
        double v = PrimitiveObjectInspectorUtils.getDouble(p, inputOI);
        numDV.addToEstimator(v);
      }
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new DoubleStatsAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((NumericStatsAgg)agg).reset();
    }
  }

  public static class GenericUDAFDecimalStatsEvaluator
      extends GenericUDAFNumericStatsEvaluator<HiveDecimal, HiveDecimalObjectInspector> {

    @AggregationType(estimable = true)
    public class DecimalStatsAgg extends NumericStatsAgg {
      @Override
      public int estimate() {
        JavaDataModel model = JavaDataModel.get();
        return super.estimate() + model.lengthOfDecimal() * 2;
      }

      @Override
      protected void update(Object p, PrimitiveObjectInspector inputOI) {
        HiveDecimal v = PrimitiveObjectInspectorUtils.getHiveDecimal(p, inputOI);
        numDV.addToEstimator(v);
      }
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new DecimalStatsAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((NumericStatsAgg)agg).reset();
    }
  }

  /**
   * GenericUDAFDateStatsEvaluator.
   */
  public static class GenericUDAFDateStatsEvaluator
      extends GenericUDAFNumericStatsEvaluator<DateWritableV2, DateObjectInspector> {

    @AggregationType(estimable = true)
    public class DateStatsAgg extends NumericStatsAgg {
      @Override
      public int estimate() {
        JavaDataModel model = JavaDataModel.get();
        return super.estimate() + model.primitive2() * 2;
      }

      @Override
      protected void update(Object p, PrimitiveObjectInspector inputOI) {
        // DateWritableV2 is mutable, DateStatsAgg needs its own copy
        DateWritableV2 v = new DateWritableV2((DateWritableV2) inputOI.getPrimitiveWritableObject(p));
        numDV.addToEstimator(v.getDays());
      }
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new DateStatsAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((NumericStatsAgg)agg).reset();
    }
  }

  /**
   * GenericUDAFTimestampStatsEvaluator.
   */
  public static class GenericUDAFTimestampStatsEvaluator
      extends GenericUDAFNumericStatsEvaluator<TimestampWritableV2, TimestampObjectInspector> {

    @AggregationType(estimable = true)
    public class TimestampStatsAgg extends NumericStatsAgg {
      @Override
      public int estimate() {
        JavaDataModel model = JavaDataModel.get();
        return super.estimate() + model.primitive2() * 2;
      }

      @Override
      protected void update(Object p, PrimitiveObjectInspector inputOI) {
        // TimestampWritableV2 is mutable, TimestampStatsAgg needs its own copy
        TimestampWritableV2 v = new TimestampWritableV2((TimestampWritableV2) inputOI.getPrimitiveWritableObject(p));
        numDV.addToEstimator(v.getSeconds());
      }
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new TimestampStatsAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((NumericStatsAgg)agg).reset();
    }
  }

  /**
   * GenericUDAFStringStatsEvaluator.
   */
  public static class GenericUDAFStringStatsEvaluator extends GenericUDAFEvaluator {

    private final static int MAX_BIT_VECTORS = 1024;

    /* Object Inspector corresponding to the input parameter.
     */
    private transient PrimitiveObjectInspector inputOI;
    private transient PrimitiveObjectInspector funcOI;
    private transient PrimitiveObjectInspector numVectorsOI;

    /* Object Inspector corresponding to the bitvector
     */
    private transient BinaryObjectInspector ndvFieldOI;

    /* Partial aggregation result returned by TerminatePartial.
     */
    private transient BytesWritable partialResult;

    /* Output of final result of the aggregation
     */
    private transient BytesWritable result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // initialize input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        funcOI = (PrimitiveObjectInspector) parameters[1];
        if (parameters.length > 2) {
          numVectorsOI = (PrimitiveObjectInspector) parameters[2];
        }
      } else {
        ndvFieldOI = (BinaryObjectInspector) parameters[0];
      }

      // initialize output
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        partialResult = new BytesWritable();
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
      } else {
        result = new BytesWritable();
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
      }
    }

    @AggregationType(estimable = true)
    public static class StringStatsAgg extends AbstractAggregationBuffer {
      public NumDistinctValueEstimator numDV;      /* Distinct value estimator */
      public boolean firstItem;
      @Override
      public int estimate() {
        JavaDataModel model = JavaDataModel.get();
        return (numDV == null) ?
            lengthFor(model) : numDV.lengthFor(model);      }
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      StringStatsAgg result = new StringStatsAgg();
      reset(result);
      return result;
    }

    public void initNDVEstimator(StringStatsAgg aggBuffer, String func, int numBitVectors) {
      aggBuffer.numDV = getEmptyNumDistinctValueEstimator(func, numBitVectors);
      aggBuffer.numDV.reset();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      StringStatsAgg myagg = (StringStatsAgg) agg;
      myagg.firstItem = true;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      Object p = parameters[0];
      StringStatsAgg myagg = (StringStatsAgg) agg;

      if (myagg.firstItem) {
        int numVectors = 0;
        String func = parameters[1] == null ? "fm" : PrimitiveObjectInspectorUtils.getString(
            parameters[1], funcOI);
        if (parameters.length > 2) {
          numVectors = PrimitiveObjectInspectorUtils.getInt(parameters[2], numVectorsOI);
          if (numVectors > MAX_BIT_VECTORS) {
            throw new HiveException("The maximum allowed value for number of bit vectors " + " is "
                + MAX_BIT_VECTORS + " , but was passed " + numVectors + " bit vectors");
          }
        }

        initNDVEstimator(myagg, func, numVectors);
        myagg.firstItem = false;
      }

      String v = PrimitiveObjectInspectorUtils.getString(p, inputOI);
      if (v != null) {
        // Add string value to NumDistinctValue Estimator
        myagg.numDV.addToEstimator(v);
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        StringStatsAgg myagg = (StringStatsAgg) agg;

        // Merge numDistinctValue Estimators
        byte[] buf = ndvFieldOI.getPrimitiveJavaObject(partial);

        if (buf != null && buf.length != 0) {
          if (myagg.numDV == null) {
            myagg.numDV = getNumDistinctValueEstimator(buf);
          } else {
            myagg.numDV.mergeEstimators(getNumDistinctValueEstimator(buf));
          }
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      StringStatsAgg myagg = (StringStatsAgg) agg;
      // Serialize numDistinctValue Estimator
      if (myagg.numDV != null) {
        byte[] buf = myagg.numDV.serialize();
        partialResult.set(buf, 0, buf.length);
      }
      return partialResult;
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      StringStatsAgg myagg = (StringStatsAgg) agg;
      if (myagg.numDV != null) {
        byte[] buf = myagg.numDV.serialize();
        result.set(buf, 0, buf.length);
      }
      return result;
    }
  }

  @InterfaceAudience.LimitedPrivate(value = { "Hive" })
  static int lengthFor(JavaDataModel model) {
    int length = model.object();
    // HiveConf hive.stats.ndv.error default produces 16
    length += model.array() * 3; // three array
    length += model.primitive1() * 16 * 2; // two int array
    length += (model.object() + model.array() + model.primitive1() + model.primitive2())
        * 16; // bitset array
    return length;
  }
}
