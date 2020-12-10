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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BytesWritable;

import static org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory.getNumDistinctValueEstimator;

public abstract class GenericUDAFComputeBitVectorBase extends AbstractGenericUDAFResolver {

  public static abstract class NumericStatsEvaluatorBase<V, OI extends PrimitiveObjectInspector>
      extends GenericUDAFEvaluator {

    /* Object Inspector corresponding to the input parameter.
     */
    protected transient PrimitiveObjectInspector inputOI;

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
    public void reset(AggregationBuffer agg) throws HiveException {
      ((NumericStatsAgg)agg).reset();
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
  }

  public static abstract class StringStatsEvaluatorBase extends GenericUDAFEvaluator {

    /* Object Inspector corresponding to the input parameter.
     */
    protected transient PrimitiveObjectInspector inputOI;

    /* Object Inspector corresponding to the bitvector
     */
    protected transient BinaryObjectInspector ndvFieldOI;

    /* Partial aggregation result returned by TerminatePartial.
     */
    protected transient BytesWritable partialResult;

    /* Output of final result of the aggregation
     */
    protected transient BytesWritable result;

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      StringStatsAgg result = new StringStatsAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      StringStatsAgg myagg = (StringStatsAgg) agg;
      myagg.firstItem = true;
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

  public static abstract class NumericStatsAgg extends GenericUDAFEvaluator.AbstractAggregationBuffer {

    public NumDistinctValueEstimator numDV;    /* Distinct value estimator */

    @Override
    public int estimate() {
      JavaDataModel model = JavaDataModel.get();
      return (numDV == null) ?
          lengthFor(model) : numDV.lengthFor(model);
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

  @GenericUDAFEvaluator.AggregationType(estimable = true)
  public static class LongStatsAgg extends NumericStatsAgg {
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

  @GenericUDAFEvaluator.AggregationType(estimable = true)
  public static class DoubleStatsAgg extends NumericStatsAgg {
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

  @GenericUDAFEvaluator.AggregationType(estimable = true)
  public static class DecimalStatsAgg extends NumericStatsAgg {
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

  @GenericUDAFEvaluator.AggregationType(estimable = true)
  public static class DateStatsAgg extends NumericStatsAgg {
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

  @GenericUDAFEvaluator.AggregationType(estimable = true)
  public static class TimestampStatsAgg extends NumericStatsAgg {
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

  @GenericUDAFEvaluator.AggregationType(estimable = true)
  public static class StringStatsAgg extends GenericUDAFEvaluator.AbstractAggregationBuffer {
    public NumDistinctValueEstimator numDV;      /* Distinct value estimator */
    public boolean firstItem;
    @Override
    public int estimate() {
      JavaDataModel model = JavaDataModel.get();
      return (numDV == null) ?
          lengthFor(model) : numDV.lengthFor(model);      }
  };

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
