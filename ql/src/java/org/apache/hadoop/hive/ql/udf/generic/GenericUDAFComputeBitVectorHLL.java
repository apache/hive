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

import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedUDAFs;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFComputeBitVectorDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFComputeBitVectorDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFComputeBitVectorFinal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFComputeBitVectorLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFComputeBitVectorString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFComputeBitVectorTimestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.stats.ColStatsProcessor.ColumnStatsType;
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

/**
 * GenericUDAFComputeBitVector. This UDAF will compute a bit vector using
 * HyperLogLog algorithm. The ndv_compute_bit_vector function can
 * be used on top of it to extract an estimate of the ndv from it.
 */
@Description(name = "compute_bit_vector_hll",
    value = "_FUNC_(x) - Computes bit vector for NDV computation.")
public class GenericUDAFComputeBitVectorHLL extends GenericUDAFComputeBitVectorBase {

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 1 ) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly 1 (col) argument is expected.");
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
      extends NumericStatsEvaluatorBase {

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // initialize input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
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
      GenericUDAFComputeBitVectorBase.NumericStatsAgg myagg = (GenericUDAFComputeBitVectorBase.NumericStatsAgg) agg;

      if (myagg.numDV == null) {
        myagg.numDV = HyperLogLog.builder().setSizeOptimized().build();
      }

      if (parameters[0] != null) {
        myagg.update(parameters[0], inputOI);
      }
    }
  }

  @VectorizedUDAFs({
      VectorUDAFComputeBitVectorLong.class,
      VectorUDAFComputeBitVectorFinal.class
  })
  public static class GenericUDAFLongStatsEvaluator
      extends GenericUDAFNumericStatsEvaluator<Long, LongObjectInspector> {

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new GenericUDAFComputeBitVectorBase.LongStatsAgg();
      reset(result);
      return result;
    }
  }

  @VectorizedUDAFs({
      VectorUDAFComputeBitVectorDouble.class,
      VectorUDAFComputeBitVectorFinal.class
  })
  public static class GenericUDAFDoubleStatsEvaluator
      extends GenericUDAFNumericStatsEvaluator<Double, DoubleObjectInspector> {

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new GenericUDAFComputeBitVectorBase.DoubleStatsAgg();
      reset(result);
      return result;
    }
  }

  @VectorizedUDAFs({
      VectorUDAFComputeBitVectorDecimal.class,
      VectorUDAFComputeBitVectorFinal.class
  })
  public static class GenericUDAFDecimalStatsEvaluator
      extends GenericUDAFNumericStatsEvaluator<HiveDecimal, HiveDecimalObjectInspector> {

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new GenericUDAFComputeBitVectorBase.DecimalStatsAgg();
      reset(result);
      return result;
    }
  }

  @VectorizedUDAFs({
      VectorUDAFComputeBitVectorLong.class,
      VectorUDAFComputeBitVectorFinal.class
  })
  public static class GenericUDAFDateStatsEvaluator
      extends GenericUDAFNumericStatsEvaluator<DateWritableV2, DateObjectInspector> {

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new GenericUDAFComputeBitVectorBase.DateStatsAgg();
      reset(result);
      return result;
    }
  }

  @VectorizedUDAFs({
      VectorUDAFComputeBitVectorTimestamp.class,
      VectorUDAFComputeBitVectorFinal.class
  })
  public static class GenericUDAFTimestampStatsEvaluator
      extends GenericUDAFNumericStatsEvaluator<TimestampWritableV2, TimestampObjectInspector> {

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new GenericUDAFComputeBitVectorBase.TimestampStatsAgg();
      reset(result);
      return result;
    }
  }

  @VectorizedUDAFs({
      VectorUDAFComputeBitVectorString.class,
      VectorUDAFComputeBitVectorFinal.class
  })
  public static class GenericUDAFStringStatsEvaluator extends StringStatsEvaluatorBase {

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // initialize input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
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
      Object p = parameters[0];
      GenericUDAFComputeBitVectorBase.StringStatsAgg myagg = (GenericUDAFComputeBitVectorBase.StringStatsAgg) agg;

      if (myagg.firstItem) {
        myagg.numDV = HyperLogLog.builder().setSizeOptimized().build();
        myagg.numDV.reset();
        myagg.firstItem = false;
      }

      String v = PrimitiveObjectInspectorUtils.getString(p, inputOI);
      if (v != null) {
        // Add string value to NumDistinctValue Estimator
        myagg.numDV.addToEstimator(v);
      }
    }
  }
}
