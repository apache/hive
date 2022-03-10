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

import org.apache.hadoop.hive.common.ndv.fm.FMSketch;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
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
 * FMSketch algorithm. The ndv_compute_bit_vector function can
 * be used on top of it to extract an estimate of the ndv from it.
 */
@Description(name = "compute_bit_vector_fm",
    value = "_FUNC_(x) - Computes bit vector for NDV computation.")
public class GenericUDAFComputeBitVectorFMSketch extends GenericUDAFComputeBitVectorBase {

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 2 ) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly 2 (col + #bitvectors) arguments are expected.");
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

  public static abstract class NumericStatsEvaluator<V, OI extends PrimitiveObjectInspector>
      extends NumericStatsEvaluatorBase {

    protected final static int MAX_BIT_VECTORS = 1024;

    protected transient PrimitiveObjectInspector numVectorsOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // initialize input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        numVectorsOI = (PrimitiveObjectInspector) parameters[1];
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
        if (parameters.length == 2) {
          numVectors = parameters[1] == null ? 0 : PrimitiveObjectInspectorUtils.getInt(
              parameters[1], numVectorsOI);
          if (numVectors > MAX_BIT_VECTORS) {
            throw new HiveException("The maximum allowed value for number of bit vectors " + " is "
                + MAX_BIT_VECTORS + ", but was passed " + numVectors + " bit vectors");
          }
        }
        myagg.numDV = new FMSketch(numVectors);
      }

      if (parameters[0] != null) {
        myagg.update(parameters[0], inputOI);
      }
    }
  }

  public static class GenericUDAFStringStatsEvaluator extends StringStatsEvaluatorBase {

    private final static int MAX_BIT_VECTORS = 1024;

    private transient PrimitiveObjectInspector funcOI;
    private transient PrimitiveObjectInspector numVectorsOI;

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
      Object p = parameters[0];
      StringStatsAgg myagg = (StringStatsAgg) agg;

      if (myagg.firstItem) {
        int numVectors = 0;
        if (parameters.length > 1) {
          numVectors = PrimitiveObjectInspectorUtils.getInt(parameters[1], numVectorsOI);
          if (numVectors > MAX_BIT_VECTORS) {
            throw new HiveException("The maximum allowed value for number of bit vectors " + " is "
                + MAX_BIT_VECTORS + " , but was passed " + numVectors + " bit vectors");
          }
        }

        myagg.numDV = new FMSketch(numVectors);
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

  public static class GenericUDAFLongStatsEvaluator
      extends NumericStatsEvaluator<Long, LongObjectInspector> {

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new GenericUDAFComputeBitVectorBase.LongStatsAgg();
      reset(result);
      return result;
    }
  }

  public static class GenericUDAFDoubleStatsEvaluator
      extends NumericStatsEvaluator<Double, DoubleObjectInspector> {

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new GenericUDAFComputeBitVectorBase.DoubleStatsAgg();
      reset(result);
      return result;
    }
  }

  public static class GenericUDAFDecimalStatsEvaluator
      extends NumericStatsEvaluator<HiveDecimal, HiveDecimalObjectInspector> {

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new GenericUDAFComputeBitVectorBase.DecimalStatsAgg();
      reset(result);
      return result;
    }
  }

  public static class GenericUDAFDateStatsEvaluator
      extends NumericStatsEvaluator<DateWritableV2, DateObjectInspector> {

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new GenericUDAFComputeBitVectorBase.DateStatsAgg();
      reset(result);
      return result;
    }
  }

  public static class GenericUDAFTimestampStatsEvaluator
      extends NumericStatsEvaluator<TimestampWritableV2, TimestampObjectInspector> {

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer result = new GenericUDAFComputeBitVectorBase.TimestampStatsAgg();
      reset(result);
      return result;
    }
  }

}
