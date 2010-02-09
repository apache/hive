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
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * GenericUDAFAverage.
 *
 */
@Description(name = "avg", value = "_FUNC_(x) - Returns the mean of a set of numbers")
public class GenericUDAFAverage implements GenericUDAFResolver {

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
      return new GenericUDAFAverageEvaluator();
    case BOOLEAN:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string type arguments are accepted but "
          + parameters[0].getTypeName() + " is passed.");
    }
  }

  /**
   * GenericUDAFAverageEvaluator.
   *
   */
  public static class GenericUDAFAverageEvaluator extends GenericUDAFEvaluator {

    // For PARTIAL1 and COMPLETE
    PrimitiveObjectInspector inputOI;

    // For PARTIAL2 and FINAL
    StructObjectInspector soi;
    StructField countField;
    StructField sumField;
    LongObjectInspector countFieldOI;
    DoubleObjectInspector sumFieldOI;

    // For PARTIAL1 and PARTIAL2
    Object[] partialResult;

    // For FINAL and COMPLETE
    DoubleWritable result;

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
        countFieldOI = (LongObjectInspector) countField
            .getFieldObjectInspector();
        sumFieldOI = (DoubleObjectInspector) sumField.getFieldObjectInspector();
      }

      // init output
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        // The output of a partial aggregation is a struct containing
        // a "long" count and a "double" sum.

        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        ArrayList<String> fname = new ArrayList<String>();
        fname.add("count");
        fname.add("sum");
        partialResult = new Object[2];
        partialResult[0] = new LongWritable(0);
        partialResult[1] = new DoubleWritable(0);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
            foi);

      } else {
        result = new DoubleWritable(0);
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
      }
    }

    static class AverageAgg implements AggregationBuffer {
      long count;
      double sum;
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AverageAgg result = new AverageAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      AverageAgg myagg = (AverageAgg) agg;
      myagg.count = 0;
      myagg.sum = 0;
    }

    boolean warned = false;

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      assert (parameters.length == 1);
      Object p = parameters[0];
      if (p != null) {
        AverageAgg myagg = (AverageAgg) agg;
        try {
          double v = PrimitiveObjectInspectorUtils.getDouble(p, inputOI);
          myagg.count++;
          myagg.sum += v;
        } catch (NumberFormatException e) {
          if (!warned) {
            warned = true;
            LOG.warn(getClass().getSimpleName() + " "
                + StringUtils.stringifyException(e));
            LOG.warn(getClass().getSimpleName()
                + " ignoring similar exceptions.");
          }
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      AverageAgg myagg = (AverageAgg) agg;
      ((LongWritable) partialResult[0]).set(myagg.count);
      ((DoubleWritable) partialResult[1]).set(myagg.sum);
      return partialResult;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      if (partial != null) {
        AverageAgg myagg = (AverageAgg) agg;
        Object partialCount = soi.getStructFieldData(partial, countField);
        Object partialSum = soi.getStructFieldData(partial, sumField);
        myagg.count += countFieldOI.get(partialCount);
        myagg.sum += sumFieldOI.get(partialSum);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      AverageAgg myagg = (AverageAgg) agg;
      if (myagg.count == 0) {
        return null;
      } else {
        result.set(myagg.sum / myagg.count);
        return result;
      }
    }
  }

}
