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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * GenericUDAFSum.
 *
 */
@Description(name = "sum", value = "_FUNC_(x) - Returns the sum of a set of numbers")
public class GenericUDAFSum extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(GenericUDAFSum.class.getName());

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

  /**
   * GenericUDAFSumHiveDecimal.
   *
   */
  public static class GenericUDAFSumHiveDecimal extends GenericUDAFEvaluator {
    private PrimitiveObjectInspector inputOI;
    private HiveDecimalWritable result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);
      result = new HiveDecimalWritable(HiveDecimal.ZERO);
      inputOI = (PrimitiveObjectInspector) parameters[0];
      // The output precision is 10 greater than the input which should cover at least
      // 10b rows. The scale is the same as the input.
      DecimalTypeInfo outputTypeInfo = null;
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        int precision = Math.min(HiveDecimal.MAX_PRECISION, inputOI.precision() + 10);
        outputTypeInfo = TypeInfoFactory.getDecimalTypeInfo(precision, inputOI.scale());
      } else {
        outputTypeInfo = (DecimalTypeInfo) inputOI.getTypeInfo();
      }
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(outputTypeInfo);
    }

    /** class for storing decimal sum value. */
    @AggregationType(estimable = false) // hard to know exactly for decimals
    static class SumHiveDecimalAgg extends AbstractAggregationBuffer {
      boolean empty;
      HiveDecimal sum;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      SumHiveDecimalAgg agg = new SumHiveDecimalAgg();
      reset(agg);
      return agg;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      SumHiveDecimalAgg bdAgg = (SumHiveDecimalAgg) agg;
      bdAgg.empty = true;
      bdAgg.sum = HiveDecimal.ZERO;
    }

    boolean warned = false;

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 1);
      try {
        merge(agg, parameters[0]);
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
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return terminate(agg);
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        SumHiveDecimalAgg myagg = (SumHiveDecimalAgg) agg;
        if (myagg.sum == null) {
          return;
        }

        myagg.empty = false;
        myagg.sum = myagg.sum.add(PrimitiveObjectInspectorUtils.getHiveDecimal(partial, inputOI));
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      SumHiveDecimalAgg myagg = (SumHiveDecimalAgg) agg;
      if (myagg.empty || myagg.sum == null) {
        return null;
      }
      result.set(myagg.sum);
      return result;
    }

  }

  /**
   * GenericUDAFSumDouble.
   *
   */
  public static class GenericUDAFSumDouble extends GenericUDAFEvaluator {
    private PrimitiveObjectInspector inputOI;
    private DoubleWritable result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);
      result = new DoubleWritable(0);
      inputOI = (PrimitiveObjectInspector) parameters[0];
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    /** class for storing double sum value. */
    @AggregationType(estimable = true)
    static class SumDoubleAgg extends AbstractAggregationBuffer {
      boolean empty;
      double sum;
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
      myagg.sum = 0;
    }

    boolean warned = false;

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 1);
      try {
        merge(agg, parameters[0]);
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
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return terminate(agg);
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        SumDoubleAgg myagg = (SumDoubleAgg) agg;
        myagg.empty = false;
        myagg.sum += PrimitiveObjectInspectorUtils.getDouble(partial, inputOI);
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

  }

  /**
   * GenericUDAFSumLong.
   *
   */
  public static class GenericUDAFSumLong extends GenericUDAFEvaluator {
    private PrimitiveObjectInspector inputOI;
    private LongWritable result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);
      result = new LongWritable(0);
      inputOI = (PrimitiveObjectInspector) parameters[0];
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    /** class for storing double sum value. */
    @AggregationType(estimable = true)
    static class SumLongAgg extends AbstractAggregationBuffer {
      boolean empty;
      long sum;
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
      myagg.sum = 0;
    }

    private boolean warned = false;

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 1);
      try {
        merge(agg, parameters[0]);
      } catch (NumberFormatException e) {
        if (!warned) {
          warned = true;
          LOG.warn(getClass().getSimpleName() + " "
              + StringUtils.stringifyException(e));
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return terminate(agg);
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        SumLongAgg myagg = (SumLongAgg) agg;
        myagg.sum += PrimitiveObjectInspectorUtils.getLong(partial, inputOI);
        myagg.empty = false;
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

  }

}
