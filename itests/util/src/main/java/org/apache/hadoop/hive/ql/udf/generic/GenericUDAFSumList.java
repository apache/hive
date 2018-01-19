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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.LongWritable;

/**
 * GenericUDAFSum.
 *
 */
@Description(name = "sum_list", value = "_FUNC_(x) - Returns the sum of a set of numbers")
public class GenericUDAFSumList extends AbstractGenericUDAFResolver {

  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFSumList.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
    throws SemanticException {
    ObjectInspector[] inspectors = info.getParameterObjectInspectors();
    if (inspectors.length != 1) {
      throw new UDFArgumentTypeException(inspectors.length - 1,
          "Exactly one argument is expected.");
    }

    if (inspectors[0].getCategory() != ObjectInspector.Category.LIST) {
      throw new UDFArgumentTypeException(0, "Argument should be a list type");
    }

    ListObjectInspector listOI = (ListObjectInspector) inspectors[0];
    ObjectInspector elementOI = listOI.getListElementObjectInspector();

    if (elementOI.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
          + elementOI.getTypeName() + " is passed.");
    }
    PrimitiveObjectInspector.PrimitiveCategory pcat =
        ((PrimitiveObjectInspector)elementOI).getPrimitiveCategory();
    return new GenericUDAFSumLong();
  }

  /**
   * GenericUDAFSumLong.
   *
   */
  public static class GenericUDAFSumLong extends GenericUDAFEvaluator {
    private ListObjectInspector listOI;
    private PrimitiveObjectInspector elementOI;
    private ObjectInspectorConverters.Converter toLong;
    private PrimitiveObjectInspector inputOI;
    private LongWritable result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);
      result = new LongWritable(0);
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        listOI = (ListObjectInspector) parameters[0];
        elementOI = (PrimitiveObjectInspector) listOI.getListElementObjectInspector();
        toLong = ObjectInspectorConverters.getConverter(elementOI,
            PrimitiveObjectInspectorFactory.javaLongObjectInspector);
      } else {
        inputOI = (PrimitiveObjectInspector) parameters[0];
      }
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

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 1);
      SumLongAgg myagg = (SumLongAgg) agg;
      int length = listOI.getListLength(parameters[0]);
      for (int i = 0; i < length; i++) {
        Object element = listOI.getListElement(parameters[0], i);
        if (element != null) {
          myagg.sum += (Long)toLong.convert(element);
          myagg.empty = false;
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
