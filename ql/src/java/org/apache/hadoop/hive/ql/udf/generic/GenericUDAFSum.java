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

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

/**
 * This class implements the COUNT aggregation function as in SQL.
 */
public class GenericUDAFSum implements GenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(
      TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }
    
    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
    }
    switch (((PrimitiveTypeInfo)parameters[0]).getPrimitiveCategory()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new GenericUDAFSumLong();
      case FLOAT:
      case DOUBLE:
      case STRING:
        return new GenericUDAFSumDouble();
      case BOOLEAN:
      default:
        throw new UDFArgumentTypeException(0,
            "Only numeric or string type arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
    }
  }
  
  public static class GenericUDAFSumDouble extends GenericUDAFEvaluator {

    PrimitiveObjectInspector inputOI;
    DoubleWritable result;
    
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      assert(parameters.length == 1);
      super.init(m, parameters);
      result = new DoubleWritable(0);
      inputOI = (PrimitiveObjectInspector)parameters[0];
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    /** class for storing double sum value */
    static class SumDoubleAgg implements AggregationBuffer {
      boolean empty;
      double sum;
    }
    
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      SumDoubleAgg result = new SumDoubleAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      SumDoubleAgg myagg = (SumDoubleAgg)agg;
      myagg.empty = true;
      myagg.sum = 0;      
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert(parameters.length == 1);
      merge(agg, parameters[0]);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return terminate(agg);
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        SumDoubleAgg myagg = (SumDoubleAgg)agg;
        myagg.empty = false;
        myagg.sum += PrimitiveObjectInspectorUtils.getDouble(partial, inputOI); 
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      SumDoubleAgg myagg = (SumDoubleAgg)agg;
      if (myagg.empty) {
        return null;
      }
      result.set(myagg.sum);
      return result;
    }

  }
  

  public static class GenericUDAFSumLong extends GenericUDAFEvaluator {

    PrimitiveObjectInspector inputOI;
    LongWritable result;
    
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      assert(parameters.length == 1);
      super.init(m, parameters);
      result = new LongWritable(0);
      inputOI = (PrimitiveObjectInspector)parameters[0];
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    /** class for storing double sum value */
    static class SumLongAgg implements AggregationBuffer {
      boolean empty;
      long sum;
    }
    
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      SumLongAgg result = new SumLongAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      SumLongAgg myagg = (SumLongAgg)agg;
      myagg.empty = true;
      myagg.sum = 0;      
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert(parameters.length == 1);
      merge(agg, parameters[0]);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return terminate(agg);
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        SumLongAgg myagg = (SumLongAgg)agg;
        myagg.empty = false;
        myagg.sum += PrimitiveObjectInspectorUtils.getLong(partial, inputOI); 
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      SumLongAgg myagg = (SumLongAgg)agg;
      if (myagg.empty) {
        return null;
      }
      result.set(myagg.sum);
      return result;
    }

  }
  

}
