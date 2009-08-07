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
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

/**
 * This class implements the COUNT aggregation function as in SQL.
 */
@description(
    name = "count",
    value = "_FUNC_(x) - Returns the count"
)
public class GenericUDAFCount implements GenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(
      TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }
    return new GenericUDAFCountEvaluator();
  }
  
  public static class GenericUDAFCountEvaluator extends GenericUDAFEvaluator {
    ObjectInspector inputOI;
    LongWritable result; 

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      assert(parameters.length == 1);
      inputOI = parameters[0];
      result = new LongWritable(0);
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }
    
    /** class for storing count value */
    static class CountAgg implements AggregationBuffer {
      long value;
    }
    
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      CountAgg result = new CountAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((CountAgg)agg).value = 0;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      assert(parameters.length == 1);
      if (parameters[0] != null) {
        ((CountAgg)agg).value ++;
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      if (partial != null) {
        long p = ((LongObjectInspector)inputOI).get(partial);
        ((CountAgg)agg).value += p;
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      result.set(((CountAgg)agg).value);
      return result;
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg)
        throws HiveException {
      return terminate(agg);
    }

  }

}
