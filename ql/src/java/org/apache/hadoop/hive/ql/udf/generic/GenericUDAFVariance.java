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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.description;
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
 * Compute the variance. This class is extended by:
 *   GenericUDAFVarianceSample
 *   GenericUDAFStd
 *   GenericUDAFStdSample
 *
 */
@description(
    name = "variance,var_pop",
    value = "_FUNC_(x) - Returns the variance of a set of numbers"
)
public class GenericUDAFVariance implements GenericUDAFResolver {
  
  static final Log LOG = LogFactory.getLog(GenericUDAFVariance.class.getName());
  
  @Override
  public GenericUDAFEvaluator getEvaluator(
      TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }
    
    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but " 
          + parameters[0].getTypeName() + " is passed.");
    }
    switch (((PrimitiveTypeInfo)parameters[0]).getPrimitiveCategory()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
        return new GenericUDAFVarianceEvaluator();
      case BOOLEAN:
      default:
        throw new UDFArgumentTypeException(0,
            "Only numeric or string type arguments are accepted but " 
            + parameters[0].getTypeName() + " is passed.");
    }
  }
  
  /**
   * Evaluate the variance using the following modification of the formula from
   * The Art of Computer Programming, vol. 2, p. 232:
   *   
   *   variance = variance1 + variance2 + n*alpha^2 + m*betha^2
   *   
   * where:
   *   - variance is sum[x-avg^2] (this is actually n times the variance) and is
   *   updated at every step.
   *   - n is the count of elements in chunk1
   *   - m is the count of elements in chunk2
   *   - alpha = avg-a
   *   - betha = avg-b
   *   - avg is the the average of all elements from both chunks
   *   - a is the average of elements in chunk1
   *   - b is the average of elements in chunk2
   * 
   */
  public static class GenericUDAFVarianceEvaluator extends GenericUDAFEvaluator {
    
    // For PARTIAL1 and COMPLETE
    PrimitiveObjectInspector inputOI;
    
    // For PARTIAL2 and FINAL
    StructObjectInspector soi;
    StructField countField;
    StructField sumField;
    StructField varianceField;
    LongObjectInspector countFieldOI;
    DoubleObjectInspector sumFieldOI;
    DoubleObjectInspector varianceFieldOI;
    
    // For PARTIAL1 and PARTIAL2
    Object[] partialResult;
    
    // For FINAL and COMPLETE
    DoubleWritable result;
    
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      assert(parameters.length == 1);
      super.init(m, parameters);
      
      // init input
      if (mode == mode.PARTIAL1 || mode == mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector)parameters[0];
      } else {
        soi = (StructObjectInspector)parameters[0];
        
        countField = soi.getStructFieldRef("count");
        sumField = soi.getStructFieldRef("sum");
        varianceField = soi.getStructFieldRef("variance");
        
        countFieldOI = 
          (LongObjectInspector)countField.getFieldObjectInspector();
        sumFieldOI = (DoubleObjectInspector)sumField.getFieldObjectInspector();
        varianceFieldOI = 
          (DoubleObjectInspector)varianceField.getFieldObjectInspector();
      }
      
      // init output
      if (mode == mode.PARTIAL1 || mode == mode.PARTIAL2) {
        // The output of a partial aggregation is a struct containing
        // a long count and doubles sum and variance. 
        
        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        
        ArrayList<String> fname = new ArrayList<String>();
        fname.add("count");
        fname.add("sum");
        fname.add("variance");
        
        partialResult = new Object[3];
        partialResult[0] = new LongWritable(0);
        partialResult[1] = new DoubleWritable(0);
        partialResult[2] = new DoubleWritable(0);
        
        return ObjectInspectorFactory.getStandardStructObjectInspector(
            fname, foi);
        
      } else {
        result = new DoubleWritable(0);
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
      }
    }

    static class StdAgg implements AggregationBuffer {
      long count; // number of elements
      double sum; // sum of elements
      double variance; // sum[x-avg^2] (this is actually n times the variance)
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      StdAgg result = new StdAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      StdAgg myagg = (StdAgg)agg;
      myagg.count = 0;
      myagg.sum = 0;     
      myagg.variance = 0;
    }
    
    boolean warned = false;
    
    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) 
    throws HiveException {
      assert(parameters.length == 1);
      Object p = parameters[0];
      if (p != null) {
        StdAgg myagg = (StdAgg)agg;
        try {
          double v = PrimitiveObjectInspectorUtils.getDouble(p, 
              (PrimitiveObjectInspector)inputOI);
         
          if(myagg.count != 0) { // if count==0 => the variance is going to be 0
                                // after 1 iteration
            double alpha = (myagg.sum + v) / (myagg.count+1) 
                            - myagg.sum / myagg.count;
            double betha = (myagg.sum + v) / (myagg.count+1) - v;
            
            // variance = variance1 + variance2 + n*alpha^2 + m*betha^2
            // => variance += n*alpha^2 + betha^2
            myagg.variance += myagg.count*alpha*alpha + betha*betha;
          }
          myagg.count++;
          myagg.sum += v;
        } catch (NumberFormatException e) {
          if (!warned) {
            warned = true;
            LOG.warn(getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
            LOG.warn(getClass().getSimpleName() + " ignoring similar exceptions.");
          }
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      StdAgg myagg = (StdAgg)agg;
      ((LongWritable)partialResult[0]).set(myagg.count);
      ((DoubleWritable)partialResult[1]).set(myagg.sum);
      ((DoubleWritable)partialResult[2]).set(myagg.variance);
      return partialResult;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        StdAgg myagg = (StdAgg)agg;
        
        Object partialCount = soi.getStructFieldData(partial, countField);
        Object partialSum = soi.getStructFieldData(partial, sumField);
        Object partialVariance = soi.getStructFieldData(partial, varianceField);
        
        long n = myagg.count;
        long m = countFieldOI.get(partialCount);
        
        if(n == 0) {
          // Just copy the information since there is nothing so far
          myagg.variance = sumFieldOI.get(partialVariance);
          myagg.count = countFieldOI.get(partialCount);
          myagg.sum = sumFieldOI.get(partialSum);
        }
        
        if(m != 0 && n != 0) {
          // Merge the two partials
          
          double a = myagg.sum;
          double b = sumFieldOI.get(partialSum);
          
          double alpha = (a+b)/(n+m) - a/n;
          double betha = (a+b)/(n+m) - b/m;
          
          // variance = variance1 + variance2 + n*alpha^2 + m*betha^2
          myagg.variance += sumFieldOI.get(partialVariance)
                            + (n*alpha*alpha + m*betha*betha);
          myagg.count += m;
          myagg.sum += b;
        }
        
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      StdAgg myagg = (StdAgg)agg;
      
      if (myagg.count == 0) { // SQL standard - return null for zero elements
        return null;
      } else {
        if(myagg.count > 1) { 
          result.set(myagg.variance / (myagg.count)); 
        } else { // for one element the variance is always 0
          result.set(0);
        }
        return result;
      }
    }
  }

}
