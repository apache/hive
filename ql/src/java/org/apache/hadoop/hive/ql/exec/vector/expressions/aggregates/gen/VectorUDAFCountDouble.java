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

package org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.
    VectorAggregateExpression.AggregationBuffer;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

/**
* VectorUDAFCountDouble. Vectorized implementation for COUNT aggregates. 
*/
@Description(name = "count", value = "_FUNC_(expr) - Returns the maximum value of expr (vectorized, type: double)")
public class VectorUDAFCountDouble extends VectorAggregateExpression {
    
    /** 
    /* class for storing the current aggregate value.
    */
    static class Aggregation implements AggregationBuffer {
      long value;
      boolean isNull;
    }
    
    private VectorExpression inputExpression;
  private LongWritable result;    
    
    public VectorUDAFCountDouble(VectorExpression inputExpression) {
      super();
      this.inputExpression = inputExpression;
      result = new LongWritable(0);
    }
    
    @Override
    public void aggregateInput(AggregationBuffer agg, VectorizedRowBatch unit) 
    throws HiveException {
      
      inputExpression.evaluate(unit);
      
      DoubleColumnVector inputVector = (DoubleColumnVector)unit.
        cols[this.inputExpression.getOutputColumn()];
      
      int batchSize = unit.size;
      
      if (batchSize == 0) {
        return;
      }
      
      Aggregation myagg = (Aggregation)agg;

      if (myagg.isNull) {
        myagg.value = 0;
        myagg.isNull = false;
      }
      
      if (inputVector.isRepeating) {
        if (inputVector.noNulls || !inputVector.isNull[0]) {
          myagg.value += batchSize;
        }
        return;
      }
      
      if (inputVector.noNulls) {
        myagg.value += batchSize;
        return;
      }
      else if (!unit.selectedInUse) {
        iterateNoSelectionHasNulls(myagg, batchSize, inputVector.isNull);
      }
      else {
        iterateSelectionHasNulls(myagg, batchSize, inputVector.isNull, unit.selected);
      }
    }
  
    private void iterateSelectionHasNulls(
        Aggregation myagg, 
        int batchSize,
        boolean[] isNull, 
        int[] selected) {
      
      for (int j=0; j< batchSize; ++j) {
        int i = selected[j];
        if (!isNull[i]) {
          myagg.value += 1;
        }
      }
    }

    private void iterateNoSelectionHasNulls(
        Aggregation myagg, 
        int batchSize, 
        boolean[] isNull) {
      
      for (int i=0; i< batchSize; ++i) {
        if (!isNull[i]) {
          myagg.value += 1;
        }
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new Aggregation();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      Aggregation myAgg = (Aggregation) agg;
      myAgg.isNull = true;
    }

    @Override
    public Object evaluateOutput(AggregationBuffer agg) throws HiveException {
    Aggregation myagg = (Aggregation) agg;
      if (myagg.isNull) {
      return null;    
      }
      else {
        result.set (myagg.value);
      return result;
      }
    }

    @Override
    public ObjectInspector getOutputObjectInspector() {
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

}

