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
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;    
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.io.LongWritable;
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

/**
* VectorUDAFMaxDouble. Vectorized implementation for MIN/MAX aggregates. 
*/
@Description(name = "max", value = "_FUNC_(expr) - Returns the maximum value of expr (vectorized, type: double)")
public class VectorUDAFMaxDouble extends VectorAggregateExpression {
    
    /** 
    /* class for storing the current aggregate value.
    */
    static private final class Aggregation implements AggregationBuffer {
      double value;
      boolean isNull;

      public void checkValue(double value) {
        if (isNull) {
          isNull = false;
          this.value = value;
        } else if (value > this.value) {
          this.value = value;
        }
      }

      @Override
      public int getVariableSize() {
        throw new UnsupportedOperationException();
      }
    }
    
    private VectorExpression inputExpression;
    private VectorExpressionWriter resultWriter;
    
    public VectorUDAFMaxDouble(VectorExpression inputExpression) {
      super();
      this.inputExpression = inputExpression;
    }
    
    @Override
    public void init(AggregationDesc desc) throws HiveException {
      resultWriter = VectorExpressionWriterFactory.genVectorExpressionWritable(
          desc.getParameters().get(0));
    }
    
    private Aggregation getCurrentAggregationBuffer(
        VectorAggregationBufferRow[] aggregationBufferSets,
        int aggregrateIndex,
        int row) {
      VectorAggregationBufferRow mySet = aggregationBufferSets[row];
      Aggregation myagg = (Aggregation) mySet.getAggregationBuffer(aggregrateIndex);
      return myagg;
    }
    
@Override
    public void aggregateInputSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregrateIndex, 
      VectorizedRowBatch batch) throws HiveException {
      
      int batchSize = batch.size;
      
      if (batchSize == 0) {
        return;
      }
      
      inputExpression.evaluate(batch);
      
      DoubleColumnVector inputVector = (DoubleColumnVector)batch.
        cols[this.inputExpression.getOutputColumn()];
      double[] vector = inputVector.vector;

      if (inputVector.noNulls) {
        if (inputVector.isRepeating) {
          iterateNoNullsRepeatingWithAggregationSelection(
            aggregationBufferSets, aggregrateIndex,
            vector[0], batchSize);
        } else {
          if (batch.selectedInUse) {
            iterateNoNullsSelectionWithAggregationSelection(
              aggregationBufferSets, aggregrateIndex,
              vector, batch.selected, batchSize);
          } else {
            iterateNoNullsWithAggregationSelection(
              aggregationBufferSets, aggregrateIndex,
              vector, batchSize);
          }
        }
      } else {
        if (inputVector.isRepeating) {
          if (batch.selectedInUse) {
            iterateHasNullsRepeatingSelectionWithAggregationSelection(
              aggregationBufferSets, aggregrateIndex,
              vector[0], batchSize, batch.selected, inputVector.isNull);
          } else {
            iterateHasNullsRepeatingWithAggregationSelection(
              aggregationBufferSets, aggregrateIndex,
              vector[0], batchSize, inputVector.isNull);
          }
        } else {
          if (batch.selectedInUse) {
            iterateHasNullsSelectionWithAggregationSelection(
              aggregationBufferSets, aggregrateIndex,
              vector, batchSize, batch.selected, inputVector.isNull);
          } else {
            iterateHasNullsWithAggregationSelection(
              aggregationBufferSets, aggregrateIndex,
              vector, batchSize, inputVector.isNull);
          }
        }
      }
    }

    private void iterateNoNullsRepeatingWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregrateIndex,
      double value,
      int batchSize) {

      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets, 
          aggregrateIndex,
          i);
        myagg.checkValue(value);
      }
    } 

    private void iterateNoNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregrateIndex,
      double[] values,
      int[] selection,
      int batchSize) {
      
      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets, 
          aggregrateIndex,
          i);
        myagg.checkValue(values[selection[i]]);
      }
    }

    private void iterateNoNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregrateIndex,
      double[] values,
      int batchSize) {
      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets, 
          aggregrateIndex,
          i);
        myagg.checkValue(values[i]);
      }
    }

    private void iterateHasNullsRepeatingSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregrateIndex,
      double value,
      int batchSize,
      int[] selection,
      boolean[] isNull) {
      
      for (int i=0; i < batchSize; ++i) {
        if (!isNull[selection[i]]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets, 
            aggregrateIndex,
            i);
          myagg.checkValue(value);
        }
      }
      
    }

    private void iterateHasNullsRepeatingWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregrateIndex,
      double value,
      int batchSize,
      boolean[] isNull) {

      for (int i=0; i < batchSize; ++i) {
        if (!isNull[i]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets, 
            aggregrateIndex,
            i);
          myagg.checkValue(value);
        }
      }
    }

    private void iterateHasNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregrateIndex,
      double[] values,
      int batchSize,
      int[] selection,
      boolean[] isNull) {

      for (int j=0; j < batchSize; ++j) {
        int i = selection[j];
        if (!isNull[i]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets, 
            aggregrateIndex,
            j);
          myagg.checkValue(values[i]);
        }
      }
   }

    private void iterateHasNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregrateIndex,
      double[] values,
      int batchSize,
      boolean[] isNull) {

      for (int i=0; i < batchSize; ++i) {
        if (!isNull[i]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets, 
            aggregrateIndex,
            i);
          myagg.checkValue(values[i]);
        }
      }
   }
    
    @Override
    public void aggregateInput(AggregationBuffer agg, VectorizedRowBatch batch) 
      throws HiveException {
        
        inputExpression.evaluate(batch);
        
        DoubleColumnVector inputVector = (DoubleColumnVector)batch.
            cols[this.inputExpression.getOutputColumn()];
        
        int batchSize = batch.size;
        
        if (batchSize == 0) {
          return;
        }
        
        Aggregation myagg = (Aggregation)agg;
  
        double[] vector = inputVector.vector;
        
        if (inputVector.isRepeating) {
          if ((inputVector.noNulls || !inputVector.isNull[0]) &&
            myagg.isNull || vector[0] < myagg.value) {
            myagg.isNull = false;
            myagg.value = vector[0];
          }
          return;
        }
        
        if (!batch.selectedInUse && inputVector.noNulls) {
          iterateNoSelectionNoNulls(myagg, vector, batchSize);
        }
        else if (!batch.selectedInUse) {
          iterateNoSelectionHasNulls(myagg, vector, batchSize, inputVector.isNull);
        }
        else if (inputVector.noNulls){
          iterateSelectionNoNulls(myagg, vector, batchSize, batch.selected);
        }
        else {
          iterateSelectionHasNulls(myagg, vector, batchSize, inputVector.isNull, batch.selected);
        }
    }
  
    private void iterateSelectionHasNulls(
        Aggregation myagg, 
        double[] vector, 
        int batchSize,
        boolean[] isNull, 
        int[] selected) {
      
      for (int j=0; j< batchSize; ++j) {
        int i = selected[j];
        if (!isNull[i]) {
          double value = vector[i];
          if (myagg.isNull) {
            myagg.isNull = false;
            myagg.value = value;
          }
          else if (value > myagg.value) {
            myagg.value = value;
          }
        }
      }
    }

    private void iterateSelectionNoNulls(
        Aggregation myagg, 
        double[] vector, 
        int batchSize, 
        int[] selected) {
      
      if (myagg.isNull) {
        myagg.value = vector[selected[0]];
        myagg.isNull = false;
      }
      
      for (int i=0; i< batchSize; ++i) {
        double value = vector[selected[i]];
        if (value > myagg.value) {
          myagg.value = value;
        }
      }
    }

    private void iterateNoSelectionHasNulls(
        Aggregation myagg, 
        double[] vector, 
        int batchSize,
        boolean[] isNull) {
      
      for(int i=0;i<batchSize;++i) {
        if (!isNull[i]) {
          double value = vector[i];
          if (myagg.isNull) { 
            myagg.value = value;
            myagg.isNull = false;
          }
          else if (value > myagg.value) {
            myagg.value = value;
          }
        }
      }
    }

    private void iterateNoSelectionNoNulls(
        Aggregation myagg, 
        double[] vector, 
        int batchSize) {
      if (myagg.isNull) {
        myagg.value = vector[0];
        myagg.isNull = false;
      }
      
      for (int i=0;i<batchSize;++i) {
        double value = vector[i];
        if (value > myagg.value) {
          myagg.value = value;
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
    public Object evaluateOutput(
        AggregationBuffer agg) throws HiveException {
    Aggregation myagg = (Aggregation) agg;
      if (myagg.isNull) {
        return null;
      }
      else {
        return resultWriter.writeValue(myagg.value);
      }
    }
    
    @Override
    public ObjectInspector getOutputObjectInspector() {
      return resultWriter.getObjectInspector();
    }

    @Override
    public int getAggregationBufferFixedSize() {
    JavaDataModel model = JavaDataModel.get();
    return JavaDataModel.alignUp(
      model.object() +
      model.primitive2(),
      model.memoryAlign());
  }

}

