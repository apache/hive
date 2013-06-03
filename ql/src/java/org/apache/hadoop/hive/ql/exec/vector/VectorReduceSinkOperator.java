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

package org.apache.hadoop.hive.ql.exec.vector;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.TerminalOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class VectorReduceSinkOperator extends TerminalOperator<ReduceSinkDesc>
  implements Serializable {

  private static final Log LOG = LogFactory.getLog(
      VectorReduceSinkOperator.class.getName());

  private static final long serialVersionUID = 1L;

  private final VectorizationContext vContext;

  /**
   * The evaluators for the key columns. Key columns decide the sort order on
   * the reducer side. Key columns are passed to the reducer in the "key".
   */
  protected transient VectorExpression[] keyEval;
  
  /**
   * The key value writers. These know how to write the necessary writable type
   * based on key column metadata, from the primitive vector type.
   */
  protected transient VectorExpressionWriter[] keyWriters;
  
  /**
   * The evaluators for the value columns. Value columns are passed to reducer
   * in the "value".
   */
  protected transient VectorExpression[] valueEval;
  
  /**
   * The output value writers. These know how to write the necessary writable type
   * based on value column metadata, from the primitive vector type.
   */
  protected transient VectorExpressionWriter[] valueWriters;

  /**
   * The evaluators for the partition columns (CLUSTER BY or DISTRIBUTE BY in
   * Hive language). Partition columns decide the reducer that the current row
   * goes to. Partition columns are not passed to reducer.
   */
  protected transient VectorExpression[] partitionEval;
  
  /**
   * The partition value writers. These know how to write the necessary writable type
   * based on partition column metadata, from the primitive vector type.
   */  
  protected transient VectorExpressionWriter[] partitionWriters;

  private int numDistributionKeys;

  private List<List<Integer>> distinctColIndices;

  private int numDistinctExprs;

  transient HiveKey keyWritable = new HiveKey();
  transient Writable value;

  transient Object[] cachedValues;
  transient Object[][] cachedKeys;
  transient Random random;

  transient Serializer keySerializer;
  transient boolean keyIsText;
  transient Serializer valueSerializer;
  transient int tag;
  transient byte[] tagByte = new byte[1];

  transient ObjectInspector keyObjectInspector;
  transient ObjectInspector valueObjectInspector;
  transient ObjectInspector[] partitionObjectInspectors;
  transient int [] keyHashCode = new int [VectorizedRowBatch.DEFAULT_SIZE];


  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      vContext.setOperatorType(OperatorType.REDUCESINK);
      keyEval = vContext.getVectorExpressions(conf.getKeyCols());
      valueEval = vContext.getVectorExpressions(conf.getValueCols());
      partitionEval = vContext.getVectorExpressions(conf.getPartitionCols());

      numDistributionKeys = conf.getNumDistributionKeys();
      distinctColIndices = conf.getDistinctColumnIndices();
      numDistinctExprs = distinctColIndices.size();

      TableDesc keyTableDesc = conf.getKeySerializeInfo();
      keySerializer = (Serializer) keyTableDesc.getDeserializerClass()
          .newInstance();
      keySerializer.initialize(null, keyTableDesc.getProperties());
      keyIsText = keySerializer.getSerializedClass().equals(Text.class);
      
      /*
       * Compute and assign the key writers and the key object inspector 
       */
      VectorExpressionWriterFactory.processVectorExpressions(
          conf.getKeyCols(), 
          conf.getOutputKeyColumnNames(),
          new VectorExpressionWriterFactory.Closure() {
            @Override
            public void assign(VectorExpressionWriter[] writers,
              ObjectInspector objectInspector) {
              keyWriters = writers;
              keyObjectInspector = objectInspector;
            }
          });
      
      String colNames = "";
      for(String colName : conf.getOutputKeyColumnNames()) {
        colNames = String.format("%s %s", colNames, colName);
      }

      LOG.info(String.format("keyObjectInspector [%s]%s => %s",
          keyObjectInspector.getClass(),
          keyObjectInspector,
          colNames));

      partitionWriters = VectorExpressionWriterFactory.getExpressionWriters(conf.getPartitionCols());
      
      TableDesc valueTableDesc = conf.getValueSerializeInfo();
      valueSerializer = (Serializer) valueTableDesc.getDeserializerClass()
          .newInstance();
      valueSerializer.initialize(null, valueTableDesc.getProperties());
      
      /*
       * Compute and assign the value writers and the value object inspector
       */
      VectorExpressionWriterFactory.processVectorExpressions(
          conf.getValueCols(),
          conf.getOutputValueColumnNames(),
          new VectorExpressionWriterFactory.Closure() {
            @Override
            public void assign(VectorExpressionWriter[] writers,
                ObjectInspector objectInspector) {
                valueWriters = writers;
                valueObjectInspector = objectInspector;
              }
          });

      colNames = "";
      for(String colName : conf.getOutputValueColumnNames()) {
        colNames = String.format("%s %s", colNames, colName);
      }

      LOG.info(String.format("valueObjectInspector [%s]%s => %s",
          valueObjectInspector.getClass(),
          valueObjectInspector,
          colNames));

      int numKeys = numDistinctExprs > 0 ? numDistinctExprs : 1;
      int keyLen = numDistinctExprs > 0 ? numDistributionKeys + 1 :
        numDistributionKeys;
      cachedKeys = new Object[numKeys][keyLen];
      cachedValues = new Object[valueEval.length];

    } catch(Exception e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    VectorizedRowBatch vrg = (VectorizedRowBatch) row;

    LOG.info(String.format("sinking %d rows, %d values, %d keys, %d parts",
        vrg.size,
        valueEval.length,
        keyEval.length,
        partitionEval.length));

    try {

      for (int i = 0; i < partitionEval.length; i++) {
        partitionEval[i].evaluate(vrg);
      }

      // run the vector evaluations
      for (int i = 0; i < valueEval.length; i++) {
         valueEval[i].evaluate(vrg);
      }
      // Evaluate the keys
      for (int i = 0; i < keyEval.length; i++) {
        keyEval[i].evaluate(vrg);
      }

      Object[] distributionKeys = new Object[numDistributionKeys];

      // Emit a (k,v) pair for each row in the batch
      //
      for (int j = 0 ; j < vrg.size; ++j) {
        int rowIndex = j;
        if (vrg.selectedInUse) {
          rowIndex = vrg.selected[j];
        }
        for (int i = 0; i < valueEval.length; i++) {
          int batchColumn = valueEval[i].getOutputColumn();
          ColumnVector vectorColumn = vrg.cols[batchColumn];
          cachedValues[i] = valueWriters[i].writeValue(vectorColumn, rowIndex);
        }
        // Serialize the value
        value = valueSerializer.serialize(cachedValues, valueObjectInspector);

        for (int i = 0; i < keyEval.length; i++) {
          int batchColumn = keyEval[i].getOutputColumn();
          ColumnVector vectorColumn = vrg.cols[batchColumn];
          distributionKeys[i] = keyWriters[i].writeValue(vectorColumn, rowIndex);
        }
        // no distinct key
        System.arraycopy(distributionKeys, 0, cachedKeys[0], 0, numDistributionKeys);
        // Serialize the keys and append the tag
        for (int i = 0; i < cachedKeys.length; i++) {
          if (keyIsText) {
            Text key = (Text) keySerializer.serialize(cachedKeys[i],
                keyObjectInspector);
            if (tag == -1) {
              keyWritable.set(key.getBytes(), 0, key.getLength());
            } else {
              int keyLength = key.getLength();
              keyWritable.setSize(keyLength + 1);
              System.arraycopy(key.getBytes(), 0, keyWritable.get(), 0, keyLength);
              keyWritable.get()[keyLength] = tagByte[0];
            }
          } else {
            // Must be BytesWritable
            BytesWritable key = (BytesWritable) keySerializer.serialize(
                cachedKeys[i], keyObjectInspector);
            if (tag == -1) {
              keyWritable.set(key.getBytes(), 0, key.getLength());
            } else {
              int keyLength = key.getLength();
              keyWritable.setSize(keyLength + 1);
              System.arraycopy(key.getBytes(), 0, keyWritable.get(), 0, keyLength);
              keyWritable.get()[keyLength] = tagByte[0];
            }
          }
          // Evaluate the HashCode
          int keyHashCode = 0;
          if (partitionEval.length == 0) {
            // If no partition cols, just distribute the data uniformly to provide
            // better
            // load balance. If the requirement is to have a single reducer, we
            // should set
            // the number of reducers to 1.
            // Use a constant seed to make the code deterministic.
            if (random == null) {
              random = new Random(12345);
            }
            keyHashCode = random.nextInt();
          } else {
            for (int p = 0; p < partitionEval.length; p++) {
              ColumnVector columnVector = vrg.cols[partitionEval[p].getOutputColumn()];
              Object partitionValue = partitionWriters[p].writeValue(columnVector, rowIndex);
              keyHashCode = keyHashCode
                  * 31
                  + ObjectInspectorUtils.hashCode(
                      partitionValue,
                      partitionWriters[p].getObjectInspector());
            }
          }
          keyWritable.setHashCode(keyHashCode);
          if (out != null) {
            out.collect(keyWritable, value);
            // Since this is a terminal operator, update counters explicitly -
            // forward is not called
            if (counterNameToEnum != null) {
              ++outputRows;
              if (outputRows % 1000 == 0) {
                incrCounter(numOutputRowsCntr, outputRows);
                outputRows = 0;
              }
            }
          }
        }
      }
    } catch (SerDeException e) {
      throw new HiveException(e);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  public VectorReduceSinkOperator (
      VectorizationContext context,
      OperatorDesc conf) {
    this.vContext = context;
    this.conf = (ReduceSinkDesc) conf;
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "RS";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.REDUCESINK;
  }

  @Override
  public boolean opAllowedBeforeMapJoin() {
    return false;
  }

}
