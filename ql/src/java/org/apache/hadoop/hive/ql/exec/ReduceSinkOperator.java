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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Reduce Sink Operator sends output to the reduce stage
 **/
public class ReduceSinkOperator extends TerminalOperator <reduceSinkDesc> implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * The evaluators for the key columns.
   * Key columns decide the sort order on the reducer side.
   * Key columns are passed to the reducer in the "key".
   */
  transient protected ExprNodeEvaluator[] keyEval;
  /**
   * The evaluators for the value columns.
   * Value columns are passed to reducer in the "value". 
   */
  transient protected ExprNodeEvaluator[] valueEval;
  /**
   * The evaluators for the partition columns (CLUSTER BY or DISTRIBUTE BY in Hive language).
   * Partition columns decide the reducer that the current row goes to.
   * Partition columns are not passed to reducer.
   */
  transient protected ExprNodeEvaluator[] partitionEval;
  
  // TODO: we use MetadataTypedColumnsetSerDe for now, till DynamicSerDe is ready
  transient Serializer keySerializer;
  transient boolean keyIsText;
  transient Serializer valueSerializer;
  transient int tag;
  transient byte[] tagByte = new byte[1];
  
  protected void initializeOp(Configuration hconf) throws HiveException {

    try {
      keyEval = new ExprNodeEvaluator[conf.getKeyCols().size()];
      int i=0;
      for(exprNodeDesc e: conf.getKeyCols()) {
        keyEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      valueEval = new ExprNodeEvaluator[conf.getValueCols().size()];
      i=0;
      for(exprNodeDesc e: conf.getValueCols()) {
        valueEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      partitionEval = new ExprNodeEvaluator[conf.getPartitionCols().size()];
      i=0;
      for(exprNodeDesc e: conf.getPartitionCols()) {
        partitionEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      tag = conf.getTag();
      tagByte[0] = (byte)tag;
      LOG.info("Using tag = " + tag);

      tableDesc keyTableDesc = conf.getKeySerializeInfo();
      keySerializer = (Serializer)keyTableDesc.getDeserializerClass().newInstance();
      keySerializer.initialize(null, keyTableDesc.getProperties());
      keyIsText = keySerializer.getSerializedClass().equals(Text.class);
      
      tableDesc valueTableDesc = conf.getValueSerializeInfo();
      valueSerializer = (Serializer)valueTableDesc.getDeserializerClass().newInstance();
      valueSerializer.initialize(null, valueTableDesc.getProperties());
      
      firstRow = true;
      initializeChildren(hconf);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  transient InspectableObject tempInspectableObject = new InspectableObject();
  transient HiveKey keyWritable = new HiveKey();
  transient Writable value;
  
  transient StructObjectInspector keyObjectInspector;
  transient StructObjectInspector valueObjectInspector;
  transient ObjectInspector[] partitionObjectInspectors;

  transient Object[] cachedKeys;
  transient Object[] cachedValues;
  
  boolean firstRow;
  
  transient Random random;
  public void process(Object row, int tag) throws HiveException {
    try {
      ObjectInspector rowInspector = inputObjInspectors[tag];
      if (firstRow) {
        firstRow = false;
        keyObjectInspector = initEvaluatorsAndReturnStruct(keyEval, conf.getOutputKeyColumnNames(), rowInspector);
        valueObjectInspector = initEvaluatorsAndReturnStruct(valueEval, conf.getOutputValueColumnNames(), rowInspector);
        partitionObjectInspectors = initEvaluators(partitionEval, rowInspector);

        cachedKeys = new Object[keyEval.length];
        cachedValues = new Object[valueEval.length];
      }
      
      
      // Evaluate the keys
      for (int i=0; i<keyEval.length; i++) {
        cachedKeys[i] = keyEval[i].evaluate(row);
      }
      
      // Serialize the keys and append the tag
      if (keyIsText) {
        Text key = (Text)keySerializer.serialize(cachedKeys, keyObjectInspector);
        if (tag == -1) {
          keyWritable.set(key.getBytes(), 0, key.getLength());
        } else {
          int keyLength = key.getLength();
          keyWritable.setSize(keyLength+1);
          System.arraycopy(key.getBytes(), 0, keyWritable.get(), 0, keyLength);
          keyWritable.get()[keyLength] = tagByte[0];
        }
      } else {
        // Must be BytesWritable
        BytesWritable key = (BytesWritable)keySerializer.serialize(cachedKeys, keyObjectInspector);
        if (tag == -1) {
          keyWritable.set(key.get(), 0, key.getSize());
        } else {
          int keyLength = key.getSize();
          keyWritable.setSize(keyLength+1);
          System.arraycopy(key.get(), 0, keyWritable.get(), 0, keyLength);
          keyWritable.get()[keyLength] = tagByte[0];
        }
      }
      
      // Set the HashCode
      int keyHashCode = 0;
      if (partitionEval.length == 0) {
        // If no partition cols, just distribute the data uniformly to provide better
        // load balance.  If the requirement is to have a single reducer, we should set
        // the number of reducers to 1.
        // Use a constant seed to make the code deterministic.
        if (random == null) {
          random = new Random(12345);
        }
        keyHashCode = random.nextInt();
      } else {
        for (int i = 0; i < partitionEval.length; i++) {
          Object o = partitionEval[i].evaluate(row);
          keyHashCode = keyHashCode * 31 
            + ObjectInspectorUtils.hashCode(o, partitionObjectInspectors[i]);
        }
      }
      keyWritable.setHashCode(keyHashCode);
      
      // Evaluate the value
      for (int i=0; i<valueEval.length; i++) {
        cachedValues[i] = valueEval[i].evaluate(row);
      }
      // Serialize the value
      value = valueSerializer.serialize(cachedValues, valueObjectInspector);
      
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
    
    try {
      if (out != null) {
        out.collect(keyWritable, value);
      }
    } catch (IOException e) {
      throw new HiveException (e);
    }
  }

  /**
   * @return the name of the operator
   */
  public String getName() {
    return new String("RS");
  }
  
}
