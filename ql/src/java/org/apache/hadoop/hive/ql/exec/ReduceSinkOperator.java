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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * Reduce Sink Operator sends output to the reduce stage.
 **/
public class ReduceSinkOperator extends BaseReduceSinkOperator<ReduceSinkDesc>
    implements Serializable {

  private static final long serialVersionUID = 1L;

  private final List<Integer> operationPathTags = new ArrayList<Integer>(); // operation path tags
  private final byte[] operationPathTagsByte = new byte[1];

  public void setOperationPathTags(List<Integer> operationPathTags) {
    this.operationPathTags.addAll(operationPathTags);
    int operationPathTagsInt = 0;
    int tmp = 1;
    for (Integer operationPathTag: operationPathTags) {
      operationPathTagsInt += tmp << operationPathTag.intValue();
    }
    operationPathTagsByte[0] = (byte) operationPathTagsInt;
  }

  public List<Integer> getOperationPathTags() {
    return this.operationPathTags;
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    try {
      ObjectInspector rowInspector = inputObjInspectors[tag];
      if (isFirstRow) {
        isFirstRow = false;
        keyObjectInspector = initEvaluatorsAndReturnStruct(keyEval,
            distinctColIndices,
            conf.getOutputKeyColumnNames(), numDistributionKeys, rowInspector);
        valueObjectInspector = initEvaluatorsAndReturnStruct(valueEval, conf
            .getOutputValueColumnNames(), rowInspector);
        partitionObjectInspectors = initEvaluators(partitionEval, rowInspector);
        int numKeys = numDistinctExprs > 0 ? numDistinctExprs : 1;
        int keyLen = numDistinctExprs > 0 ? numDistributionKeys + 1 :
          numDistributionKeys;
        cachedKeys = new Object[numKeys][keyLen];
        cachedValues = new Object[valueEval.length];
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
        for (int i = 0; i < partitionEval.length; i++) {
          Object o = partitionEval[i].evaluate(row);
          keyHashCode = keyHashCode * 31
              + ObjectInspectorUtils.hashCode(o, partitionObjectInspectors[i]);
        }
      }

      // Evaluate the value
      for (int i = 0; i < valueEval.length; i++) {
        cachedValues[i] = valueEval[i].evaluate(row);
      }
      // Serialize the value
      value = valueSerializer.serialize(cachedValues, valueObjectInspector);

      // Evaluate the keys
      Object[] distributionKeys = new Object[numDistributionKeys];
      for (int i = 0; i < numDistributionKeys; i++) {
        distributionKeys[i] = keyEval[i].evaluate(row);
      }

      if (numDistinctExprs > 0) {
        // with distinct key(s)
        for (int i = 0; i < numDistinctExprs; i++) {
          System.arraycopy(distributionKeys, 0, cachedKeys[i], 0, numDistributionKeys);
          Object[] distinctParameters =
            new Object[distinctColIndices.get(i).size()];
          for (int j = 0; j < distinctParameters.length; j++) {
            distinctParameters[j] =
              keyEval[distinctColIndices.get(i).get(j)].evaluate(row);
          }
          cachedKeys[i][numDistributionKeys] =
              new StandardUnion((byte)i, distinctParameters);
        }
      } else {
        // no distinct key
        System.arraycopy(distributionKeys, 0, cachedKeys[0], 0, numDistributionKeys);
      }
      // Serialize the keys and append the tag
      for (int i = 0; i < cachedKeys.length; i++) {
        if (keyIsText) {
          Text key = (Text) keySerializer.serialize(cachedKeys[i],
              keyObjectInspector);
          if (tag == -1) {
            keyWritable.set(key.getBytes(), 0, key.getLength());
          } else {
            int keyLength = key.getLength();
            if (!this.getConf().getNeedsOperationPathTagging()) {
              keyWritable.setSize(keyLength + 1);
            } else {
              keyWritable.setSize(keyLength + 2);
            }
            System.arraycopy(key.getBytes(), 0, keyWritable.get(), 0, keyLength);
            if (!this.getConf().getNeedsOperationPathTagging()) {
              keyWritable.get()[keyLength] = tagByte[0];
            } else {
              keyWritable.get()[keyLength] = operationPathTagsByte[0];
              keyWritable.get()[keyLength + 1] = tagByte[0];
            }
          }
        } else {
          // Must be BytesWritable
          BytesWritable key = (BytesWritable) keySerializer.serialize(
              cachedKeys[i], keyObjectInspector);
          if (tag == -1) {
            keyWritable.set(key.getBytes(), 0, key.getLength());
          } else {
            int keyLength = key.getLength();
            if (!this.getConf().getNeedsOperationPathTagging()) {
              keyWritable.setSize(keyLength + 1);
            } else {
              keyWritable.setSize(keyLength + 2);
            }
            System.arraycopy(key.getBytes(), 0, keyWritable.get(), 0, keyLength);
            if (!this.getConf().getNeedsOperationPathTagging()) {
              keyWritable.get()[keyLength] = tagByte[0];
            } else {
              keyWritable.get()[keyLength] = operationPathTagsByte[0];
              keyWritable.get()[keyLength + 1] = tagByte[0];
            }
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
    } catch (SerDeException e) {
      throw new HiveException(e);
    } catch (IOException e) {
      throw new HiveException(e);
    }
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
}
