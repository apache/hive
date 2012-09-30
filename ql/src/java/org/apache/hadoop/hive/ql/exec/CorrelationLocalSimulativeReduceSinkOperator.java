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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CorrelationLocalSimulativeReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.eclipse.jdt.core.dom.ThisExpression;

/**
 * CorrelationLocalSimulativeReduceSinkOperator simulates a ReduceSinkOperator and sends output to
 * another operator (JOIN or GBY). CorrelationLocalSimulativeReduceSinkOperator is used only in
 * reduce phase. Basically, it is a bridge from one JOIN or GBY operator to another JOIN or GBY
 * operator. A CorrelationLocalSimulativeReduceSinkOperator will take care actions of startGroup and
 * endGroup of its succeeding JOIN or GBY operator.
 * Example: A query involves a JOIN operator and a GBY operator and the GBY operator consume the
 * output of the JOIN operator. In this case, if join keys and group by keys are the same, we do not
 * need to shuffle the data again, since data has been already partitioned by the JOIN operator.
 * Thus, in CorrelationOptimizer, the ReduceSinkOperator between JOIN and GBY operator will be
 * replaced by a CorrelationLocalSimulativeReduceSinkOperator and the JOIN operator and GBY operator
 * will be executed in a single reduce phase.
 **/
public class CorrelationLocalSimulativeReduceSinkOperator
  extends BaseReduceSinkOperator<CorrelationLocalSimulativeReduceSinkDesc> {

  private static final long serialVersionUID = 1L;
  protected static final Log LOG = LogFactory.getLog(
      CorrelationLocalSimulativeReduceSinkOperator.class.getName());

  private transient TableDesc keyTableDesc;

  private transient Deserializer inputKeyDeserializer;

  private transient SerDe inputValueDeserializer;

  private transient ByteWritable tagWritable;

  private transient ObjectInspector outputKeyObjectInspector;
  private transient ObjectInspector outputValueObjectInspector;

  private List<Object> forwardedRow;
  private Object keyObject;
  private Object valueObject;

  private BytesWritable groupKey;

  private static String[] fieldNames;

  static {
    List<String> fieldNameArray = new ArrayList<String>();
    for (Utilities.ReduceField r : Utilities.ReduceField.values()) {
      fieldNameArray.add(r.toString());
    }
    fieldNames = fieldNameArray.toArray(new String[0]);
  }

  public CorrelationLocalSimulativeReduceSinkOperator() {
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    forwardedRow = new ArrayList<Object>(3);
    tagByte = new byte[1];
    tagWritable = new ByteWritable();
    tempInspectableObject = new InspectableObject();
    keyWritable = new HiveKey();
    assert childOperatorsArray.length == 1;
    try {
      keyEval = new ExprNodeEvaluator[conf.getKeyCols().size()];
      int i = 0;
      for (ExprNodeDesc e : conf.getKeyCols()) {
        keyEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      numDistributionKeys = conf.getNumDistributionKeys();
      distinctColIndices = conf.getDistinctColumnIndices();
      numDistinctExprs = distinctColIndices.size();

      valueEval = new ExprNodeEvaluator[conf.getValueCols().size()];
      i = 0;
      for (ExprNodeDesc e : conf.getValueCols()) {
        valueEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      tag = conf.getTag();
      tagByte[0] = (byte) tag;
      tagWritable.set(tagByte[0]);
      LOG.info("Using tag = " + tag);

      TableDesc keyTableDesc = conf.getKeySerializeInfo();
      keySerializer = (Serializer) keyTableDesc.getDeserializerClass()
          .newInstance();
      keySerializer.initialize(null, keyTableDesc.getProperties());
      keyIsText = keySerializer.getSerializedClass().equals(Text.class);

      inputKeyDeserializer = ReflectionUtils.newInstance(keyTableDesc
          .getDeserializerClass(), null);
      inputKeyDeserializer.initialize(null, keyTableDesc.getProperties());
      outputKeyObjectInspector = inputKeyDeserializer.getObjectInspector();

      TableDesc valueTableDesc = conf.getValueSerializeInfo();
      valueSerializer = (Serializer) valueTableDesc.getDeserializerClass()
          .newInstance();
      valueSerializer.initialize(null, valueTableDesc.getProperties());

      inputValueDeserializer = (SerDe) ReflectionUtils.newInstance(
          valueTableDesc.getDeserializerClass(), null);
      inputValueDeserializer.initialize(null, valueTableDesc
          .getProperties());
      outputValueObjectInspector = inputValueDeserializer.getObjectInspector();

      ObjectInspector rowInspector = inputObjInspectors[0];

      keyObjectInspector = initEvaluatorsAndReturnStruct(keyEval,
          distinctColIndices,
          conf.getOutputKeyColumnNames(), numDistributionKeys, rowInspector);
      valueObjectInspector = initEvaluatorsAndReturnStruct(valueEval, conf
          .getOutputValueColumnNames(), rowInspector);
      int numKeys = numDistinctExprs > 0 ? numDistinctExprs : 1;
      int keyLen = numDistinctExprs > 0 ? numDistributionKeys + 1 :
        numDistributionKeys;
      cachedKeys = new Object[numKeys][keyLen];
      cachedValues = new Object[valueEval.length];
      assert cachedKeys.length == 1;

      List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
      ois.add(outputKeyObjectInspector);
      ois.add(outputValueObjectInspector);
      ois.add(PrimitiveObjectInspectorFactory.writableByteObjectInspector);

      outputObjInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(Arrays.asList(fieldNames), ois);

      LOG.info("Simulative ReduceSink inputObjInspectors"
          + ((StructObjectInspector) inputObjInspectors[0]).getTypeName());

      LOG.info("Simulative ReduceSink outputObjInspectors "
          + this.getChildOperators().get(0).getParentOperators().indexOf(this) +
          " " + ((StructObjectInspector) outputObjInspector).getTypeName());

      initializeChildren(hconf);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    try {
      // Evaluate the value
      for (int i = 0; i < valueEval.length; i++) {
        cachedValues[i] = valueEval[i].evaluate(row);
      }
      // Serialize the value
      value = valueSerializer.serialize(cachedValues, valueObjectInspector);
      valueObject = inputValueDeserializer.deserialize(value);

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
              new StandardUnion((byte) i, distinctParameters);
        }
      } else {
        // no distinct key
        System.arraycopy(distributionKeys, 0, cachedKeys[0], 0, numDistributionKeys);
      }

      for (int i = 0; i < cachedKeys.length; i++) {
        if (keyIsText) {
          Text key = (Text) keySerializer.serialize(cachedKeys[i],
              keyObjectInspector);
          keyWritable.set(key.getBytes(), 0, key.getLength());
        } else {
          // Must be BytesWritable
          BytesWritable key = (BytesWritable) keySerializer.serialize(
              cachedKeys[i], keyObjectInspector);
          keyWritable.set(key.getBytes(), 0, key.getLength());
        }

        if (!keyWritable.equals(groupKey)) {
          try {
            keyObject = inputKeyDeserializer.deserialize(keyWritable);
          } catch (Exception e) {
            throw new HiveException(
                "Hive Runtime Error: Unable to deserialize reduce input key from "
                    + Utilities.formatBinaryString(keyWritable.get(), 0,
                        keyWritable.getSize()) + " with properties "
                        + keyTableDesc.getProperties(), e);
          }
          if (groupKey == null) { // the first group
            groupKey = new BytesWritable();
          } else {
            // if its child has not been ended, end it
            if (!keyWritable.equals(childOperatorsArray[0].getBytesWritableGroupKey())) {
              childOperatorsArray[0].endGroup();
            }
          }
          groupKey.set(keyWritable.get(), 0, keyWritable.getSize());
          if (!groupKey.equals(childOperatorsArray[0].getBytesWritableGroupKey())) {
            childOperatorsArray[0].startGroup();
            childOperatorsArray[0].setGroupKeyObject(keyObject);
            childOperatorsArray[0].setBytesWritableGroupKey(groupKey);
          }
        }
        forwardedRow.clear();
        forwardedRow.add(keyObject);
        forwardedRow.add(valueObject);
        forwardedRow.add(tagWritable);
        forward(forwardedRow, outputObjInspector);
      }
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      Operator<? extends OperatorDesc> child = childOperatorsArray[0];
      if (child.allInitializedParentsAreClosed()) {
        LOG.info("All parents of " + child.getName() + " (id: " + child.getIdentifier() +
            ") has been closed. Invoke its endGroup");
        childOperatorsArray[0].endGroup();
      }
    }
  }

  @Override
  public void startGroup() throws HiveException {
    // do nothing
  }

  @Override
  public void endGroup() throws HiveException {
    // do nothing
  }

  @Override
  public void setGroupKeyObject(Object keyObject) {
    // do nothing
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "CLSReduceSink";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.CORRELATIONLOCALSIMULATIVEREDUCESINK;
  }
}
