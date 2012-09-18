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
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Reduce Sink Operator sends output to the reduce stage.
 **/
public class ReduceSinkOperator extends TerminalOperator<ReduceSinkDesc>
    implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * The evaluators for the key columns. Key columns decide the sort order on
   * the reducer side. Key columns are passed to the reducer in the "key".
   */
  protected transient ExprNodeEvaluator[] keyEval;
  /**
   * The evaluators for the value columns. Value columns are passed to reducer
   * in the "value".
   */
  protected transient ExprNodeEvaluator[] valueEval;
  /**
   * The evaluators for the partition columns (CLUSTER BY or DISTRIBUTE BY in
   * Hive language). Partition columns decide the reducer that the current row
   * goes to. Partition columns are not passed to reducer.
   */
  protected transient ExprNodeEvaluator[] partitionEval;

  // TODO: we use MetadataTypedColumnsetSerDe for now, till DynamicSerDe is
  // ready
  transient Serializer keySerializer;
  transient boolean keyIsText;
  transient Serializer valueSerializer;
  transient int tag;
  transient byte[] tagByte = new byte[1];
  transient protected int numDistributionKeys;
  transient protected int numDistinctExprs;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

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

      partitionEval = new ExprNodeEvaluator[conf.getPartitionCols().size()];
      i = 0;
      for (ExprNodeDesc e : conf.getPartitionCols()) {
        partitionEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      tag = conf.getTag();
      tagByte[0] = (byte) tag;
      LOG.info("Using tag = " + tag);

      TableDesc keyTableDesc = conf.getKeySerializeInfo();
      keySerializer = (Serializer) keyTableDesc.getDeserializerClass()
          .newInstance();
      keySerializer.initialize(null, keyTableDesc.getProperties());
      keyIsText = keySerializer.getSerializedClass().equals(Text.class);

      TableDesc valueTableDesc = conf.getValueSerializeInfo();
      valueSerializer = (Serializer) valueTableDesc.getDeserializerClass()
          .newInstance();
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

  transient Object[][] cachedKeys;
  transient Object[] cachedValues;
  transient List<List<Integer>> distinctColIndices;

  boolean firstRow;

  transient Random random;

  /**
   * Initializes array of ExprNodeEvaluator. Adds Union field for distinct
   * column indices for group by.
   * Puts the return values into a StructObjectInspector with output column
   * names.
   *
   * If distinctColIndices is empty, the object inspector is same as
   * {@link Operator#initEvaluatorsAndReturnStruct(ExprNodeEvaluator[], List, ObjectInspector)}
   */
  protected static StructObjectInspector initEvaluatorsAndReturnStruct(
      ExprNodeEvaluator[] evals, List<List<Integer>> distinctColIndices,
      List<String> outputColNames,
      int length, ObjectInspector rowInspector)
      throws HiveException {
    int inspectorLen = evals.length > length ? length + 1 : evals.length;
    List<ObjectInspector> sois = new ArrayList<ObjectInspector>(inspectorLen);

    // keys
    ObjectInspector[] fieldObjectInspectors = initEvaluators(evals, 0, length, rowInspector);
    sois.addAll(Arrays.asList(fieldObjectInspectors));

    if (evals.length > length) {
      // union keys
      List<ObjectInspector> uois = new ArrayList<ObjectInspector>();
      for (List<Integer> distinctCols : distinctColIndices) {
        List<String> names = new ArrayList<String>();
        List<ObjectInspector> eois = new ArrayList<ObjectInspector>();
        int numExprs = 0;
        for (int i : distinctCols) {
          names.add(HiveConf.getColumnInternalName(numExprs));
          eois.add(evals[i].initialize(rowInspector));
          numExprs++;
        }
        uois.add(ObjectInspectorFactory.getStandardStructObjectInspector(names, eois));
      }
      UnionObjectInspector uoi =
        ObjectInspectorFactory.getStandardUnionObjectInspector(uois);
      sois.add(uoi);
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(outputColNames, sois );
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    try {
      ObjectInspector rowInspector = inputObjInspectors[tag];
      if (firstRow) {
        firstRow = false;
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
