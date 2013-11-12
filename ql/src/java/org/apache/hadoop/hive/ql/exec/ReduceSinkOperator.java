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
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
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
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * Reduce Sink Operator sends output to the reduce stage.
 **/
public class ReduceSinkOperator extends TerminalOperator<ReduceSinkDesc>
    implements Serializable, TopNHash.BinaryCollector {

  private static final long serialVersionUID = 1L;
  protected transient OutputCollector out;

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
  protected transient Serializer keySerializer;
  protected transient boolean keyIsText;
  protected transient Serializer valueSerializer;
  transient int tag;
  protected transient byte[] tagByte = new byte[1];
  transient protected int numDistributionKeys;
  transient protected int numDistinctExprs;
  transient String inputAlias;  // input alias of this RS for join (used for PPD)

  public void setInputAlias(String inputAlias) {
    this.inputAlias = inputAlias;
  }

  public String getInputAlias() {
    return inputAlias;
  }

  public void setOutputCollector(OutputCollector _out) {
    this.out = _out;
  }

  // picks topN K:V pairs from input.
  protected transient TopNHash reducerHash = new TopNHash();
  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    try {
      List<ExprNodeDesc> keys = conf.getKeyCols();
      keyEval = new ExprNodeEvaluator[keys.size()];
      int i = 0;
      for (ExprNodeDesc e : keys) {
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
        int index = ExprNodeDescUtils.indexOf(e, keys);
        partitionEval[i++] = index < 0 ? ExprNodeEvaluatorFactory.get(e): keyEval[index];
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

      int limit = conf.getTopN();
      float memUsage = conf.getTopNMemoryUsage();
      if (limit >= 0 && memUsage > 0) {
        reducerHash.initialize(limit, memUsage, conf.isMapGroupBy(), this);
      }

      firstRow = true;
      initializeChildren(hconf);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  transient InspectableObject tempInspectableObject = new InspectableObject();
  protected transient HiveKey keyWritable = new HiveKey();

  protected transient ObjectInspector keyObjectInspector;
  protected transient ObjectInspector valueObjectInspector;
  transient ObjectInspector[] partitionObjectInspectors;

  protected transient Object[] cachedValues;
  protected transient List<List<Integer>> distinctColIndices;
  /**
   * This two dimensional array holds key data and a corresponding Union object
   * which contains the tag identifying the aggregate expression for distinct columns.
   *
   * If there is no distict expression, cachedKeys is simply like this.
   * cachedKeys[0] = [col0][col1]
   *
   * with two distict expression, union(tag:key) is attatched for each distinct expression
   * cachedKeys[0] = [col0][col1][0:dist1]
   * cachedKeys[1] = [col0][col1][1:dist2]
   *
   * in this case, child GBY evaluates distict values with expression like KEY.col2:0.dist1
   * see {@link ExprNodeColumnEvaluator}
   */
  // TODO: we only ever use one row of these at a time. Why do we need to cache multiple?
  protected transient Object[][] cachedKeys;
  boolean firstRow;
  protected transient Random random;

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

    if (outputColNames.size() > length) {
      // union keys
      assert distinctColIndices != null;
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
  @SuppressWarnings("unchecked")
  public void processOp(Object row, int tag) throws HiveException {
    try {
      ObjectInspector rowInspector = inputObjInspectors[tag];
      if (firstRow) {
        firstRow = false;
        // TODO: this is fishy - we init object inspectors based on first tag. We
        //       should either init for each tag, or if rowInspector doesn't really
        //       matter, then we can create this in ctor and get rid of firstRow.
        keyObjectInspector = initEvaluatorsAndReturnStruct(keyEval,
            distinctColIndices,
            conf.getOutputKeyColumnNames(), numDistributionKeys, rowInspector);
        valueObjectInspector = initEvaluatorsAndReturnStruct(valueEval, conf
            .getOutputValueColumnNames(), rowInspector);
        partitionObjectInspectors = initEvaluators(partitionEval, rowInspector);
        int numKeys = numDistinctExprs > 0 ? numDistinctExprs : 1;
        int keyLen = numDistinctExprs > 0 ? numDistributionKeys + 1 : numDistributionKeys;
        cachedKeys = new Object[numKeys][keyLen];
        cachedValues = new Object[valueEval.length];
      }

      // Determine distKeyLength (w/o distincts), and then add the first if present.
      populateCachedDistributionKeys(row, 0);
      HiveKey firstKey = toHiveKey(cachedKeys[0], tag, null);
      int distKeyLength = firstKey.getDistKeyLength();
      if (numDistinctExprs > 0) {
        populateCachedDistinctKeys(row, 0);
        firstKey = toHiveKey(cachedKeys[0], tag, distKeyLength);
      }

      // Try to store the first key. If it's not excluded, we will proceed.
      int firstIndex = reducerHash.tryStoreKey(firstKey);
      if (firstIndex == TopNHash.EXCLUDE) return; // Nothing to do.
      // Compute value and hashcode - we'd either store or forward them.
      BytesWritable value = makeValueWritable(row);
      int hashCode = computeHashCode(row);
      if (firstIndex == TopNHash.FORWARD) {
        firstKey.setHashCode(hashCode);
        collect(firstKey, value);
      } else {
        assert firstIndex >= 0;
        reducerHash.storeValue(firstIndex, value, hashCode, false);
      }

      // All other distinct keys will just be forwarded. This could be optimized...
      for (int i = 1; i < numDistinctExprs; i++) {
        System.arraycopy(cachedKeys[0], 0, cachedKeys[i], 0, numDistributionKeys);
        populateCachedDistinctKeys(row, i);
        HiveKey hiveKey = toHiveKey(cachedKeys[i], tag, distKeyLength);
        hiveKey.setHashCode(hashCode);
        collect(hiveKey, value);
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private void populateCachedDistributionKeys(Object row, int index) throws HiveException {
    for (int i = 0; i < numDistributionKeys; i++) {
      cachedKeys[index][i] = keyEval[i].evaluate(row);
    }
    if (cachedKeys[0].length > numDistributionKeys) {
      cachedKeys[index][numDistributionKeys] = null;
    }
  }

  /**
   * Populate distinct keys part of cachedKeys for a particular row.
   * @param row the row
   * @param index the cachedKeys index to write to
   */
  private void populateCachedDistinctKeys(Object row, int index) throws HiveException {
    StandardUnion union;
    cachedKeys[index][numDistributionKeys] = union = new StandardUnion(
          (byte)index, new Object[distinctColIndices.get(index).size()]);
    Object[] distinctParameters = (Object[]) union.getObject();
    for (int distinctParamI = 0; distinctParamI < distinctParameters.length; distinctParamI++) {
      distinctParameters[distinctParamI] =
          keyEval[distinctColIndices.get(index).get(distinctParamI)].evaluate(row);
    }
    union.setTag((byte) index);
  }

  private int computeHashCode(Object row) throws HiveException {
    // Evaluate the HashCode
    int keyHashCode = 0;
    if (partitionEval.length == 0) {
      // If no partition cols, just distribute the data uniformly to provide better
      // load balance. If the requirement is to have a single reducer, we should set
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
    return keyHashCode;
  }

  // Serialize the keys and append the tag
  protected HiveKey toHiveKey(Object obj, int tag, Integer distLength) throws SerDeException {
    BinaryComparable key = (BinaryComparable)keySerializer.serialize(obj, keyObjectInspector);
    int keyLength = key.getLength();
    if (tag == -1) {
      keyWritable.set(key.getBytes(), 0, keyLength);
    } else {
      keyWritable.setSize(keyLength + 1);
      System.arraycopy(key.getBytes(), 0, keyWritable.get(), 0, keyLength);
      keyWritable.get()[keyLength] = tagByte[0];
    }
    keyWritable.setDistKeyLength((distLength == null) ? keyLength : distLength);
    return keyWritable;
  }

  public void collect(byte[] key, byte[] value, int hash) throws IOException {
    HiveKey keyWritable = new HiveKey(key, hash);
    BytesWritable valueWritable = new BytesWritable(value);
    collect(keyWritable, valueWritable);
  }

  protected void collect(BytesWritable keyWritable, Writable valueWritable) throws IOException {
    // Since this is a terminal operator, update counters explicitly -
    // forward is not called
    if (null != out) {
      out.collect(keyWritable, valueWritable);
    }
    if (++outputRows % 1000 == 0) {
      if (counterNameToEnum != null) {
        incrCounter(numOutputRowsCntr, outputRows);
      }
      increaseForward(outputRows);
      outputRows = 0;
    }
  }

  private BytesWritable makeValueWritable(Object row) throws Exception {
    // Evaluate the value
    for (int i = 0; i < valueEval.length; i++) {
      cachedValues[i] = valueEval[i].evaluate(row);
    }
    // Serialize the value
    return (BytesWritable) valueSerializer.serialize(cachedValues, valueObjectInspector);
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      reducerHash.flush();
    }
    super.closeOp(abort);
    out = null;
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
