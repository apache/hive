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
import org.apache.hadoop.hive.ql.io.AcidUtils;
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
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.hash.MurmurHash;

import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.UNIFORM;

/**
 * Reduce Sink Operator sends output to the reduce stage.
 **/
public class ReduceSinkOperator extends TerminalOperator<ReduceSinkDesc>
    implements Serializable, TopNHash.BinaryCollector {

  static {
    PTFUtils.makeTransient(ReduceSinkOperator.class, "inputAliases", "valueIndex");
  }

  /**
   * Counters.
   */
  public static enum Counter {
    RECORDS_OUT_INTERMEDIATE
  }

  private static final long serialVersionUID = 1L;
  private static final MurmurHash hash = (MurmurHash) MurmurHash.getInstance();

  private transient ObjectInspector[] partitionObjectInspectors;
  private transient ObjectInspector[] bucketObjectInspectors;
  private transient int buckColIdxInKey;
  private boolean firstRow;
  private transient int tag;
  private boolean skipTag = false;
  private transient InspectableObject tempInspectableObject = new InspectableObject();
  private transient int[] valueIndex; // index for value(+ from keys, - from values)

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
  /**
   * Evaluators for bucketing columns. This is used to compute bucket number.
   */
  protected transient ExprNodeEvaluator[] bucketEval = null;
  // TODO: we use MetadataTypedColumnsetSerDe for now, till DynamicSerDe is ready
  protected transient Serializer keySerializer;
  protected transient boolean keyIsText;
  protected transient Serializer valueSerializer;
  protected transient byte[] tagByte = new byte[1];
  protected transient int numDistributionKeys;
  protected transient int numDistinctExprs;
  protected transient String[] inputAliases;  // input aliases of this RS for join (used for PPD)
  protected transient boolean useUniformHash = false;
  // picks topN K:V pairs from input.
  protected transient TopNHash reducerHash = new TopNHash();
  protected transient HiveKey keyWritable = new HiveKey();
  protected transient ObjectInspector keyObjectInspector;
  protected transient ObjectInspector valueObjectInspector;
  protected transient Object[] cachedValues;
  protected transient List<List<Integer>> distinctColIndices;
  protected transient Random random;
  protected transient int bucketNumber;

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

  private StructField recIdField; // field to look for record identifier in
  private StructField bucketField; // field to look for bucket in record identifier
  private StructObjectInspector acidRowInspector; // row inspector used by acid options
  private StructObjectInspector recIdInspector; // OI for the record identifier
  private IntObjectInspector bucketInspector; // OI for the bucket field in the record id

  protected transient long numRows = 0;
  protected transient long cntr = 1;
  private final transient LongWritable recordCounter = new LongWritable();

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    try {

      numRows = 0;

      String context = hconf.get(Operator.CONTEXT_NAME_KEY, "");
      if (context != null && !context.isEmpty()) {
        context = "_" + context.replace(" ","_");
      }
      statsMap.put(Counter.RECORDS_OUT_INTERMEDIATE + context, recordCounter);

      List<ExprNodeDesc> keys = conf.getKeyCols();

      if (isLogDebugEnabled) {
        LOG.debug("keys size is " + keys.size());
        for (ExprNodeDesc k : keys) {
          LOG.debug("Key exprNodeDesc " + k.getExprString());
        }
      }

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

      if (conf.getBucketCols() != null && !conf.getBucketCols().isEmpty()) {
        bucketEval = new ExprNodeEvaluator[conf.getBucketCols().size()];

        i = 0;
        for (ExprNodeDesc e : conf.getBucketCols()) {
          int index = ExprNodeDescUtils.indexOf(e, keys);
          bucketEval[i++] = index < 0 ? ExprNodeEvaluatorFactory.get(e) : keyEval[index];
        }

        buckColIdxInKey = conf.getPartitionCols().size();
      }

      tag = conf.getTag();
      tagByte[0] = (byte) tag;
      skipTag = conf.getSkipTag();
      if (isLogInfoEnabled) {
        LOG.info("Using tag = " + tag);
      }

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
        reducerHash = conf.isPTFReduceSink() ? new PTFTopNHash() : reducerHash;
        reducerHash.initialize(limit, memUsage, conf.isMapGroupBy(), this);
      }

      useUniformHash = conf.getReducerTraits().contains(UNIFORM);

      firstRow = true;
      initializeChildren(hconf);
    } catch (Exception e) {
      String msg = "Error initializing ReduceSinkOperator: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(e);
    }
  }


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
        if (conf.getWriteType() == AcidUtils.Operation.UPDATE ||
            conf.getWriteType() == AcidUtils.Operation.DELETE) {
          assert rowInspector instanceof StructObjectInspector :
              "Exptected rowInspector to be instance of StructObjectInspector but it is a " +
                  rowInspector.getClass().getName();
          acidRowInspector = (StructObjectInspector)rowInspector;
          // The record identifier is always in the first column
          recIdField = acidRowInspector.getAllStructFieldRefs().get(0);
          recIdInspector = (StructObjectInspector)recIdField.getFieldObjectInspector();
          // The bucket field is in the second position
          bucketField = recIdInspector.getAllStructFieldRefs().get(1);
          bucketInspector = (IntObjectInspector)bucketField.getFieldObjectInspector();
        }

        if (isLogInfoEnabled) {
          LOG.info("keys are " + conf.getOutputKeyColumnNames() + " num distributions: " +
              conf.getNumDistributionKeys());
        }
        keyObjectInspector = initEvaluatorsAndReturnStruct(keyEval,
            distinctColIndices,
            conf.getOutputKeyColumnNames(), numDistributionKeys, rowInspector);
        valueObjectInspector = initEvaluatorsAndReturnStruct(valueEval,
            conf.getOutputValueColumnNames(), rowInspector);
        partitionObjectInspectors = initEvaluators(partitionEval, rowInspector);
        if (bucketEval != null) {
          bucketObjectInspectors = initEvaluators(bucketEval, rowInspector);
        }
        int numKeys = numDistinctExprs > 0 ? numDistinctExprs : 1;
        int keyLen = numDistinctExprs > 0 ? numDistributionKeys + 1 : numDistributionKeys;
        cachedKeys = new Object[numKeys][keyLen];
        cachedValues = new Object[valueEval.length];
      }

      // Determine distKeyLength (w/o distincts), and then add the first if present.
      populateCachedDistributionKeys(row, 0);

      // replace bucketing columns with hashcode % numBuckets
      if (bucketEval != null) {
        bucketNumber = computeBucketNumber(row, conf.getNumBuckets());
        cachedKeys[0][buckColIdxInKey] = new Text(String.valueOf(bucketNumber));
      } else if (conf.getWriteType() == AcidUtils.Operation.UPDATE ||
          conf.getWriteType() == AcidUtils.Operation.DELETE) {
        // In the non-partitioned case we still want to compute the bucket number for updates and
        // deletes.
        bucketNumber = computeBucketNumber(row, conf.getNumBuckets());
      }

      HiveKey firstKey = toHiveKey(cachedKeys[0], tag, null);
      int distKeyLength = firstKey.getDistKeyLength();
      if (numDistinctExprs > 0) {
        populateCachedDistinctKeys(row, 0);
        firstKey = toHiveKey(cachedKeys[0], tag, distKeyLength);
      }

      final int hashCode;

      // distKeyLength doesn't include tag, but includes buckNum in cachedKeys[0]
      if (useUniformHash && partitionEval.length > 0) {
        hashCode = computeMurmurHash(firstKey);
      } else {
        hashCode = computeHashCode(row, bucketNumber);
      }

      firstKey.setHashCode(hashCode);

      /*
       * in case of TopN for windowing, we need to distinguish between rows with
       * null partition keys and rows with value 0 for partition keys.
       */
      boolean partKeyNull = conf.isPTFReduceSink() && partitionKeysAreNull(row);

      // Try to store the first key. If it's not excluded, we will proceed.
      int firstIndex = reducerHash.tryStoreKey(firstKey, partKeyNull);
      if (firstIndex == TopNHash.EXCLUDE) return; // Nothing to do.
      // Compute value and hashcode - we'd either store or forward them.
      BytesWritable value = makeValueWritable(row);

      if (firstIndex == TopNHash.FORWARD) {
        collect(firstKey, value);
      } else {
        assert firstIndex >= 0;
        reducerHash.storeValue(firstIndex, firstKey.hashCode(), value, false);
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

  private int computeBucketNumber(Object row, int numBuckets) throws HiveException {
    int buckNum = 0;

    if (conf.getWriteType() == AcidUtils.Operation.UPDATE ||
        conf.getWriteType() == AcidUtils.Operation.DELETE) {
      // We don't need to evalute the hash code.  Instead read the bucket number directly from
      // the row.  I don't need to evaluate any expressions as I know I am reading the ROW__ID
      // column directly.
      Object recIdValue = acidRowInspector.getStructFieldData(row, recIdField);
      buckNum = bucketInspector.get(recIdInspector.getStructFieldData(recIdValue, bucketField));
      if (isLogTraceEnabled) {
        LOG.trace("Acid choosing bucket number " + buckNum);
      }
    } else {
      for (int i = 0; i < bucketEval.length; i++) {
        Object o = bucketEval[i].evaluate(row);
        buckNum = buckNum * 31 + ObjectInspectorUtils.hashCode(o, bucketObjectInspectors[i]);
      }
    }

    if (buckNum < 0) {
      buckNum = -1 * buckNum;
    }

    return buckNum % numBuckets;
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

  protected final int computeMurmurHash(HiveKey firstKey) {
    return hash.hash(firstKey.getBytes(), firstKey.getDistKeyLength(), 0);
  }

  private int computeHashCode(Object row, int buckNum) throws HiveException {
    // Evaluate the HashCode
    int keyHashCode = 0;
    if (partitionEval.length == 0) {
      // If no partition cols and not doing an update or delete, just distribute the data uniformly
      // to provide better load balance. If the requirement is to have a single reducer, we should
      // set the number of reducers to 1. Use a constant seed to make the code deterministic.
      // For acid operations make sure to send all records with the same key to the same
      // FileSinkOperator, as the RecordUpdater interface can't manage multiple writers for a file.
      if (conf.getWriteType() == AcidUtils.Operation.NOT_ACID) {
        if (random == null) {
          random = new Random(12345);
        }
        keyHashCode = random.nextInt();
      } else {
        keyHashCode = 1;
      }
    } else {
      for (int i = 0; i < partitionEval.length; i++) {
        Object o = partitionEval[i].evaluate(row);
        keyHashCode = keyHashCode * 31
            + ObjectInspectorUtils.hashCode(o, partitionObjectInspectors[i]);
      }
    }
    int hashCode = buckNum < 0 ? keyHashCode : keyHashCode * 31 + buckNum;
    if (isLogTraceEnabled) {
      LOG.trace("Going to return hash code " + hashCode);
    }
    return hashCode;
  }

  private boolean partitionKeysAreNull(Object row) throws HiveException {
    if ( partitionEval.length != 0 ) {
      for (int i = 0; i < partitionEval.length; i++) {
        Object o = partitionEval[i].evaluate(row);
        if ( o != null ) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  // Serialize the keys and append the tag
  protected HiveKey toHiveKey(Object obj, int tag, Integer distLength) throws SerDeException {
    BinaryComparable key = (BinaryComparable)keySerializer.serialize(obj, keyObjectInspector);
    int keyLength = key.getLength();
    if (tag == -1 || skipTag) {
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
      numRows++;
      if (isLogInfoEnabled) {
        if (numRows == cntr) {
          cntr *= 10;
          LOG.info(toString() + ": records written - " + numRows);
        }
      }
      out.collect(keyWritable, valueWritable);
    }
  }

  private BytesWritable makeValueWritable(Object row) throws Exception {
    int length = valueEval.length;

    // in case of bucketed table, insert the bucket number as the last column in value
    if (bucketEval != null) {
      length -= 1;
      cachedValues[length] = new Text(String.valueOf(bucketNumber));
    }

    // Evaluate the value
    for (int i = 0; i < length; i++) {
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
    if (isLogInfoEnabled) {
      LOG.info(toString() + ": records written - " + numRows);
    }
    recordCounter.set(numRows);
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

  public void setSkipTag(boolean value) {
    this.skipTag = value;
  }

  public void setValueIndex(int[] valueIndex) {
    this.valueIndex = valueIndex;
  }

  public int[] getValueIndex() {
    return valueIndex;
  }

  public void setInputAliases(String[] inputAliases) {
    this.inputAliases = inputAliases;
  }

  public String[] getInputAliases() {
    return inputAliases;
  }

  public void setOutputCollector(OutputCollector _out) {
    this.out = _out;
  }
}
