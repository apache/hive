/*
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

import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.UNIFORM;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBucketNumber;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hive.common.util.Murmur3;

/**
 * Reduce Sink Operator sends output to the reduce stage.
 **/
public class ReduceSinkOperator extends TerminalOperator<ReduceSinkDesc>
    implements Serializable, TopNHash.BinaryCollector {

  private static final long serialVersionUID = 1L;

  private transient ObjectInspector[] partitionObjectInspectors;
  private transient ObjectInspector[] bucketObjectInspectors;
  private transient int buckColIdxInKey;
  /**
   * {@link org.apache.hadoop.hive.ql.optimizer.SortedDynPartitionOptimizer}
   */
  private transient int buckColIdxInKeyForSdpo = -1;
  private boolean firstRow;
  private transient int tag;
  private boolean skipTag = false;
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
  protected transient Serializer keySerializer;
  protected transient boolean keyIsText;
  protected transient Serializer valueSerializer;
  protected transient byte[] tagByte = new byte[1];
  protected transient int numDistributionKeys;
  protected transient int numDistinctExprs;
  protected transient String[] inputAliases;  // input aliases of this RS for join (used for PPD)
  protected transient boolean useUniformHash = false;
  // picks topN K:V pairs from input.
  protected transient TopNHash reducerHash;
  protected transient HiveKey keyWritable = new HiveKey();
  protected transient ObjectInspector keyObjectInspector;
  protected transient ObjectInspector valueObjectInspector;
  protected transient Object[] cachedValues;
  protected transient List<List<Integer>> distinctColIndices;
  protected transient Random random;

  protected transient BiFunction<Object[], ObjectInspector[], Integer> hashFunc;

  /**
   * This two dimensional array holds key data and a corresponding Union object
   * which contains the tag identifying the aggregate expression for distinct columns.
   *
   * If there is no distinct expression, cachedKeys is simply like this.
   * cachedKeys[0] = [col0][col1]
   *
   * with two distict expression, union(tag:key) is attached for each distinct expression
   * cachedKeys[0] = [col0][col1][0:dist1]
   * cachedKeys[1] = [col0][col1][1:dist2]
   *
   * in this case, child GBY evaluates distict values with expression like KEY.col2:0.dist1
   * see {@link ExprNodeColumnEvaluator}
   */
  // TODO: we only ever use one row of these at a time. Why do we need to cache multiple?
  protected transient Object[][] cachedKeys;

  protected transient long cntr = 1;
  protected transient long logEveryNRows = 0;

  /** Kryo ctor. */
  protected ReduceSinkOperator() {
    super();
  }

  public ReduceSinkOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    try {

      numRows = 0;
      cntr = 1;
      logEveryNRows = HiveConf.getLongVar(hconf, HiveConf.ConfVars.HIVE_LOG_N_RECORDS);

      List<ExprNodeDesc> keys = conf.getKeyCols();

      if (LOG.isDebugEnabled()) {
        LOG.debug("keys size is " + keys.size());
        for (ExprNodeDesc k : keys) {
          LOG.debug("Key exprNodeDesc " + k.getExprString());
        }
      }

      keyEval = new ExprNodeEvaluator[keys.size()];
      int i = 0;
      for (ExprNodeDesc e : keys) {
        if (e instanceof ExprNodeGenericFuncDesc && ((ExprNodeGenericFuncDesc) e).getGenericUDF() instanceof GenericUDFBucketNumber) {
          buckColIdxInKeyForSdpo = i;
        }
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
      LOG.info("Using tag = " + tag);

      TableDesc keyTableDesc = conf.getKeySerializeInfo();
      AbstractSerDe keySerDe = keyTableDesc.getSerDeClass().newInstance();
      keySerDe.initialize(null, keyTableDesc.getProperties(), null);

      keySerializer = keySerDe;
      keyIsText = keySerializer.getSerializedClass().equals(Text.class);

      TableDesc valueTableDesc = conf.getValueSerializeInfo();
      AbstractSerDe valueSerDe = valueTableDesc.getSerDeClass()
          .newInstance();
      valueSerDe.initialize(null, valueTableDesc.getProperties(), null);

      valueSerializer = valueSerDe;

      int limit = conf.getTopN();
      float memUsage = conf.getTopNMemoryUsage();

      if (limit >= 0 && memUsage > 0) {
        reducerHash = conf.isPTFReduceSink() ? new PTFTopNHash() : new TopNHash();
        reducerHash.initialize(limit, memUsage, conf.isMapGroupBy(), this, conf, hconf);
      }

      useUniformHash = conf.getReducerTraits().contains(UNIFORM);

      firstRow = true;
      // acidOp flag has to be checked to use JAVA hash which works like
      // identity function for integers, necessary to read RecordIdentifier
      // incase of ACID updates/deletes.
      boolean acidOp = conf.getWriteType() == AcidUtils.Operation.UPDATE ||
          conf.getWriteType() == AcidUtils.Operation.DELETE;
      hashFunc = getConf().getBucketingVersion() == 2 && !acidOp ?
          ObjectInspectorUtils::getBucketHashCode :
          ObjectInspectorUtils::getBucketHashCodeOld;
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
  public void process(Object row, int tag) throws HiveException {
    try {
      ObjectInspector rowInspector = inputObjInspectors[tag];
      if (firstRow) {
        firstRow = false;
        // TODO: this is fishy - we init object inspectors based on first tag. We
        //       should either init for each tag, or if rowInspector doesn't really
        //       matter, then we can create this in ctor and get rid of firstRow.
        LOG.info("keys are " + conf.getOutputKeyColumnNames() + " num distributions: " + conf.getNumDistributionKeys());
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
      populateCachedDistributionKeys(row);

      // replace bucketing columns with hashcode % numBuckets
      int bucketNumber = -1;
      if (bucketEval != null) {
        bucketNumber = computeBucketNumber(row, conf.getNumBuckets());
        cachedKeys[0][buckColIdxInKey] = new Text(String.valueOf(bucketNumber));
      }
      if (buckColIdxInKeyForSdpo != -1) {
        cachedKeys[0][buckColIdxInKeyForSdpo] = new Text(String.valueOf(bucketNumber));
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

      // Try to store the first key.
      // if TopNHashes aren't active, always forward
      // if TopNHashes are active, proceed if not already excluded (i.e order by limit)
      final int firstIndex =
          (reducerHash != null) ? reducerHash.tryStoreKey(firstKey, partKeyNull) : TopNHash.FORWARD;
      if (firstIndex == TopNHash.EXCLUDE)
       {
        return; // Nothing to do.
      }
      // Compute value and hashcode - we'd either store or forward them.
      BytesWritable value = makeValueWritable(row);

      if (firstIndex == TopNHash.FORWARD) {
        collect(firstKey, value);
      } else {
        // invariant: reducerHash != null
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

  private int computeBucketNumber(Object row, int numBuckets)
          throws HiveException, SerDeException {
    Object[] bucketFieldValues = new Object[bucketEval.length];
    for (int i = 0; i < bucketEval.length; i++) {
      bucketFieldValues[i] = bucketEval[i].evaluate(row);
    }
    return ObjectInspectorUtils.getBucketNumber(
        hashFunc.apply(bucketFieldValues, bucketObjectInspectors), numBuckets);
  }

  private void populateCachedDistributionKeys(Object row) throws HiveException {
    for (int i = 0; i < numDistributionKeys; i++) {
      cachedKeys[0][i] = keyEval[i].evaluate(row);
    }
    if (cachedKeys[0].length > numDistributionKeys) {
      cachedKeys[0][numDistributionKeys] = null;
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
    return Murmur3.hash32(firstKey.getBytes(), firstKey.getDistKeyLength(), 0);
  }

  /**
   * For Acid Update/Delete case, we expect a single partitionEval of the form
   * UDFToInteger(ROW__ID) and buckNum == -1 so that the result of this method
   * is to return the bucketId extracted from ROW__ID unless it optimized by
   * {@link org.apache.hadoop.hive.ql.optimizer.SortedDynPartitionOptimizer}
   */
  private int computeHashCode(Object row, int buckNum) throws HiveException {
    // Evaluate the HashCode
    int keyHashCode = 0;
    if (partitionEval.length == 0) {
      // If no partition cols, just distribute the data uniformly
      // to provide better load balance. If the requirement is to have a single reducer, we should
      // set the number of reducers to 1. Use a constant seed to make the code deterministic.
      if (random == null) {
        random = new Random(12345);
      }
      keyHashCode = random.nextInt();
    } else {
      Object[] bucketFieldValues = new Object[partitionEval.length];
      for(int i = 0; i < partitionEval.length; i++) {
        bucketFieldValues[i] = partitionEval[i].evaluate(row);
      }
      keyHashCode = hashFunc.apply(bucketFieldValues, partitionObjectInspectors);
    }
    int hashCode = buckNum < 0 ? keyHashCode : keyHashCode * 31 + buckNum;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Going to return hash code " + hashCode);
    }
    if (conf.isCompaction()) {
      int bucket;
      Object bucketProperty = ((Object[]) row)[2];
      if (bucketProperty == null) {
        return hashCode;
      }
      if (bucketProperty instanceof Writable) {
        bucket = ((IntWritable) bucketProperty).get();
      } else {
        bucket = (int) bucketProperty;
      }
      return BucketCodec.determineVersion(bucket).decodeWriterId(bucket);
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

  @Override
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
      runTimeNumRows++;
      if (numRows == cntr) {
        cntr = logEveryNRows == 0 ? cntr * 10 : numRows + logEveryNRows;
        if (cntr < 0 || numRows < 0) {
          cntr = 0;
          numRows = 1;
        }
        LOG.info("{}: records written - {}", this, numRows);
      }
      out.collect(keyWritable, valueWritable);
    }
  }

  private BytesWritable makeValueWritable(Object row) throws Exception {
    int length = valueEval.length;

    // Evaluate the value
    for (int i = 0; i < length; i++) {
      cachedValues[i] = valueEval[i].evaluate(row);
    }

    // Serialize the value
    return (BytesWritable) valueSerializer.serialize(cachedValues, valueObjectInspector);
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    if (!abort && reducerHash != null) {
      reducerHash.flush();
    }
    runTimeNumRows = numRows;
    super.closeOp(abort);
    out = null;
    random = null;
    reducerHash = null;
    LOG.info("{}: Total records written - {}. abort - {}", this, numRows, abort);
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

  @Override
  public boolean getIsReduceSink() {
    return true;
  }

  @Override
  public String getReduceOutputName() {
    return conf.getOutputName();
  }

  @Override
  public void setOutputCollector(OutputCollector _out) {
    this.out = _out;
  }

  @Override
  public void replaceTabAlias(String oldAlias, String newAlias) {
    super.replaceTabAlias(oldAlias, newAlias);
    ExprNodeDescUtils.replaceTabAlias(getConf().getColumnExprMap(), oldAlias, newAlias);
    ExprNodeDescUtils.replaceTabAlias(getConf().getBucketCols(), oldAlias, newAlias);
    ExprNodeDescUtils.replaceTabAlias(getConf().getPartitionCols(), oldAlias, newAlias);
    ExprNodeDescUtils.replaceTabAlias(getConf().getKeyCols(), oldAlias, newAlias);
  }
}
