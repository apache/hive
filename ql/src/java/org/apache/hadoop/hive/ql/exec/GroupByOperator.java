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

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

/**
 * GroupBy operator implementation.
 */
public class GroupByOperator extends Operator<GroupByDesc> implements
    Serializable {

  private static final Log LOG = LogFactory.getLog(GroupByOperator.class
      .getName());

  private static final long serialVersionUID = 1L;
  private static final int NUMROWSESTIMATESIZE = 1000;

  protected transient ExprNodeEvaluator[] keyFields;
  protected transient ObjectInspector[] keyObjectInspectors;
  protected transient Object[] keyObjects;

  protected transient ExprNodeEvaluator[][] aggregationParameterFields;
  protected transient ObjectInspector[][] aggregationParameterObjectInspectors;
  protected transient ObjectInspector[][] aggregationParameterStandardObjectInspectors;
  protected transient Object[][] aggregationParameterObjects;
  // In the future, we may allow both count(DISTINCT a) and sum(DISTINCT a) in
  // the same SQL clause,
  // so aggregationIsDistinct is a boolean array instead of a single number.
  protected transient boolean[] aggregationIsDistinct;

  transient GenericUDAFEvaluator[] aggregationEvaluators;

  protected transient ArrayList<ObjectInspector> objectInspectors;
  transient ArrayList<String> fieldNames;

  // Used by sort-based GroupBy: Mode = COMPLETE, PARTIAL1, PARTIAL2,
  // MERGEPARTIAL
  protected transient ArrayList<Object> currentKeys;
  protected transient ArrayList<Object> newKeys;
  protected transient AggregationBuffer[] aggregations;
  protected transient Object[][] aggregationsParametersLastInvoke;

  // Used by hash-based GroupBy: Mode = HASH, PARTIALS
  protected transient HashMap<KeyWrapper, AggregationBuffer[]> hashAggregations;

  // Used by hash distinct aggregations when hashGrpKeyNotRedKey is true
  protected transient HashSet<ArrayList<Object>> keysCurrentGroup;

  transient boolean bucketGroup;

  transient boolean firstRow;
  transient long totalMemory;
  transient boolean hashAggr;
  // The reduction is happening on the reducer, and the grouping key and
  // reduction keys are different.
  // For example: select a, count(distinct b) from T group by a
  // The data is sprayed by 'b' and the reducer is grouping it by 'a'
  transient boolean groupKeyIsNotReduceKey;
  transient boolean firstRowInGroup;
  transient long numRowsInput;
  transient long numRowsHashTbl;
  transient int groupbyMapAggrInterval;
  transient long numRowsCompareHashAggr;
  transient float minReductionHashAggr;

  // current Key ObjectInspectors are standard ObjectInspectors
  protected transient ObjectInspector[] currentKeyObjectInspectors;
  // new Key ObjectInspectors are objectInspectors from the parent
  transient StructObjectInspector newKeyObjectInspector;
  transient StructObjectInspector currentKeyObjectInspector;

  /**
   * This is used to store the position and field names for variable length
   * fields.
   **/
  class varLenFields {
    int aggrPos;
    List<Field> fields;

    varLenFields(int aggrPos, List<Field> fields) {
      this.aggrPos = aggrPos;
      this.fields = fields;
    }

    int getAggrPos() {
      return aggrPos;
    }

    List<Field> getFields() {
      return fields;
    }
  };

  // for these positions, some variable primitive type (String) is used, so size
  // cannot be estimated. sample it at runtime.
  transient List<Integer> keyPositionsSize;

  // for these positions, some variable primitive type (String) is used for the
  // aggregation classes
  transient List<varLenFields> aggrPositions;

  transient int fixedRowSize;
  transient long maxHashTblMemory;
  transient int totalVariableSize;
  transient int numEntriesVarSize;
  transient int numEntriesHashTable;
  transient int countAfterReport;
  transient int heartbeatInterval;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    totalMemory = Runtime.getRuntime().totalMemory();
    numRowsInput = 0;
    numRowsHashTbl = 0;

    heartbeatInterval = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVESENDHEARTBEAT);
    countAfterReport = 0;

    assert (inputObjInspectors.length == 1);
    ObjectInspector rowInspector = inputObjInspectors[0];

    // init keyFields
    keyFields = new ExprNodeEvaluator[conf.getKeys().size()];
    keyObjectInspectors = new ObjectInspector[conf.getKeys().size()];
    currentKeyObjectInspectors = new ObjectInspector[conf.getKeys().size()];
    keyObjects = new Object[conf.getKeys().size()];
    for (int i = 0; i < keyFields.length; i++) {
      keyFields[i] = ExprNodeEvaluatorFactory.get(conf.getKeys().get(i));
      keyObjectInspectors[i] = keyFields[i].initialize(rowInspector);
      currentKeyObjectInspectors[i] = ObjectInspectorUtils
          .getStandardObjectInspector(keyObjectInspectors[i],
          ObjectInspectorCopyOption.WRITABLE);
      keyObjects[i] = null;
    }
    newKeys = new ArrayList<Object>(keyFields.length);

    // init aggregationParameterFields
    aggregationParameterFields = new ExprNodeEvaluator[conf.getAggregators()
        .size()][];
    aggregationParameterObjectInspectors = new ObjectInspector[conf
        .getAggregators().size()][];
    aggregationParameterStandardObjectInspectors = new ObjectInspector[conf
        .getAggregators().size()][];
    aggregationParameterObjects = new Object[conf.getAggregators().size()][];
    for (int i = 0; i < aggregationParameterFields.length; i++) {
      ArrayList<ExprNodeDesc> parameters = conf.getAggregators().get(i)
          .getParameters();
      aggregationParameterFields[i] = new ExprNodeEvaluator[parameters.size()];
      aggregationParameterObjectInspectors[i] = new ObjectInspector[parameters
          .size()];
      aggregationParameterStandardObjectInspectors[i] = new ObjectInspector[parameters
          .size()];
      aggregationParameterObjects[i] = new Object[parameters.size()];
      for (int j = 0; j < parameters.size(); j++) {
        aggregationParameterFields[i][j] = ExprNodeEvaluatorFactory
            .get(parameters.get(j));
        aggregationParameterObjectInspectors[i][j] = aggregationParameterFields[i][j]
            .initialize(rowInspector);
        aggregationParameterStandardObjectInspectors[i][j] = ObjectInspectorUtils
            .getStandardObjectInspector(
            aggregationParameterObjectInspectors[i][j],
            ObjectInspectorCopyOption.WRITABLE);
        aggregationParameterObjects[i][j] = null;
      }
    }
    // init aggregationIsDistinct
    aggregationIsDistinct = new boolean[conf.getAggregators().size()];
    for (int i = 0; i < aggregationIsDistinct.length; i++) {
      aggregationIsDistinct[i] = conf.getAggregators().get(i).getDistinct();
    }

    // init aggregationClasses
    aggregationEvaluators = new GenericUDAFEvaluator[conf.getAggregators()
        .size()];
    for (int i = 0; i < aggregationEvaluators.length; i++) {
      AggregationDesc agg = conf.getAggregators().get(i);
      aggregationEvaluators[i] = agg.getGenericUDAFEvaluator();
    }

    // init objectInspectors
    int totalFields = keyFields.length + aggregationEvaluators.length;
    objectInspectors = new ArrayList<ObjectInspector>(totalFields);
    for (ExprNodeEvaluator keyField : keyFields) {
      objectInspectors.add(null);
    }
    for (int i = 0; i < aggregationEvaluators.length; i++) {
      ObjectInspector roi = aggregationEvaluators[i].init(conf.getAggregators()
          .get(i).getMode(), aggregationParameterObjectInspectors[i]);
      objectInspectors.add(roi);
    }

    bucketGroup = conf.getBucketGroup();
    aggregationsParametersLastInvoke = new Object[conf.getAggregators().size()][];
    if (conf.getMode() != GroupByDesc.Mode.HASH || bucketGroup) {
      aggregations = newAggregations();
      hashAggr = false;
    } else {
      hashAggregations = new HashMap<KeyWrapper, AggregationBuffer[]>();
      aggregations = newAggregations();
      hashAggr = true;
      keyPositionsSize = new ArrayList<Integer>();
      aggrPositions = new ArrayList<varLenFields>();
      groupbyMapAggrInterval = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVEGROUPBYMAPINTERVAL);

      // compare every groupbyMapAggrInterval rows
      numRowsCompareHashAggr = groupbyMapAggrInterval;
      minReductionHashAggr = HiveConf.getFloatVar(hconf,
          HiveConf.ConfVars.HIVEMAPAGGRHASHMINREDUCTION);
      groupKeyIsNotReduceKey = conf.getGroupKeyNotReductionKey();
      if (groupKeyIsNotReduceKey) {
        keysCurrentGroup = new HashSet<ArrayList<Object>>();
      }
    }

    fieldNames = conf.getOutputColumnNames();

    for (int i = 0; i < keyFields.length; i++) {
      objectInspectors.set(i, currentKeyObjectInspectors[i]);
    }

    // Generate key names
    ArrayList<String> keyNames = new ArrayList<String>(keyFields.length);
    for (int i = 0; i < keyFields.length; i++) {
      keyNames.add(fieldNames.get(i));
    }
    newKeyObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(keyNames, Arrays
        .asList(keyObjectInspectors));
    currentKeyObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(keyNames, Arrays
        .asList(currentKeyObjectInspectors));

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(fieldNames, objectInspectors);

    firstRow = true;
    // estimate the number of hash table entries based on the size of each
    // entry. Since the size of a entry
    // is not known, estimate that based on the number of entries
    if (hashAggr) {
      computeMaxEntriesHashAggr(hconf);
    }
    initializeChildren(hconf);
  }

  /**
   * Estimate the number of entries in map-side hash table. The user can specify
   * the total amount of memory to be used by the map-side hash. By default, all
   * available memory is used. The size of each row is estimated, rather
   * crudely, and the number of entries are figure out based on that.
   *
   * @return number of entries that can fit in hash table - useful for map-side
   *         aggregation only
   **/
  private void computeMaxEntriesHashAggr(Configuration hconf) throws HiveException {
    maxHashTblMemory = (long) (HiveConf.getFloatVar(hconf,
        HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY) * Runtime.getRuntime().maxMemory());
    estimateRowSize();
  }

  private static final int javaObjectOverHead = 64;
  private static final int javaHashEntryOverHead = 64;
  private static final int javaSizePrimitiveType = 16;
  private static final int javaSizeUnknownType = 256;

  /**
   * The size of the element at position 'pos' is returned, if possible. If the
   * datatype is of variable length, STRING, a list of such key positions is
   * maintained, and the size for such positions is then actually calculated at
   * runtime.
   *
   * @param pos
   *          the position of the key
   * @param c
   *          the type of the key
   * @return the size of this datatype
   **/
  private int getSize(int pos, PrimitiveCategory category) {
    switch (category) {
    case VOID:
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
      return javaSizePrimitiveType;
    case STRING:
      keyPositionsSize.add(new Integer(pos));
      return javaObjectOverHead;
    default:
      return javaSizeUnknownType;
    }
  }

  /**
   * The size of the element at position 'pos' is returned, if possible. If the
   * field is of variable length, STRING, a list of such field names for the
   * field position is maintained, and the size for such positions is then
   * actually calculated at runtime.
   *
   * @param pos
   *          the position of the key
   * @param c
   *          the type of the key
   * @param f
   *          the field to be added
   * @return the size of this datatype
   **/
  private int getSize(int pos, Class<?> c, Field f) {
    if (c.isPrimitive()
        || c.isInstance(new Boolean(true))
        || c.isInstance(new Byte((byte) 0))
        || c.isInstance(new Short((short) 0))
        || c.isInstance(new Integer(0))
        || c.isInstance(new Long(0))
        || c.isInstance(new Float(0))
        || c.isInstance(new Double(0))) {
      return javaSizePrimitiveType;
    }

    if (c.isInstance(new String())) {
      int idx = 0;
      varLenFields v = null;
      for (idx = 0; idx < aggrPositions.size(); idx++) {
        v = aggrPositions.get(idx);
        if (v.getAggrPos() == pos) {
          break;
        }
      }

      if (idx == aggrPositions.size()) {
        v = new varLenFields(pos, new ArrayList<Field>());
        aggrPositions.add(v);
      }

      v.getFields().add(f);
      return javaObjectOverHead;
    }

    return javaSizeUnknownType;
  }

  /**
   * @param pos
   *          position of the key
   * @param typeinfo
   *          type of the input
   * @return the size of this datatype
   **/
  private int getSize(int pos, TypeInfo typeInfo) {
    if (typeInfo instanceof PrimitiveTypeInfo) {
      return getSize(pos, ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
    }
    return javaSizeUnknownType;
  }

  /**
   * @return the size of each row
   **/
  private void estimateRowSize() throws HiveException {
    // estimate the size of each entry -
    // a datatype with unknown size (String/Struct etc. - is assumed to be 256
    // bytes for now).
    // 64 bytes is the overhead for a reference
    fixedRowSize = javaHashEntryOverHead;

    ArrayList<ExprNodeDesc> keys = conf.getKeys();

    // Go over all the keys and get the size of the fields of fixed length. Keep
    // track of the variable length keys
    for (int pos = 0; pos < keys.size(); pos++) {
      fixedRowSize += getSize(pos, keys.get(pos).getTypeInfo());
    }

    // Go over all the aggregation classes and and get the size of the fields of
    // fixed length. Keep track of the variable length
    // fields in these aggregation classes.
    for (int i = 0; i < aggregationEvaluators.length; i++) {

      fixedRowSize += javaObjectOverHead;
      Class<? extends AggregationBuffer> agg = aggregationEvaluators[i]
          .getNewAggregationBuffer().getClass();
      Field[] fArr = ObjectInspectorUtils.getDeclaredNonStaticFields(agg);
      for (Field f : fArr) {
        fixedRowSize += getSize(i, f.getType(), f);
      }
    }
  }

  protected AggregationBuffer[] newAggregations() throws HiveException {
    AggregationBuffer[] aggs = new AggregationBuffer[aggregationEvaluators.length];
    for (int i = 0; i < aggregationEvaluators.length; i++) {
      aggs[i] = aggregationEvaluators[i].getNewAggregationBuffer();
      // aggregationClasses[i].reset(aggs[i]);
    }
    return aggs;
  }

  protected void resetAggregations(AggregationBuffer[] aggs) throws HiveException {
    for (int i = 0; i < aggs.length; i++) {
      aggregationEvaluators[i].reset(aggs[i]);
    }
  }

  /*
   * Update aggregations. If the aggregation is for distinct, in case of hash
   * aggregation, the client tells us whether it is a new entry. For sort-based
   * aggregations, the last row is compared with the current one to figure out
   * whether it has changed. As a cleanup, the lastInvoke logic can be pushed in
   * the caller, and this function can be independent of that. The client should
   * always notify whether it is a different row or not.
   *
   * @param aggs the aggregations to be evaluated
   *
   * @param row the row being processed
   *
   * @param rowInspector the inspector for the row
   *
   * @param hashAggr whether hash aggregation is being performed or not
   *
   * @param newEntryForHashAggr only valid if it is a hash aggregation, whether
   * it is a new entry or not
   */
  protected void updateAggregations(AggregationBuffer[] aggs, Object row,
      ObjectInspector rowInspector, boolean hashAggr,
      boolean newEntryForHashAggr, Object[][] lastInvoke) throws HiveException {

    for (int ai = 0; ai < aggs.length; ai++) {

      // Calculate the parameters
      Object[] o = new Object[aggregationParameterFields[ai].length];
      for (int pi = 0; pi < aggregationParameterFields[ai].length; pi++) {
        o[pi] = aggregationParameterFields[ai][pi].evaluate(row);
      }

      // Update the aggregations.
      if (aggregationIsDistinct[ai]) {
        if (hashAggr) {
          if (newEntryForHashAggr) {
            aggregationEvaluators[ai].aggregate(aggs[ai], o);
          }
        } else {
          if (lastInvoke[ai] == null) {
            lastInvoke[ai] = new Object[o.length];
          }
          if (ObjectInspectorUtils.compare(o,
              aggregationParameterObjectInspectors[ai], lastInvoke[ai],
              aggregationParameterStandardObjectInspectors[ai]) != 0) {
            aggregationEvaluators[ai].aggregate(aggs[ai], o);
            for (int pi = 0; pi < o.length; pi++) {
              lastInvoke[ai][pi] = ObjectInspectorUtils.copyToStandardObject(
                  o[pi], aggregationParameterObjectInspectors[ai][pi],
                  ObjectInspectorCopyOption.WRITABLE);
            }
          }
        }
      } else {
        aggregationEvaluators[ai].aggregate(aggs[ai], o);
      }
    }
  }

  @Override
  public void startGroup() throws HiveException {
    firstRowInGroup = true;
  }

  @Override
  public void endGroup() throws HiveException {
    if (groupKeyIsNotReduceKey) {
      keysCurrentGroup.clear();
    }
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    firstRow = false;
    ObjectInspector rowInspector = inputObjInspectors[tag];
    // Total number of input rows is needed for hash aggregation only
    if (hashAggr && !groupKeyIsNotReduceKey) {
      numRowsInput++;
      // if hash aggregation is not behaving properly, disable it
      if (numRowsInput == numRowsCompareHashAggr) {
        numRowsCompareHashAggr += groupbyMapAggrInterval;
        // map-side aggregation should reduce the entries by at-least half
        if (numRowsHashTbl > numRowsInput * minReductionHashAggr) {
          LOG.warn("Disable Hash Aggr: #hash table = " + numRowsHashTbl
              + " #total = " + numRowsInput + " reduction = " + 1.0
              * (numRowsHashTbl / numRowsInput) + " minReduction = "
              + minReductionHashAggr);
          flush(true);
          hashAggr = false;
        } else {
          LOG.trace("Hash Aggr Enabled: #hash table = " + numRowsHashTbl
              + " #total = " + numRowsInput + " reduction = " + 1.0
              * (numRowsHashTbl / numRowsInput) + " minReduction = "
              + minReductionHashAggr);
        }
      }
    }

    try {
      countAfterReport++;

      // Compute the keys
      newKeys.clear();
      for (int i = 0; i < keyFields.length; i++) {
        if (keyObjectInspectors[i] == null) {
          keyObjectInspectors[i] = keyFields[i].initialize(rowInspector);
        }
        keyObjects[i] = keyFields[i].evaluate(row);
        newKeys.add(keyObjects[i]);
      }

      if (hashAggr) {
        processHashAggr(row, rowInspector, newKeys);
      } else {
        processAggr(row, rowInspector, newKeys);
      }

      firstRowInGroup = false;

      if (countAfterReport != 0 && (countAfterReport % heartbeatInterval) == 0
          && (reporter != null)) {
        reporter.progress();
        countAfterReport = 0;
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private static ArrayList<Object> deepCopyElements(Object[] keys,
      ObjectInspector[] keyObjectInspectors,
      ObjectInspectorCopyOption copyOption) {
    ArrayList<Object> result = new ArrayList<Object>(keys.length);
    deepCopyElements(keys, keyObjectInspectors, result, copyOption);
    return result;
  }

  private static void deepCopyElements(Object[] keys,
      ObjectInspector[] keyObjectInspectors, ArrayList<Object> result,
      ObjectInspectorCopyOption copyOption) {
    result.clear();
    for (int i = 0; i < keys.length; i++) {
      result.add(ObjectInspectorUtils.copyToStandardObject(keys[i],
          keyObjectInspectors[i], copyOption));
    }
  }

  class KeyWrapper {
    int hashcode;
    ArrayList<Object> keys;
    // decide whether this is already in hashmap (keys in hashmap are deepcopied
    // version, and we need to use 'currentKeyObjectInspector').
    boolean copy = false;

    KeyWrapper() {
    }

    public KeyWrapper(int hashcode, ArrayList<Object> copiedKeys) {
      this(hashcode, copiedKeys, false);
    }

    public KeyWrapper(int hashcode, ArrayList<Object> copiedKeys,
        boolean inHashMap) {
      super();
      this.hashcode = hashcode;
      keys = copiedKeys;
      copy = inHashMap;
    }

    @Override
    public int hashCode() {
      return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
      ArrayList<Object> copied_in_hashmap = ((KeyWrapper) obj).keys;
      if (!copy) {
        return ObjectInspectorUtils.compare(copied_in_hashmap,
            currentKeyObjectInspector, keys, newKeyObjectInspector) == 0;
      } else {
        return ObjectInspectorUtils.compare(copied_in_hashmap,
            currentKeyObjectInspector, keys, currentKeyObjectInspector) == 0;
      }
    }
  }

  KeyWrapper keyProber = new KeyWrapper();

  private void processHashAggr(Object row, ObjectInspector rowInspector,
      ArrayList<Object> newKeys) throws HiveException {
    // Prepare aggs for updating
    AggregationBuffer[] aggs = null;
    boolean newEntryForHashAggr = false;

    keyProber.hashcode = newKeys.hashCode();
    // use this to probe the hashmap
    keyProber.keys = newKeys;

    // hash-based aggregations
    aggs = hashAggregations.get(keyProber);
    ArrayList<Object> newDefaultKeys = null;
    if (aggs == null) {
      newDefaultKeys = deepCopyElements(keyObjects, keyObjectInspectors,
          ObjectInspectorCopyOption.WRITABLE);
      KeyWrapper newKeyProber = new KeyWrapper(keyProber.hashcode,
          newDefaultKeys, true);
      aggs = newAggregations();
      hashAggregations.put(newKeyProber, aggs);
      newEntryForHashAggr = true;
      numRowsHashTbl++; // new entry in the hash table
    }

    // If the grouping key and the reduction key are different, a set of
    // grouping keys for the current reduction key are maintained in
    // keysCurrentGroup
    // Peek into the set to find out if a new grouping key is seen for the given
    // reduction key
    if (groupKeyIsNotReduceKey) {
      if (newDefaultKeys == null) {
        newDefaultKeys = deepCopyElements(keyObjects, keyObjectInspectors,
            ObjectInspectorCopyOption.WRITABLE);
      }
      newEntryForHashAggr = keysCurrentGroup.add(newDefaultKeys);
    }

    // Update the aggs
    updateAggregations(aggs, row, rowInspector, true, newEntryForHashAggr, null);

    // We can only flush after the updateAggregations is done, or the
    // potentially new entry "aggs"
    // can be flushed out of the hash table.

    // Based on user-specified parameters, check if the hash table needs to be
    // flushed.
    // If the grouping key is not the same as reduction key, flushing can only
    // happen at boundaries
    if ((!groupKeyIsNotReduceKey || firstRowInGroup)
        && shouldBeFlushed(newKeys)) {
      flush(false);
    }
  }

  // Non-hash aggregation
  private void processAggr(Object row, ObjectInspector rowInspector,
      ArrayList<Object> newKeys) throws HiveException {
    // Prepare aggs for updating
    AggregationBuffer[] aggs = null;
    Object[][] lastInvoke = null;
    boolean keysAreEqual = ObjectInspectorUtils.compare(newKeys,
        newKeyObjectInspector, currentKeys, currentKeyObjectInspector) == 0;

    // Forward the current keys if needed for sort-based aggregation
    if (currentKeys != null && !keysAreEqual) {
      forward(currentKeys, aggregations);
      countAfterReport = 0;
    }

    // Need to update the keys?
    if (currentKeys == null || !keysAreEqual) {
      if (currentKeys == null) {
        currentKeys = new ArrayList<Object>(keyFields.length);
      }
      deepCopyElements(keyObjects, keyObjectInspectors, currentKeys,
          ObjectInspectorCopyOption.WRITABLE);

      // Reset the aggregations
      resetAggregations(aggregations);

      // clear parameters in last-invoke
      for (int i = 0; i < aggregationsParametersLastInvoke.length; i++) {
        aggregationsParametersLastInvoke[i] = null;
      }
    }

    aggs = aggregations;

    lastInvoke = aggregationsParametersLastInvoke;
    // Update the aggs

    updateAggregations(aggs, row, rowInspector, false, false, lastInvoke);
  }

  /**
   * Based on user-parameters, should the hash table be flushed.
   *
   * @param newKeys
   *          keys for the row under consideration
   **/
  private boolean shouldBeFlushed(ArrayList<Object> newKeys) {
    int numEntries = hashAggregations.size();

    // The fixed size for the aggregation class is already known. Get the
    // variable portion of the size every NUMROWSESTIMATESIZE rows.
    if ((numEntriesHashTable == 0) || ((numEntries % NUMROWSESTIMATESIZE) == 0)) {
      for (Integer pos : keyPositionsSize) {
        Object key = newKeys.get(pos.intValue());
        // Ignore nulls
        if (key != null) {
          if (key instanceof String) {
            totalVariableSize += ((String) key).length();
          } else if (key instanceof Text) {
            totalVariableSize += ((Text) key).getLength();
          }
        }
      }

      AggregationBuffer[] aggs = null;
      if (aggrPositions.size() > 0) {
        aggs = hashAggregations.get(newKeys);
      }

      for (varLenFields v : aggrPositions) {
        int aggrPos = v.getAggrPos();
        List<Field> fieldsVarLen = v.getFields();
        AggregationBuffer agg = aggs[aggrPos];

        try {
          for (Field f : fieldsVarLen) {
            totalVariableSize += ((String) f.get(agg)).length();
          }
        } catch (IllegalAccessException e) {
          assert false;
        }
      }

      numEntriesVarSize++;

      // Update the number of entries that can fit in the hash table
      numEntriesHashTable =
          (int) (maxHashTblMemory / (fixedRowSize + (totalVariableSize / numEntriesVarSize)));
      LOG.trace("Hash Aggr: #hash table = " + numEntries
          + " #max in hash table = " + numEntriesHashTable);
    }

    // flush if necessary
    if (numEntries >= numEntriesHashTable) {
      return true;
    }
    return false;
  }

  private void flush(boolean complete) throws HiveException {

    countAfterReport = 0;

    // Currently, the algorithm flushes 10% of the entries - this can be
    // changed in the future

    if (complete) {
      Iterator<Map.Entry<KeyWrapper, AggregationBuffer[]>> iter = hashAggregations
          .entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<KeyWrapper, AggregationBuffer[]> m = iter.next();
        forward(m.getKey().keys, m.getValue());
      }
      hashAggregations.clear();
      hashAggregations = null;
      LOG.warn("Hash Table completed flushed");
      return;
    }

    int oldSize = hashAggregations.size();
    LOG.warn("Hash Tbl flush: #hash table = " + oldSize);
    Iterator<Map.Entry<KeyWrapper, AggregationBuffer[]>> iter = hashAggregations
        .entrySet().iterator();
    int numDel = 0;
    while (iter.hasNext()) {
      Map.Entry<KeyWrapper, AggregationBuffer[]> m = iter.next();
      forward(m.getKey().keys, m.getValue());
      iter.remove();
      numDel++;
      if (numDel * 10 >= oldSize) {
        LOG.warn("Hash Table flushed: new size = " + hashAggregations.size());
        return;
      }
    }
  }

  transient Object[] forwardCache;

  /**
   * Forward a record of keys and aggregation results.
   *
   * @param keys
   *          The keys in the record
   * @throws HiveException
   */
  protected void forward(ArrayList<Object> keys, AggregationBuffer[] aggs)
      throws HiveException {
    int totalFields = keys.size() + aggs.length;
    if (forwardCache == null) {
      forwardCache = new Object[totalFields];
    }
    for (int i = 0; i < keys.size(); i++) {
      forwardCache[i] = keys.get(i);
    }
    for (int i = 0; i < aggs.length; i++) {
      forwardCache[keys.size() + i] = aggregationEvaluators[i]
          .evaluate(aggs[i]);
    }
    forward(forwardCache, outputObjInspector);
  }

  /**
   * We need to forward all the aggregations to children.
   *
   */
  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      try {
        // If there is no grouping key and no row came to this operator
        if (firstRow && (keyFields.length == 0)) {
          firstRow = false;

          // There is no grouping key - simulate a null row
          // This is based on the assumption that a null row is ignored by
          // aggregation functions
          for (int ai = 0; ai < aggregations.length; ai++) {

            // o is set to NULL in order to distinguish no rows at all
            Object[] o;
            if (aggregationParameterFields[ai].length > 0) {
              o = new Object[aggregationParameterFields[ai].length];
            } else {
              o = null;
            }

            // Calculate the parameters
            for (int pi = 0; pi < aggregationParameterFields[ai].length; pi++) {
              o[pi] = null;
            }
            aggregationEvaluators[ai].aggregate(aggregations[ai], o);
          }

          // create dummy keys - size 0
          forward(new ArrayList<Object>(0), aggregations);
        } else {
          if (hashAggregations != null) {
            LOG.warn("Begin Hash Table flush at close: size = "
                + hashAggregations.size());
            Iterator iter = hashAggregations.entrySet().iterator();
            while (iter.hasNext()) {
              Map.Entry<KeyWrapper, AggregationBuffer[]> m = (Map.Entry) iter
                  .next();
              forward(m.getKey().keys, m.getValue());
              iter.remove();
            }
            hashAggregations.clear();
          } else if (aggregations != null) {
            // sort-based aggregations
            if (currentKeys != null) {
              forward(currentKeys, aggregations);
            }
            currentKeys = null;
          } else {
            // The GroupByOperator is not initialized, which means there is no
            // data
            // (since we initialize the operators when we see the first record).
            // Just do nothing here.
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new HiveException(e);
      }
    }
  }

  // Group by contains the columns needed - no need to aggregate from children
  public List<String> genColLists(
      HashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx) {
    List<String> colLists = new ArrayList<String>();
    ArrayList<ExprNodeDesc> keys = conf.getKeys();
    for (ExprNodeDesc key : keys) {
      colLists = Utilities.mergeUniqElems(colLists, key.getCols());
    }

    ArrayList<AggregationDesc> aggrs = conf.getAggregators();
    for (AggregationDesc aggr : aggrs) {
      ArrayList<ExprNodeDesc> params = aggr.getParameters();
      for (ExprNodeDesc param : params) {
        colLists = Utilities.mergeUniqElems(colLists, param.getCols());
      }
    }

    return colLists;
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return new String("GBY");
  }

  @Override
  public int getType() {
    return OperatorType.GROUPBY;
  }
}
