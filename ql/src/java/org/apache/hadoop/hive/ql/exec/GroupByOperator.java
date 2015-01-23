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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javolution.util.FastBitSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyBinary;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * GroupBy operator implementation.
 */
public class GroupByOperator extends Operator<GroupByDesc> {

  private static final long serialVersionUID = 1L;
  private static final int NUMROWSESTIMATESIZE = 1000;

  private transient ExprNodeEvaluator[] keyFields;
  private transient ObjectInspector[] keyObjectInspectors;

  private transient ExprNodeEvaluator[][] aggregationParameterFields;
  private transient ObjectInspector[][] aggregationParameterObjectInspectors;
  private transient ObjectInspector[][] aggregationParameterStandardObjectInspectors;
  private transient Object[][] aggregationParameterObjects;
  
  // so aggregationIsDistinct is a boolean array instead of a single number.
  private transient boolean[] aggregationIsDistinct;
  // Map from integer tag to distinct aggrs
  private transient Map<Integer, Set<Integer>> distinctKeyAggrs =
    new HashMap<Integer, Set<Integer>>();
  // Map from integer tag to non-distinct aggrs with key parameters.
  private transient Map<Integer, Set<Integer>> nonDistinctKeyAggrs =
    new HashMap<Integer, Set<Integer>>();
  // List of non-distinct aggrs.
  private transient List<Integer> nonDistinctAggrs = new ArrayList<Integer>();
  // Union expr for distinct keys
  private transient ExprNodeEvaluator unionExprEval;

  private transient GenericUDAFEvaluator[] aggregationEvaluators;
  private transient boolean[] estimableAggregationEvaluators;

  // Used by sort-based GroupBy: Mode = COMPLETE, PARTIAL1, PARTIAL2,
  // MERGEPARTIAL
  private transient KeyWrapper currentKeys;
  private transient KeyWrapper newKeys;
  private transient AggregationBuffer[] aggregations;
  private transient Object[][] aggregationsParametersLastInvoke;

  // Used by hash-based GroupBy: Mode = HASH, PARTIALS
  private transient HashMap<KeyWrapper, AggregationBuffer[]> hashAggregations;

  // Used by hash distinct aggregations when hashGrpKeyNotRedKey is true
  private transient HashSet<KeyWrapper> keysCurrentGroup;

  private transient boolean firstRow;
  private transient boolean hashAggr;
  // The reduction is happening on the reducer, and the grouping key and
  // reduction keys are different.
  // For example: select a, count(distinct b) from T group by a
  // The data is sprayed by 'b' and the reducer is grouping it by 'a'
  private transient boolean groupKeyIsNotReduceKey;
  private transient boolean firstRowInGroup;
  private transient long numRowsInput;
  private transient long numRowsHashTbl;
  private transient int groupbyMapAggrInterval;
  private transient long numRowsCompareHashAggr;
  private transient float minReductionHashAggr;

  private transient int outputKeyLength;

  // current Key ObjectInspectors are standard ObjectInspectors
  private transient ObjectInspector[] currentKeyObjectInspectors;

  private transient MemoryMXBean memoryMXBean;

  private transient boolean groupingSetsPresent;      // generates grouping set
  private transient int groupingSetsPosition;         // position of grouping set, generally the last of keys
  private transient List<Integer> groupingSets;       // declared grouping set values  
  private transient FastBitSet[] groupingSetsBitSet;  // bitsets acquired from grouping set values 
  private transient Text[] newKeysGroupingSets;

  // for these positions, some variable primitive type (String) is used, so size
  // cannot be estimated. sample it at runtime.
  private transient List<Integer> keyPositionsSize;

  // for these positions, some variable primitive type (String) is used for the
  // aggregation classes
  private transient List<Field>[] aggrPositions;

  private transient int fixedRowSize;

  private transient int totalVariableSize;
  private transient int numEntriesVarSize;

  private transient int countAfterReport;   // report or forward
  private transient int heartbeatInterval;

  /**
   * Total amount of memory allowed for JVM heap.
   */
  protected transient long maxMemory;

  /**
   * Max memory usable by the hashtable before it should flush.
   */
  protected transient long maxHashTblMemory;

  /**
   * configure percent of memory threshold usable by QP.
   */
  protected transient float memoryThreshold;

  /**
   * Current number of entries in the hash table.
   */
  protected transient int numEntriesHashTable;

  public static FastBitSet groupingSet2BitSet(int value) {
    FastBitSet bits = new FastBitSet();
    int index = 0;
    while (value != 0) {
      if (value % 2 != 0) {
        bits.set(index);
      }
      ++index;
      value = value >>> 1;
    }
    return bits;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    numRowsInput = 0;
    numRowsHashTbl = 0;

    heartbeatInterval = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVESENDHEARTBEAT);
    countAfterReport = 0;
    groupingSetsPresent = conf.isGroupingSetsPresent();
    ObjectInspector rowInspector = inputObjInspectors[0];

    // init keyFields
    int numKeys = conf.getKeys().size();

    keyFields = new ExprNodeEvaluator[numKeys];
    keyObjectInspectors = new ObjectInspector[numKeys];
    currentKeyObjectInspectors = new ObjectInspector[numKeys];
    for (int i = 0; i < numKeys; i++) {
      keyFields[i] = ExprNodeEvaluatorFactory.get(conf.getKeys().get(i));
      keyObjectInspectors[i] = keyFields[i].initialize(rowInspector);
      currentKeyObjectInspectors[i] = ObjectInspectorUtils
        .getStandardObjectInspector(keyObjectInspectors[i],
        ObjectInspectorCopyOption.WRITABLE);
    }

    // Initialize the constants for the grouping sets, so that they can be re-used for
    // each row
    if (groupingSetsPresent) {
      groupingSets = conf.getListGroupingSets();
      groupingSetsPosition = conf.getGroupingSetPosition();
      newKeysGroupingSets = new Text[groupingSets.size()];
      groupingSetsBitSet = new FastBitSet[groupingSets.size()];

      int pos = 0;
      for (Integer groupingSet: groupingSets) {
        // Create the mapping corresponding to the grouping set
        newKeysGroupingSets[pos] = new Text(String.valueOf(groupingSet));
        groupingSetsBitSet[pos] = groupingSet2BitSet(groupingSet);
        pos++;
      }
    }

    // initialize unionExpr for reduce-side
    // reduce KEY has union field as the last field if there are distinct
    // aggregates in group-by.
    List<? extends StructField> sfs =
      ((StructObjectInspector) rowInspector).getAllStructFieldRefs();
    if (sfs.size() > 0) {
      StructField keyField = sfs.get(0);
      if (keyField.getFieldName().toUpperCase().equals(
          Utilities.ReduceField.KEY.name())) {
        ObjectInspector keyObjInspector = keyField.getFieldObjectInspector();
        if (keyObjInspector instanceof StructObjectInspector) {
          List<? extends StructField> keysfs =
            ((StructObjectInspector) keyObjInspector).getAllStructFieldRefs();
          if (keysfs.size() > 0) {
            // the last field is the union field, if any
            StructField sf = keysfs.get(keysfs.size() - 1);
            if (sf.getFieldObjectInspector().getCategory().equals(
                ObjectInspector.Category.UNION)) {
              unionExprEval = ExprNodeEvaluatorFactory.get(
                new ExprNodeColumnDesc(TypeInfoUtils.getTypeInfoFromObjectInspector(
                sf.getFieldObjectInspector()),
                keyField.getFieldName() + "." + sf.getFieldName(), null,
                false));
              unionExprEval.initialize(rowInspector);
            }
          }
        }
      }
    }
    // init aggregationParameterFields
    ArrayList<AggregationDesc> aggrs = conf.getAggregators();
    aggregationParameterFields = new ExprNodeEvaluator[aggrs.size()][];
    aggregationParameterObjectInspectors = new ObjectInspector[aggrs.size()][];
    aggregationParameterStandardObjectInspectors = new ObjectInspector[aggrs.size()][];
    aggregationParameterObjects = new Object[aggrs.size()][];
    aggregationIsDistinct = new boolean[aggrs.size()];
    for (int i = 0; i < aggrs.size(); i++) {
      AggregationDesc aggr = aggrs.get(i);
      ArrayList<ExprNodeDesc> parameters = aggr.getParameters();
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
        if (unionExprEval != null) {
          String[] names = parameters.get(j).getExprString().split("\\.");
          // parameters of the form : KEY.colx:t.coly
          if (Utilities.ReduceField.KEY.name().equals(names[0]) && names.length > 2) {
            String name = names[names.length - 2];
            int tag = Integer.parseInt(name.split("\\:")[1]);
            if (aggr.getDistinct()) {
              // is distinct
              Set<Integer> set = distinctKeyAggrs.get(tag);
              if (null == set) {
                set = new HashSet<Integer>();
                distinctKeyAggrs.put(tag, set);
              }
              if (!set.contains(i)) {
                set.add(i);
              }
            } else {
              Set<Integer> set = nonDistinctKeyAggrs.get(tag);
              if (null == set) {
                set = new HashSet<Integer>();
                nonDistinctKeyAggrs.put(tag, set);
              }
              if (!set.contains(i)) {
                set.add(i);
              }
            }
          } else {
            // will be KEY._COLx or VALUE._COLx
            if (!nonDistinctAggrs.contains(i)) {
              nonDistinctAggrs.add(i);
            }
          }
        } else {
          if (aggr.getDistinct()) {
            aggregationIsDistinct[i] = true;
          }
        }
        aggregationParameterStandardObjectInspectors[i][j] = ObjectInspectorUtils
            .getStandardObjectInspector(
            aggregationParameterObjectInspectors[i][j],
            ObjectInspectorCopyOption.WRITABLE);
        aggregationParameterObjects[i][j] = null;
      }
      if (parameters.size() == 0) {
        // for ex: count(*)
        if (!nonDistinctAggrs.contains(i)) {
          nonDistinctAggrs.add(i);
        }
      }
    }

    // init aggregationClasses
    aggregationEvaluators = new GenericUDAFEvaluator[conf.getAggregators()
        .size()];
    for (int i = 0; i < aggregationEvaluators.length; i++) {
      AggregationDesc agg = conf.getAggregators().get(i);
      aggregationEvaluators[i] = agg.getGenericUDAFEvaluator();
    }

    MapredContext context = MapredContext.get();
    if (context != null) {
      for (GenericUDAFEvaluator genericUDAFEvaluator : aggregationEvaluators) {
        context.setup(genericUDAFEvaluator);
      }
    }

    aggregationsParametersLastInvoke = new Object[conf.getAggregators().size()][];
    if ((conf.getMode() != GroupByDesc.Mode.HASH || conf.getBucketGroup()) &&
      (!groupingSetsPresent)) {
      aggregations = newAggregations();
      hashAggr = false;
    } else {
      hashAggregations = new HashMap<KeyWrapper, AggregationBuffer[]>(256);
      aggregations = newAggregations();
      hashAggr = true;
      keyPositionsSize = new ArrayList<Integer>();
      aggrPositions = new List[aggregations.length];
      groupbyMapAggrInterval = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVEGROUPBYMAPINTERVAL);

      // compare every groupbyMapAggrInterval rows
      numRowsCompareHashAggr = groupbyMapAggrInterval;
      minReductionHashAggr = HiveConf.getFloatVar(hconf,
          HiveConf.ConfVars.HIVEMAPAGGRHASHMINREDUCTION);
      groupKeyIsNotReduceKey = conf.getGroupKeyNotReductionKey();
      if (groupKeyIsNotReduceKey) {
        keysCurrentGroup = new HashSet<KeyWrapper>();
      }
    }

    List<String> fieldNames = new ArrayList<String>(conf.getOutputColumnNames());

    // grouping id should be pruned, which is the last of key columns
    // see ColumnPrunerGroupByProc
    outputKeyLength = conf.pruneGroupingSetId() ? keyFields.length - 1 : keyFields.length;

    // init objectInspectors
    ObjectInspector[] objectInspectors = 
        new ObjectInspector[outputKeyLength + aggregationEvaluators.length];
    for (int i = 0; i < outputKeyLength; i++) {
      objectInspectors[i] = currentKeyObjectInspectors[i];
    }
    for (int i = 0; i < aggregationEvaluators.length; i++) {
      objectInspectors[outputKeyLength + i] = aggregationEvaluators[i].init(conf.getAggregators()
          .get(i).getMode(), aggregationParameterObjectInspectors[i]);
    }

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(fieldNames, Arrays.asList(objectInspectors));

    KeyWrapperFactory keyWrapperFactory =
      new KeyWrapperFactory(keyFields, keyObjectInspectors, currentKeyObjectInspectors);

    newKeys = keyWrapperFactory.getKeyWrapper();

    firstRow = true;
    // estimate the number of hash table entries based on the size of each
    // entry. Since the size of a entry
    // is not known, estimate that based on the number of entries
    if (hashAggr) {
      computeMaxEntriesHashAggr(hconf);
    }
    memoryMXBean = ManagementFactory.getMemoryMXBean();
    maxMemory = memoryMXBean.getHeapMemoryUsage().getMax();
    memoryThreshold = this.getConf().getMemoryThreshold();
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
    float memoryPercentage = this.getConf().getGroupByMemoryUsage();
    maxHashTblMemory = (long) (memoryPercentage * Runtime.getRuntime().maxMemory());
    estimateRowSize();
  }

  public static final int javaObjectOverHead = 64;
  public static final int javaHashEntryOverHead = 64;
  public static final int javaSizePrimitiveType = 16;
  public static final int javaSizeUnknownType = 256;

  /**
   * The size of the element at position 'pos' is returned, if possible. If the
   * datatype is of variable length, STRING, a list of such key positions is
   * maintained, and the size for such positions is then actually calculated at
   * runtime.
   *
   * @param pos
   *          the position of the key
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
    case BINARY:
      keyPositionsSize.add(new Integer(pos));
      return javaObjectOverHead;
    case TIMESTAMP:
      return javaObjectOverHead + javaSizePrimitiveType;
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
        || c.isInstance(Boolean.valueOf(true))
        || c.isInstance(Byte.valueOf((byte) 0))
        || c.isInstance(Short.valueOf((short) 0))
        || c.isInstance(Integer.valueOf(0))
        || c.isInstance(Long.valueOf(0))
        || c.isInstance(new Float(0))
        || c.isInstance(new Double(0))) {
      return javaSizePrimitiveType;
    }

    if (c.isInstance(new Timestamp(0))){
      return javaObjectOverHead + javaSizePrimitiveType;
    }

    if (c.isInstance(new String()) || c.isInstance(new ByteArrayRef())) {
      if (aggrPositions[pos] == null) {
        aggrPositions[pos] = new ArrayList<Field>();
      }
      aggrPositions[pos].add(f);
      return javaObjectOverHead;
    }

    return javaSizeUnknownType;
  }

  /**
   * @param pos
   *          position of the key
   * @param typeInfo
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
    estimableAggregationEvaluators = new boolean[aggregationEvaluators.length];
    for (int i = 0; i < aggregationEvaluators.length; i++) {

      fixedRowSize += javaObjectOverHead;
      AggregationBuffer agg = aggregationEvaluators[i].getNewAggregationBuffer();
      if (GenericUDAFEvaluator.isEstimable(agg)) {
        estimableAggregationEvaluators[i] = true;
        continue;
      }
      Field[] fArr = ObjectInspectorUtils.getDeclaredNonStaticFields(agg.getClass());
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
    if (unionExprEval == null) {
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
      return;
    }

    if (distinctKeyAggrs.size() > 0) {
      // evaluate union object
      UnionObject uo = (UnionObject) (unionExprEval.evaluate(row));
      int unionTag = uo.getTag();

      // update non-distinct key aggregations : "KEY._colx:t._coly"
      if (nonDistinctKeyAggrs.get(unionTag) != null) {
        for (int pos : nonDistinctKeyAggrs.get(unionTag)) {
          Object[] o = new Object[aggregationParameterFields[pos].length];
          for (int pi = 0; pi < aggregationParameterFields[pos].length; pi++) {
            o[pi] = aggregationParameterFields[pos][pi].evaluate(row);
          }
          aggregationEvaluators[pos].aggregate(aggs[pos], o);
        }
      }
      // there may be multi distinct clauses for one column
      // update them all.
      if (distinctKeyAggrs.get(unionTag) != null) {
        for (int i : distinctKeyAggrs.get(unionTag)) {
          Object[] o = new Object[aggregationParameterFields[i].length];
          for (int pi = 0; pi < aggregationParameterFields[i].length; pi++) {
            o[pi] = aggregationParameterFields[i][pi].evaluate(row);
          }

          if (hashAggr) {
            if (newEntryForHashAggr) {
              aggregationEvaluators[i].aggregate(aggs[i], o);
            }
          } else {
            if (lastInvoke[i] == null) {
              lastInvoke[i] = new Object[o.length];
            }
            if (ObjectInspectorUtils.compare(o,
                aggregationParameterObjectInspectors[i],
                lastInvoke[i],
                aggregationParameterStandardObjectInspectors[i]) != 0) {
              aggregationEvaluators[i].aggregate(aggs[i], o);
              for (int pi = 0; pi < o.length; pi++) {
                lastInvoke[i][pi] = ObjectInspectorUtils.copyToStandardObject(
                    o[pi], aggregationParameterObjectInspectors[i][pi],
                    ObjectInspectorCopyOption.WRITABLE);
              }
            }
          }
        }
      }

      // update non-distinct groupby key or value aggregations: 'KEY._COLx or VALUE._colx'
      // these aggregations should be updated only once.
      if (unionTag == 0) {
        for (int pos : nonDistinctAggrs) {
          Object[] o = new Object[aggregationParameterFields[pos].length];
          for (int pi = 0; pi < aggregationParameterFields[pos].length; pi++) {
            o[pi] = aggregationParameterFields[pos][pi].evaluate(row);
          }
          aggregationEvaluators[pos].aggregate(aggs[pos], o);
        }
      }
    } else {
      for (int ai = 0; ai < aggs.length; ai++) {
        // there is no distinct aggregation,
        // update all aggregations
        Object[] o = new Object[aggregationParameterFields[ai].length];
        for (int pi = 0; pi < aggregationParameterFields[ai].length; pi++) {
          o[pi] = aggregationParameterFields[ai][pi].evaluate(row);
        }
        aggregationEvaluators[ai].aggregate(aggs[ai], o);
      }
    }
  }

  @Override
  public void startGroup() throws HiveException {
    firstRowInGroup = true;
    super.startGroup();
  }

  @Override
  public void endGroup() throws HiveException {
    if (groupKeyIsNotReduceKey) {
      keysCurrentGroup.clear();
    }
  }

  private void processKey(Object row,
      ObjectInspector rowInspector) throws HiveException {
    if (hashAggr) {
      newKeys.setHashKey();
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
          flushHashTable(true);
          hashAggr = false;
        } else {
          if (isLogTraceEnabled) {
            LOG.trace("Hash Aggr Enabled: #hash table = " + numRowsHashTbl
                + " #total = " + numRowsInput + " reduction = " + 1.0
                * (numRowsHashTbl / numRowsInput) + " minReduction = "
                + minReductionHashAggr);
          }
        }
      }
    }

    try {
      countAfterReport++;
      newKeys.getNewKey(row, rowInspector);

      if (groupingSetsPresent) {
        Object[] newKeysArray = newKeys.getKeyArray();
        Object[] cloneNewKeysArray = new Object[newKeysArray.length];
        for (int keyPos = 0; keyPos < groupingSetsPosition; keyPos++) {
          cloneNewKeysArray[keyPos] = newKeysArray[keyPos];
        }

        for (int groupingSetPos = 0; groupingSetPos < groupingSets.size(); groupingSetPos++) {
          for (int keyPos = 0; keyPos < groupingSetsPosition; keyPos++) {
            newKeysArray[keyPos] = null;
          }

          FastBitSet bitset = groupingSetsBitSet[groupingSetPos];
          // Some keys need to be left to null corresponding to that grouping set.
          for (int keyPos = bitset.nextSetBit(0); keyPos >= 0;
            keyPos = bitset.nextSetBit(keyPos+1)) {
            newKeysArray[keyPos] = cloneNewKeysArray[keyPos];
          }

          newKeysArray[groupingSetsPosition] = newKeysGroupingSets[groupingSetPos];
          processKey(row, rowInspector);
        }
      } else {
        processKey(row, rowInspector);
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private void processHashAggr(Object row, ObjectInspector rowInspector,
      KeyWrapper newKeys) throws HiveException {
    // Prepare aggs for updating
    AggregationBuffer[] aggs = null;
    boolean newEntryForHashAggr = false;

    // hash-based aggregations
    aggs = hashAggregations.get(newKeys);
    if (aggs == null) {
      KeyWrapper newKeyProber = newKeys.copyKey();
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
      newEntryForHashAggr = keysCurrentGroup.add(newKeys.copyKey());
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
      flushHashTable(false);
    }
  }

  // Non-hash aggregation
  private void processAggr(Object row,
      ObjectInspector rowInspector,
      KeyWrapper newKeys) throws HiveException {
    // Prepare aggs for updating
    AggregationBuffer[] aggs = null;
    Object[][] lastInvoke = null;
    //boolean keysAreEqual = (currentKeys != null && newKeys != null)?
    //  newKeyStructEqualComparer.areEqual(currentKeys, newKeys) : false;

    boolean keysAreEqual = (currentKeys != null && newKeys != null)?
        newKeys.equals(currentKeys) : false;

    // Forward the current keys if needed for sort-based aggregation
    if (currentKeys != null && !keysAreEqual) {
      // This is to optimize queries of the form:
      // select count(distinct key) from T
      // where T is sorted and bucketized by key
      // Partial aggregation is performed on the mapper, and the
      // reducer gets 1 row (partial result) per mapper.
      if (!conf.isDontResetAggrsDistinct()) {
        forward(currentKeys.getKeyArray(), aggregations);
        countAfterReport = 0;
      }
    }

    // Need to update the keys?
    if (currentKeys == null || !keysAreEqual) {
      if (currentKeys == null) {
        currentKeys = newKeys.copyKey();
      } else {
        currentKeys.copyKey(newKeys);
      }

      // Reset the aggregations
      // For distincts optimization with sorting/bucketing, perform partial aggregation
      if (!conf.isDontResetAggrsDistinct()) {
        resetAggregations(aggregations);
      }

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
  private boolean shouldBeFlushed(KeyWrapper newKeys) {
    int numEntries = hashAggregations.size();
    long usedMemory;
    float rate;

    // The fixed size for the aggregation class is already known. Get the
    // variable portion of the size every NUMROWSESTIMATESIZE rows.
    if ((numEntriesHashTable == 0) || ((numEntries % NUMROWSESTIMATESIZE) == 0)) {
      //check how much memory left memory
      usedMemory = memoryMXBean.getHeapMemoryUsage().getUsed();
      rate = (float) usedMemory / (float) maxMemory;
      if(rate > memoryThreshold){
        return true;
      }
      for (Integer pos : keyPositionsSize) {
        Object key = newKeys.getKeyArray()[pos.intValue()];
        // Ignore nulls
        if (key != null) {
          if (key instanceof LazyString) {
              totalVariableSize +=
                  ((LazyPrimitive<LazyStringObjectInspector, Text>) key).
                      getWritableObject().getLength();
          } else if (key instanceof String) {
            totalVariableSize += ((String) key).length();
          } else if (key instanceof Text) {
            totalVariableSize += ((Text) key).getLength();
          } else if (key instanceof LazyBinary) {
            totalVariableSize +=
                ((LazyPrimitive<LazyBinaryObjectInspector, BytesWritable>) key).
                    getWritableObject().getLength();
          } else if (key instanceof BytesWritable) {
            totalVariableSize += ((BytesWritable) key).getLength();
          } else if (key instanceof ByteArrayRef) {
            totalVariableSize += ((ByteArrayRef) key).getData().length;
          }
        }
      }

      AggregationBuffer[] aggs = hashAggregations.get(newKeys);
      for (int i = 0; i < aggs.length; i++) {
        AggregationBuffer agg = aggs[i];
        if (estimableAggregationEvaluators[i]) {
          totalVariableSize += ((GenericUDAFEvaluator.AbstractAggregationBuffer)agg).estimate();
          continue;
        }
        if (aggrPositions[i] != null) {
          totalVariableSize += estimateSize(agg, aggrPositions[i]);
        }
      }

      numEntriesVarSize++;

      // Update the number of entries that can fit in the hash table
      numEntriesHashTable =
          (int) (maxHashTblMemory / (fixedRowSize + (totalVariableSize / numEntriesVarSize)));
      if (isLogTraceEnabled) {
        LOG.trace("Hash Aggr: #hash table = " + numEntries
            + " #max in hash table = " + numEntriesHashTable);
      }
    }

    // flush if necessary
    if (numEntries >= numEntriesHashTable) {
      return true;
    }
    return false;
  }

  private int estimateSize(AggregationBuffer agg, List<Field> fields) {
    int length = 0;
    for (Field f : fields) {
      try {
        Object o = f.get(agg);
        if (o instanceof String){
          length += ((String)o).length();
        }
        else if (o instanceof ByteArrayRef){
          length += ((ByteArrayRef)o).getData().length;
        }
      } catch (Exception e) {
        // continue.. null out the field?
      }
    }
    return length;
  }

  /**
   * Flush hash table. This method is used by hash-based aggregations
   * @param complete
   * @throws HiveException
   */
  private void flushHashTable(boolean complete) throws HiveException {

    countAfterReport = 0;

    // Currently, the algorithm flushes 10% of the entries - this can be
    // changed in the future

    if (complete) {
      Iterator<Map.Entry<KeyWrapper, AggregationBuffer[]>> iter = hashAggregations
          .entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<KeyWrapper, AggregationBuffer[]> m = iter.next();
        forward(m.getKey().getKeyArray(), m.getValue());
      }
      hashAggregations.clear();
      hashAggregations = null;
      LOG.info("Hash Table completed flushed");
      return;
    }

    int oldSize = hashAggregations.size();
    LOG.info("Hash Tbl flush: #hash table = " + oldSize);
    Iterator<Map.Entry<KeyWrapper, AggregationBuffer[]>> iter = hashAggregations
        .entrySet().iterator();
    int numDel = 0;
    while (iter.hasNext()) {
      Map.Entry<KeyWrapper, AggregationBuffer[]> m = iter.next();
      forward(m.getKey().getKeyArray(), m.getValue());
      iter.remove();
      numDel++;
      if (numDel * 10 >= oldSize) {
        LOG.info("Hash Table flushed: new size = " + hashAggregations.size());
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
  private void forward(Object[] keys, AggregationBuffer[] aggs) throws HiveException {

    if (forwardCache == null) {
      forwardCache = new Object[outputKeyLength + aggs.length];
    }

    for (int i = 0; i < outputKeyLength; i++) {
      forwardCache[i] = keys[i];
    }
    for (int i = 0; i < aggs.length; i++) {
      forwardCache[outputKeyLength + i] = aggregationEvaluators[i].evaluate(aggs[i]);
    }

    forward(forwardCache, outputObjInspector);
  }

  /**
   * Forward all aggregations to children. It is only used by DemuxOperator.
   * @throws HiveException
   */
  @Override
  public void flush() throws HiveException{
    try {
      if (hashAggregations != null) {
        LOG.info("Begin Hash Table flush: size = "
            + hashAggregations.size());
        Iterator iter = hashAggregations.entrySet().iterator();
        while (iter.hasNext()) {
          Map.Entry<KeyWrapper, AggregationBuffer[]> m = (Map.Entry) iter
              .next();

          forward(m.getKey().getKeyArray(), m.getValue());
          iter.remove();
        }
        hashAggregations.clear();
      } else if (aggregations != null) {
        // sort-based aggregations
        if (currentKeys != null) {
          forward(currentKeys.getKeyArray(), aggregations);
        }
        currentKeys = null;
      } else {
        // The GroupByOperator is not initialized, which means there is no
        // data
        // (since we initialize the operators when we see the first record).
        // Just do nothing here.
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
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
          forward(new Object[0], aggregations);
        } else {
          flush();
        }
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }
  }

  // Group by contains the columns needed - no need to aggregate from children
  public List<String> genColLists(
      HashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx) {
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
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "GBY";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.GROUPBY;
  }

  /**
   * we can push the limit above GBY (running in Reducer), since that will generate single row
   * for each group. This doesn't necessarily hold for GBY (running in Mappers),
   * so we don't push limit above it.
   */
  @Override
  public boolean acceptLimitPushdown() {
    return getConf().getMode() == GroupByDesc.Mode.MERGEPARTIAL ||
        getConf().getMode() == GroupByDesc.Mode.COMPLETE;
  }
}
