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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Join operator implementation.
 */
public abstract class CommonJoinOperator<T extends JoinDesc> extends
    Operator<T> implements Serializable {
  private static final long serialVersionUID = 1L;
  static final protected Log LOG = LogFactory.getLog(CommonJoinOperator.class
      .getName());

  public static class IntermediateObject {
    ArrayList<Object>[] objs;
    int curSize;

    public IntermediateObject(ArrayList<Object>[] objs, int curSize) {
      this.objs = objs;
      this.curSize = curSize;
    }

    public ArrayList<Object>[] getObjs() {
      return objs;
    }

    public int getCurSize() {
      return curSize;
    }

    public void pushObj(ArrayList<Object> newObj) {
      objs[curSize++] = newObj;
    }

    public void popObj() {
      curSize--;
    }

    public Object topObj() {
      return objs[curSize - 1];
    }
  }

  transient protected int numAliases; // number of aliases
  /**
   * The expressions for join outputs.
   */
  transient protected Map<Byte, List<ExprNodeEvaluator>> joinValues;
  /**
   * The ObjectInspectors for the join inputs.
   */
  transient protected Map<Byte, List<ObjectInspector>> joinValuesObjectInspectors;
  /**
   * The standard ObjectInspectors for the join inputs.
   */
  transient protected Map<Byte, List<ObjectInspector>> joinValuesStandardObjectInspectors;

  transient static protected Byte[] order; // order in which the results should
                                           // be output
  transient protected JoinCondDesc[] condn;
  transient protected boolean noOuterJoin;
  transient private Object[] dummyObj; // for outer joins, contains the
  // potential nulls for the concerned
  // aliases
  transient protected RowContainer<ArrayList<Object>>[] dummyObjVectors; // empty
                                                                         // rows
                                                                         // for
                                                                         // each
                                                                         // table
  transient protected int totalSz; // total size of the composite object

  // keys are the column names. basically this maps the position of the column
  // in
  // the output of the CommonJoinOperator to the input columnInfo.
  transient private Map<Integer, Set<String>> posToAliasMap;

  transient LazyBinarySerDe[] spillTableSerDe;
  transient protected Map<Byte, TableDesc> spillTableDesc; // spill tables are
                                                           // used if the join
                                                           // input is too large
                                                           // to fit in memory

  HashMap<Byte, RowContainer<ArrayList<Object>>> storage; // map b/w table alias
                                                          // to RowContainer
  int joinEmitInterval = -1;
  int joinCacheSize = 0;
  int nextSz = 0;
  transient Byte lastAlias = null;

  transient boolean handleSkewJoin = false;

  protected int populateJoinKeyValue(Map<Byte, List<ExprNodeEvaluator>> outMap,
      Map<Byte, List<ExprNodeDesc>> inputMap) {

    int total = 0;

    Iterator<Map.Entry<Byte, List<ExprNodeDesc>>> entryIter = inputMap
        .entrySet().iterator();
    while (entryIter.hasNext()) {
      Map.Entry<Byte, List<ExprNodeDesc>> e = entryIter.next();
      Byte key = order[e.getKey()];

      List<ExprNodeDesc> expr = e.getValue();
      int sz = expr.size();
      total += sz;

      List<ExprNodeEvaluator> valueFields = new ArrayList<ExprNodeEvaluator>();

      for (int j = 0; j < sz; j++) {
        valueFields.add(ExprNodeEvaluatorFactory.get(expr.get(j)));
      }

      outMap.put(key, valueFields);
    }

    return total;
  }

  protected static HashMap<Byte, List<ObjectInspector>> getObjectInspectorsFromEvaluators(
      Map<Byte, List<ExprNodeEvaluator>> exprEntries,
      ObjectInspector[] inputObjInspector) throws HiveException {
    HashMap<Byte, List<ObjectInspector>> result = new HashMap<Byte, List<ObjectInspector>>();
    for (Entry<Byte, List<ExprNodeEvaluator>> exprEntry : exprEntries
        .entrySet()) {
      Byte alias = exprEntry.getKey();
      List<ExprNodeEvaluator> exprList = exprEntry.getValue();
      ArrayList<ObjectInspector> fieldOIList = new ArrayList<ObjectInspector>();
      for (int i = 0; i < exprList.size(); i++) {
        fieldOIList.add(exprList.get(i).initialize(inputObjInspector[alias]));
      }
      result.put(alias, fieldOIList);
    }
    return result;
  }

  protected static HashMap<Byte, List<ObjectInspector>> getStandardObjectInspectors(
      Map<Byte, List<ObjectInspector>> aliasToObjectInspectors) {
    HashMap<Byte, List<ObjectInspector>> result = new HashMap<Byte, List<ObjectInspector>>();
    for (Entry<Byte, List<ObjectInspector>> oiEntry : aliasToObjectInspectors
        .entrySet()) {
      Byte alias = oiEntry.getKey();
      List<ObjectInspector> oiList = oiEntry.getValue();
      ArrayList<ObjectInspector> fieldOIList = new ArrayList<ObjectInspector>(
          oiList.size());
      for (int i = 0; i < oiList.size(); i++) {
        fieldOIList.add(ObjectInspectorUtils.getStandardObjectInspector(oiList
            .get(i), ObjectInspectorCopyOption.WRITABLE));
      }
      result.put(alias, fieldOIList);
    }
    return result;

  }

  protected static <T extends JoinDesc> ObjectInspector getJoinOutputObjectInspector(
      Byte[] order, Map<Byte, List<ObjectInspector>> aliasToObjectInspectors,
      T conf) {
    ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
    for (Byte alias : order) {
      List<ObjectInspector> oiList = aliasToObjectInspectors.get(alias);
      structFieldObjectInspectors.addAll(oiList);
    }

    StructObjectInspector joinOutputObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(conf.getOutputColumnNames(),
            structFieldObjectInspectors);
    return joinOutputObjectInspector;
  }

  Configuration hconf;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    this.handleSkewJoin = conf.getHandleSkewJoin();
    this.hconf = hconf;
    LOG.info("COMMONJOIN "
        + ((StructObjectInspector) inputObjInspectors[0]).getTypeName());
    totalSz = 0;
    // Map that contains the rows for each alias
    storage = new HashMap<Byte, RowContainer<ArrayList<Object>>>();

    numAliases = conf.getExprs().size();

    joinValues = new HashMap<Byte, List<ExprNodeEvaluator>>();

    if (order == null) {
      order = conf.getTagOrder();
    }
    condn = conf.getConds();
    noOuterJoin = conf.isNoOuterJoin();

    totalSz = populateJoinKeyValue(joinValues, conf.getExprs());

    joinValuesObjectInspectors = getObjectInspectorsFromEvaluators(joinValues,
        inputObjInspectors);
    joinValuesStandardObjectInspectors = getStandardObjectInspectors(joinValuesObjectInspectors);

    dummyObj = new Object[numAliases];
    dummyObjVectors = new RowContainer[numAliases];

    joinEmitInterval = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEJOINEMITINTERVAL);
    joinCacheSize = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEJOINCACHESIZE);

    // construct dummy null row (indicating empty table) and
    // construct spill table serde which is used if input is too
    // large to fit into main memory.
    byte pos = 0;
    for (Byte alias : order) {
      int sz = conf.getExprs().get(alias).size();
      ArrayList<Object> nr = new ArrayList<Object>(sz);

      for (int j = 0; j < sz; j++) {
        nr.add(null);
      }
      dummyObj[pos] = nr;
      // there should be only 1 dummy object in the RowContainer
      RowContainer<ArrayList<Object>> values = getRowContainer(hconf, pos,
          alias, 1);
      values.add((ArrayList<Object>) dummyObj[pos]);
      dummyObjVectors[pos] = values;

      // if serde is null, the input doesn't need to be spilled out
      // e.g., the output columns does not contains the input table
      RowContainer rc = getRowContainer(hconf, pos, alias, joinCacheSize);
      storage.put(pos, rc);

      pos++;
    }

    forwardCache = new Object[totalSz];

    outputObjInspector = getJoinOutputObjectInspector(order,
        joinValuesStandardObjectInspectors, conf);
    LOG.info("JOIN "
        + ((StructObjectInspector) outputObjInspector).getTypeName()
        + " totalsz = " + totalSz);

  }

  RowContainer getRowContainer(Configuration hconf, byte pos, Byte alias,
      int containerSize) throws HiveException {
    TableDesc tblDesc = getSpillTableDesc(alias);
    SerDe serde = getSpillSerDe(alias);

    if (serde == null) {
      containerSize = 1;
    }

    RowContainer rc = new RowContainer(containerSize, hconf);
    StructObjectInspector rcOI = null;
    if (tblDesc != null) {
      // arbitrary column names used internally for serializing to spill table
      List<String> colNames = Utilities.getColumnNames(tblDesc.getProperties());
      // object inspector for serializing input tuples
      rcOI = ObjectInspectorFactory.getStandardStructObjectInspector(colNames,
          joinValuesStandardObjectInspectors.get(pos));
    }

    rc.setSerDe(serde, rcOI);
    rc.setTableDesc(tblDesc);
    return rc;
  }

  private SerDe getSpillSerDe(byte alias) {
    TableDesc desc = getSpillTableDesc(alias);
    if (desc == null) {
      return null;
    }
    SerDe sd = (SerDe) ReflectionUtils.newInstance(desc.getDeserializerClass(),
        null);
    try {
      sd.initialize(null, desc.getProperties());
    } catch (SerDeException e) {
      e.printStackTrace();
      return null;
    }
    return sd;
  }

  transient boolean newGroupStarted = false;

  public TableDesc getSpillTableDesc(Byte alias) {
    if (spillTableDesc == null || spillTableDesc.size() == 0) {
      initSpillTables();
    }
    return spillTableDesc.get(alias);
  }

  public Map<Byte, TableDesc> getSpillTableDesc() {
    if (spillTableDesc == null) {
      initSpillTables();
    }
    return spillTableDesc;
  }

  private void initSpillTables() {
    Map<Byte, List<ExprNodeDesc>> exprs = conf.getExprs();
    spillTableDesc = new HashMap<Byte, TableDesc>(exprs.size());
    for (int tag = 0; tag < exprs.size(); tag++) {
      List<ExprNodeDesc> valueCols = exprs.get((byte) tag);
      int columnSize = valueCols.size();
      StringBuilder colNames = new StringBuilder();
      StringBuilder colTypes = new StringBuilder();
      if (columnSize <= 0) {
        continue;
      }
      for (int k = 0; k < columnSize; k++) {
        String newColName = tag + "_VALUE_" + k; // any name, it does not
                                                 // matter.
        colNames.append(newColName);
        colNames.append(',');
        colTypes.append(valueCols.get(k).getTypeString());
        colTypes.append(',');
      }
      // remove the last ','
      colNames.setLength(colNames.length() - 1);
      colTypes.setLength(colTypes.length() - 1);
      TableDesc tblDesc = new TableDesc(LazyBinarySerDe.class,
          SequenceFileInputFormat.class, HiveSequenceFileOutputFormat.class,
          Utilities.makeProperties(
              org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, ""
                  + Utilities.ctrlaCode,
              org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS, colNames
                  .toString(),
              org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES,
              colTypes.toString()));
      spillTableDesc.put((byte) tag, tblDesc);
    }
  }

  @Override
  public void startGroup() throws HiveException {
    LOG.trace("Join: Starting new group");
    newGroupStarted = true;
    for (RowContainer<ArrayList<Object>> alw : storage.values()) {
      alw.clear();
    }
  }

  protected int getNextSize(int sz) {
    // A very simple counter to keep track of join entries for a key
    if (sz >= 100000) {
      return sz + 100000;
    }

    return 2 * sz;
  }

  transient protected Byte alias;

  /**
   * Return the value as a standard object. StandardObject can be inspected by a
   * standard ObjectInspector.
   */
  protected static ArrayList<Object> computeValues(Object row,
      List<ExprNodeEvaluator> valueFields, List<ObjectInspector> valueFieldsOI)
      throws HiveException {

    // Compute the values
    ArrayList<Object> nr = new ArrayList<Object>(valueFields.size());
    for (int i = 0; i < valueFields.size(); i++) {

      nr.add(ObjectInspectorUtils.copyToStandardObject(valueFields.get(i)
          .evaluate(row), valueFieldsOI.get(i),
          ObjectInspectorCopyOption.WRITABLE));
    }

    return nr;
  }

  transient Object[] forwardCache;

  private void createForwardJoinObject(IntermediateObject intObj,
      boolean[] nullsArr) throws HiveException {
    int p = 0;
    for (int i = 0; i < numAliases; i++) {
      Byte alias = order[i];
      int sz = joinValues.get(alias).size();
      if (nullsArr[i]) {
        for (int j = 0; j < sz; j++) {
          forwardCache[p++] = null;
        }
      } else {
        ArrayList<Object> obj = intObj.getObjs()[i];
        for (int j = 0; j < sz; j++) {
          forwardCache[p++] = obj.get(j);
        }
      }
    }
    forward(forwardCache, outputObjInspector);
  }

  private void copyOldArray(boolean[] src, boolean[] dest) {
    for (int i = 0; i < src.length; i++) {
      dest[i] = src[i];
    }
  }

  private ArrayList<boolean[]> joinObjectsInnerJoin(
      ArrayList<boolean[]> resNulls, ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull) {
    if (newObjNull) {
      return resNulls;
    }
    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];
      if (!oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = false;
        resNulls.add(newNulls);
      }
    }
    return resNulls;
  }

  /**
   * Implement semi join operator.
   */
  private ArrayList<boolean[]> joinObjectsLeftSemiJoin(
      ArrayList<boolean[]> resNulls, ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull) {
    if (newObjNull) {
      return resNulls;
    }
    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];
      if (!oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = false;
        resNulls.add(newNulls);
      }
    }
    return resNulls;
  }

  private ArrayList<boolean[]> joinObjectsLeftOuterJoin(
      ArrayList<boolean[]> resNulls, ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull) {
    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      copyOldArray(oldNulls, newNulls);
      if (oldObjNull) {
        newNulls[oldNulls.length] = true;
      } else {
        newNulls[oldNulls.length] = newObjNull;
      }
      resNulls.add(newNulls);
    }
    return resNulls;
  }

  private ArrayList<boolean[]> joinObjectsRightOuterJoin(
      ArrayList<boolean[]> resNulls, ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull, boolean firstRow) {
    if (newObjNull) {
      return resNulls;
    }

    if (inputNulls.isEmpty() && firstRow) {
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      for (int i = 0; i < intObj.getCurSize() - 1; i++) {
        newNulls[i] = true;
      }
      newNulls[intObj.getCurSize() - 1] = newObjNull;
      resNulls.add(newNulls);
      return resNulls;
    }

    boolean allOldObjsNull = firstRow;

    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      if (!oldNulls[left]) {
        allOldObjsNull = false;
        break;
      }
    }

    nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];

      if (!oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
      } else if (allOldObjsNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        for (int i = 0; i < intObj.getCurSize() - 1; i++) {
          newNulls[i] = true;
        }
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
        return resNulls;
      }
    }
    return resNulls;
  }

  private ArrayList<boolean[]> joinObjectsFullOuterJoin(
      ArrayList<boolean[]> resNulls, ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull, boolean firstRow) {
    if (newObjNull) {
      Iterator<boolean[]> nullsIter = inputNulls.iterator();
      while (nullsIter.hasNext()) {
        boolean[] oldNulls = nullsIter.next();
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
      }
      return resNulls;
    }

    if (inputNulls.isEmpty() && firstRow) {
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      for (int i = 0; i < intObj.getCurSize() - 1; i++) {
        newNulls[i] = true;
      }
      newNulls[intObj.getCurSize() - 1] = newObjNull;
      resNulls.add(newNulls);
      return resNulls;
    }

    boolean allOldObjsNull = firstRow;

    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      if (!oldNulls[left]) {
        allOldObjsNull = false;
        break;
      }
    }
    boolean rhsPreserved = false;

    nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];

      if (!oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
      } else if (oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = true;
        resNulls.add(newNulls);

        if (allOldObjsNull && !rhsPreserved) {
          newNulls = new boolean[intObj.getCurSize()];
          for (int i = 0; i < oldNulls.length; i++) {
            newNulls[i] = true;
          }
          newNulls[oldNulls.length] = false;
          resNulls.add(newNulls);
          rhsPreserved = true;
        }
      }
    }
    return resNulls;
  }

  /*
   * The new input is added to the list of existing inputs. Each entry in the
   * array of inputNulls denotes the entries in the intermediate object to be
   * used. The intermediate object is augmented with the new object, and list of
   * nulls is changed appropriately. The list will contain all non-nulls for a
   * inner join. The outer joins are processed appropriately.
   */
  private ArrayList<boolean[]> joinObjects(ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int joinPos,
      boolean firstRow) {
    ArrayList<boolean[]> resNulls = new ArrayList<boolean[]>();
    boolean newObjNull = newObj == dummyObj[joinPos] ? true : false;
    if (joinPos == 0) {
      if (newObjNull) {
        return null;
      }
      boolean[] nulls = new boolean[1];
      nulls[0] = newObjNull;
      resNulls.add(nulls);
      return resNulls;
    }

    int left = condn[joinPos - 1].getLeft();
    int type = condn[joinPos - 1].getType();

    // process all nulls for RIGHT and FULL OUTER JOINS
    if (((type == JoinDesc.RIGHT_OUTER_JOIN) || (type == JoinDesc.FULL_OUTER_JOIN))
        && !newObjNull && (inputNulls == null) && firstRow) {
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      for (int i = 0; i < newNulls.length - 1; i++) {
        newNulls[i] = true;
      }
      newNulls[newNulls.length - 1] = false;
      resNulls.add(newNulls);
      return resNulls;
    }

    if (inputNulls == null) {
      return null;
    }

    if (type == JoinDesc.INNER_JOIN) {
      return joinObjectsInnerJoin(resNulls, inputNulls, newObj, intObj, left,
          newObjNull);
    } else if (type == JoinDesc.LEFT_OUTER_JOIN) {
      return joinObjectsLeftOuterJoin(resNulls, inputNulls, newObj, intObj,
          left, newObjNull);
    } else if (type == JoinDesc.RIGHT_OUTER_JOIN) {
      return joinObjectsRightOuterJoin(resNulls, inputNulls, newObj, intObj,
          left, newObjNull, firstRow);
    } else if (type == JoinDesc.LEFT_SEMI_JOIN) {
      return joinObjectsLeftSemiJoin(resNulls, inputNulls, newObj, intObj,
          left, newObjNull);
    }

    assert (type == JoinDesc.FULL_OUTER_JOIN);
    return joinObjectsFullOuterJoin(resNulls, inputNulls, newObj, intObj, left,
        newObjNull, firstRow);
  }

  /*
   * genObject is a recursive function. For the inputs, a array of bitvectors is
   * maintained (inputNulls) where each entry denotes whether the element is to
   * be used or not (whether it is null or not). The size of the bitvector is
   * same as the number of inputs under consideration currently. When all inputs
   * are accounted for, the output is forwared appropriately.
   */
  private void genObject(ArrayList<boolean[]> inputNulls, int aliasNum,
      IntermediateObject intObj, boolean firstRow) throws HiveException {
    boolean childFirstRow = firstRow;
    boolean skipping = false;

    if (aliasNum < numAliases) {

      // search for match in the rhs table
      RowContainer<ArrayList<Object>> aliasRes = storage.get(order[aliasNum]);

      for (ArrayList<Object> newObj = aliasRes.first(); newObj != null; newObj = aliasRes
          .next()) {

        // check for skipping in case of left semi join
        if (aliasNum > 0
            && condn[aliasNum - 1].getType() == JoinDesc.LEFT_SEMI_JOIN
            && newObj != dummyObj[aliasNum]) { // successful match
          skipping = true;
        }

        intObj.pushObj(newObj);

        // execute the actual join algorithm
        ArrayList<boolean[]> newNulls = joinObjects(inputNulls, newObj, intObj,
            aliasNum, childFirstRow);

        // recursively call the join the other rhs tables
        genObject(newNulls, aliasNum + 1, intObj, firstRow);

        intObj.popObj();
        firstRow = false;

        // if left-semi-join found a match, skipping the rest of the rows in the
        // rhs table of the semijoin
        if (skipping) {
          break;
        }
      }
    } else {
      if (inputNulls == null) {
        return;
      }
      Iterator<boolean[]> nullsIter = inputNulls.iterator();
      while (nullsIter.hasNext()) {
        boolean[] nullsVec = nullsIter.next();
        createForwardJoinObject(intObj, nullsVec);
      }
    }
  }

  /**
   * Forward a record of join results.
   * 
   * @throws HiveException
   */
  @Override
  public void endGroup() throws HiveException {
    LOG.trace("Join Op: endGroup called: numValues=" + numAliases);

    checkAndGenObject();
  }

  private void genUniqueJoinObject(int aliasNum, IntermediateObject intObj)
      throws HiveException {
    if (aliasNum == numAliases) {
      int p = 0;
      for (int i = 0; i < numAliases; i++) {
        int sz = joinValues.get(order[i]).size();
        ArrayList<Object> obj = intObj.getObjs()[i];
        for (int j = 0; j < sz; j++) {
          forwardCache[p++] = obj.get(j);
        }
      }

      forward(forwardCache, outputObjInspector);
      return;
    }

    RowContainer<ArrayList<Object>> alias = storage.get(order[aliasNum]);
    for (ArrayList<Object> row = alias.first(); row != null; row = alias.next()) {
      intObj.pushObj(row);
      genUniqueJoinObject(aliasNum + 1, intObj);
      intObj.popObj();
    }
  }

  protected void checkAndGenObject() throws HiveException {
    if (condn[0].getType() == JoinDesc.UNIQUE_JOIN) {
      new IntermediateObject(new ArrayList[numAliases], 0);

      // Check if results need to be emitted.
      // Results only need to be emitted if there is a non-null entry in a table
      // that is preserved or if there are no non-null entries
      boolean preserve = false; // Will be true if there is a non-null entry
      // in a preserved table
      boolean hasNulls = false; // Will be true if there are null entries
      for (int i = 0; i < numAliases; i++) {
        Byte alias = order[i];
        RowContainer<ArrayList<Object>> alw = storage.get(alias);
        if (alw.size() == 0) {
          alw.add((ArrayList<Object>) dummyObj[i]);
          hasNulls = true;
        } else if (condn[i].getPreserved()) {
          preserve = true;
        }
      }

      if (hasNulls && !preserve) {
        return;
      }

      LOG.trace("calling genUniqueJoinObject");
      genUniqueJoinObject(0, new IntermediateObject(new ArrayList[numAliases],
          0));
      LOG.trace("called genUniqueJoinObject");
    } else {
      // does any result need to be emitted
      for (int i = 0; i < numAliases; i++) {
        Byte alias = order[i];
        RowContainer<ArrayList<Object>> alw = storage.get(alias);
        if (alw.size() == 0) {
          if (noOuterJoin) {
            LOG.trace("No data for alias=" + i);
            return;
          } else {
            alw.add((ArrayList<Object>) dummyObj[i]);
          }
        }
      }

      LOG.trace("calling genObject");
      genObject(null, 0, new IntermediateObject(new ArrayList[numAliases], 0),
          true);
      LOG.trace("called genObject");
    }
  }

  /**
   * All done
   * 
   */
  @Override
  public void closeOp(boolean abort) throws HiveException {
    LOG.trace("Join Op close");
    for (RowContainer<ArrayList<Object>> alw : storage.values()) {
      if (alw != null) {
        alw.clear(); // clean up the temp files
      }
    }
    storage.clear();
  }

  @Override
  public String getName() {
    return "JOIN";
  }

  /**
   * @return the posToAliasMap
   */
  public Map<Integer, Set<String>> getPosToAliasMap() {
    return posToAliasMap;
  }

  /**
   * @param posToAliasMap
   *          the posToAliasMap to set
   */
  public void setPosToAliasMap(Map<Integer, Set<String>> posToAliasMap) {
    this.posToAliasMap = posToAliasMap;
  }

}
