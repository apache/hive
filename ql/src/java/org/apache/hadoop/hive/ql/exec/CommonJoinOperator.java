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
import java.util.Stack;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.joinCond;
import org.apache.hadoop.hive.ql.plan.joinDesc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Join operator implementation.
 */
public class CommonJoinOperator<T extends joinDesc> extends Operator<T> implements Serializable {
  private static final long serialVersionUID = 1L;
  static final protected Log LOG = LogFactory.getLog(CommonJoinOperator.class.getName());

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

    public void pushObj(ArrayList<Object> obj) {
      objs[curSize++] = obj;
    }

    public void popObj() {
      curSize--;
    }
  }

  transient protected int numValues; // number of aliases
  transient protected Map<Byte, List<ExprNodeEvaluator>> joinValues;
  transient protected Map<Byte, List<ObjectInspector>>   joinValuesObjectInspectors;
  
  transient static protected Byte[] order; // order in which the results should be output
  transient protected joinCond[] condn;
  transient protected boolean noOuterJoin;
  transient private Object[] dummyObj; // for outer joins, contains the
                                       // potential nulls for the concerned
                                       // aliases
  transient private Vector<ArrayList<Object>>[] dummyObjVectors;
  transient private Stack<Iterator<ArrayList<Object>>> iterators;
  transient protected int totalSz; // total size of the composite object
  transient ObjectInspector joinOutputObjectInspector;
  
  // keys are the column names. basically this maps the position of the column in 
  // the output of the CommonJoinOperator to the input columnInfo.
  transient private Map<Integer, Set<String>> posToAliasMap;

  HashMap<Byte, Vector<ArrayList<Object>>> storage;
  int joinEmitInterval = -1;
  int nextSz = 0;
  transient Byte lastAlias = null;
  
  protected int populateJoinKeyValue(Map<Byte, List<ExprNodeEvaluator>> outMap,
      Map<Byte, List<exprNodeDesc>> inputMap) {

    int total = 0;

    Iterator<Map.Entry<Byte, List<exprNodeDesc>>> entryIter = inputMap.entrySet().iterator();
    while (entryIter.hasNext()) {
      Map.Entry<Byte, List<exprNodeDesc>> e = (Map.Entry<Byte, List<exprNodeDesc>>) entryIter.next();
      Byte key = (Byte) e.getKey();

      List<exprNodeDesc> expr = (List<exprNodeDesc>) e.getValue();
      int sz = expr.size();
      total += sz;

      List<ExprNodeEvaluator> valueFields = new ArrayList<ExprNodeEvaluator>();

      for (int j = 0; j < sz; j++)
        valueFields.add(ExprNodeEvaluatorFactory.get(expr.get(j)));

      outMap.put(key, valueFields);
    }
    
    return total;
  }

  public void initializeOp(Configuration hconf, Reporter reporter, ObjectInspector[] inputObjInspector) throws HiveException {
    LOG.info("COMMONJOIN " + ((StructObjectInspector)inputObjInspector[0]).getTypeName());   
    totalSz = 0;
    // Map that contains the rows for each alias
    storage = new HashMap<Byte, Vector<ArrayList<Object>>>();

    numValues = conf.getExprs().size();
    
    joinValues = new HashMap<Byte, List<ExprNodeEvaluator>>();
    joinValuesObjectInspectors = new HashMap<Byte, List<ObjectInspector>>();

    if (order == null) {
      order = new Byte[numValues];
      for (int i = 0; i < numValues; i++)
        order[i] = (byte) i;
    }
    condn = conf.getConds();
    noOuterJoin = conf.getNoOuterJoin();

    totalSz = populateJoinKeyValue(joinValues, conf.getExprs());

    dummyObj = new Object[numValues];
    dummyObjVectors = new Vector[numValues];

    int pos = 0;
    for (Byte alias : order) {
      int sz = conf.getExprs().get(alias).size();
      ArrayList<Object> nr = new ArrayList<Object>(sz);

      for (int j = 0; j < sz; j++)
        nr.add(null);

      dummyObj[pos] = nr;
      Vector<ArrayList<Object>> values = new Vector<ArrayList<Object>>();
      values.add((ArrayList<Object>) dummyObj[pos]);
      dummyObjVectors[pos] = values;
      pos++;
    }

    iterators = new Stack<Iterator<ArrayList<Object>>>();
    
    joinEmitInterval = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEJOINEMITINTERVAL);
    
    forwardCache = new Object[totalSz];
  }

  public void startGroup() throws HiveException {
    LOG.trace("Join: Starting new group");
    storage.clear();
    for (Byte alias : order)
      storage.put(alias, new Vector<ArrayList<Object>>());
  }

  private int getNextSize(int sz) {
    // A very simple counter to keep track of join entries for a key
    if (sz >= 100000)
      return sz + 100000;
    
    return 2 * sz;
  }

  transient protected Byte alias;
  
  protected ArrayList<Object> computeValues(Object row, ObjectInspector rowInspector,
    List<ExprNodeEvaluator> valueFields, Map<Byte, List<ObjectInspector>> joinExprsObjectInspectors) throws HiveException {
    
    // Get the valueFields Object Inspectors
    List<ObjectInspector> valueFieldOI = joinExprsObjectInspectors.get(alias);
    if (valueFieldOI == null) {
      // Initialize the ExprEvaluator if necessary
      valueFieldOI = new ArrayList<ObjectInspector>();
      for (int i=0; i<valueFields.size(); i++) {
        valueFieldOI.add(valueFields.get(i).initialize(rowInspector));
      }
      joinExprsObjectInspectors.put(alias, valueFieldOI);
    }
    
    // Compute the values
    ArrayList<Object> nr = new ArrayList<Object>(valueFields.size());
    for (int i=0; i<valueFields.size(); i++) {
      nr.add(ObjectInspectorUtils.copyToStandardObject(
          valueFields.get(i).evaluate(row),
          valueFieldOI.get(i)));
    }
    
    return nr;
  }
  
  public void process(Object row, ObjectInspector rowInspector, int tag)
      throws HiveException {
    try {
      // get alias
      alias = (byte)tag;

      if ((lastAlias == null) || (!lastAlias.equals(alias)))
        nextSz = joinEmitInterval;
      
      ArrayList<Object> nr = computeValues(row, rowInspector, joinValues.get(alias), joinValuesObjectInspectors);
      
      // number of rows for the key in the given table
      int sz = storage.get(alias).size();

      // Are we consuming too much memory
      if (alias == numValues - 1) {
        if (sz == joinEmitInterval) {
          // The input is sorted by alias, so if we are already in the last join operand,
          // we can emit some results now.
          // Note this has to be done before adding the current row to the storage,
          // to preserve the correctness for outer joins.
          checkAndGenObject();
          storage.get(alias).clear();
        }
      } else {
        if (sz == nextSz) {
          // Output a warning if we reached at least 1000 rows for a join operand
          // We won't output a warning for the last join operand since the size
          // will never goes to joinEmitInterval.
          StructObjectInspector soi = (StructObjectInspector)rowInspector;
          StructField sf = soi.getStructFieldRef(Utilities.ReduceField.KEY.toString());
          Object keyObject = soi.getStructFieldData(row, sf);
          LOG.warn("table " + alias + " has " + sz + " rows for join key " + keyObject);
          nextSz = getNextSize(nextSz);
        }
      }

      // Add the value to the vector
      storage.get(alias).add(nr);

    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  transient Object[] forwardCache;
  
  private void createForwardJoinObject(IntermediateObject intObj,
      boolean[] nullsArr) throws HiveException {
    int p = 0;
    for (int i = 0; i < numValues; i++) {
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
    forward(forwardCache, joinOutputObjectInspector);
  }

  private void copyOldArray(boolean[] src, boolean[] dest) {
    for (int i = 0; i < src.length; i++)
      dest[i] = src[i];
  }

  private Vector<boolean[]> joinObjectsInnerJoin(Vector<boolean[]> resNulls,
      Vector<boolean[]> inputNulls, ArrayList<Object> newObj,
      IntermediateObject intObj, int left, boolean newObjNull) {
    if (newObjNull)
      return resNulls;
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

  private Vector<boolean[]> joinObjectsLeftOuterJoin(
      Vector<boolean[]> resNulls, Vector<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull) {
    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      copyOldArray(oldNulls, newNulls);
      if (oldObjNull)
        newNulls[oldNulls.length] = true;
      else
        newNulls[oldNulls.length] = newObjNull;
      resNulls.add(newNulls);
    }
    return resNulls;
  }

  private Vector<boolean[]> joinObjectsRightOuterJoin(
      Vector<boolean[]> resNulls, Vector<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull, boolean firstRow) {
    if (newObjNull)
      return resNulls;

    if (inputNulls.isEmpty() && firstRow) {
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      for (int i = 0; i < intObj.getCurSize() - 1; i++)
        newNulls[i] = true;
      newNulls[intObj.getCurSize()-1] = newObjNull;
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
        for (int i = 0; i < intObj.getCurSize() - 1; i++)
          newNulls[i] = true;
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
        return resNulls;
      }
    }
    return resNulls;
  }

  private Vector<boolean[]> joinObjectsFullOuterJoin(
      Vector<boolean[]> resNulls, Vector<boolean[]> inputNulls,
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
      for (int i = 0; i < intObj.getCurSize() - 1; i++)
        newNulls[i] = true;
      newNulls[intObj.getCurSize()-1] = newObjNull;
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
          for (int i = 0; i < oldNulls.length; i++)
            newNulls[i] = true;
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
  private Vector<boolean[]> joinObjects(Vector<boolean[]> inputNulls,
                                        ArrayList<Object> newObj, IntermediateObject intObj, 
                                        int joinPos, boolean firstRow) {
    Vector<boolean[]> resNulls = new Vector<boolean[]>();
    boolean newObjNull = newObj == dummyObj[joinPos] ? true : false;
    if (joinPos == 0) {
      if (newObjNull)
        return null;
      boolean[] nulls = new boolean[1];
      nulls[0] = newObjNull;
      resNulls.add(nulls);
      return resNulls;
    }

    int left = condn[joinPos - 1].getLeft();
    int type = condn[joinPos - 1].getType();

    // process all nulls for RIGHT and FULL OUTER JOINS
    if (((type == joinDesc.RIGHT_OUTER_JOIN) || (type == joinDesc.FULL_OUTER_JOIN))
        && !newObjNull && (inputNulls == null) && firstRow) {
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      for (int i = 0; i < newNulls.length - 1; i++)
        newNulls[i] = true;
      newNulls[newNulls.length - 1] = false;
      resNulls.add(newNulls);
      return resNulls;
    }

    if (inputNulls == null)
      return null;

    if (type == joinDesc.INNER_JOIN)
      return joinObjectsInnerJoin(resNulls, inputNulls, newObj, intObj, left,
          newObjNull);
    else if (type == joinDesc.LEFT_OUTER_JOIN)
      return joinObjectsLeftOuterJoin(resNulls, inputNulls, newObj, intObj,
          left, newObjNull);
    else if (type == joinDesc.RIGHT_OUTER_JOIN)
      return joinObjectsRightOuterJoin(resNulls, inputNulls, newObj, intObj,
                                       left, newObjNull, firstRow);
    assert (type == joinDesc.FULL_OUTER_JOIN);
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
  private void genObject(Vector<boolean[]> inputNulls, int aliasNum,
                         IntermediateObject intObj, boolean firstRow) throws HiveException {
    boolean childFirstRow = firstRow;
    if (aliasNum < numValues) {
      Iterator<ArrayList<Object>> aliasRes = storage.get(order[aliasNum])
          .iterator();
      iterators.push(aliasRes);
      while (aliasRes.hasNext()) {
        ArrayList<Object> newObj = aliasRes.next();
        intObj.pushObj(newObj);
        Vector<boolean[]> newNulls = joinObjects(inputNulls, newObj, intObj,
                                                 aliasNum, childFirstRow);
        genObject(newNulls, aliasNum + 1, intObj, firstRow);
        intObj.popObj();
        firstRow = false;
      }
      iterators.pop();
    } else {
      if (inputNulls == null)
        return;
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
  public void endGroup() throws HiveException {
    LOG.trace("Join Op: endGroup called: numValues=" + numValues);
    checkAndGenObject();
  }

  protected void checkAndGenObject() throws HiveException {
    // does any result need to be emitted
    for (int i = 0; i < numValues; i++) {
      Byte alias = order[i];
      if (storage.get(alias).iterator().hasNext() == false) {
        if (noOuterJoin) {
          LOG.trace("No data for alias=" + i);
          return;
        } else {
          storage.put(alias, dummyObjVectors[i]);
        }
      }
    }

    LOG.trace("calling genObject");
    genObject(null, 0, new IntermediateObject(new ArrayList[numValues], 0), true);
    LOG.trace("called genObject");
  }

  /**
   * All done
   * 
   */
  public void close(boolean abort) throws HiveException {
    LOG.trace("Join Op close");
    super.close(abort);
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
   * @param posToAliasMap the posToAliasMap to set
   */
  public void setPosToAliasMap(Map<Integer, Set<String>> posToAliasMap) {
    this.posToAliasMap = posToAliasMap;
  }
  
}
