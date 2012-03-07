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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.persistence.AbstractMapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;


public abstract class AbstractMapJoinOperator <T extends MapJoinDesc> extends CommonJoinOperator<T> implements
    Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * The expressions for join inputs's join keys.
   */
  protected transient Map<Byte, List<ExprNodeEvaluator>> joinKeys;
  /**
   * The ObjectInspectors for the join inputs's join keys.
   */
  protected transient Map<Byte, List<ObjectInspector>> joinKeysObjectInspectors;
  /**
   * The standard ObjectInspectors for the join inputs's join keys.
   */
  protected transient Map<Byte, List<ObjectInspector>> joinKeysStandardObjectInspectors;

  protected transient int posBigTable = -1; // one of the tables that is not in memory
  transient int mapJoinRowsKey; // rows for a given key

  protected transient RowContainer<ArrayList<Object>> emptyList = null;

  transient int numMapRowsRead;

  private static final transient String[] FATAL_ERR_MSG = {
      null, // counter value 0 means no error
      "Mapside join exceeds available memory. "
          + "Please try removing the mapjoin hint."
      };

  transient boolean firstRow;


  public AbstractMapJoinOperator() {
  }

  public AbstractMapJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mjop) {
    super((CommonJoinOperator)mjop);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    numMapRowsRead = 0;
    firstRow = true;

    joinKeys = new HashMap<Byte, List<ExprNodeEvaluator>>();

    JoinUtil.populateJoinKeyValue(joinKeys, conf.getKeys(),order,NOTSKIPBIGTABLE);
    joinKeysObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinKeys,
        inputObjInspectors,NOTSKIPBIGTABLE);
    joinKeysStandardObjectInspectors = JoinUtil.getStandardObjectInspectors(
        joinKeysObjectInspectors,NOTSKIPBIGTABLE);

    // all other tables are small, and are cached in the hash table
    posBigTable = conf.getPosBigTable();

    emptyList = new RowContainer<ArrayList<Object>>(1, hconf);
    RowContainer bigPosRC = JoinUtil.getRowContainer(hconf,
        rowContainerStandardObjectInspectors.get((byte) posBigTable),
        order[posBigTable], joinCacheSize,spillTableDesc, conf,noOuterJoin);
    storage.put((byte) posBigTable, bigPosRC);

    mapJoinRowsKey = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEMAPJOINROWSIZE);

    List<? extends StructField> structFields = ((StructObjectInspector) outputObjInspector)
        .getAllStructFieldRefs();
    if (conf.getOutputColumnNames().size() < structFields.size()) {
      List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
      for (Byte alias : order) {
        int sz = conf.getExprs().get(alias).size();
        List<Integer> retained = conf.getRetainList().get(alias);
        for (int i = 0; i < sz; i++) {
          int pos = retained.get(i);
          structFieldObjectInspectors.add(structFields.get(pos)
              .getFieldObjectInspector());
        }
      }
      outputObjInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(conf.getOutputColumnNames(),
          structFieldObjectInspectors);
    }
    initializeChildren(hconf);
  }


  @Override
  protected void fatalErrorMessage(StringBuilder errMsg, long counterCode) {
    errMsg.append("Operator " + getOperatorId() + " (id=" + id + "): "
        + FATAL_ERR_MSG[(int) counterCode]);
  }

  @Override
  public OperatorType getType() {
    return OperatorType.MAPJOIN;
  }

  // returns true if there are elements in key list and any of them is null
  protected boolean hasAnyNulls(ArrayList<Object> key) {
    if (key != null && key.size() > 0) {
      for (int i = 0; i < key.size(); i++) {
        if (key.get(i) == null && (nullsafes == null || !nullsafes[i])) {
          return true;
        }
      }
    }
    return false;
  }

  // returns true if there are elements in key list and any of them is null
  protected boolean hasAnyNulls(Object[] key) {
    if (key != null && key.length> 0) {
      for (int i = 0; i < key.length; i++) {
        if (key[i] == null && (nullsafes == null || !nullsafes[i])) {
          return true;
        }
      }
    }
    return false;
  }

  // returns true if there are elements in key list and any of them is null
  protected boolean hasAnyNulls(AbstractMapJoinKey key) {
    return key.hasAnyNulls(nullsafes);
  }

}
