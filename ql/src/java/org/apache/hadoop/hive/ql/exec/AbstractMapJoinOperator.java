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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public abstract class AbstractMapJoinOperator <T extends MapJoinDesc> extends CommonJoinOperator<T> implements
    Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * The expressions for join inputs's join keys.
   */
  protected transient List<ExprNodeEvaluator>[] joinKeys;
  /**
   * The ObjectInspectors for the join inputs's join keys.
   */
  protected transient List<ObjectInspector>[] joinKeysObjectInspectors;

  protected transient byte posBigTable = -1; // pos of driver alias

  protected transient RowContainer<List<Object>> emptyList = null;

  transient int numMapRowsRead;

  public AbstractMapJoinOperator() {
  }

  public AbstractMapJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mjop) {
    super((CommonJoinOperator)mjop);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Collection<Future<?>> initializeOp(Configuration hconf) throws HiveException {
    if (conf.getGenJoinKeys()) {
      int tagLen = conf.getTagLength();
      joinKeys = new List[tagLen];
      JoinUtil.populateJoinKeyValue(joinKeys, conf.getKeys(), NOTSKIPBIGTABLE);
      joinKeysObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinKeys,
          inputObjInspectors,NOTSKIPBIGTABLE, tagLen);
    }

    Collection<Future<?>> result = super.initializeOp(hconf);

    numMapRowsRead = 0;

    // all other tables are small, and are cached in the hash table
    posBigTable = (byte) conf.getPosBigTable();

    emptyList = new RowContainer<List<Object>>(1, hconf, reporter);

    RowContainer<List<Object>> bigPosRC = JoinUtil.getRowContainer(hconf,
        rowContainerStandardObjectInspectors[posBigTable],
        posBigTable, joinCacheSize,spillTableDesc, conf,
        !hasFilter(posBigTable), reporter);
    storage[posBigTable] = bigPosRC;

    return result;
  }

  @Override
  protected List<ObjectInspector> getValueObjectInspectors(
      byte alias, List<ObjectInspector>[] aliasToObjectInspectors) {
    List<ObjectInspector> inspectors = aliasToObjectInspectors[alias];
    List<Integer> retained = conf.getRetainList().get(alias);
    if (inspectors.size() == retained.size()) {
      return inspectors;
    }
    List<ObjectInspector> retainedOIs = new ArrayList<ObjectInspector>();
    for (int index : retained) {
      retainedOIs.add(inspectors.get(index));
    }
    return retainedOIs;
  }

  @Override
  public OperatorType getType() {
    return OperatorType.MAPJOIN;
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    super.closeOp(abort);
    emptyList = null;
    joinKeys = null;
  }
}
