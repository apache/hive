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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.DummyStoreDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;

/**
 * For SortMerge joins, this is a dummy operator, which stores the row for the
 * small table before it reaches the sort merge join operator.
 *
 * Consider a query like:
 *
 * select * from
 *   (subq1 --> has a filter)
 *   join
 *   (subq2 --> has a filter)
 * on some key
 *
 * Let us assume that subq1 is the small table (either specified by the user or inferred
 * automatically). Since there can be multiple buckets/partitions for the table corresponding
 * to subq1 given a file in subq2, a priority queue is present in SMBMapJoinOperator to scan the
 * various buckets and fetch the least row (corresponding to the join key). The tree corresponding
 * to subq1 needs to be evaluated in order to compute the join key (since the select list for the
 * join key can move across different object inspectors).
 *
 * Therefore the following operator tree is created:
 *
 * TableScan (subq1) --> Select --> Filter --> DummyStore
 *                                                         \
 *                                                          \     SMBJoin
 *                                                          /
 *                                                         /
 * TableScan (subq2) --> Select --> Filter
 *
 * In order to fetch the row with the least join key from the small table, the row from subq1
 * is partially processed, and stored in DummyStore. For the actual processing of the join,
 * SMBJoin (child of DummyStore) is processed for the transformed row. Note that in the absence of
 * support for joins for sub-queries, this was not needed, since all transformations were done
 * after SMBJoin, or for the small tables, nothing could have been present between TableScan and
 * SMBJoin.
 */
public class DummyStoreOperator extends Operator<DummyStoreDesc> implements Serializable {

  protected transient InspectableObject result;

  public DummyStoreOperator() {
    super();
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    /*
     * The conversion to standard object inspector was necessitated by HIVE-5973. The issue 
     * happens when a select operator preceeds this operator as in the case of a subquery. The 
     * select operator does not allocate a new object to hold the deserialized row. This affects 
     * the operation of the SMB join which puts the object in a priority queue. Since all elements
     * of the priority queue point to the same object, the join was resulting in incorrect 
     * results.
     *
     * So the fix is to make a copy of the object as done in the processOp phase below. This
     * however necessitates a change in the object inspector that can be used in processing the
     * row downstream.
     */
    outputObjInspector = ObjectInspectorUtils.getStandardObjectInspector(inputObjInspectors[0]);
    result = new InspectableObject(null, outputObjInspector);
    initializeChildren(hconf);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    // Store the row. See comments above for why we need a new copy of the row.
    result.o = ObjectInspectorUtils.copyToStandardObject(row, inputObjInspectors[0],
        ObjectInspectorCopyOption.WRITABLE);
  }

  @Override
  public void reset() {
    result = new InspectableObject(null, result.oi);
  }

  public InspectableObject getResult() {
    return result;
  }

  @Override
  public OperatorType getType() {
    return OperatorType.FORWARD;
  }
}
