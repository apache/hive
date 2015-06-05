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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.DemuxDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hive.common.util.ReflectionUtil;

/**
 * DemuxOperator is an operator used by MapReduce Jobs optimized by
 * CorrelationOptimizer. If used, DemuxOperator is the first operator in reduce
 * phase. In the case that multiple operation paths are merged into a single one, it will dispatch
 * the record to corresponding child operators (Join or GBY).
 */
public class DemuxOperator extends Operator<DemuxDesc>
  implements Serializable {

  private static final long serialVersionUID = 1L;
  protected static final Log LOG = LogFactory.getLog(DemuxOperator.class.getName());

  // Counters for debugging, we cannot use existing counters (cntr and nextCntr)
  // in Operator since we want to individually track the number of rows from
  // different paths.
  private transient long[] cntrs;
  private transient long[] nextCntrs;

  // The mapping from a newTag to its corresponding oldTag.
  // oldTag is the tag assigned to ReduceSinkOperators BEFORE Correlation Optimizer
  // optimizes the operator tree. newTag is the tag assigned to ReduceSinkOperators
  // AFTER Correlation Optimizer optimizes the operator tree.
  // Example: we have an operator tree shown below ...
  //        JOIN2
  //       /     \
  //   GBY1       JOIN1
  //    |         /    \
  //   RS1       RS2   RS3
  // If GBY1, JOIN1, and JOIN2 are executed in the same Reducer
  // (optimized by Correlation Optimizer), we will have ...
  // oldTag: RS1:0, RS2:0, RS3:1
  // newTag: RS1:0, RS2:1, RS3:2
  // We need to know the mapping from the newTag to oldTag and revert
  // the newTag to oldTag to make operators in the operator tree
  // function correctly.
  private int[] newTagToOldTag;

  // The mapping from a newTag to the index of the corresponding child
  // of this operator.
  private int[] newTagToChildIndex;

  // The mapping from the index of a child operator to its corresponding
  // inputObjectInspectors
  private ObjectInspector[][] childInputObjInspectors;

  private int childrenDone;

  // The index of the child which the last row was forwarded to in a key group.
  private int lastChildIndex;

  // Since DemuxOperator may appear multiple times in MuxOperator's parents list.
  // We use newChildIndexTag instead of childOperatorsTag.
  // Example:
  //         JOIN
  //           |
  //          MUX
  //         / | \
  //        /  |  \
  //       /   |   \
  //       |  GBY  |
  //       \   |   /
  //        \  |  /
  //         DEMUX
  // In this case, the parent list of MUX is [DEMUX, GBY, DEMUX],
  // so we need to have two childOperatorsTags (the index of this DemuxOperator in
  // its children's parents lists, also see childOperatorsTag in Operator) at here.
  private int[][] newChildOperatorsTag;

  @Override
  protected Collection<Future<?>> initializeOp(Configuration hconf) throws HiveException {
    Collection<Future<?>> result = super.initializeOp(hconf);
    // A DemuxOperator should have at least one child
    if (childOperatorsArray.length == 0) {
      throw new HiveException(
          "Expected number of children is at least 1. Found : " + childOperatorsArray.length);
    }

    newTagToOldTag = toArray(conf.getNewTagToOldTag());
    newTagToChildIndex = toArray(conf.getNewTagToChildIndex());
    childInputObjInspectors = new ObjectInspector[childOperators.size()][];
    cntrs = new long[newTagToOldTag.length];
    nextCntrs = new long[newTagToOldTag.length];

    try {
      // We populate inputInspectors for all children of this DemuxOperator.
      // Those inputObjectInspectors are stored in childInputObjInspectors.
      for (int i = 0; i < newTagToOldTag.length; i++) {
        int newTag = i;
        int oldTag = newTagToOldTag[i];
        int childIndex = newTagToChildIndex[newTag];
        cntrs[newTag] = 0;
        nextCntrs[newTag] = 0;
        TableDesc keyTableDesc = conf.getKeysSerializeInfos().get(newTag);
        Deserializer inputKeyDeserializer = ReflectionUtil.newInstance(keyTableDesc
            .getDeserializerClass(), null);
        SerDeUtils.initializeSerDe(inputKeyDeserializer, null, keyTableDesc.getProperties(), null);

        TableDesc valueTableDesc = conf.getValuesSerializeInfos().get(newTag);
        Deserializer inputValueDeserializer = ReflectionUtil.newInstance(valueTableDesc
            .getDeserializerClass(), null);
        SerDeUtils.initializeSerDe(inputValueDeserializer, null, valueTableDesc.getProperties(),
                                   null);

        List<ObjectInspector> oi = new ArrayList<ObjectInspector>();
        oi.add(inputKeyDeserializer.getObjectInspector());
        oi.add(inputValueDeserializer.getObjectInspector());
        int childParentsCount = conf.getChildIndexToOriginalNumParents().get(childIndex);
        // Multiple newTags can point to the same child (e.g. when the child is a JoinOperator).
        // So, we first check if childInputObjInspectors contains the key of childIndex.
        if (childInputObjInspectors[childIndex] == null) {
          childInputObjInspectors[childIndex] = new ObjectInspector[childParentsCount];
        }
        ObjectInspector[] ois = childInputObjInspectors[childIndex];
        ois[oldTag] = ObjectInspectorFactory
            .getStandardStructObjectInspector(Utilities.reduceFieldNameList, oi);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    childrenDone = 0;
    newChildOperatorsTag = new int[childOperators.size()][];
    for (int i = 0; i < childOperators.size(); i++) {
      Operator<? extends OperatorDesc> child = childOperators.get(i);
      List<Integer> childOperatorTags = new ArrayList<Integer>();
      if (child instanceof MuxOperator) {
        // This DemuxOperator can appear multiple times in MuxOperator's
        // parentOperators
        int index = 0;
        for (Operator<? extends OperatorDesc> parent: child.getParentOperators()) {
          if (this == parent) {
            childOperatorTags.add(index);
          }
          index++;
        }
      } else {
        childOperatorTags.add(child.getParentOperators().indexOf(this));
      }
      newChildOperatorsTag[i] = toArray(childOperatorTags);
    }
    if (isLogInfoEnabled) {
      LOG.info("newChildOperatorsTag " + Arrays.toString(newChildOperatorsTag));
    }
    return result;
  }

  private int[] toArray(List<Integer> list) {
    int[] array = new int[list.size()];
    for (int i = 0; i < list.size(); i++) {
      array[i] = list.get(i);
    }
    return array;
  }

  private int[] toArray(Map<Integer, Integer> map) {
    int[] array = new int[map.size()];
    for (Entry<Integer, Integer> entry : map.entrySet()) {
      array[entry.getKey()] = entry.getValue();
    }
    return array;
  }

  // Each child should has its own outputObjInspector
  @Override
  protected void initializeChildren(Configuration hconf) throws HiveException {
    state = State.INIT;
    if (isLogInfoEnabled) {
      LOG.info("Operator " + id + " " + getName() + " initialized");
      LOG.info("Initializing children of " + id + " " + getName());
    }
    for (int i = 0; i < childOperatorsArray.length; i++) {
      if (isLogInfoEnabled) {
	LOG.info("Initializing child " + i + " " + childOperatorsArray[i].getIdentifier() + " " +
	    childOperatorsArray[i].getName() +
	    " " + childInputObjInspectors[i].length);
      }
      // We need to initialize those MuxOperators first because if we first
      // initialize other operators, the states of all parents of those MuxOperators
      // are INIT (including this DemuxOperator),
      // but the inputInspector of those MuxOperators has not been set.
      if (childOperatorsArray[i] instanceof MuxOperator) {
        // If this DemuxOperator directly connects to a MuxOperator,
        // that MuxOperator must be the parent of a JoinOperator.
        // In this case, that MuxOperator should be initialized
        // by multiple parents (of that MuxOperator).
        ObjectInspector[] ois = childInputObjInspectors[i];
        for (int j = 0; j < ois.length; j++) {
          if (ois[j] != null) {
            childOperatorsArray[i].initialize(hconf, ois[j], j);
          }
        }
      } else {
        continue;
      }
      if (reporter != null) {
        childOperatorsArray[i].setReporter(reporter);
      }
    }
    for (int i = 0; i < childOperatorsArray.length; i++) {
      if (isLogInfoEnabled) {
	LOG.info("Initializing child " + i + " " + childOperatorsArray[i].getIdentifier() + " " +
	    childOperatorsArray[i].getName() +
	    " " + childInputObjInspectors[i].length);
      }
      if (!(childOperatorsArray[i] instanceof MuxOperator)) {
        childOperatorsArray[i].initialize(hconf, childInputObjInspectors[i]);
      } else {
        continue;
      }
      if (reporter != null) {
        childOperatorsArray[i].setReporter(reporter);
      }
    }
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    int currentChildIndex = newTagToChildIndex[tag];

    // Check if we start to forward rows to a new child.
    // If so, in the current key group, rows will not be forwarded
    // to those children which have an index less than the currentChildIndex.
    // We can call flush the buffer of children from lastChildIndex (inclusive)
    // to currentChildIndex (exclusive) and propagate processGroup to those children.
    endGroupIfNecessary(currentChildIndex);

    int oldTag = newTagToOldTag[tag];
    if (isLogDebugEnabled) {
      cntrs[tag]++;
      if (cntrs[tag] == nextCntrs[tag]) {
        LOG.debug(id + " (newTag, childIndex, oldTag)=(" + tag + ", " + currentChildIndex + ", "
            + oldTag + "), forwarding " + cntrs[tag] + " rows");
        nextCntrs[tag] = getNextCntr(cntrs[tag]);
      }
    }

    Operator<? extends OperatorDesc> child = childOperatorsArray[currentChildIndex];
    if (child.getDone()) {
      childrenDone++;
    } else {
      child.process(row, oldTag);
    }

    // if all children are done, this operator is also done
    if (childrenDone == childOperatorsArray.length) {
      setDone(true);
    }
  }

  @Override
  public void forward(Object row, ObjectInspector rowInspector)
      throws HiveException {
    // DemuxOperator forwards a row to exactly one child in its children list
    // based on the tag and newTagToChildIndex in processOp() method.
    // So we need not to do anything in here.
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    for (int i = 0 ; i < newTagToOldTag.length; i++) {
      int newTag = i;
      int oldTag = newTagToOldTag[i];
      int childIndex = newTagToChildIndex[newTag];
      if (isLogInfoEnabled) {
	LOG.info(id + " (newTag, childIndex, oldTag)=(" + newTag + ", " + childIndex + ", "
	    + oldTag + "),  forwarded " + cntrs[newTag] + " rows");
      }
    }
  }

  /**
   * We assume that the input rows associated with the same key are ordered by
   * the tag. Because a tag maps to a childindex, when we see a new childIndex,
   * we will not see the last childIndex (lastChildIndex) again before we start
   * a new key group. So, we can call flush the buffer of children
   * from lastChildIndex (inclusive) to currentChildIndex (exclusive) and
   * propagate processGroup to those children.
   * @param currentChildIndex the childIndex we have right now.
   * @throws HiveException
   */
  private void endGroupIfNecessary(int currentChildIndex) throws HiveException {
    if (lastChildIndex != currentChildIndex) {
      for (int i = lastChildIndex; i < currentChildIndex; i++) {
        Operator<? extends OperatorDesc> child = childOperatorsArray[i];
        child.flush();
        child.endGroup();
        for (int childTag: newChildOperatorsTag[i]) {
          child.processGroup(childTag);
        }
      }
      lastChildIndex = currentChildIndex;
    }
  }

  @Override
  public void startGroup() throws HiveException {
    lastChildIndex = 0;
    super.startGroup();
  }

  @Override
  public void endGroup() throws HiveException {
    if (childOperators == null) {
      return;
    }

    // We will start a new key group. We can call flush the buffer
    // of children from lastChildIndex (inclusive) to the last child and
    // propagate processGroup to those children.
    for (int i = lastChildIndex; i < childOperatorsArray.length; i++) {
      Operator<? extends OperatorDesc> child = childOperatorsArray[i];
      child.flush();
      child.endGroup();
      for (int childTag: newChildOperatorsTag[i]) {
        child.processGroup(childTag);
      }
    }
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "DEMUX";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.DEMUX;
  }
}
