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
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.DemuxDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.util.ReflectionUtils;

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

  /**
   * Handler is used to assign original tag (oldTag) to a row and
   * track how many rows are forwarded to every child of DemuxOperator.
   */
  protected static class Handler {
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
    private final byte newTag;
    private final byte oldTag;
    private final byte childIndex;
    private final ByteWritable oldTagByteWritable;
    private final List<Object> forwardedRow;

    // counters for debugging
    private transient long cntr = 0;
    private transient long nextCntr = 1;

    private long getNextCntr(long cntr) {
      // A very simple counter to keep track of number of rows processed by an
      // operator. It dumps
      // every 1 million times, and quickly before that
      if (cntr >= 1000000) {
        return cntr + 1000000;
      }
      return 10 * cntr;
    }

    public long getCntr() {
      return this.cntr;
    }

    private final Log log;
    private final boolean isLogInfoEnabled;
    private final String id;

    public Handler(byte newTag, byte childIndex, byte oldTag, Log LOG, String id)
            throws HiveException {
      this.newTag = newTag;
      this.oldTag = oldTag;
      this.childIndex = childIndex;
      this.oldTagByteWritable = new ByteWritable(oldTag);
      this.log = LOG;
      this.isLogInfoEnabled = LOG.isInfoEnabled();
      this.id = id;
      this.forwardedRow = new ArrayList<Object>(3);
    }

    public byte getOldTag() {
      return oldTag;
    }

    public Object process(Object row) throws HiveException {
      forwardedRow.clear();
      List<Object> thisRow = (List<Object>) row;
      forwardedRow.add(thisRow.get(0));
      forwardedRow.add(thisRow.get(1));
      forwardedRow.add(oldTagByteWritable);

      if (isLogInfoEnabled) {
        cntr++;
        if (cntr == nextCntr) {
          log.info(id + " (newTag, childIndex, oldTag)=(" + newTag + ", " + childIndex + ", "
              + oldTag + "), forwarding " + cntr + " rows");
          nextCntr = getNextCntr(cntr);
        }
      }

      return forwardedRow;
    }

    public void printCloseOpLog() {
      log.info(id + " (newTag, childIndex, oldTag)=(" + newTag + ", " + childIndex + ", "
          + oldTag + "),  forwarded " + cntr + " rows");
    }
  }

  // The mapping from a newTag to its corresponding oldTag. Please see comments in
  // DemuxOperator.Handler for explanations of newTag and oldTag.
  private Map<Integer, Integer> newTagToOldTag =
      new HashMap<Integer, Integer>();

  // The mapping from a newTag to the index of the corresponding child
  // of this operator.
  private Map<Integer, Integer> newTagToChildIndex =
      new HashMap<Integer, Integer>();

  // The mapping from a newTag to its corresponding handler
  private Map<Integer, Handler> newTagToDispatchHandler =
      new HashMap<Integer, Handler>();

  // The mapping from the index of a child operator to its corresponding
  // inputObjectInspectors
  private Map<Integer, ObjectInspector[]> childInputObjInspectors;

  private int childrenDone;

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
  private List<List<Integer>> newChildOperatorsTag;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    this.newTagToOldTag = conf.getNewTagToOldTag();
    this.newTagToChildIndex = conf.getNewTagToChildIndex();
    this.newTagToDispatchHandler = new HashMap<Integer, Handler>();
    this.childInputObjInspectors = new HashMap<Integer, ObjectInspector[]>();

    // For every newTag (every newTag corresponds to a ReduceSinkOperator),
    // create a handler. Also, we initialize childInputObjInspectors at here.
    for (Entry<Integer, Integer> entry: newTagToOldTag.entrySet()) {
      int newTag = entry.getKey();
      int oldTag = entry.getValue();
      int childIndex = newTagToChildIndex.get(newTag);
      Handler handler =
          new Handler((byte)newTag, (byte)childIndex, (byte)oldTag, LOG, id);
      newTagToDispatchHandler.put(newTag, handler);
      int childParentsCount = conf.getChildIndexToOriginalNumParents().get(childIndex);
      childInputObjInspectors.put(childIndex, new ObjectInspector[childParentsCount]);
    }

    try {
      // We populate inputInspectors for all children of this DemuxOperator.
      // Those inputObjectInspectors are stored in childInputObjInspectors.
      for (Entry<Integer, Integer> e1: newTagToOldTag.entrySet()) {
        int newTag = e1.getKey();
        int oldTag = e1.getValue();
        int childIndex = newTagToChildIndex.get(newTag);
        TableDesc keyTableDesc = conf.getKeysSerializeInfos().get(newTag);
        Deserializer inputKeyDeserializer = (SerDe) ReflectionUtils.newInstance(keyTableDesc
            .getDeserializerClass(), null);
        inputKeyDeserializer.initialize(null, keyTableDesc.getProperties());

        TableDesc valueTableDesc = conf.getValuesSerializeInfos().get(newTag);
        Deserializer inputValueDeserializer = (SerDe) ReflectionUtils.newInstance(valueTableDesc
            .getDeserializerClass(), null);
        inputValueDeserializer.initialize(null, valueTableDesc.getProperties());

        List<ObjectInspector> oi = new ArrayList<ObjectInspector>();
        oi.add(inputKeyDeserializer.getObjectInspector());
        oi.add(inputValueDeserializer.getObjectInspector());
        oi.add(PrimitiveObjectInspectorFactory.writableByteObjectInspector);
        ObjectInspector[] ois = childInputObjInspectors.get(childIndex);
        ois[oldTag] = ObjectInspectorFactory
            .getStandardStructObjectInspector(Utilities.fieldNameList, oi);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    this.childrenDone = 0;
    newChildOperatorsTag = new ArrayList<List<Integer>>();
    for (Operator<? extends OperatorDesc> child: childOperators) {
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
      newChildOperatorsTag.add(childOperatorTags);
    }
    LOG.info("newChildOperatorsTag " + newChildOperatorsTag);
    initializeChildren(hconf);
  }

  // Each child should has its own outputObjInspector
  @Override
  protected void initializeChildren(Configuration hconf) throws HiveException {
    state = State.INIT;
    LOG.info("Operator " + id + " " + getName() + " initialized");
    if (childOperators == null) {
      return;
    }
    LOG.info("Initializing children of " + id + " " + getName());
    for (int i = 0; i < childOperatorsArray.length; i++) {
      LOG.info("Initializing child " + i + " " + childOperatorsArray[i].getIdentifier() + " " +
          childOperatorsArray[i].getName() +
          " " + childInputObjInspectors.get(i).length);
      // We need to initialize those MuxOperators first because if we first
      // initialize other operators, the states of all parents of those MuxOperators
      // are INIT (including this DemuxOperator),
      // but the inputInspector of those MuxOperators has not been set.
      if (childOperatorsArray[i] instanceof MuxOperator) {
        // If this DemuxOperator directly connects to a MuxOperator,
        // that MuxOperator must be the parent of a JoinOperator.
        // In this case, that MuxOperator should be initialized
        // by multiple parents (of that MuxOperator).
        ObjectInspector[] ois = childInputObjInspectors.get(i);
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
      LOG.info("Initializing child " + i + " " + childOperatorsArray[i].getIdentifier() + " " +
          childOperatorsArray[i].getName() +
          " " + childInputObjInspectors.get(i).length);
      if (!(childOperatorsArray[i] instanceof MuxOperator)) {
        childOperatorsArray[i].initialize(hconf, childInputObjInspectors.get(i));
      } else {
        continue;
      }
      if (reporter != null) {
        childOperatorsArray[i].setReporter(reporter);
      }
    }
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    int newTag = tag;
    forward(row, inputObjInspectors[newTag]);
  }

  @Override
  public void forward(Object row, ObjectInspector rowInspector)
      throws HiveException {
    if ((++outputRows % 1000) == 0) {
      if (counterNameToEnum != null) {
        incrCounter(numOutputRowsCntr, outputRows);
        outputRows = 0;
      }
    }

    if (childOperatorsArray == null && childOperators != null) {
      throw new HiveException("Internal Hive error during operator initialization.");
    }

    if ((childOperatorsArray == null) || (getDone())) {
      return;
    }

    List<Object> thisRow = (List<Object>) row;
    assert thisRow.size() == 3;
    int newTag = ((ByteWritable) thisRow.get(2)).get();
    Handler handler = newTagToDispatchHandler.get(newTag);
    int childIndex = newTagToChildIndex.get(newTag);
    Operator<? extends OperatorDesc> o = childOperatorsArray[childIndex];
    if (o.getDone()) {
      childrenDone++;
    } else {
      o.process(handler.process(row), handler.getOldTag());
    }

    // if all children are done, this operator is also done
    if (childrenDone == childOperatorsArray.length) {
      setDone(true);
    }

  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    // log the number of rows forwarded from each dispatcherHandler
    for (Handler handler: newTagToDispatchHandler.values()) {
      handler.printCloseOpLog();
    }
  }

  @Override
  public void endGroup() throws HiveException {
    if (childOperators == null) {
      return;
    }

    if (fatalError) {
      return;
    }

    for (int i = 0; i < childOperatorsArray.length; i++) {
      Operator<? extends OperatorDesc> child = childOperatorsArray[i];
      child.flush();
      child.endGroup();
      for (Integer childTag: newChildOperatorsTag.get(i)) {
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
