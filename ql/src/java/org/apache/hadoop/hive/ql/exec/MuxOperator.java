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
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MuxDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

/**
 * MuxOperator is used in the Reduce side of MapReduce jobs optimized by Correlation Optimizer.
 * Correlation Optimizer will remove unnecessary ReduceSinkOperaotrs,
 * and MuxOperators are used to replace those ReduceSinkOperaotrs.
 * Example: The original operator tree is ...
 *      JOIN2
 *      /    \
 *     RS4   RS5
 *    /        \
 *   GBY1     JOIN1
 *    |       /    \
 *   RS1     RS2   RS3
 * If GBY1, JOIN1, and JOIN2 can be executed in the same reducer
 * (optimized by Correlation Optimizer).
 * The new operator tree will be ...
 *      JOIN2
 *        |
 *       MUX
 *      /   \
 *    GBY1  JOIN1
 *      \    /
 *       DEMUX
 *      /  |  \
 *     /   |   \
 *    /    |    \
 *   RS1   RS2   RS3
 *
 * A MuxOperator has two functions.
 * First, it will construct key, value and tag structure for
 * the input of Join Operators.
 * Second, it is a part of operator coordination mechanism which makes sure the operator tree
 * in the Reducer can work correctly.
 */
public class MuxOperator extends Operator<MuxDesc> implements Serializable{

  private static final long serialVersionUID = 1L;
  protected static final Log LOG = LogFactory.getLog(MuxOperator.class.getName());

  /**
   * Handler is used to construct the key-value structure.
   * This structure is needed by child JoinOperators and GroupByOperators of
   * a MuxOperator to function correctly.
   */
  protected static class Handler {
    private final ObjectInspector outputObjInspector;
    private final int tag;

    /**
     * The evaluators for the key columns. Key columns decide the sort order on
     * the reducer side. Key columns are passed to the reducer in the "key".
     */
    private final ExprNodeEvaluator[] keyEval;
    /**
     * The evaluators for the value columns. Value columns are passed to reducer
     * in the "value".
     */
    private final ExprNodeEvaluator[] valueEval;
    private final Object[] outputKey;
    private final Object[] outputValue;
    private final List<Object> forwardedRow;

    public Handler(ObjectInspector inputObjInspector,
        List<ExprNodeDesc> keyCols,
        List<ExprNodeDesc> valueCols,
        List<String> outputKeyColumnNames,
        List<String> outputValueColumnNames,
        Integer tag) throws HiveException {

      keyEval = new ExprNodeEvaluator[keyCols.size()];
      int i = 0;
      for (ExprNodeDesc e: keyCols) {
        keyEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }
      outputKey = new Object[keyEval.length];

      valueEval = new ExprNodeEvaluator[valueCols.size()];
      i = 0;
      for (ExprNodeDesc e: valueCols) {
        valueEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }
      outputValue = new Object[valueEval.length];

      this.tag = tag;

      ObjectInspector keyObjectInspector = initEvaluatorsAndReturnStruct(keyEval,
          outputKeyColumnNames, inputObjInspector);
      ObjectInspector valueObjectInspector = initEvaluatorsAndReturnStruct(valueEval,
          outputValueColumnNames, inputObjInspector);
      List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
      ois.add(keyObjectInspector);
      ois.add(valueObjectInspector);
      this.outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
              Utilities.reduceFieldNameList, ois);
      this.forwardedRow = new ArrayList<Object>(Utilities.reduceFieldNameList.size());
    }

    public ObjectInspector getOutputObjInspector() {
      return outputObjInspector;
    }

    public int getTag() {
      return tag;
    }

    public Object process(Object row) throws HiveException {
      // Evaluate the keys
      for (int i = 0; i < keyEval.length; i++) {
        outputKey[i] = keyEval[i].evaluate(row);
      }
      // Evaluate the value
      for (int i = 0; i < valueEval.length; i++) {
        outputValue[i] = valueEval[i].evaluate(row);
      }
      forwardedRow.clear();
      // JoinOperator assumes the key is backed by an list.
      // To be consistent, the value array is also converted
      // to a list.
      forwardedRow.add(Arrays.asList(outputKey));
      forwardedRow.add(Arrays.asList(outputValue));
      return forwardedRow;
    }
  }

  private transient ObjectInspector[] outputObjectInspectors;
  private transient int numParents;
  private transient boolean[] forward;
  private transient boolean[] processGroupCalled;
  private Handler[] handlers;

  // Counters for debugging, we cannot use existing counters (cntr and nextCntr)
  // in Operator since we want to individually track the number of rows from different inputs.
  private transient long[] cntrs;
  private transient long[] nextCntrs;

  @Override
  protected Collection<Future<?>> initializeOp(Configuration hconf) throws HiveException {
    Collection<Future<?>> result = super.initializeOp(hconf);

    // A MuxOperator should only have a single child
    if (childOperatorsArray.length != 1) {
      throw new HiveException(
          "Expected number of children is 1. Found : " + childOperatorsArray.length);
    }
    numParents = getNumParent();
    forward = new boolean[numParents];
    processGroupCalled = new boolean[numParents];
    outputObjectInspectors = new ObjectInspector[numParents];
    handlers = new Handler[numParents];
    cntrs = new long[numParents];
    nextCntrs = new long[numParents];
    for (int i = 0; i < numParents; i++) {
      processGroupCalled[i] = false;
      if (conf.getParentToKeyCols().get(i) == null) {
        // We do not need to evaluate the input row for this parent.
        // So, we can just forward it to the child of this MuxOperator.
        handlers[i] = null;
        forward[i] = true;
        outputObjectInspectors[i] = inputObjInspectors[i];
      } else {
        handlers[i] = new Handler(
            inputObjInspectors[i],
            conf.getParentToKeyCols().get(i),
            conf.getParentToValueCols().get(i),
            conf.getParentToOutputKeyColumnNames().get(i),
            conf.getParentToOutputValueColumnNames().get(i),
            conf.getParentToTag().get(i));
        forward[i] = false;
        outputObjectInspectors[i] = handlers[i].getOutputObjInspector();
      }
      cntrs[i] = 0;
      nextCntrs[i] = 1;
    }
    return result;
  }

  /**
   * Calls initialize on each of the children with outputObjetInspector as the
   * output row format.
   */
  @Override
  protected void initializeChildren(Configuration hconf) throws HiveException {
    state = State.INIT;
    if (isLogInfoEnabled) {
      LOG.info("Operator " + id + " " + getName() + " initialized");
    }
    if (childOperators == null || childOperators.isEmpty()) {
      return;
    }
    if (isLogInfoEnabled) {
      LOG.info("Initializing children of " + id + " " + getName());
    }
    childOperatorsArray[0].initialize(hconf, outputObjectInspectors);
    if (reporter != null) {
      childOperatorsArray[0].setReporter(reporter);
    }
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    if (isLogInfoEnabled) {
      cntrs[tag]++;
      if (cntrs[tag] == nextCntrs[tag]) {
        LOG.info(id + ", tag=" + tag + ", forwarding " + cntrs[tag] + " rows");
        nextCntrs[tag] = getNextCntr(cntrs[tag]);
      }
    }

    int childrenDone = 0;
    for (int i = 0; i < childOperatorsArray.length; i++) {
      Operator<? extends OperatorDesc> child = childOperatorsArray[i];
      if (child.getDone()) {
        childrenDone++;
      } else {
        if (forward[tag]) {
          // No need to evaluate, just forward it.
          child.process(row, tag);
        } else {
          // Call the corresponding handler to evaluate this row and
          // forward the result
          child.process(handlers[tag].process(row), handlers[tag].getTag());
        }
      }
    }

    // if all children are done, this operator is also done
    if (childrenDone == childOperatorsArray.length) {
      setDone(true);
    }
  }

  @Override
  public void forward(Object row, ObjectInspector rowInspector)
      throws HiveException {
    // Because we need to revert the tag of a row to its old tag and
    // we cannot pass new tag to this method which is used to get
    // the old tag from the mapping of newTagToOldTag, we bypass
    // this method in MuxOperator and directly call process on children
    // in process() method..
  }

  @Override
  public void startGroup() throws HiveException{
    for (int i = 0; i < numParents; i++) {
      processGroupCalled[i] = false;
    }
    super.startGroup();
  }

  @Override
  public void endGroup() throws HiveException {
    // do nothing
  }

  @Override
  public void processGroup(int tag) throws HiveException {
    processGroupCalled[tag] = true;
    boolean shouldProceed = true;
    for (int i = 0; i < numParents; i++) {
      if (!processGroupCalled[i]) {
        shouldProceed = false;
        break;
      }
    }
    if (shouldProceed) {
      Operator<? extends OperatorDesc> child = childOperatorsArray[0];
      int childTag = childOperatorsTag[0];
      child.flush();
      child.endGroup();
      child.processGroup(childTag);
    }
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    if (isLogInfoEnabled) {
      for (int i = 0; i < numParents; i++) {
        LOG.info(id + ", tag=" + i + ", forwarded " + cntrs[i] + " rows");
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
    return "MUX";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.MUX;
  }
}
