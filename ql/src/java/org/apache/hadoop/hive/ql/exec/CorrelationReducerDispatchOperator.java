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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CorrelationReducerDispatchDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * CorrelationReducerDispatchOperator is an operator used by MapReduce join optimized by
 * CorrelationOptimizer. If used, CorrelationReducerDispatchOperator is the first operator in reduce
 * phase. In the case that multiple operation paths are merged into a single one, it will dispatch
 * the record to corresponding JOIN or GBY operators. Every child of this operator is associated
 * with a DispatcherHnadler, which evaluates the input row of this operator and then select
 * corresponding fields for its associated child.
 */
public class CorrelationReducerDispatchOperator extends Operator<CorrelationReducerDispatchDesc>
  implements Serializable {

  private static final long serialVersionUID = 1L;
  private static String[] fieldNames;
  static {
    List<String> fieldNameArray = new ArrayList<String>();
    for (Utilities.ReduceField r : Utilities.ReduceField.values()) {
      fieldNameArray.add(r.toString());
    }
    fieldNames = fieldNameArray.toArray(new String[0]);
  }

  protected static class DispatchHandler {

    protected Log l4j = LogFactory.getLog(this.getClass().getName());

    private final ObjectInspector[] inputObjInspector;
    private ObjectInspector outputObjInspector;
    private ObjectInspector keyObjInspector;
    private ObjectInspector valueObjInspector;
    private final byte inputTag;
    private final byte outputTag;
    private final byte childIndx;
    private final ByteWritable outputTagByteWritable;
    private final SelectDesc keySelectDesc;
    private final SelectDesc valueSelectDesc;
    private ExprNodeEvaluator[] keyEval;
    private ExprNodeEvaluator[] eval;

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

    private final Log LOG;
    private final boolean isLogInfoEnabled;
    private final String id;

    public DispatchHandler(ObjectInspector[] inputObjInspector, byte inputTag, byte childIndx,
        byte outputTag,
        SelectDesc valueSelectDesc, SelectDesc keySelectDesc, Log LOG, String id)
            throws HiveException {
      this.inputObjInspector = inputObjInspector;
      assert this.inputObjInspector.length == 1;
      this.inputTag = inputTag;
      this.childIndx = childIndx;
      this.outputTag = outputTag;
      this.valueSelectDesc = valueSelectDesc;
      this.keySelectDesc = keySelectDesc;
      this.outputTagByteWritable = new ByteWritable(outputTag);
      this.LOG = LOG;
      this.isLogInfoEnabled = LOG.isInfoEnabled();
      this.id = id;
      init();
    }

    private void init() throws HiveException {
      List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
      if (keySelectDesc.isSelStarNoCompute()) {
        ois.add((ObjectInspector) ((List) inputObjInspector[0]).get(0));
      } else {
        List<ExprNodeDesc> colList = this.keySelectDesc.getColList();
        keyEval = new ExprNodeEvaluator[colList.size()];
        for (int k = 0; k < colList.size(); k++) {
          assert (colList.get(k) != null);
          keyEval[k] = ExprNodeEvaluatorFactory.get(colList.get(k));
        }
        keyObjInspector =
            initEvaluatorsAndReturnStruct(keyEval, keySelectDesc
                .getOutputColumnNames(), ((StandardStructObjectInspector) inputObjInspector[0])
                .getAllStructFieldRefs().get(0).getFieldObjectInspector());

        ois.add(keyObjInspector);
        l4j.info("Key: input tag " + (int) inputTag + ", output tag " + (int) outputTag
            + ", SELECT inputOIForThisTag"
            + ((StructObjectInspector) inputObjInspector[0]).getTypeName());
      }
      if (valueSelectDesc.isSelStarNoCompute()) {
        ois.add((ObjectInspector) ((List) inputObjInspector[0]).get(1));
      } else {
        List<ExprNodeDesc> colList = this.valueSelectDesc.getColList();
        eval = new ExprNodeEvaluator[colList.size()];
        for (int k = 0; k < colList.size(); k++) {
          assert (colList.get(k) != null);
          eval[k] = ExprNodeEvaluatorFactory.get(colList.get(k));
        }
        valueObjInspector =
            initEvaluatorsAndReturnStruct(eval, valueSelectDesc
                .getOutputColumnNames(), ((StandardStructObjectInspector) inputObjInspector[0])
                .getAllStructFieldRefs().get(1).getFieldObjectInspector());

        ois.add(valueObjInspector);
        l4j.info("input tag " + (int) inputTag + ", output tag " + (int) outputTag
            + ", SELECT inputOIForThisTag"
            + ((StructObjectInspector) inputObjInspector[0]).getTypeName());
      }
      ois.add(PrimitiveObjectInspectorFactory.writableByteObjectInspector);
      outputObjInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(Arrays.asList(fieldNames), ois);
      l4j.info("input tag " + (int) inputTag + ", output tag " + (int) outputTag
          + ", SELECT outputObjInspector"
          + ((StructObjectInspector) outputObjInspector).getTypeName());
    }

    public ObjectInspector getOutputObjInspector() {
      return outputObjInspector;
    }

    public Object process(Object row) throws HiveException {
      List<Object> keyOutput = new ArrayList<Object>(keyEval.length);
      Object[] valueOutput = new Object[eval.length];
      List<Object> outputRow = new ArrayList<Object>(3);
      List thisRow = (List) row;
      if (keySelectDesc.isSelStarNoCompute()) {
        outputRow.add(thisRow.get(0));
      } else {
        Object key = thisRow.get(0);
        for (int j = 0; j < keyEval.length; j++) {
          try {
            keyOutput.add(keyEval[j].evaluate(key));
          } catch (HiveException e) {
            throw e;
          } catch (RuntimeException e) {
            throw new HiveException("Error evaluating "
                + keySelectDesc.getColList().get(j).getExprString(), e);
          }
        }
        outputRow.add(keyOutput);
      }

      if (valueSelectDesc.isSelStarNoCompute()) {
        outputRow.add(thisRow.get(1));
      } else {
        Object value = thisRow.get(1);
        for (int j = 0; j < eval.length; j++) {
          try {
            valueOutput[j] = eval[j].evaluate(value);
          } catch (HiveException e) {
            throw e;
          } catch (RuntimeException e) {
            throw new HiveException("Error evaluating "
                + valueSelectDesc.getColList().get(j).getExprString(), e);
          }
        }
        outputRow.add(valueOutput);
      }
      outputRow.add(outputTagByteWritable);

      if (isLogInfoEnabled) {
        cntr++;
        if (cntr == nextCntr) {
          LOG.info(id + "(inputTag, childIndx, outputTag)=(" + inputTag + ", " + childIndx + ", "
              + outputTag + "), forwarding " + cntr + " rows");
          nextCntr = getNextCntr(cntr);
        }
      }

      return outputRow;
    }

    public void printCloseOpLog() {
      LOG.info(id + "(inputTag, childIndx, outputTag)=(" + inputTag + ", " + childIndx + ", "
          + outputTag + "),  forwarded " + cntr + " rows");
    }
  }

  // inputTag->(Child->List<outputTag>)
  private Map<Integer, Map<Integer, List<Integer>>> dispatchConf;
  // inputTag->(Child->List<SelectDesc>)
  private Map<Integer, Map<Integer, List<SelectDesc>>> dispatchValueSelectDescConf;
  // inputTag->(Child->List<SelectDesc>)
  private Map<Integer, Map<Integer, List<SelectDesc>>> dispatchKeySelectDescConf;
  // inputTag->(Child->List<DispatchHandler>)
  private Map<Integer, Map<Integer, List<DispatchHandler>>> dispatchHandlers;
  // Child->(outputTag->DispatchHandler)
  private Map<Integer, Map<Integer, DispatchHandler>> child2OutputTag2DispatchHandlers;
  // Child->Child's inputObjInspectors
  private Map<Integer, ObjectInspector[]> childInputObjInspectors;

  private int operationPathTag;
  private int inputTag;

  private Object[] lastDispatchedRows;
  private int[] lastDispatchedTags;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    dispatchConf = conf.getDispatchConf();
    dispatchValueSelectDescConf = conf.getDispatchValueSelectDescConf();
    dispatchKeySelectDescConf = conf.getDispatchKeySelectDescConf();
    dispatchHandlers = new HashMap<Integer, Map<Integer, List<DispatchHandler>>>();
    for (Entry<Integer, Map<Integer, List<Integer>>> entry : dispatchConf.entrySet()) {
      Map<Integer, List<DispatchHandler>> tmp =
          new HashMap<Integer, List<DispatchHandler>>();
      for (Entry<Integer, List<Integer>> child2outputTag : entry.getValue().entrySet()) {
        tmp.put(child2outputTag.getKey(), new ArrayList<DispatchHandler>());
        int indx = 0;
        for (Integer outputTag : child2outputTag.getValue()) {
          ObjectInspector[] thisInputObjectInspector =
              new ObjectInspector[] {inputObjInspectors[entry.getKey()]};
          Integer thisInputTag = entry.getKey();
          Integer thisChildIndx = child2outputTag.getKey();
          SelectDesc thisValueSelectDesc = dispatchValueSelectDescConf.get(thisInputTag)
              .get(thisChildIndx).get(indx);
          SelectDesc thisKeySelectDesc = dispatchKeySelectDescConf.get(thisInputTag)
              .get(thisChildIndx).get(indx);
          tmp.get(child2outputTag.getKey()).add(
              new DispatchHandler(thisInputObjectInspector,
                  thisInputTag.byteValue(), thisChildIndx.byteValue(), outputTag.byteValue(),
                  thisValueSelectDesc, thisKeySelectDesc, LOG, id));
          indx++;
        }
      }
      dispatchHandlers.put(entry.getKey(), tmp);
    }

    child2OutputTag2DispatchHandlers = new HashMap<Integer, Map<Integer, DispatchHandler>>();
    for (Entry<Integer, Map<Integer, List<Integer>>> entry : dispatchConf.entrySet()) {
      for (Entry<Integer, List<Integer>> child2outputTag : entry.getValue().entrySet()) {
        if (!child2OutputTag2DispatchHandlers.containsKey(child2outputTag.getKey())) {
          child2OutputTag2DispatchHandlers.put(child2outputTag.getKey(),
              new HashMap<Integer, DispatchHandler>());
        }
        int indx = 0;
        for (Integer outputTag : child2outputTag.getValue()) {
          child2OutputTag2DispatchHandlers.get(child2outputTag.getKey()).
            put(outputTag,
              dispatchHandlers.get(entry.getKey()).get(child2outputTag.getKey()).get(indx));
          indx++;
        }
      }
    }

    childInputObjInspectors = new HashMap<Integer, ObjectInspector[]>();
    for (Entry<Integer, Map<Integer, DispatchHandler>> entry : child2OutputTag2DispatchHandlers
        .entrySet()) {
      Integer l = Collections.max(entry.getValue().keySet());
      ObjectInspector[] childObjInspectors = new ObjectInspector[l.intValue() + 1];
      for (Entry<Integer, DispatchHandler> e : entry.getValue().entrySet()) {
        if (e.getKey().intValue() == -1) {
          assert childObjInspectors.length == 1;
          childObjInspectors[0] = e.getValue().getOutputObjInspector();
        } else {
          childObjInspectors[e.getKey().intValue()] = e.getValue().getOutputObjInspector();
        }
      }
      childInputObjInspectors.put(entry.getKey(), childObjInspectors);
    }

    lastDispatchedRows = new Object[childOperatorsArray.length];
    lastDispatchedTags = new int[childOperatorsArray.length];
    for (int i = 0; i < childOperatorsArray.length; i++) {
      lastDispatchedRows[i] = null;
      lastDispatchedTags[i] = -1;
    }

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
      childOperatorsArray[i].initialize(hconf, childInputObjInspectors.get(i));
      if (reporter != null) {
        childOperatorsArray[i].setReporter(reporter);
      }
    }
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    List<Object> thisRow = (List<Object>) row;
    assert thisRow.size() == 4;
    operationPathTag = ((ByteWritable) thisRow.get(3)).get();
    inputTag = ((ByteWritable) thisRow.get(2)).get();
    forward(thisRow.subList(0, 3), inputObjInspectors[inputTag]);
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

    int childrenDone = 0;
    int forwardFlag = 1;
    assert childOperatorsArray.length <= 8;
    for (int i = 0; i < childOperatorsArray.length; i++) {
      Operator<? extends OperatorDesc> o = childOperatorsArray[i];
      if (o.getDone()) {
        childrenDone++;
      } else {
        int isProcess = (operationPathTag & (forwardFlag << i));
        if (isProcess != 0) {
          if (o.getName().equals(GroupByOperator.getOperatorName())) {
            GroupByOperator gbyop = (GroupByOperator) o;
            gbyop.setForcedForward(false);
            if (!this.bytesWritableGroupKey.equals(o.getBytesWritableGroupKey())) {
              o.setBytesWritableGroupKey(this.bytesWritableGroupKey);
            }
          }
          for (int j = 0; j < dispatchHandlers.get(inputTag).get(i).size(); j++) {
            Object dispatchedRow = dispatchHandlers.get(inputTag).get(i).get(j).process(row);
            int dispatchedTag = dispatchConf.get(inputTag).get(i).get(j);
            o.process(dispatchedRow, dispatchedTag);
            lastDispatchedRows[i] = dispatchedRow;
            lastDispatchedTags[i] = dispatchedTag;
          }
        }
        if (isProcess == 0 && o.getName().equals(GroupByOperator.getOperatorName())) {
          if (lastDispatchedRows[i] != null &&
              !this.bytesWritableGroupKey.equals(o.getBytesWritableGroupKey())) {
            GroupByOperator gbyop = (GroupByOperator) o;
            gbyop.setForcedForward(true);
            o.setBytesWritableGroupKey(this.bytesWritableGroupKey);
            o.process(lastDispatchedRows[i], lastDispatchedTags[i]);
          }
        }
      }
    }

    // if all children are done, this operator is also done
    if (childrenDone == childOperatorsArray.length) {
      setDone(true);
    }
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    // log the number of rows forwarded from each dispatcherHandler
    for (Map<Integer, List<DispatchHandler>> childIndx2DispatchHandlers : dispatchHandlers
        .values()) {
      for (List<DispatchHandler> dispatchHandlers : childIndx2DispatchHandlers.values()) {
        for (DispatchHandler dispatchHandler : dispatchHandlers) {
          dispatchHandler.printCloseOpLog();
        }
      }
    }
  }

  @Override
  public void setGroupKeyObject(Object keyObject) {
    this.groupKeyObject = keyObject;
    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.setGroupKeyObject(keyObject);
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
    return "CDP";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.CORRELATIONREDUCERDISPATCH;
  }
}
