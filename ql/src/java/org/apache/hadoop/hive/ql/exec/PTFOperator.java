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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.PTFDeserializer;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFInputDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLeadLag;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class PTFOperator extends Operator<PTFDesc> implements Serializable {

  private static final long serialVersionUID = 1L;
  boolean isMapOperator;

  transient KeyWrapperFactory keyWrapperFactory;
  protected transient KeyWrapper currentKeys;
  protected transient KeyWrapper newKeys;
  /*
   * for map-side invocation of PTFs, we cannot utilize the currentkeys null check
   * to decide on invoking startPartition in streaming mode. Hence this extra flag.
   */
  transient boolean firstMapRow;
  transient Configuration hiveConf;
  transient PTFInvocation ptfInvocation;

  /*
   * 1. Find out if the operator is invoked at Map-Side or Reduce-side
   * 2. Get the deserialized QueryDef
   * 3. Reconstruct the transient variables in QueryDef
   * 4. Create input partition to store rows coming from previous operator
   */
  @Override
  protected Collection<Future<?>> initializeOp(Configuration jobConf) throws HiveException {
    Collection<Future<?>> result = super.initializeOp(jobConf);
    hiveConf = jobConf;
    isMapOperator = conf.isMapSide();
    currentKeys = null;

    reconstructQueryDef(hiveConf);

    if (isMapOperator) {
      PartitionedTableFunctionDef tDef = conf.getStartOfChain();
      outputObjInspector = tDef.getRawInputShape().getOI();
    } else {
      outputObjInspector = conf.getFuncDef().getOutputShape().getOI();
    }

    setupKeysWrapper(inputObjInspectors[0]);

    ptfInvocation = setupChain();
    ptfInvocation.initializeStreaming(jobConf, isMapOperator);
    firstMapRow = true;
    return result;
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    super.closeOp(abort);
    ptfInvocation.finishPartition();
    ptfInvocation.close();
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    if (!isMapOperator ) {
      /*
       * checkif current row belongs to the current accumulated Partition:
       * - If not:
       *  - process the current Partition
       *  - reset input Partition
       * - set currentKey to the newKey if it is null or has changed.
       */
      newKeys.getNewKey(row, inputObjInspectors[0]);
      boolean keysAreEqual = (currentKeys != null && newKeys != null)?
              newKeys.equals(currentKeys) : false;

      if (currentKeys != null && !keysAreEqual) {
        ptfInvocation.finishPartition();
      }

      if (currentKeys == null || !keysAreEqual) {
        ptfInvocation.startPartition();
        if (currentKeys == null) {
          currentKeys = newKeys.copyKey();
        } else {
          currentKeys.copyKey(newKeys);
        }
      }
    } else if ( firstMapRow ) {
      ptfInvocation.startPartition();
      firstMapRow = false;
    }

    ptfInvocation.processRow(row);
  }

  /**
   * Initialize the visitor to use the QueryDefDeserializer Use the order
   * defined in QueryDefWalker to visit the QueryDef
   *
   * @param hiveConf
   * @throws HiveException
   */
  protected void reconstructQueryDef(Configuration hiveConf) throws HiveException {

    PTFDeserializer dS =
        new PTFDeserializer(conf, (StructObjectInspector)inputObjInspectors[0], hiveConf);
    dS.initializePTFChain(conf.getFuncDef());
  }

  protected void setupKeysWrapper(ObjectInspector inputOI) throws HiveException {
    PartitionDef pDef = conf.getStartOfChain().getPartition();
    List<PTFExpressionDef> exprs = pDef.getExpressions();
    int numExprs = exprs.size();
    ExprNodeEvaluator[] keyFields = new ExprNodeEvaluator[numExprs];
    ObjectInspector[] keyOIs = new ObjectInspector[numExprs];
    ObjectInspector[] currentKeyOIs = new ObjectInspector[numExprs];

    for(int i=0; i<numExprs; i++) {
      PTFExpressionDef exprDef = exprs.get(i);
      /*
       * Why cannot we just use the ExprNodeEvaluator on the column?
       * - because on the reduce-side it is initialized based on the rowOI of the HiveTable
       *   and not the OI of the parent of this Operator on the reduce-side
       */
      keyFields[i] = ExprNodeEvaluatorFactory.get(exprDef.getExprNode());
      keyOIs[i] = keyFields[i].initialize(inputOI);
      currentKeyOIs[i] =
          ObjectInspectorUtils.getStandardObjectInspector(keyOIs[i],
              ObjectInspectorCopyOption.WRITABLE);
    }

    keyWrapperFactory = new KeyWrapperFactory(keyFields, keyOIs, currentKeyOIs);
    newKeys = keyWrapperFactory.getKeyWrapper();
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "PTF";
  }


  @Override
  public OperatorType getType() {
    return OperatorType.PTF;
  }

  private PTFInvocation setupChain() {
    Stack<PartitionedTableFunctionDef> fnDefs = new Stack<PartitionedTableFunctionDef>();
    PTFInputDef iDef = conf.getFuncDef();

    while (iDef instanceof PartitionedTableFunctionDef) {
      fnDefs.push((PartitionedTableFunctionDef) iDef);
      iDef = ((PartitionedTableFunctionDef) iDef).getInput();
    }

    PTFInvocation curr = null, first = null;

    while(!fnDefs.isEmpty()) {
      PartitionedTableFunctionDef currFn = fnDefs.pop();
      curr = new PTFInvocation(curr, currFn.getTFunction());
      if ( first == null ) {
        first = curr;
      }
    }
    return first;
  }

  public static void connectLeadLagFunctionsToPartition(PTFDesc ptfDesc,
      PTFPartitionIterator<Object> pItr) throws HiveException {
    List<ExprNodeGenericFuncDesc> llFnDescs = ptfDesc.getLlInfo().getLeadLagExprs();
    if (llFnDescs == null) {
      return;
    }
    for (ExprNodeGenericFuncDesc llFnDesc : llFnDescs) {
      GenericUDFLeadLag llFn = (GenericUDFLeadLag) llFnDesc
          .getGenericUDF();
      llFn.setpItr(pItr);
    }
  }

  /*
   * Responsible for the flow of rows through the PTF Chain.
   * An Invocation wraps a TableFunction.
   * The PTFOp hands the chain each row through the processRow call.
   * It also notifies the chain of when a Partition starts/finishes.
   *
   * There are several combinations depending
   * whether the TableFunction and its successor support Streaming or Batch mode.
   *
   * Combination 1: Streaming + Streaming
   * - Start Partition: invoke startPartition on tabFn.
   * - Process Row: invoke process Row on tabFn.
   *   Any output rows hand to next tabFn in chain or forward to next Operator.
   * - Finish Partition: invoke finishPartition on tabFn.
   *   Any output rows hand to next tabFn in chain or forward to next Operator.
   *
   * Combination 2: Streaming + Batch
   * same as Combination 1
   *
   * Combination 3: Batch + Batch
   * - Start Partition: create or reset the Input Partition for the tabFn
   *   caveat is: if prev is also batch and it is not providing an Output Iterator
   *   then we can just use its Output Partition.
   * - Process Row: collect row in Input Partition
   * - Finish Partition : invoke evaluate on tabFn on Input Partition
   *   If function gives an Output Partition: set it on next Invocation's Input Partition
   *   If function gives an Output Iterator: iterate and call processRow on next Invocation.
   *   For last Invocation in chain: forward rows to next Operator.
   *
   * Combination 3: Batch + Stream
   * Similar to Combination 3, except Finish Partition behavior slightly different
   * - Finish Partition : invoke evaluate on tabFn on Input Partition
   *   iterate output rows: hand to next tabFn in chain or forward to next Operator.
   *
   */
  class PTFInvocation {

    PTFInvocation prev;
    PTFInvocation next;
    TableFunctionEvaluator tabFn;
    PTFPartition inputPart;
    PTFPartition outputPart;
    Iterator<Object> outputPartRowsItr;

    public PTFInvocation(PTFInvocation prev, TableFunctionEvaluator tabFn) {
      this.prev = prev;
      this.tabFn = tabFn;
      if ( prev != null ) {
        prev.next = this;
      }
    }

    boolean isOutputIterator() {
      return tabFn.canAcceptInputAsStream() || tabFn.canIterateOutput();
    }

    boolean isStreaming() {
      return tabFn.canAcceptInputAsStream();
    }

    void initializeStreaming(Configuration cfg, boolean isMapSide) throws HiveException {
      PartitionedTableFunctionDef tabDef = tabFn.getTableDef();
      PTFInputDef inputDef = tabDef.getInput();
      ObjectInspector inputOI = conf.getStartOfChain() == tabDef ?
          inputObjInspectors[0] : inputDef.getOutputShape().getOI();

      tabFn.initializeStreaming(cfg, (StructObjectInspector) inputOI, isMapSide);

      if ( next != null ) {
        next.initializeStreaming(cfg, isMapSide);
      }
    }

    void startPartition() throws HiveException {
      if ( isStreaming() ) {
        tabFn.startPartition();
      } else {
        if ( prev == null || prev.isOutputIterator() ) {
          if ( inputPart == null ) {
            createInputPartition();
          } else {
            inputPart.reset();
          }
        }
      }
      if ( next != null ) {
        next.startPartition();
      }
    }

    void processRow(Object row) throws HiveException {
      if ( isStreaming() ) {
        handleOutputRows(tabFn.processRow(row));
      } else {
        inputPart.append(row);
      }
    }

    void handleOutputRows(List<Object> outRows) throws HiveException {
      if ( outRows != null ) {
        for (Object orow : outRows ) {
          if ( next != null ) {
            next.processRow(orow);
          } else {
            forward(orow, outputObjInspector);
          }
        }
      }
    }

    void finishPartition() throws HiveException {
      if ( isStreaming() ) {
        handleOutputRows(tabFn.finishPartition());
      } else {
        if ( tabFn.canIterateOutput() ) {
          outputPartRowsItr = inputPart == null ? null :
            tabFn.iterator(inputPart.iterator());
        } else {
          outputPart = inputPart == null ? null : tabFn.execute(inputPart);
          outputPartRowsItr = outputPart == null ? null : outputPart.iterator();
        }
        if ( next != null ) {
          if (!next.isStreaming() && !isOutputIterator() ) {
            next.inputPart = outputPart;
          } else {
            if ( outputPartRowsItr != null ) {
              while(outputPartRowsItr.hasNext() ) {
                next.processRow(outputPartRowsItr.next());
              }
            }
          }
        }
      }

      if ( next != null ) {
        next.finishPartition();
      } else {
        if (!isStreaming() ) {
          if ( outputPartRowsItr != null ) {
            while(outputPartRowsItr.hasNext() ) {
              forward(outputPartRowsItr.next(), outputObjInspector);
            }
          }
        }
      }
    }

    /**
     * Create a new Partition.
     * A partition has 2 OIs: the OI for the rows being put in and the OI for the rows
     * coming out. You specify the output OI by giving the Serde to use to Serialize.
     * Typically these 2 OIs are the same; but not always. For the
     * first PTF in a chain the OI of the incoming rows is dictated by the Parent Op
     * to this PTFOp. The output OI from the Partition is typically LazyBinaryStruct, but
     * not always. In the case of Noop/NoopMap we keep the Strcuture the same as
     * what is given to us.
     * <p>
     * The Partition we want to create here is for feeding the First table function in the chain.
     * So for map-side processing use the Serde from the output Shape its InputDef.
     * For reduce-side processing use the Serde from its RawInputShape(the shape
     * after map-side processing).
     * @param oi
     * @param hiveConf
     * @param isMapSide
     * @return
     * @throws HiveException
     */
    private void createInputPartition() throws HiveException {
      PartitionedTableFunctionDef tabDef = tabFn.getTableDef();
      PTFInputDef inputDef = tabDef.getInput();
      ObjectInspector inputOI = conf.getStartOfChain() == tabDef ?
          inputObjInspectors[0] : inputDef.getOutputShape().getOI();

      SerDe serde = conf.isMapSide() ? tabDef.getInput().getOutputShape().getSerde() :
        tabDef.getRawInputShape().getSerde();
      StructObjectInspector outputOI = conf.isMapSide() ? tabDef.getInput().getOutputShape().getOI() :
        tabDef.getRawInputShape().getOI();
      inputPart = PTFPartition.create(conf.getCfg(),
          serde,
          (StructObjectInspector) inputOI,
          outputOI);
    }

    void close() {
      if ( inputPart != null ) {
        inputPart.close();
        inputPart = null;
      }
      tabFn.close();
      if ( next != null ) {
        next.close();
      }
    }
  }

}
