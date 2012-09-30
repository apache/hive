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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CorrelationCompositeDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.LongWritable;

/**
 * Correlation composite operator implementation. This operator is used only in map phase for
 * sharing table scan. Suppose that there are multiple operation paths (e.g. two different
 * predicates on a table ) that share a common table. A row will be processed by these operation
 * paths. To tag which operation paths actually forward this row, CorrelationCompositeOperator is
 * used. For a row, this operator will buffer forwarded rows from its parents and then tag this row
 * with a operation path tag indicating which paths forwarded this row. Right now, since operation
 * path tag used in ReduceSinkOperator has 1 byte, this operator can have at most 8 parents
 * (operation paths). For example, suppose that the common table is T and predicates P1 and P2 will
 * be used in sub-queries SQ1 and SQ2, respectively. The CorrelationCompositeOperator
 * will apply P1 and P2 on the row and tag the record based on if P1 or P2 is true.
 **/
public class CorrelationCompositeOperator extends Operator<CorrelationCompositeDesc> implements
  Serializable {

  public static enum Counter {
    FORWARDED
  }

  private static final long serialVersionUID = 1L;

  private ReduceSinkOperator correspondingReduceSinkOperators;

  private transient final LongWritable forwarded_count;

  private transient boolean isFirstRow;

  private int[] allOperationPathTags;

  private Object[] rowBuffer; // buffer the output from multiple parents

  public CorrelationCompositeOperator() {
    super();
    forwarded_count = new LongWritable();
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    isFirstRow = true;
    rowBuffer = new Object[parentOperators.size()];
    correspondingReduceSinkOperators = conf.getCorrespondingReduceSinkOperator();
    allOperationPathTags = conf.getAllOperationPathTags();
    statsMap.put(Counter.FORWARDED, forwarded_count);
    outputObjInspector =
        ObjectInspectorUtils.getStandardObjectInspector(outputObjInspector,
            ObjectInspectorCopyOption.JAVA);

    // initialize its children
    initializeChildren(hconf);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    rowBuffer[tag] =
        ObjectInspectorUtils.copyToStandardObject(row, inputObjInspectors[tag],
            ObjectInspectorCopyOption.JAVA);
  }

  private void evaluateBuffer() throws HiveException {
    List<Integer> operationPathTags = new ArrayList<Integer>();
    boolean isForward = false;
    Object forwardedRow = null;
    for (int i = 0; i < rowBuffer.length; i++) {
      if (rowBuffer[i] != null) {
        isForward = true;
        operationPathTags.add(allOperationPathTags[i]);
        if (forwardedRow == null) {
          forwardedRow = rowBuffer[i];
        }
      }
    }
    if (isForward) {
      assert correspondingReduceSinkOperators != null;
      correspondingReduceSinkOperators.setOperationPathTags(operationPathTags);
      forwarded_count.set(forwarded_count.get() + 1);
      forward(forwardedRow, null);
    }
    for (int i = 0; i < rowBuffer.length; i++) {
      rowBuffer[i] = null;
    }
  }

  @Override
  public void setRowNumber(long rowNumber) throws HiveException {
    this.rowNumber = rowNumber;
    if (childOperators == null) {
      return;
    }
    for (int i = 0; i < childOperatorsArray.length; i++) {
      assert rowNumber >= childOperatorsArray[i].getRowNumber();
      if (rowNumber != childOperatorsArray[i].getRowNumber()) {
        childOperatorsArray[i].setRowNumber(rowNumber);
      }
    }
    if (isFirstRow) {
      for (int i = 0; i < rowBuffer.length; i++) {
        rowBuffer[i] = null;
      }
      isFirstRow = false;
    } else {
      evaluateBuffer();
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      evaluateBuffer();
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
    return "CCO";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.CORRELATIONCOMPOSITE;
  }

}
