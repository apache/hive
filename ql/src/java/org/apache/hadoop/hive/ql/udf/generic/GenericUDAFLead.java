/*
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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;

@WindowFunctionDescription
(
    description = @Description(
                name = "lead",
                value = "_FUNC_(expr, amt, default)"
                ),
    supportsWindow = false,
    pivotResult = true,
    impliesOrder = true
)
public class GenericUDAFLead extends GenericUDAFLeadLag {

  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFLead.class.getName());


  @Override
  protected String functionName() {
    return "Lead";
  }

  @Override
  protected GenericUDAFLeadLagEvaluator createLLEvaluator() {
   return new GenericUDAFLeadEvaluator();
  }

  public static class GenericUDAFLeadEvaluator extends GenericUDAFLeadLagEvaluator {

    public GenericUDAFLeadEvaluator() {
    }

    /*
     * used to initialize Streaming Evaluator.
     */
    protected GenericUDAFLeadEvaluator(GenericUDAFLeadLagEvaluator src) {
      super(src);
    }

    @Override
    protected LeadLagBuffer getNewLLBuffer() throws HiveException {
     return new LeadBuffer();
    }
    
    @Override
    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrmDef) {

      return new GenericUDAFLeadEvaluatorStreaming(this);
    }

  }

  static class LeadBuffer implements LeadLagBuffer {
    ArrayList<Object> values;
    int leadAmt;
    Object[] leadWindow;
    int nextPosInWindow;
    int lastRowIdx;

    public void initialize(int leadAmt) {
      this.leadAmt = leadAmt;
      values = new ArrayList<Object>();
      leadWindow = new Object[leadAmt];
      nextPosInWindow = 0;
      lastRowIdx = -1;
    }

    public void addRow(Object leadExprValue, Object defaultValue) {
      int row = lastRowIdx + 1;
      int leadRow = row - leadAmt;
      if ( leadRow >= 0 ) {
        values.add(leadExprValue);
      }
      leadWindow[nextPosInWindow] = defaultValue;
      nextPosInWindow = (nextPosInWindow + 1) % leadAmt;
      lastRowIdx++;
    }

    public Object terminate() {
      /*
       * if there are fewer than leadAmt values in leadWindow; start reading from the first position.
       * Otherwise the window starts from nextPosInWindow.
       */
      if ( lastRowIdx < leadAmt ) {
        nextPosInWindow = 0;
      }
      for(int i=0; i < leadAmt; i++) {
        values.add(leadWindow[nextPosInWindow]);
        nextPosInWindow = (nextPosInWindow + 1) % leadAmt;
      }
      return values;
    }

  }

  /*
   * StreamingEval: wrap regular eval. on getNext remove first row from values
   * and return it.
   */
  static class GenericUDAFLeadEvaluatorStreaming extends
      GenericUDAFLeadEvaluator implements ISupportStreamingModeForWindowing {

    protected GenericUDAFLeadEvaluatorStreaming(GenericUDAFLeadLagEvaluator src) {
      super(src);
    }

    @Override
    public Object getNextResult(AggregationBuffer agg) throws HiveException {
      LeadBuffer lb = (LeadBuffer) agg;
      if (!lb.values.isEmpty()) {
        Object res = lb.values.remove(0);
        if (res == null) {
          return ISupportStreamingModeForWindowing.NULL_RESULT;
        }
        return res;
      }
      return null;
    }

    @Override
    public int getRowsRemainingAfterTerminate() throws HiveException {
      return getAmt();
    }
  }

}
