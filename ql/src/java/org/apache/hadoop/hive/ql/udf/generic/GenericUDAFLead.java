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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;

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

  static final Log LOG = LogFactory.getLog(GenericUDAFLead.class.getName());


  @Override
  protected String functionName() {
    return "Lead";
  }

  @Override
  protected GenericUDAFLeadLagEvaluator createLLEvaluator() {
   return new GenericUDAFLeadEvaluator();
  }

  public static class GenericUDAFLeadEvaluator extends GenericUDAFLeadLagEvaluator {

    @Override
    protected LeadLagBuffer getNewLLBuffer() throws HiveException {
     return new LeadBuffer();
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

}
