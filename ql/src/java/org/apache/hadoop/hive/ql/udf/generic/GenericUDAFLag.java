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
                name = "lag",
                value = "_FUNC_(expr, amt, default)"
                ),
    supportsWindow = false,
    pivotResult = true,
    impliesOrder = true
)
public class GenericUDAFLag extends GenericUDAFLeadLag {

  static final Log LOG = LogFactory.getLog(GenericUDAFLag.class.getName());


  @Override
  protected String functionName() {
    return "Lag";
  }

  @Override
  protected GenericUDAFLeadLagEvaluator createLLEvaluator() {
    return new GenericUDAFLagEvaluator();
  }

  public static class GenericUDAFLagEvaluator extends GenericUDAFLeadLagEvaluator {

    @Override
    protected LeadLagBuffer getNewLLBuffer() throws HiveException {
     return new LagBuffer();
    }
  }

  static class LagBuffer implements LeadLagBuffer {
    ArrayList<Object> values;
    int lagAmt;
    ArrayList<Object> lagValues;
    int lastRowIdx;

    public void initialize(int lagAmt) {
      this.lagAmt = lagAmt;
      lagValues = new ArrayList<Object>(lagAmt);
      values = new ArrayList<Object>();
      lastRowIdx = -1;
    }

    public void addRow(Object currValue, Object defaultValue) {
      int row = lastRowIdx + 1;
      if ( row < lagAmt) {
        lagValues.add(defaultValue);
      }
      values.add(currValue);
      lastRowIdx++;
    }

    public Object terminate() {

      /*
       * if partition is smaller than the lagAmt;
       * the entire partition is in lagValues.
       */
      if ( values.size() < lagAmt ) {
        return lagValues;
      }

      int lastIdx = values.size() - 1;
      for(int i = 0; i < lagAmt; i++) {
        values.remove(lastIdx - i);
      }
      values.addAll(0, lagValues);
      return values;
    }
  }
}
