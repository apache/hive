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

package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Limit operator implementation Limits the number of rows to be passed on.
 **/
public class VectorLimitOperator extends LimitOperator  {

  private static final long serialVersionUID = 1L;

  public VectorLimitOperator() {
    super();
  }

  public VectorLimitOperator(VectorizationContext vContext, OperatorDesc conf) {
    this.conf = (LimitDesc) conf;
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    VectorizedRowBatch batch = (VectorizedRowBatch) row;

    if (currCount < limit) {
      batch.size = Math.min(batch.size, limit - currCount);
      forward(row, inputObjInspectors[tag]);
      currCount += batch.size;
    } else {
      setDone(true);
    }
  }
}
