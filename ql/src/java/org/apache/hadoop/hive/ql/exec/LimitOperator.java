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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;

/**
 * Limit operator implementation Limits the number of rows to be passed on.
 **/
public class LimitOperator extends Operator<LimitDesc> implements Serializable {
  private static final long serialVersionUID = 1L;

  transient protected int limit;
  transient protected int currCount;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    limit = conf.getLimit();
    currCount = 0;
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    if (currCount < limit) {
      forward(row, inputObjInspectors[tag]);
      currCount++;
    } else {
      setDone(true);
    }
  }

  @Override
  public String getName() {
    return "LIM";
  }

  @Override
  public int getType() {
    return OperatorType.LIMIT;
  }

}
