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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.LateralViewForwardDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;

/**
 * LateralViewForwardOperator. This operator sits at the head of the operator
 * DAG for a lateral view. This does nothing, but it aids the predicate push
 * down during traversal to identify when a lateral view occurs.
 *
 */
public class LateralViewForwardOperator extends Operator<LateralViewForwardDesc> {

  private static final long serialVersionUID = 1L;

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    forward(row, inputObjInspectors[tag]);
  }

  @Override
  public String getName() {
    return "LVF";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.LATERALVIEWFORWARD;
  }
}
