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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.Set;

import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.parse.ParseContext;

/*
 * This is a pluggable policy to chose the candidate map-join table for converting a join to a
 * sort merge join. The leftmost table is chosen as the join table.
 */
public class LeftmostBigTableSelectorForAutoSMJ implements BigTableSelectorForAutoSMJ {
  public int getBigTablePosition(ParseContext parseContext, JoinOperator joinOp,
      Set<Integer> bigTableCandidates) {
    return 0;
  }
}
