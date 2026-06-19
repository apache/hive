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

package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.Map;

import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

/**
 * Context class for operator tree walker for partition pruner.
 * TODO: this class may be not useful.
 */
public class OpWalkerCtx implements NodeProcessorCtx {

  /**
   * Map from tablescan operator to partition pruning predicate that is
   * initialized from the ParseContext.
   */
  private final Map<TableScanOperator, ExprNodeDesc> opToPartPruner;

  /**
   * Constructor.
   */
  public OpWalkerCtx(Map<TableScanOperator, ExprNodeDesc> opToPartPruner) {
    this.opToPartPruner = opToPartPruner;
  }

  public Map<TableScanOperator, ExprNodeDesc> getOpToPartPruner() {
    return opToPartPruner;
  }
}
