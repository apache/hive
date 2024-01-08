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

package org.apache.hadoop.hive.ql.optimizer.correlation;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_OPT_REDUCE_DEDUPLICATION_MIN_REDUCER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SCRIPT_OPERATOR_TRUST;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_MAPSIDE_AGGREGATE;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;

abstract class AbstractCorrelationProcCtx implements NodeProcessorCtx {
  private ParseContext pctx;
  // For queries using script, the optimization cannot be applied without user's confirmation
  // If script preserves alias and value for columns related to keys, user can set this true
  private final boolean trustScript;

  // This is min number of reducer for deduped RS to avoid query executed on
  // too small number of reducers. For example, queries GroupBy+OrderBy can be executed by
  // only one reducer if this configuration does not prevent
  private final int minReducer;
  private final Set<Operator<?>> removedOps;
  private final boolean isMapAggr;

  public AbstractCorrelationProcCtx(ParseContext pctx) {
    removedOps = new HashSet<Operator<?>>();
    trustScript = pctx.getConf().getBoolVar(HIVE_SCRIPT_OPERATOR_TRUST);
    minReducer = pctx.getConf().getIntVar(HIVE_OPT_REDUCE_DEDUPLICATION_MIN_REDUCER);
    isMapAggr = pctx.getConf().getBoolVar(HIVE_MAPSIDE_AGGREGATE);
    this.pctx = pctx;
  }

  public ParseContext getPctx() {
    return pctx;
  }

  public void setPctx(ParseContext pctx) {
    this.pctx = pctx;
  }

  public boolean trustScript() {
    return trustScript;
  }

  public int minReducer() {
    return minReducer;
  }

  public boolean hasBeenRemoved(Operator<?> rsOp) {
    return removedOps.contains(rsOp);
  }

  public boolean addRemovedOperator(Operator<?> rsOp) {
    return removedOps.add(rsOp);
  }

  public boolean isMapAggr() {
    return isMapAggr;
  }
}
