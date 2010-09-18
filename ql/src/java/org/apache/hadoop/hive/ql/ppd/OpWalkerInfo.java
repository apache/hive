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
package org.apache.hadoop.hive.ql.ppd;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;

/**
 * Context class for operator walker of predicate pushdown.
 */
public class OpWalkerInfo implements NodeProcessorCtx {
  /**
   * Operator to Pushdown Predicates Map. This keeps track of the final pushdown
   * predicates for each operator as you walk the Op Graph from child to parent
   */
  private final HashMap<Operator<? extends Serializable>, ExprWalkerInfo> opToPushdownPredMap;
  private final Map<Operator<? extends Serializable>, OpParseContext> opToParseCtxMap;
  private final ParseContext pGraphContext;

  public OpWalkerInfo(ParseContext pGraphContext) {
    this.pGraphContext = pGraphContext;
    opToParseCtxMap = pGraphContext.getOpParseCtx();
    opToPushdownPredMap = new HashMap<Operator<? extends Serializable>, ExprWalkerInfo>();
  }

  public ExprWalkerInfo getPrunedPreds(Operator<? extends Serializable> op) {
    return opToPushdownPredMap.get(op);
  }

  public ExprWalkerInfo putPrunedPreds(Operator<? extends Serializable> op,
      ExprWalkerInfo value) {
    return opToPushdownPredMap.put(op, value);
  }

  public RowResolver getRowResolver(Node op) {
    return opToParseCtxMap.get(op).getRR();
  }

  public OpParseContext put(Operator<? extends Serializable> key,
      OpParseContext value) {
    return opToParseCtxMap.put(key, value);
  }

  public ParseContext getParseContext() {
    return pGraphContext;
  }
}
