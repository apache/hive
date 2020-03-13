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

import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * <p> The goal of this rule is to find suitable table scan operators that could reduce the
 * number of rows decoded at runtime using extra available information.
 *
 * <p> First the rule checks all the available MapJoin operators that could propagate the
 * smaller HashTable on the probing side (where TS is) to filter-out rows that would
 * never match. To do so the HashTable information is pushed down to the TS properties.
 * If the a single TS is used by multiple operators (shared-word), this rule
 * can not be applied.
 *
 * <p> Second this rule can be extended to support static filter expressions like:
 *    select * from sales where sold_state = 'PR';
 *
 * <p>The optimization only works with the Tez execution engine running on Llap.
 *
 */
public class ProbeDecodeOptimizer extends Transform {

  private final static Logger LOG = LoggerFactory.getLogger(ProbeDecodeOptimizer.class);

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    // Get all TS operators
    final List<Operator<?>> topOps = new ArrayList<>();
    topOps.addAll(pctx.getTopOps().values());

    if (topOps.isEmpty()) {
      // Nothing to do here - is this even possible?
      return pctx;
    }

    Map<TableScanOperator, MapJoinOperator> tableJoinMap = new HashMap<>();
    for (Operator<?> parentOp: topOps) {
      // Make sure every parent Operator has a single branch and there is no shared-work
      if (parentOp.getNumChild() > 1) {
        continue;
      }

      // Traverse graph and find MapJoins
      Deque<Operator<?>> deque = new LinkedList<>();
      deque.add(parentOp);
      while (!deque.isEmpty()) {
        Operator<?> op = deque.pollLast();
        if (op instanceof MapJoinOperator) {
          // MapJoin candidate
          MapJoinOperator mop = (MapJoinOperator) op;
          // Make sure this is a valid single (Number) key MapJoin and not Dynamic or BucketMapJoin
          if (!mop.getConf().isBucketMapJoin() && !mop.getConf().isDynamicPartitionHashJoin() && isValidMapJoin(mop)) {
            LOG.debug("ProbeDecode Mapping TS {} -> MJ {}", parentOp.getOperatorId(), mop.getOperatorId());
            tableJoinMap.put((TableScanOperator) parentOp, mop);
          }
        }
        deque.addAll(op.getChildOperators());
      }
    }

    // TODO: what if we have multiple MapJoins per TS?

    // TODO: some operators like VectorPTFEvaluatorStreamingDecimalMax do not allow selected -- take this into account here?

    // Propagate MapJoin information to the mapped TS operator (to be used by MapWork)
    for (Map.Entry<TableScanOperator, MapJoinOperator> entry: tableJoinMap.entrySet()) {

      String mjCacheKey = entry.getValue().getConf().getCacheKey();

      if (mjCacheKey == null) {
        // Generate cache key if it has not been yet generated
        mjCacheKey = MapJoinDesc.generateCacheKey(entry.getValue().getOperatorId());
        // Set in the conf of the map join operator
        entry.getValue().getConf().setCacheKey(mjCacheKey);
      }
      // At this point we know is a single Key MapJoin so propagate key column info
      byte posBigTable = (byte) entry.getValue().getConf().getPosBigTable();
      Byte[] order = entry.getValue().getConf().getTagOrder();
      Byte mjSmallTablePos = (order[0] == posBigTable ? order[1] : order[0]);

      List<ExprNodeDesc> keyDesc = entry.getValue().getConf().getKeys().get(posBigTable);
      ExprNodeColumnDesc keyCol = (ExprNodeColumnDesc) keyDesc.get(0);

      entry.getKey().setProbeDecodeContext(new TableScanOperator.ProbeDecodeContext(mjCacheKey, mjSmallTablePos, keyCol.getColumn()));

      LOG.debug("ProbeDecode MapJoin {} -> TS {}  with CacheKey {} MapJoin Pos {} ColName {}",
          entry.getValue().getName(), entry.getKey().getName(), mjCacheKey, mjSmallTablePos, keyCol.getColumn());
    }
    return pctx;
  }


  // Is a MapJoin with a single Key of Number type (Long/Int/Short)
  private boolean isValidMapJoin(MapJoinOperator mapJoinOp) {
    Map<Byte, List<ExprNodeDesc>> keyExprs = mapJoinOp.getConf().getKeys();
    List<ExprNodeDesc> bigTableKeyExprs = keyExprs.get( (byte) mapJoinOp.getConf().getPosBigTable());
    return (bigTableKeyExprs.size() == 1)
        && !(((PrimitiveTypeInfo) bigTableKeyExprs.get(0).getTypeInfo()).getPrimitiveCategory().
        equals(PrimitiveCategory.STRING) ||
            ((PrimitiveTypeInfo) bigTableKeyExprs.get(0).getTypeInfo()).getPrimitiveCategory().
        equals(PrimitiveCategory.BYTE));
  }
}
