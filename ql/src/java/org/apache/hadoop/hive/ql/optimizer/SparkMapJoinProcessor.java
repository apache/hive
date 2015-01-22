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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

import com.google.common.base.Preconditions;

public class SparkMapJoinProcessor extends MapJoinProcessor {

  /**
   * convert a regular join to a a map-side join.
   *
   * @param conf
   * @param opParseCtxMap
   * @param op join operator
   * @param joinTree qb join tree
   * @param bigTablePos position of the source to be read as part of
   *                   map-reduce framework. All other sources are cached in memory
   * @param noCheckOuterJoin
   * @param validateMapJoinTree
   */
  @Override
  public MapJoinOperator convertMapJoin(HiveConf conf,
                                        LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtxMap,
                                        JoinOperator op, boolean leftSrc, String[] baseSrc, List<String> mapAliases,
                                        int bigTablePos, boolean noCheckOuterJoin,
                                        boolean validateMapJoinTree) throws SemanticException {

    // outer join cannot be performed on a table which is being cached
    JoinCondDesc[] condns = op.getConf().getConds();

    if (!noCheckOuterJoin) {
      if (checkMapJoin(bigTablePos, condns) < 0) {
        throw new SemanticException(ErrorMsg.NO_OUTER_MAPJOIN.getMsg());
      }
    }

    // create the map-join operator
    MapJoinOperator mapJoinOp = convertJoinOpMapJoinOp(conf, opParseCtxMap,
        op, op.getConf().isLeftInputJoin(), op.getConf().getBaseSrc(),
        op.getConf().getMapAliases(), bigTablePos, noCheckOuterJoin);

    // 1. remove RS as parent for the big table branch
    // 2. remove old join op from child set of all the RSs
    List<Operator<? extends OperatorDesc>> parentOps = mapJoinOp.getParentOperators();
    for (int i = 0; i < parentOps.size(); i++) {
      Operator<? extends OperatorDesc> parentOp = parentOps.get(i);
      parentOp.getChildOperators().remove(op);
      if (i == bigTablePos) {
        List<Operator<? extends OperatorDesc>> grandParentOps = parentOp.getParentOperators();
        Preconditions.checkArgument(grandParentOps.size() == 1,
            "AssertionError: expect number of parents to be 1, but was " + grandParentOps.size());
        Operator<? extends OperatorDesc> grandParentOp = grandParentOps.get(0);
        grandParentOp.replaceChild(parentOp, mapJoinOp);
        mapJoinOp.replaceParent(parentOp, grandParentOp);
      }
    }

    return mapJoinOp;
  }
}
