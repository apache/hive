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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Implementation of rule-based join table reordering optimization. User passes
 * hints to specify which tables are to be streamed and they are moved to have
 * largest tag so that they are processed last.
 * In future, once statistics are implemented, this transformation can also be
 * done based on costs.
 */
public class JoinReorder implements Transform {
  /**
   * Estimate the size of the output based on the STREAMTABLE hints. To do so
   * the whole tree is traversed. Possible sizes:
   *   0: the operator and its subtree don't contain any big tables
   *   1: the subtree of the operator contains a big table
   *   2: the operator is a big table
   *
   * @param operator  The operator which output size is to be estimated
   * @param bigTables Set of tables that should be streamed
   * @return The estimated size - 0 (no streamed tables), 1 (streamed tables in
   * subtree) or 2 (a streamed table)
   */
  private int getOutputSize(Operator<? extends Serializable> operator,
                            Set<String> bigTables) {
    // If a join operator contains a big subtree, there is a chance that its
    // output is also big, so the output size is 1 (medium)
    if (operator instanceof JoinOperator) {
      for(Operator<? extends Serializable> o: operator.getParentOperators()) {
        if (getOutputSize(o, bigTables) != 0) {
          return 1;
        }
      }
    }

    // If a table is in bigTables then its output is big (2)
    if (operator instanceof TableScanOperator) {
      String alias = ((TableScanOperator)operator).getConf().getAlias();
      if (bigTables.contains(alias)) {
        return 2;
      }
    }

    // For all other kinds of operators, assume the output is as big as the
    // the biggest output from a parent
    int maxSize = 0;
    if (operator.getParentOperators() != null) {
      for(Operator<? extends Serializable> o: operator.getParentOperators()) {
        int current = getOutputSize(o, bigTables);
        if (current > maxSize) {
          maxSize = current;
        }
      }
    }

    return maxSize;
  }

  /**
   * Find all big tables from STREAMTABLE hints
   *
   * @param joinCtx The join context
   * @return Set of all big tables
   */
  private Set<String> getBigTables(ParseContext joinCtx) {
    Set<String> bigTables = new HashSet<String>();

    for (QBJoinTree qbJoin: joinCtx.getJoinContext().values()) {
      if (qbJoin.getStreamAliases() != null) {
        bigTables.addAll(qbJoin.getStreamAliases());
      }
    }

    return bigTables;
  }

  /**
   * Reorder the tables in a join operator appropriately (by reordering the tags
   * of the reduces sinks)
   *
   * @param joinOp The join operator to be processed
   * @param bigTables Set of all big tables
   */
  private void reorder(JoinOperator joinOp, Set<String> bigTables) {
    int count = joinOp.getParentOperators().size();

    // Find the biggest reduce sink
    int biggestPos  = count - 1;
    int biggestSize = getOutputSize(joinOp.getParentOperators().get(biggestPos),
                                    bigTables);
    for (int i = 0; i < count - 1; i++) {
      int currSize = getOutputSize(joinOp.getParentOperators().get(i),
                                   bigTables);
      if (currSize > biggestSize) {
        biggestSize = currSize;
        biggestPos = i;
      }
    }

    // Reorder tags if need be
    if (biggestPos != (count - 1)) {
      Byte[] tagOrder = joinOp.getConf().getTagOrder();
      Byte temp = tagOrder[biggestPos];
      tagOrder[biggestPos] = tagOrder[count-1];
      tagOrder[count-1] = temp;

      // Update tags of reduce sinks
      ((ReduceSinkOperator)joinOp.getParentOperators().get(biggestPos))
        .getConf().setTag(count-1);
      ((ReduceSinkOperator)joinOp.getParentOperators().get(count-1)).getConf()
        .setTag(biggestPos);
    }
  }

  /**
   * Transform the query tree. For each join, check which reduce sink will
   * output the biggest result (based on STREAMTABLE hints) and give it the
   * biggest tag so that it gets streamed.
   *
   * @param pactx current parse context
   */
  public ParseContext transform(ParseContext pactx) throws SemanticException {
    Set<String> bigTables = getBigTables(pactx);

    for (JoinOperator joinOp: pactx.getJoinContext().keySet()) {
      reorder(joinOp, bigTables);
    }

    return pactx;
  }
}
