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

package org.apache.hadoop.hive.ql.impala.rex;

import org.apache.calcite.util.Pair;
import org.apache.impala.analysis.Expr;

/**
 * Interface used to traverse RexInputRefs.
 *
 * An InputRef RexNode always refers to another RexNode.  The question is
 * where to find it.  In the case of an ImpalaProjectRel, we need to refer
 * to the input RelNode.  In all other nodes, the InputRef refers to a RexNode
 * within the RelNode.  The existence of this interface allows this rex package
 * to be self-contained.
 */
public interface ReferrableNode {
  /**
   * Returns the Impala Expr with the given index number.
   * If this is a non-Project node, it will return a SlotRef.
   * If this is a Project node, it will return the Expr tree
   * associated with the projection.
   */
  public Expr getExpr(int index);

  /**
   * Return number of output expressions of this plan node
   */
  public int numOutputExprs();

  /**
   * Returns a pair where first item is the max index of the output
   * expression (e.g if there are 3 RexInputRefs: $2, $5, $7, this
   * value will be '7'.  The second item is the total number of
   * output exprs.  In the above example it will be 3
   */
  public Pair<Integer, Integer> getMaxIndexInfo();
 
}
