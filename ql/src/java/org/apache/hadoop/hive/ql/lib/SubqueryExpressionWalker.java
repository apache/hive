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

package org.apache.hadoop.hive.ql.lib;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

public class SubqueryExpressionWalker extends ExpressionWalker {

  /**
   * Constructor.
   *
   * @param disp
   * dispatcher to call for each op encountered
   */
  public SubqueryExpressionWalker(SemanticDispatcher disp) {
    super(disp);
  }


  /**
   * We should bypass subquery since we have already processed and created logical plan
   * (in genLogicalPlan) for subquery at this point.
   * SubQueryExprProcessor will use generated plan and creates appropriate ExprNodeSubQueryDesc.
   */
  protected boolean shouldByPass(Node childNode, Node parentNode) {
    if(parentNode instanceof ASTNode
            && ((ASTNode)parentNode).getType() == HiveParser.TOK_SUBQUERY_EXPR) {
      ASTNode parentOp = (ASTNode)parentNode;
      //subquery either in WHERE <LHS> IN <SUBQUERY> form OR WHERE EXISTS <SUBQUERY> form
      //in first case LHS should not be bypassed
      assert(parentOp.getChildCount() == 2 || parentOp.getChildCount()==3);
      if(parentOp.getChildCount() == 3 && (ASTNode)childNode == parentOp.getChild(2)) {
        return false;
      }
      return true;
    }
    return false;
  }

}

