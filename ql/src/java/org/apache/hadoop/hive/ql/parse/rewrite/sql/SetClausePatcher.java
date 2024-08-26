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
package org.apache.hadoop.hive.ql.parse.rewrite.sql;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import java.util.Map;

public class SetClausePatcher {
  /**
   * Patch up the projection list for updates, putting back the original set expressions.
   * Walk through the projection list and replace the column names with the
   * expressions from the original update.  Under the TOK_SELECT (see above) the structure
   * looks like:
   * <pre>
   * TOK_SELECT -&gt; TOK_SELEXPR -&gt; expr
   *           \-&gt; TOK_SELEXPR -&gt; expr ...
   * </pre>
   */
  public void patchProjectionForUpdate(ASTNode insertBranch, Map<Integer, ASTNode> setColExprs) {
    ASTNode rewrittenSelect = (ASTNode) insertBranch.getChildren().get(1);
    assert rewrittenSelect.getToken().getType() == HiveParser.TOK_SELECT :
        "Expected TOK_SELECT as second child of TOK_INSERT but found " + rewrittenSelect.getName();
    for (Map.Entry<Integer, ASTNode> entry : setColExprs.entrySet()) {
      ASTNode selExpr = (ASTNode) rewrittenSelect.getChildren().get(entry.getKey());
      assert selExpr.getToken().getType() == HiveParser.TOK_SELEXPR :
          "Expected child of TOK_SELECT to be TOK_SELEXPR but was " + selExpr.getName();
      // Now, change it's child
      selExpr.setChild(0, entry.getValue());
    }
  }
}
