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
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class WhereClausePatcher {
  public void patch(ASTNode rewrittenInsert, ASTNode whereTree) throws SemanticException {
    if (rewrittenInsert.getToken().getType() != HiveParser.TOK_INSERT) {
      throw new SemanticException(
          "Expected TOK_INSERT as second child of TOK_QUERY but found " + rewrittenInsert.getName());
    }
    // The structure of the AST for the rewritten insert statement is:
    // TOK_QUERY -> TOK_FROM
    //          \-> TOK_INSERT -> TOK_INSERT_INTO
    //                        \-> TOK_SELECT
    //                        \-> TOK_SORTBY
    // Or
    // TOK_QUERY -> TOK_FROM
    //          \-> TOK_INSERT -> TOK_INSERT_INTO
    //                        \-> TOK_SELECT
    //
    // The following adds the TOK_WHERE and its subtree from the original query as a child of
    // TOK_INSERT, which is where it would have landed if it had been there originally in the
    // string.  We do it this way because it's easy then turning the original AST back into a
    // string and reparsing it.
    if (rewrittenInsert.getChildren().size() == 3) {
      // We have to move the SORT_BY over one, so grab it and then push it to the second slot,
      // and put the where in the first slot
      ASTNode sortBy = (ASTNode) rewrittenInsert.getChildren().get(2);
      assert sortBy.getToken().getType() == HiveParser.TOK_SORTBY :
          "Expected TOK_SORTBY to be third child of TOK_INSERT, but found " + sortBy.getName();
      rewrittenInsert.addChild(sortBy);
      rewrittenInsert.setChild(2, whereTree);
    } else {
      ASTNode select = (ASTNode) rewrittenInsert.getChildren().get(1);
      assert select.getToken().getType() == HiveParser.TOK_SELECT :
          "Expected TOK_SELECT to be second child of TOK_INSERT, but found " + select.getName();
      rewrittenInsert.addChild(whereTree);
    }
  }
}
