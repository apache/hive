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
package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Map;

public interface MergeRewriter {
  void handleSource(boolean hasWhenNotMatchedClause, String sourceAlias, String onClauseAsText);

  void handleWhenNotMatchedInsert(String targetFullName, String columnListText, String hintStr,
                                  String valuesClause, String predicate, String extraPredicate);

  void handleWhenMatchedUpdate(Table targetTable, String targetAlias, Map<String, String> newValues, String hintStr,
                               String onClauseAsString, String extraPredicate, String deleteExtraPredicate);


  void handleWhenMatchedDelete(String hintStr, String onClauseAsString, String extraPredicate,
                               String updateExtraPredicate);

  void handleCardinalityViolation(String targetAlias, String onClauseAsString, Hive db, HiveConf conf)
      throws SemanticException;

  /**
   * This sets the destination name prefix for update clause.
   * @param insClauseIdx index of insert clause in the rewritten multi-insert represents the merge update clause.
   * @param rewrittenCtx the {@link Context} stores the prefixes
   * @return the number of prefixes set.
   */
  int addDestNamePrefixOfUpdate(int insClauseIdx, Context rewrittenCtx);
}
