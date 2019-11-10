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
package org.apache.hadoop.hive.ql.hooks;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * This hook helps to do some cleanup after tests are creating scheduled queries.
 */
public class ScheduledQueryCreationRegistryHook extends AbstractSemanticAnalyzerHook {

  private static Set<String> knownSchedules = new HashSet<String>();

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast) throws SemanticException {
    int type = ast.getType();
    switch (type) {
    case HiveParser.TOK_CREATE_SCHEDULED_QUERY:
      registerCreated(ast.getChild(0).getText());
    }

    return super.preAnalyze(context, ast);
  }

  private void registerCreated(String scheduleName) {
    knownSchedules.add(scheduleName);
  }

  public static Set<String> getSchedules() {
    return knownSchedules;
  }

}
