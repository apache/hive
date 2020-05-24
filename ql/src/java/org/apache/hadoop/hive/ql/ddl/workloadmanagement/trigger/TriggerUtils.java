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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.trigger;

import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.ql.ddl.workloadmanagement.WMUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.wm.ExecutionTrigger;

/**
 * Common utilities for Trigger related ddl operations.
 */
public final class TriggerUtils {
  private TriggerUtils() {
    throw new UnsupportedOperationException("TriggerUtils should not be instantiated");
  }

  public static String buildTriggerExpression(ASTNode node) throws SemanticException {
    if (node.getType() != HiveParser.TOK_TRIGGER_EXPRESSION || node.getChildCount() == 0) {
      throw new SemanticException("Invalid trigger expression.");
    }

    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < node.getChildCount(); ++i) {
      builder.append(node.getChild(i).getText()); // Don't strip quotes.
      builder.append(' ');
    }
    builder.deleteCharAt(builder.length() - 1);
    return builder.toString();
  }

  public static String buildTriggerActionExpression(ASTNode node) throws SemanticException {
    switch (node.getType()) {
    case HiveParser.KW_KILL:
      return "KILL";
    case HiveParser.KW_MOVE:
      if (node.getChildCount() != 1) {
        throw new SemanticException("Invalid move to clause in trigger action.");
      }
      String poolPath = WMUtils.poolPath(node.getChild(0));
      return "MOVE TO " + poolPath;
    default:
      throw new SemanticException("Unknown token in action clause: " + node.getType());
    }
  }

  public static void validateTrigger(WMTrigger trigger) throws HiveException {
    try {
      ExecutionTrigger.fromWMTrigger(trigger);
    } catch (IllegalArgumentException e) {
      throw new HiveException(e);
    }
  }
}
