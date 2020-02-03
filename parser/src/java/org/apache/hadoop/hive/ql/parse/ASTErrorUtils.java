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
package org.apache.hadoop.hive.ql.parse;

import org.antlr.runtime.tree.Tree;

public class ASTErrorUtils {

  private static int getLine(ASTNode tree) {
    if (tree.getChildCount() == 0) {
      return tree.getToken().getLine();
    }

    return getLine((ASTNode) tree.getChild(0));
  }

  private static int getCharPositionInLine(ASTNode tree) {
    if (tree.getChildCount() == 0) {
      return tree.getToken().getCharPositionInLine();
    }

    return getCharPositionInLine((ASTNode) tree.getChild(0));
  }

  // Dirty hack as this will throw away spaces and other things - find a better
  // way!
  public static String getText(ASTNode tree) {
    if (tree.getChildCount() == 0) {
      return tree.getText();
    }
    return getText((ASTNode) tree.getChild(tree.getChildCount() - 1));
  }

  public static String getMsg(String mesg, ASTNode tree) {
    StringBuilder sb = new StringBuilder();
    renderPosition(sb, tree);
    sb.append(" ");
    sb.append(mesg);
    sb.append(" '");
    sb.append(getText(tree));
    sb.append("'");
    renderOrigin(sb, tree.getOrigin());
    return sb.toString();
  }

  static final String LINE_SEP = System.getProperty("line.separator");

  public static void renderOrigin(StringBuilder sb, ASTNodeOrigin origin) {
    while (origin != null) {
      sb.append(" in definition of ");
      sb.append(origin.getObjectType());
      sb.append(" ");
      sb.append(origin.getObjectName());
      sb.append(" [");
      sb.append(LINE_SEP);
      sb.append(origin.getObjectDefinition());
      sb.append(LINE_SEP);
      sb.append("] used as ");
      sb.append(origin.getUsageAlias());
      sb.append(" at ");
      ASTNode usageNode = origin.getUsageNode();
      renderPosition(sb, usageNode);
      origin = usageNode.getOrigin();
    }
  }

  private static void renderPosition(StringBuilder sb, ASTNode tree) {
    sb.append("Line ");
    sb.append(getLine(tree));
    sb.append(":");
    sb.append(getCharPositionInLine(tree));
  }

  public static String renderPosition(ASTNode n) {
    StringBuilder sb = new StringBuilder();
    renderPosition(sb, n);
    return sb.toString();
  }

  public static String getMsg(String mesg, Tree tree) {
    return getMsg(mesg, (ASTNode) tree);
  }

  public static String getMsg(String mesg, ASTNode tree, String reason) {
    return getMsg(mesg, tree) + ": " + reason;
  }

  public static String getMsg(String mesg, Tree tree, String reason) {
    return getMsg(mesg, (ASTNode) tree, reason);
  }
}
