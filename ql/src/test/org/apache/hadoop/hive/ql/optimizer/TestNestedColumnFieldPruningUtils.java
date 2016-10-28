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

import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

@RunWith(Parameterized.class)
public class TestNestedColumnFieldPruningUtils {
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
      { "root[a]", new String[] { "root.a.b.c" }, "root[a]" },
      { "root[a[b[d,e]],c]", new String[] { "root.a.b.c" }, "root[a[b[d,e,c]],c]" },
      { "root[a[b[c]]]", new String[] { "root.a.b.c.d" }, "root[a[b[c]]]" },
      { null, new String[] { "a.b.c" }, "a[b[c]]" },
      { null, new String[] { "a.b", "a.c" }, "a[b,c]" },
      { "a[b]", new String[] { "a.b.c" }, "a[b]" } });
  }

  @Parameterized.Parameter(value = 0)
  public String origTreeExpr;
  @Parameterized.Parameter(value = 1)
  public String[] paths;
  @Parameterized.Parameter(value = 2)
  public String resTreeExpr;

  @org.junit.Test
  public void testAddNodeByPath() {
    FieldNode root = null;
    if (origTreeExpr != null) {
      root = buildTreeByExpr(origTreeExpr);
      Assert.assertEquals("The original tree is built incorrect", root.toString(), origTreeExpr);
    }
    for (String p : paths) {
      root = NestedColumnFieldPruningUtils.addNodeByPath(root, p);
    }
    Assert.assertEquals(resTreeExpr, root.toString());
  }

  private static boolean isSpecialChar(char element) {
    return (element == '[') || (element == ']') || (element == ',');
  }

  private static FieldNode buildTreeByExpr(String expr) {
    int index = 0;
    LinkedList<FieldNode> fieldStack = new LinkedList<>();
    while (index < expr.length()) {
      int i = index;
      if (isSpecialChar(expr.charAt(i))) {
        if ((expr.charAt(index) == ',') || (expr.charAt(index) == ']')) {
          FieldNode node = fieldStack.pop();
          FieldNode pre = fieldStack.peek();
          pre.addFieldNodes(node);
        }
        index++;
      } else {
        while (i < expr.length() && !isSpecialChar(expr.charAt(i))) {
          i++;
        }
        FieldNode current = new FieldNode(expr.substring(index, i));
        fieldStack.push(current);
        index = i;
      }
    }
    return fieldStack.pop();
  }
}
