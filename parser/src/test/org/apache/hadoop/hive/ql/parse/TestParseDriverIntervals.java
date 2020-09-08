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

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.lib.Node;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestParseDriverIntervals {

  private String query;
  private ParseDriver parseDriver;

  @Parameters(name = "{0}")
  public static List<Object[]> getParameters() {
    List<Object[]> ret = new ArrayList<>();
    ret.add(new Object[] { "select 1 days" });
    ret.add(new Object[] { "select (1) days" });
    ret.add(new Object[] { "select (1) day" });
    ret.add(new Object[] { "select interval (1+1) days" });
    ret.add(new Object[] { "select interval 1 days" });
    ret.add(new Object[] { "select interval '1' days" });
    ret.add(new Object[] { "select interval (x) days" });
    ret.add(new Object[] { "select interval (x+1) days" });
    ret.add(new Object[] { "select interval (1+x) days" });
    ret.add(new Object[] { "select interval (1+1) days" });
    ret.add(new Object[] { "select interval (x+1) days" });

    return ret;
  }

  public TestParseDriverIntervals(String query) {
    parseDriver = new ParseDriver();
    this.query = query;
  }

  @Test
  public void parseInterval() throws Exception {
    ASTNode root = parseDriver.parse(query).getTree();
    assertNotNull("failed: " + query, findFunctionNode(root));
    System.out.println(root.dump());
  }

  private ASTNode findFunctionNode(ASTNode n) {
    if (n.getType() == HiveParser.TOK_FUNCTION) {
      if ("internal_interval".equals(n.getChild(0).getText())) {
        return n;
      }
    }
    List<Node> children = n.getChildren();
    if (children != null) {
      for (Node c : children) {
        ASTNode r = findFunctionNode((ASTNode) c);
        if (r != null) {
          return r;
        }
      }
    }
    return null;
  }
}
