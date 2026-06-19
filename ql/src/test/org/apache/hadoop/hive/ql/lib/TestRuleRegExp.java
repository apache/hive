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

import static org.junit.Assert.*;

import java.util.List;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Test;

public class TestRuleRegExp {

  public class TestNode implements Node {
    private String name;

    TestNode (String name) {
      this.name = name;
    }

    @Override
    public List<? extends Node> getChildren() {
      return null;
    }

    @Override
    public String getName() {
      return name;
    }
  }

  @Test
  public void testPatternWithoutWildCardChar() {
    String patternStr =
      ReduceSinkOperator.getOperatorName() + "%" +
      SelectOperator.getOperatorName() + "%" +
      FileSinkOperator.getOperatorName() + "%";
    RuleRegExp rule1 = new RuleRegExp("R1", patternStr);
    assertEquals(rule1.rulePatternIsValidWithoutWildCardChar(), true);
    assertEquals(rule1.rulePatternIsValidWithWildCardChar(), false);
    // positive test
    Stack<Node> ns1 = new Stack<Node>();
    ns1.push(new TestNode(ReduceSinkOperator.getOperatorName()));
    ns1.push(new TestNode(SelectOperator.getOperatorName()));
    ns1.push(new TestNode(FileSinkOperator.getOperatorName()));
    try {
      assertEquals(rule1.cost(ns1), patternStr.length());
    } catch (SemanticException e) {
      fail(e.getMessage());
	}
    // negative test
    Stack<Node> ns2 = new Stack<Node>();
    ns2.push(new TestNode(ReduceSinkOperator.getOperatorName()));
    ns1.push(new TestNode(TableScanOperator.getOperatorName()));
    ns1.push(new TestNode(FileSinkOperator.getOperatorName()));
    try {
      assertEquals(rule1.cost(ns2), -1);
    } catch (SemanticException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testPatternWithWildCardChar() {
    RuleRegExp rule1 =  new RuleRegExp("R1",
      "(" + TableScanOperator.getOperatorName() + "%"
      + FilterOperator.getOperatorName() + "%)|("
      + TableScanOperator.getOperatorName() + "%"
      + FileSinkOperator.getOperatorName() + "%)");
    assertEquals(rule1.rulePatternIsValidWithoutWildCardChar(), false);
    assertEquals(rule1.rulePatternIsValidWithWildCardChar(), true);
    // positive test
    Stack<Node> ns1 = new Stack<Node>();
    ns1.push(new TestNode(TableScanOperator.getOperatorName()));
    ns1.push(new TestNode(FilterOperator.getOperatorName()));
    Stack<Node> ns2 = new Stack<Node>();
    ns2.push(new TestNode(TableScanOperator.getOperatorName()));
    ns2.push(new TestNode(FileSinkOperator.getOperatorName()));
    try {
      assertNotEquals(rule1.cost(ns1), -1);
      assertNotEquals(rule1.cost(ns2), -1);
    } catch (SemanticException e) {
      fail(e.getMessage());
	}
    // negative test
    Stack<Node> ns3 = new Stack<Node>();
    ns3.push(new TestNode(ReduceSinkOperator.getOperatorName()));
    ns3.push(new TestNode(ReduceSinkOperator.getOperatorName()));
    ns3.push(new TestNode(FileSinkOperator.getOperatorName()));
    try {
      assertEquals(rule1.cost(ns3), -1);
    } catch (SemanticException e) {
      fail(e.getMessage());
    }
  }

}
