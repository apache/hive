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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import java.util.Arrays;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.FilterPlan;
import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.MultiScanPlan;
import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.PartitionFilterGenerator;
import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.PlanResult;
import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.ScanPlan;
import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.ScanPlan.ScanMarker;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LogicalOperator;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.Operator;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeNode;
import org.junit.Assert;
import org.junit.Test;

public class TestHBaseFilterPlanUtil {
  final boolean INCLUSIVE = true;

  /**
   * Test the function that compares byte arrays
   */
  @Test
  public void testCompare() {

    Assert.assertEquals(-1, HBaseFilterPlanUtil.compare(new byte[] { 1, 2 }, new byte[] { 1, 3 }));
    Assert.assertEquals(-1,
        HBaseFilterPlanUtil.compare(new byte[] { 1, 2, 3 }, new byte[] { 1, 3 }));
    Assert.assertEquals(-1,
        HBaseFilterPlanUtil.compare(new byte[] { 1, 2 }, new byte[] { 1, 2, 3 }));

    Assert.assertEquals(0, HBaseFilterPlanUtil.compare(new byte[] { 3, 2 }, new byte[] { 3, 2 }));

    Assert
        .assertEquals(1, HBaseFilterPlanUtil.compare(new byte[] { 3, 2, 1 }, new byte[] { 3, 2 }));
    Assert
        .assertEquals(1, HBaseFilterPlanUtil.compare(new byte[] { 3, 3, 1 }, new byte[] { 3, 2 }));

  }

  /**
   * Test function that finds greater/lesser marker
   */
  @Test
  public void testgetComparedMarker() {
    ScanMarker l;
    ScanMarker r;

    // equal plans
    l = new ScanMarker(new byte[] { 1, 2 }, INCLUSIVE);
    r = new ScanMarker(new byte[] { 1, 2 }, INCLUSIVE);
    assertFirstGreater(l, r);

    l = new ScanMarker(new byte[] { 1, 2 }, !INCLUSIVE);
    r = new ScanMarker(new byte[] { 1, 2 }, !INCLUSIVE);
    assertFirstGreater(l, r);

    l = new ScanMarker(null, !INCLUSIVE);
    r = new ScanMarker(null, !INCLUSIVE);
    assertFirstGreater(l, r);

    // create l is greater because of inclusive flag
    l = new ScanMarker(new byte[] { 1, 2 }, !INCLUSIVE);
    r = new ScanMarker(null, !INCLUSIVE);
    // the rule for null vs non-null is different
    // non-null is both smaller and greater than null
    Assert.assertEquals(l, ScanPlan.getComparedMarker(l, r, true));
    Assert.assertEquals(l, ScanPlan.getComparedMarker(r, l, true));
    Assert.assertEquals(l, ScanPlan.getComparedMarker(l, r, false));
    Assert.assertEquals(l, ScanPlan.getComparedMarker(r, l, false));

    // create l that is greater because of the bytes
    l = new ScanMarker(new byte[] { 1, 2, 0 }, INCLUSIVE);
    r = new ScanMarker(new byte[] { 1, 2 }, INCLUSIVE);
    assertFirstGreater(l, r);

  }

  private void assertFirstGreater(ScanMarker big, ScanMarker small) {
    Assert.assertEquals(big, ScanPlan.getComparedMarker(big, small, true));
    Assert.assertEquals(big, ScanPlan.getComparedMarker(small, big, true));
    Assert.assertEquals(small, ScanPlan.getComparedMarker(big, small, false));
    Assert.assertEquals(small, ScanPlan.getComparedMarker(small, big, false));
  }

  /**
   * Test ScanPlan AND operation
   */
  @Test
  public void testScanPlanAnd() {
    ScanPlan l = new ScanPlan();
    ScanPlan r = new ScanPlan();
    l.setStartMarker(new ScanMarker(new byte[] { 10 }, INCLUSIVE));
    r.setStartMarker(new ScanMarker(new byte[] { 10 }, INCLUSIVE));

    ScanPlan res;
    // both equal
    res = l.and(r).getPlans().get(0);
    Assert.assertEquals(new ScanMarker(new byte[] { 10 }, INCLUSIVE), res.getStartMarker());

    // add equal end markers as well, and test AND again
    l.setEndMarker(new ScanMarker(new byte[] { 20 }, INCLUSIVE));
    r.setEndMarker(new ScanMarker(new byte[] { 20 }, INCLUSIVE));
    res = l.and(r).getPlans().get(0);
    Assert.assertEquals(new ScanMarker(new byte[] { 10 }, INCLUSIVE), res.getStartMarker());
    Assert.assertEquals(new ScanMarker(new byte[] { 20 }, INCLUSIVE), res.getEndMarker());

    l.setEndMarker(new ScanMarker(null, INCLUSIVE));
    r.setStartMarker(new ScanMarker(null, !INCLUSIVE));
    // markers with non null bytes are both lesser and greator
    Assert.assertEquals(l.getStartMarker(), res.getStartMarker());
    Assert.assertEquals(r.getEndMarker(), res.getEndMarker());

    l.setStartMarker(new ScanMarker(new byte[] { 10, 11 }, !INCLUSIVE));
    l.setEndMarker(new ScanMarker(new byte[] { 20, 21 }, INCLUSIVE));

    r.setStartMarker(new ScanMarker(new byte[] { 10, 10 }, INCLUSIVE));
    r.setEndMarker(new ScanMarker(new byte[] { 15 }, INCLUSIVE));
    res = l.and(r).getPlans().get(0);
    // start of l is greater, end of r is smaller
    Assert.assertEquals(l.getStartMarker(), res.getStartMarker());
    Assert.assertEquals(r.getEndMarker(), res.getEndMarker());

  }

  /**
   * Test ScanPlan OR operation
   */
  @Test
  public void testScanPlanOr() {
    ScanPlan l = new ScanPlan();
    ScanPlan r = new ScanPlan();
    l.setStartMarker(new ScanMarker(new byte[] { 10 }, INCLUSIVE));
    r.setStartMarker(new ScanMarker(new byte[] { 11 }, INCLUSIVE));

    FilterPlan res1 = l.or(r);
    Assert.assertEquals(2, res1.getPlans().size());
    res1.getPlans().get(0).getStartMarker().equals(l.getStartMarker());
    res1.getPlans().get(1).getStartMarker().equals(r.getStartMarker());

    FilterPlan res2 = res1.or(r);
    Assert.assertEquals(3, res2.getPlans().size());
  }

  /**
   * Test MultiScanPlan OR
   */
  @Test
  public void testMultiScanPlanOr() {

    MultiScanPlan l = createMultiScanPlan(new ScanPlan());
    MultiScanPlan r = createMultiScanPlan(new ScanPlan());
    // verify OR of two multi plans with one plan each
    Assert.assertEquals(2, l.or(r).getPlans().size());

    // verify OR of multi plan with a single scanplan
    Assert.assertEquals(2, l.or(new ScanPlan()).getPlans().size());
    Assert.assertEquals(2, (new ScanPlan()).or(l).getPlans().size());

    // verify or of two multiplans with more than one scan plan
    r = createMultiScanPlan(new ScanPlan(), new ScanPlan());
    Assert.assertEquals(3, l.or(r).getPlans().size());
    Assert.assertEquals(3, r.or(l).getPlans().size());

  }

  private MultiScanPlan createMultiScanPlan(ScanPlan... scanPlans) {
    return new MultiScanPlan(Arrays.asList(scanPlans));
  }

  /**
   * Test MultiScanPlan AND
   */
  @Test
  public void testMultiScanPlanAnd() {
    MultiScanPlan l = createMultiScanPlan(new ScanPlan());
    MultiScanPlan r = createMultiScanPlan(new ScanPlan());

    // two MultiScanPlan with single scan plans should result in new FilterPlan
    // with just one scan
    Assert.assertEquals(1, l.and(r).getPlans().size());

    // l has one ScanPlan, r has two. AND result should have two
    r = createMultiScanPlan(new ScanPlan(), new ScanPlan());
    Assert.assertEquals(2, l.and(r).getPlans().size());
    Assert.assertEquals(2, r.and(l).getPlans().size());

    // l has 2 ScanPlans, r has 3. AND result should have 6
    l = createMultiScanPlan(new ScanPlan(), new ScanPlan());
    r = createMultiScanPlan(new ScanPlan(), new ScanPlan(), new ScanPlan());
    Assert.assertEquals(6, l.and(r).getPlans().size());
    Assert.assertEquals(6, r.and(l).getPlans().size());
  }

  /**
   * Test plan generation from LeafNode
   *
   * @throws MetaException
   */
  @Test
  public void testLeafNodePlan() throws MetaException {

    final String KEY = "k1";
    final String VAL = "v1";
    final byte[] VAL_BYTES = PartitionFilterGenerator.toBytes(VAL);
    LeafNode l = new LeafNode();
    l.keyName = KEY;
    l.value = VAL;
    final ScanMarker DEFAULT_SCANMARKER = new ScanMarker(null, false);

    l.operator = Operator.EQUALS;
    verifyPlan(l, KEY, new ScanMarker(VAL_BYTES, INCLUSIVE), new ScanMarker(VAL_BYTES, INCLUSIVE));

    l.operator = Operator.GREATERTHAN;
    verifyPlan(l, KEY, new ScanMarker(VAL_BYTES, !INCLUSIVE), DEFAULT_SCANMARKER);

    l.operator = Operator.GREATERTHANOREQUALTO;
    verifyPlan(l, KEY, new ScanMarker(VAL_BYTES, INCLUSIVE), DEFAULT_SCANMARKER);

    l.operator = Operator.LESSTHAN;
    verifyPlan(l, KEY, DEFAULT_SCANMARKER, new ScanMarker(VAL_BYTES, !INCLUSIVE));

    l.operator = Operator.LESSTHANOREQUALTO;
    verifyPlan(l, KEY, DEFAULT_SCANMARKER, new ScanMarker(VAL_BYTES, INCLUSIVE));

    // following leaf node plans should currently have true for 'has unsupported condition',
    // because of the unsupported operator
    l.operator = Operator.NOTEQUALS;
    verifyPlan(l, KEY, DEFAULT_SCANMARKER, DEFAULT_SCANMARKER, true);

    l.operator = Operator.NOTEQUALS2;
    verifyPlan(l, KEY, DEFAULT_SCANMARKER, DEFAULT_SCANMARKER, true);

    l.operator = Operator.LIKE;
    verifyPlan(l, KEY, DEFAULT_SCANMARKER, DEFAULT_SCANMARKER, true);

    // following leaf node plans should currently have true for 'has unsupported condition',
    // because of the condition is not on first key
    l.operator = Operator.EQUALS;
    verifyPlan(l, "NOT_FIRST_PART", DEFAULT_SCANMARKER, DEFAULT_SCANMARKER, true);

    l.operator = Operator.NOTEQUALS;
    verifyPlan(l, "NOT_FIRST_PART", DEFAULT_SCANMARKER, DEFAULT_SCANMARKER, true);

    // if tree is null, it should return equivalent of full scan, and true
    // for 'has unsupported condition'
    verifyPlan(null, KEY, DEFAULT_SCANMARKER, DEFAULT_SCANMARKER, true);

  }

  private void verifyPlan(TreeNode l, String keyName, ScanMarker startMarker, ScanMarker endMarker)
      throws MetaException {
    verifyPlan(l, keyName, startMarker, endMarker, false);
  }

  private void verifyPlan(TreeNode l, String keyName, ScanMarker startMarker, ScanMarker endMarker,
      boolean hasUnsupportedCondition) throws MetaException {
    ExpressionTree e = null;
    if (l != null) {
      e = new ExpressionTree();
      e.setRootForTest(l);
    }
    PlanResult planRes = HBaseFilterPlanUtil.getFilterPlan(e, keyName);
    FilterPlan plan = planRes.plan;
    Assert.assertEquals("Has unsupported condition", hasUnsupportedCondition,
        planRes.hasUnsupportedCondition);
    Assert.assertEquals(1, plan.getPlans().size());
    ScanPlan splan = plan.getPlans().get(0);
    Assert.assertEquals(startMarker, splan.getStartMarker());
    Assert.assertEquals(endMarker, splan.getEndMarker());
  }

  /**
   * Test plan generation from TreeNode
   *
   * @throws MetaException
   */
  @Test
  public void testTreeNodePlan() throws MetaException {

    final String KEY = "k1";
    final String VAL1 = "10";
    final String VAL2 = "11";
    final byte[] VAL1_BYTES = PartitionFilterGenerator.toBytes(VAL1);
    final byte[] VAL2_BYTES = PartitionFilterGenerator.toBytes(VAL2);
    LeafNode l = new LeafNode();
    l.keyName = KEY;
    l.value = VAL1;
    final ScanMarker DEFAULT_SCANMARKER = new ScanMarker(null, false);

    LeafNode r = new LeafNode();
    r.keyName = KEY;
    r.value = VAL2;

    TreeNode tn = new TreeNode(l, LogicalOperator.AND, r);

    // verify plan for - k1 >= '10' and k1 < '11'
    l.operator = Operator.GREATERTHANOREQUALTO;
    r.operator = Operator.LESSTHAN;
    verifyPlan(tn, KEY, new ScanMarker(VAL1_BYTES, INCLUSIVE), new ScanMarker(VAL2_BYTES,
        !INCLUSIVE));

    // verify plan for - k1 >= '10' and k1 > '11'
    l.operator = Operator.GREATERTHANOREQUALTO;
    r.operator = Operator.GREATERTHAN;
    verifyPlan(tn, KEY, new ScanMarker(VAL2_BYTES, !INCLUSIVE), DEFAULT_SCANMARKER);

    // verify plan for - k1 >= '10' or k1 > '11'
    tn = new TreeNode(l, LogicalOperator.OR, r);
    ExpressionTree e = new ExpressionTree();
    e.setRootForTest(tn);
    PlanResult planRes = HBaseFilterPlanUtil.getFilterPlan(e, KEY);
    Assert.assertEquals(2, planRes.plan.getPlans().size());
    Assert.assertEquals(false, planRes.hasUnsupportedCondition);

    // verify plan for - k1 >= '10' and (k1 >= '10' or k1 > '11')
    TreeNode tn2 = new TreeNode(l, LogicalOperator.AND, tn);
    e = new ExpressionTree();
    e.setRootForTest(tn2);
    planRes = HBaseFilterPlanUtil.getFilterPlan(e, KEY);
    Assert.assertEquals(2, planRes.plan.getPlans().size());
    Assert.assertEquals(false, planRes.hasUnsupportedCondition);

    // verify plan for  (k1 >= '10' and (k1 >= '10' or k1 > '11')) or k1 LIKE '2'
    // plan should return true for hasUnsupportedCondition
    LeafNode klike = new LeafNode();
    klike.keyName = KEY;
    klike.value = VAL1;
    klike.operator = Operator.LIKE;
    TreeNode tn3 = new TreeNode(tn2, LogicalOperator.OR, klike);
    e = new ExpressionTree();
    e.setRootForTest(tn3);
    planRes = HBaseFilterPlanUtil.getFilterPlan(e, KEY);
    Assert.assertEquals(3, planRes.plan.getPlans().size());
    Assert.assertEquals(true, planRes.hasUnsupportedCondition);


  }

}
