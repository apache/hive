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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.FilterPlan;
import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.MultiScanPlan;
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

import com.google.common.primitives.Shorts;

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
    l = new ScanMarker("1", INCLUSIVE, "int");
    r = new ScanMarker("1", INCLUSIVE, "int");
    assertFirstGreater(l, r);

    l = new ScanMarker("1", !INCLUSIVE, "int");
    r = new ScanMarker("1", !INCLUSIVE, "int");
    assertFirstGreater(l, r);

    assertFirstGreater(null, null);

    // create l is greater because of inclusive flag
    l = new ScanMarker("1", !INCLUSIVE, "int");
    // the rule for null vs non-null is different
    // non-null is both smaller and greater than null
    Assert.assertEquals(l, ScanPlan.getComparedMarker(l, null, true));
    Assert.assertEquals(l, ScanPlan.getComparedMarker(null, l, true));
    Assert.assertEquals(l, ScanPlan.getComparedMarker(l, null, false));
    Assert.assertEquals(l, ScanPlan.getComparedMarker(null, l, false));

    // create l that is greater because of the bytes
    l = new ScanMarker("2", INCLUSIVE, "int");
    r = new ScanMarker("1", INCLUSIVE, "int");
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
    l.setStartMarker("a", "int", "10", INCLUSIVE);
    r.setStartMarker("a", "int", "10", INCLUSIVE);

    ScanPlan res;
    // both equal
    res = l.and(r).getPlans().get(0);
    Assert.assertEquals(new ScanMarker("10", INCLUSIVE, "int"), res.markers.get("a").startMarker);

    // add equal end markers as well, and test AND again
    l.setEndMarker("a", "int", "20", INCLUSIVE);
    r.setEndMarker("a", "int", "20", INCLUSIVE);
    res = l.and(r).getPlans().get(0);
    Assert.assertEquals(new ScanMarker("10", INCLUSIVE, "int"), res.markers.get("a").startMarker);
    Assert.assertEquals(new ScanMarker("20", INCLUSIVE, "int"), res.markers.get("a").endMarker);

    l.setStartMarker("a", "int", "10", !INCLUSIVE);
    l.setEndMarker("a", "int", "20", INCLUSIVE);

    r.setStartMarker("a", "int", "10", INCLUSIVE);
    r.setEndMarker("a", "int", "15", INCLUSIVE);
    res = l.and(r).getPlans().get(0);
    // start of l is greater, end of r is smaller
    Assert.assertEquals(l.markers.get("a").startMarker, res.markers.get("a").startMarker);
    Assert.assertEquals(r.markers.get("a").endMarker, res.markers.get("a").endMarker);

  }

  /**
   * Test ScanPlan OR operation
   */
  @Test
  public void testScanPlanOr() {
    ScanPlan l = new ScanPlan();
    ScanPlan r = new ScanPlan();
    l.setStartMarker("a", "int", "1", INCLUSIVE);
    r.setStartMarker("a", "int", "11", INCLUSIVE);

    FilterPlan res1 = l.or(r);
    Assert.assertEquals(2, res1.getPlans().size());
    res1.getPlans().get(0).markers.get("a").startMarker.equals(l.markers.get("a").startMarker);
    res1.getPlans().get(1).markers.get("a").startMarker.equals(r.markers.get("a").startMarker);

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
    final String OTHERKEY = "k2";
    LeafNode l = new LeafNode();
    l.keyName = KEY;
    l.value = VAL;
    final ScanMarker DEFAULT_SCANMARKER = null;
    List<FieldSchema> parts = new ArrayList<FieldSchema>();
    parts.add(new FieldSchema(KEY, "int", null));
    parts.add(new FieldSchema(OTHERKEY, "int", null));

    l.operator = Operator.EQUALS;
    verifyPlan(l, parts, KEY, new ScanMarker(VAL, INCLUSIVE, "int"), new ScanMarker(VAL, INCLUSIVE, "int"));

    l.operator = Operator.GREATERTHAN;
    verifyPlan(l, parts, KEY, new ScanMarker(VAL, !INCLUSIVE, "int"), DEFAULT_SCANMARKER);

    l.operator = Operator.GREATERTHANOREQUALTO;
    verifyPlan(l, parts, KEY, new ScanMarker(VAL, INCLUSIVE, "int"), DEFAULT_SCANMARKER);

    l.operator = Operator.LESSTHAN;
    verifyPlan(l, parts, KEY, DEFAULT_SCANMARKER, new ScanMarker(VAL, !INCLUSIVE, "int"));

    l.operator = Operator.LESSTHANOREQUALTO;
    verifyPlan(l, parts, KEY, DEFAULT_SCANMARKER, new ScanMarker(VAL, INCLUSIVE, "int"));

    // following leaf node plans should currently have true for 'has unsupported condition',
    // because of the condition is not on first key
    l.operator = Operator.EQUALS;
    verifyPlan(l, parts, OTHERKEY, DEFAULT_SCANMARKER, DEFAULT_SCANMARKER, false);

    // if tree is null, it should return equivalent of full scan, and true
    // for 'has unsupported condition'
    verifyPlan(null, parts, KEY, DEFAULT_SCANMARKER, DEFAULT_SCANMARKER, true);

  }

  private void verifyPlan(TreeNode l, List<FieldSchema> parts, String keyName, ScanMarker startMarker, ScanMarker endMarker)
      throws MetaException {
    verifyPlan(l, parts, keyName, startMarker, endMarker, false);
  }

  private void verifyPlan(TreeNode l, List<FieldSchema> parts, String keyName, ScanMarker startMarker, ScanMarker endMarker,
      boolean hasUnsupportedCondition) throws MetaException {
    ExpressionTree e = null;
    if (l != null) {
      e = new ExpressionTree();
      e.setRootForTest(l);
    }
    PlanResult planRes = HBaseFilterPlanUtil.getFilterPlan(e, parts);
    FilterPlan plan = planRes.plan;
    Assert.assertEquals("Has unsupported condition", hasUnsupportedCondition,
        planRes.hasUnsupportedCondition);
    Assert.assertEquals(1, plan.getPlans().size());
    ScanPlan splan = plan.getPlans().get(0);
    if (startMarker != null) {
      Assert.assertEquals(startMarker, splan.markers.get(keyName).startMarker);
    } else {
      Assert.assertTrue(splan.markers.get(keyName)==null ||
          splan.markers.get(keyName).startMarker==null);
    }
    if (endMarker != null) {
      Assert.assertEquals(endMarker, splan.markers.get(keyName).endMarker);
    } else {
      Assert.assertTrue(splan.markers.get(keyName)==null ||
          splan.markers.get(keyName).endMarker==null);
    }
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
    LeafNode l = new LeafNode();
    l.keyName = KEY;
    l.value = VAL1;
    final ScanMarker DEFAULT_SCANMARKER = null;

    List<FieldSchema> parts = new ArrayList<FieldSchema>();
    parts.add(new FieldSchema("k1", "int", null));

    LeafNode r = new LeafNode();
    r.keyName = KEY;
    r.value = VAL2;

    TreeNode tn = new TreeNode(l, LogicalOperator.AND, r);

    // verify plan for - k1 >= '10' and k1 < '11'
    l.operator = Operator.GREATERTHANOREQUALTO;
    r.operator = Operator.LESSTHAN;
    verifyPlan(tn, parts, KEY, new ScanMarker(VAL1, INCLUSIVE, "int"), new ScanMarker(VAL2,
        !INCLUSIVE, "int"));

    // verify plan for - k1 >= '10' and k1 > '11'
    l.operator = Operator.GREATERTHANOREQUALTO;
    r.operator = Operator.GREATERTHAN;
    verifyPlan(tn, parts, KEY, new ScanMarker(VAL2, !INCLUSIVE, "int"), DEFAULT_SCANMARKER);

    // verify plan for - k1 >= '10' or k1 > '11'
    tn = new TreeNode(l, LogicalOperator.OR, r);
    ExpressionTree e = new ExpressionTree();
    e.setRootForTest(tn);
    PlanResult planRes = HBaseFilterPlanUtil.getFilterPlan(e, parts);
    Assert.assertEquals(2, planRes.plan.getPlans().size());
    Assert.assertEquals(false, planRes.hasUnsupportedCondition);

    // verify plan for - k1 >= '10' and (k1 >= '10' or k1 > '11')
    TreeNode tn2 = new TreeNode(l, LogicalOperator.AND, tn);
    e = new ExpressionTree();
    e.setRootForTest(tn2);
    planRes = HBaseFilterPlanUtil.getFilterPlan(e, parts);
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
    planRes = HBaseFilterPlanUtil.getFilterPlan(e, parts);
    Assert.assertEquals(3, planRes.plan.getPlans().size());
    Assert.assertEquals(false, planRes.hasUnsupportedCondition);


  }

  @Test
  public void testPartitionKeyScannerAllString() throws Exception {
    List<FieldSchema> parts = new ArrayList<FieldSchema>();
    parts.add(new FieldSchema("year", "string", null));
    parts.add(new FieldSchema("month", "string", null));
    parts.add(new FieldSchema("state", "string", null));

    // One prefix key and one minor key range
    ExpressionTree exprTree = PartFilterExprUtil.getFilterParser("year = 2015 and state = 'CA'").tree;
    PlanResult planRes = HBaseFilterPlanUtil.getFilterPlan(exprTree, parts);

    Assert.assertEquals(planRes.plan.getPlans().size(), 1);

    ScanPlan sp = planRes.plan.getPlans().get(0);
    byte[] startRowSuffix = sp.getStartRowSuffix("testdb", "testtb", parts);
    byte[] endRowSuffix = sp.getEndRowSuffix("testdb", "testtb", parts);
    RowFilter filter = (RowFilter)sp.getFilter(parts);

    // scan range contains the major key year, rowfilter contains minor key state
    Assert.assertTrue(Bytes.contains(startRowSuffix, "2015".getBytes()));
    Assert.assertTrue(Bytes.contains(endRowSuffix, "2015".getBytes()));
    Assert.assertFalse(Bytes.contains(startRowSuffix, "CA".getBytes()));
    Assert.assertFalse(Bytes.contains(endRowSuffix, "CA".getBytes()));

    PartitionKeyComparator comparator = (PartitionKeyComparator)filter.getComparator();
    Assert.assertEquals(comparator.ranges.size(), 1);
    Assert.assertEquals(comparator.ranges.get(0).keyName, "state");

    // Two prefix key and one LIKE operator
    exprTree = PartFilterExprUtil.getFilterParser("year = 2015 and month > 10 "
        + "and month <= 11 and state like 'C%'").tree;
    planRes = HBaseFilterPlanUtil.getFilterPlan(exprTree, parts);

    Assert.assertEquals(planRes.plan.getPlans().size(), 1);

    sp = planRes.plan.getPlans().get(0);
    startRowSuffix = sp.getStartRowSuffix("testdb", "testtb", parts);
    endRowSuffix = sp.getEndRowSuffix("testdb", "testtb", parts);
    filter = (RowFilter)sp.getFilter(parts);

    // scan range contains the major key value year/month, rowfilter contains LIKE operator
    Assert.assertTrue(Bytes.contains(startRowSuffix, "2015".getBytes()));
    Assert.assertTrue(Bytes.contains(endRowSuffix, "2015".getBytes()));
    Assert.assertTrue(Bytes.contains(startRowSuffix, "10".getBytes()));
    Assert.assertTrue(Bytes.contains(endRowSuffix, "11".getBytes()));

    comparator = (PartitionKeyComparator)filter.getComparator();
    Assert.assertEquals(comparator.ops.size(), 1);
    Assert.assertEquals(comparator.ops.get(0).keyName, "state");

    // One prefix key, one minor key range and one LIKE operator
    exprTree = PartFilterExprUtil.getFilterParser("year >= 2014 and month > 10 "
        + "and month <= 11 and state like 'C%'").tree;
    planRes = HBaseFilterPlanUtil.getFilterPlan(exprTree, parts);

    Assert.assertEquals(planRes.plan.getPlans().size(), 1);

    sp = planRes.plan.getPlans().get(0);
    startRowSuffix = sp.getStartRowSuffix("testdb", "testtb", parts);
    endRowSuffix = sp.getEndRowSuffix("testdb", "testtb", parts);
    filter = (RowFilter)sp.getFilter(parts);

    // scan range contains the major key value year (low bound), rowfilter contains minor key state
    // and LIKE operator
    Assert.assertTrue(Bytes.contains(startRowSuffix, "2014".getBytes()));

    comparator = (PartitionKeyComparator)filter.getComparator();
    Assert.assertEquals(comparator.ranges.size(), 1);
    Assert.assertEquals(comparator.ranges.get(0).keyName, "month");
    Assert.assertEquals(comparator.ops.size(), 1);
    Assert.assertEquals(comparator.ops.get(0).keyName, "state");

    // Condition contains or
    exprTree = PartFilterExprUtil.getFilterParser("year = 2014 and (month > 10 "
        + "or month < 3)").tree;
    planRes = HBaseFilterPlanUtil.getFilterPlan(exprTree, parts);

    sp = planRes.plan.getPlans().get(0);
    startRowSuffix = sp.getStartRowSuffix("testdb", "testtb", parts);
    endRowSuffix = sp.getEndRowSuffix("testdb", "testtb", parts);
    filter = (RowFilter)sp.getFilter(parts);

    // The first ScanPlan contains year = 2014 and month > 10
    Assert.assertTrue(Bytes.contains(startRowSuffix, "2014".getBytes()));
    Assert.assertTrue(Bytes.contains(endRowSuffix, "2014".getBytes()));
    Assert.assertTrue(Bytes.contains(startRowSuffix, "10".getBytes()));

    sp = planRes.plan.getPlans().get(1);
    startRowSuffix = sp.getStartRowSuffix("testdb", "testtb", parts);
    endRowSuffix = sp.getEndRowSuffix("testdb", "testtb", parts);
    filter = (RowFilter)sp.getFilter(parts);

    // The first ScanPlan contains year = 2014 and month < 3
    Assert.assertTrue(Bytes.contains(startRowSuffix, "2014".getBytes()));
    Assert.assertTrue(Bytes.contains(endRowSuffix, "2014".getBytes()));
    Assert.assertTrue(Bytes.contains(endRowSuffix, "3".getBytes()));
  }

  @Test
  public void testPartitionKeyScannerMixedType() throws Exception {
    List<FieldSchema> parts = new ArrayList<FieldSchema>();
    parts.add(new FieldSchema("year", "int", null));
    parts.add(new FieldSchema("month", "int", null));
    parts.add(new FieldSchema("state", "string", null));

    // One prefix key and one minor key range
    ExpressionTree exprTree = PartFilterExprUtil.getFilterParser("year = 2015 and state = 'CA'").tree;
    PlanResult planRes = HBaseFilterPlanUtil.getFilterPlan(exprTree, parts);

    Assert.assertEquals(planRes.plan.getPlans().size(), 1);

    ScanPlan sp = planRes.plan.getPlans().get(0);
    byte[] startRowSuffix = sp.getStartRowSuffix("testdb", "testtb", parts);
    byte[] endRowSuffix = sp.getEndRowSuffix("testdb", "testtb", parts);
    RowFilter filter = (RowFilter)sp.getFilter(parts);

    // scan range contains the major key year, rowfilter contains minor key state
    Assert.assertTrue(Bytes.contains(startRowSuffix, Shorts.toByteArray((short)2015)));
    Assert.assertTrue(Bytes.contains(endRowSuffix, Shorts.toByteArray((short)2016)));

    PartitionKeyComparator comparator = (PartitionKeyComparator)filter.getComparator();
    Assert.assertEquals(comparator.ranges.size(), 1);
    Assert.assertEquals(comparator.ranges.get(0).keyName, "state");
  }
}
