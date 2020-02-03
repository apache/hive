/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.predicate;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;
import org.apache.hadoop.hive.accumulo.TestAccumuloDefaultIndexScanner;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloRowIdColumnMapping;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToString;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPlus;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertNotNull;

/**
 *
 */
public class TestAccumuloRangeGenerator {

  private AccumuloPredicateHandler handler;
  private HiveAccumuloRowIdColumnMapping rowIdMapping;
  private Configuration conf;

  @Before
  public void setup() {
    handler = AccumuloPredicateHandler.getInstance();
    rowIdMapping = new HiveAccumuloRowIdColumnMapping(AccumuloHiveConstants.ROWID,
        ColumnEncoding.STRING,"row", TypeInfoFactory.stringTypeInfo.toString());
    conf = new Configuration(true);
  }

  @Test
  public void testRangeConjunction() throws Exception {
    // rowId >= 'f'
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "f");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), children);
    assertNotNull(node);

    // rowId <= 'm'
    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null,
        false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "m");
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrLessThan(), children2);
    assertNotNull(node2);

    // And UDF
    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPAnd(), bothFilters);

    // Should generate [f,m]
    List<Range> expectedRanges = Arrays
        .asList(new Range(new Key("f"), true, new Key("m\0"), false));

    AccumuloRangeGenerator rangeGenerator = new AccumuloRangeGenerator(conf, handler, rowIdMapping, "rid");
    SemanticDispatcher disp = new DefaultRuleDispatcher(rangeGenerator,
        Collections.<SemanticRule, SemanticNodeProcessor> emptyMap(), null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(both);
    HashMap<Node,Object> nodeOutput = new HashMap<Node,Object>();

    try {
      ogw.startWalking(topNodes, nodeOutput);
    } catch (SemanticException ex) {
      throw new RuntimeException(ex);
    }

    Object result = nodeOutput.get(both);
    Assert.assertNotNull(result);
    Assert.assertTrue("Result from graph walk was not a List", result instanceof List);
    @SuppressWarnings("unchecked")
    List<Range> actualRanges = (List<Range>) result;
    Assert.assertEquals(expectedRanges, actualRanges);
  }

  @Test
  public void testRangeDisjunction() throws Exception {
    // rowId >= 'f'
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "f");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), children);
    assertNotNull(node);

    // rowId <= 'm'
    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null,
        false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "m");
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrLessThan(), children2);
    assertNotNull(node2);

    // Or UDF
    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPOr(), bothFilters);

    // Should generate (-inf,+inf)
    List<Range> expectedRanges = Arrays.asList(new Range());

    AccumuloRangeGenerator rangeGenerator = new AccumuloRangeGenerator(conf, handler, rowIdMapping, "rid");
    SemanticDispatcher disp = new DefaultRuleDispatcher(rangeGenerator,
        Collections.<SemanticRule, SemanticNodeProcessor> emptyMap(), null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(both);
    HashMap<Node,Object> nodeOutput = new HashMap<Node,Object>();

    try {
      ogw.startWalking(topNodes, nodeOutput);
    } catch (SemanticException ex) {
      throw new RuntimeException(ex);
    }

    Object result = nodeOutput.get(both);
    Assert.assertNotNull(result);
    Assert.assertTrue("Result from graph walk was not a List", result instanceof List);
    @SuppressWarnings("unchecked")
    List<Range> actualRanges = (List<Range>) result;
    Assert.assertEquals(expectedRanges, actualRanges);
  }

  @Test
  public void testRangeConjunctionWithDisjunction() throws Exception {
    // rowId >= 'h'
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "h");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), children);
    assertNotNull(node);

    // rowId <= 'd'
    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null,
        false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "d");
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrLessThan(), children2);
    assertNotNull(node2);

    // rowId >= 'q'
    ExprNodeDesc column3 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null,
        false);
    ExprNodeDesc constant3 = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "q");
    List<ExprNodeDesc> children3 = Lists.newArrayList();
    children3.add(column3);
    children3.add(constant3);
    ExprNodeDesc node3 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), children3);
    assertNotNull(node3);

    // Or UDF, (rowId <= 'd' or rowId >= 'q')
    List<ExprNodeDesc> orFilters = Lists.newArrayList();
    orFilters.add(node2);
    orFilters.add(node3);
    ExprNodeGenericFuncDesc orNode = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPOr(), orFilters);

    // And UDF, (rowId >= 'h' and (rowId <= 'd' or rowId >= 'q'))
    List<ExprNodeDesc> andFilters = Lists.newArrayList();
    andFilters.add(node);
    andFilters.add(orNode);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPAnd(), andFilters);

    // Should generate ['q', +inf)
    List<Range> expectedRanges = Arrays.asList(new Range(new Key("q"), true, null, false));

    AccumuloRangeGenerator rangeGenerator = new AccumuloRangeGenerator(conf, handler, rowIdMapping, "rid");
    SemanticDispatcher disp = new DefaultRuleDispatcher(rangeGenerator,
        Collections.<SemanticRule, SemanticNodeProcessor> emptyMap(), null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(both);
    HashMap<Node,Object> nodeOutput = new HashMap<Node,Object>();

    try {
      ogw.startWalking(topNodes, nodeOutput);
    } catch (SemanticException ex) {
      throw new RuntimeException(ex);
    }

    Object result = nodeOutput.get(both);
    Assert.assertNotNull(result);
    Assert.assertTrue("Result from graph walk was not a List", result instanceof List);
    @SuppressWarnings("unchecked")
    List<Range> actualRanges = (List<Range>) result;
    Assert.assertEquals(expectedRanges, actualRanges);
  }

  @Test
  public void testPartialRangeConjunction() throws Exception {
    // rowId >= 'f'
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "f");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), children);
    assertNotNull(node);

    // anythingElse <= 'foo'
    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "anythingElse",
        null, false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "foo");
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrLessThan(), children2);
    assertNotNull(node2);

    // And UDF
    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPAnd(), bothFilters);

    // Should generate [f,+inf)
    List<Range> expectedRanges = Arrays.asList(new Range(new Key("f"), true, null, false));

    AccumuloRangeGenerator rangeGenerator = new AccumuloRangeGenerator(conf, handler, rowIdMapping, "rid");
    SemanticDispatcher disp = new DefaultRuleDispatcher(rangeGenerator,
        Collections.<SemanticRule, SemanticNodeProcessor> emptyMap(), null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(both);
    HashMap<Node,Object> nodeOutput = new HashMap<Node,Object>();

    try {
      ogw.startWalking(topNodes, nodeOutput);
    } catch (SemanticException ex) {
      throw new RuntimeException(ex);
    }

    Object result = nodeOutput.get(both);
    Assert.assertNotNull(result);
    Assert.assertTrue("Result from graph walk was not a List", result instanceof List);
    @SuppressWarnings("unchecked")
    List<Range> actualRanges = (List<Range>) result;
    Assert.assertEquals(expectedRanges, actualRanges);
  }

  @Test
  public void testDateRangeConjunction() throws Exception {
    // rowId >= '2014-01-01'
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo,
        Date.valueOf("2014-01-01"));
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), children);
    assertNotNull(node);

    // rowId <= '2014-07-01'
    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null,
        false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo,
        Date.valueOf("2014-07-01"));
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPLessThan(), children2);
    assertNotNull(node2);

    // And UDF
    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPAnd(), bothFilters);

    // Should generate [2014-01-01, 2014-07-01)
    List<Range> expectedRanges = Arrays.asList(new Range(new Key("2014-01-01"), true, new Key(
        "2014-07-01"), false));

    AccumuloRangeGenerator rangeGenerator = new AccumuloRangeGenerator(conf, handler, rowIdMapping, "rid");
    SemanticDispatcher disp = new DefaultRuleDispatcher(rangeGenerator,
        Collections.<SemanticRule, SemanticNodeProcessor> emptyMap(), null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(both);
    HashMap<Node,Object> nodeOutput = new HashMap<Node,Object>();

    try {
      ogw.startWalking(topNodes, nodeOutput);
    } catch (SemanticException ex) {
      throw new RuntimeException(ex);
    }

    Object result = nodeOutput.get(both);
    Assert.assertNotNull(result);
    Assert.assertTrue("Result from graph walk was not a List", result instanceof List);
    @SuppressWarnings("unchecked")
    List<Range> actualRanges = (List<Range>) result;
    Assert.assertEquals(expectedRanges, actualRanges);
  }

  @Test
  public void testCastExpression() throws Exception {
    // 40 and 50
    ExprNodeDesc fourty = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo,
        40), fifty = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 50);

    // +
    GenericUDFOPPlus plus = new GenericUDFOPPlus();

    // 40 + 50
    ExprNodeGenericFuncDesc addition = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo, plus, Arrays.asList(fourty, fifty));

    // cast(.... as string)
    GenericUDFToString stringCast = new GenericUDFToString();

    // cast (40 + 50 as string)
    ExprNodeGenericFuncDesc cast = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        stringCast, "cast", Collections.<ExprNodeDesc> singletonList(addition));

    ExprNodeDesc key = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "key", null,
        false);

    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), Arrays.asList(key, cast));

    AccumuloRangeGenerator rangeGenerator = new AccumuloRangeGenerator(conf, handler, rowIdMapping, "key");
    SemanticDispatcher disp = new DefaultRuleDispatcher(rangeGenerator,
        Collections.<SemanticRule, SemanticNodeProcessor> emptyMap(), null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(node);
    HashMap<Node,Object> nodeOutput = new HashMap<Node,Object>();

    try {
      ogw.startWalking(topNodes, nodeOutput);
    } catch (SemanticException ex) {
      throw new RuntimeException(ex);
    }

    // Don't fail -- would be better to actually compute a range of [90,+inf)
    Object result = nodeOutput.get(node);
    Assert.assertNull(result);
  }

  @Test
  public void testRangeOverNonRowIdField() throws Exception {
    // foo >= 'f'
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "foo", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "f");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), children);
    assertNotNull(node);

    // foo <= 'm'
    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "foo", null,
        false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "m");
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrLessThan(), children2);
    assertNotNull(node2);

    // And UDF
    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPAnd(), bothFilters);

    AccumuloRangeGenerator rangeGenerator = new AccumuloRangeGenerator(conf, handler, rowIdMapping, "rid");
    SemanticDispatcher disp = new DefaultRuleDispatcher(rangeGenerator,
        Collections.<SemanticRule, SemanticNodeProcessor> emptyMap(), null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(both);
    HashMap<Node,Object> nodeOutput = new HashMap<Node,Object>();

    try {
      ogw.startWalking(topNodes, nodeOutput);
    } catch (SemanticException ex) {
      throw new RuntimeException(ex);
    }

    // Filters are not over the rowid, therefore scan everything
    Object result = nodeOutput.get(both);
    Assert.assertNull(result);
  }

  @Test
  public void testRangeOverStringIndexedField() throws Exception {
    // age >= '10'
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "age", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "10");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), children);
    assertNotNull(node);

    // age <= '50'
    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "age", null,
        false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "50");
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrLessThan(), children2);
    assertNotNull(node2);

    // And UDF
    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPAnd(), bothFilters);

    AccumuloRangeGenerator rangeGenerator = new AccumuloRangeGenerator(conf, handler, rowIdMapping, "rid");
    rangeGenerator.setIndexScanner(TestAccumuloDefaultIndexScanner.buildMockHandler(10));
    SemanticDispatcher disp = new DefaultRuleDispatcher(rangeGenerator,
        Collections.<SemanticRule, SemanticNodeProcessor> emptyMap(), null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(both);
    HashMap<Node,Object> nodeOutput = new HashMap<Node,Object>();

    try {
      ogw.startWalking(topNodes, nodeOutput);
    } catch (SemanticException ex) {
      throw new RuntimeException(ex);
    }

    // Filters are using an index which should match 3 rows
    Object result = nodeOutput.get(both);
    if ( result instanceof  List) {
      List results = (List) result;
      Assert.assertEquals(3, results.size());
      Assert.assertTrue("does not contain row1", results.contains(new Range("row1")));
      Assert.assertTrue("does not contain row2", results.contains(new Range("row2")));
      Assert.assertTrue("does not contain row3", results.contains(new Range("row3")));
    } else {
      Assert.fail("Results not a list");
    }
  }

  @Test
  public void testRangeOverIntegerIndexedField() throws Exception {
    // cars >= 2
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "cars", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 2);
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), children);
    assertNotNull(node);

    //  cars <= 9
    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "cars", null,
        false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 9);
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrLessThan(), children2);
    assertNotNull(node2);

    // And UDF
    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPAnd(), bothFilters);

    AccumuloRangeGenerator rangeGenerator = new AccumuloRangeGenerator(conf, handler, rowIdMapping, "rid");
    rangeGenerator.setIndexScanner(TestAccumuloDefaultIndexScanner.buildMockHandler(10));
    SemanticDispatcher disp = new DefaultRuleDispatcher(rangeGenerator,
        Collections.<SemanticRule, SemanticNodeProcessor> emptyMap(), null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(both);
    HashMap<Node,Object> nodeOutput = new HashMap<Node,Object>();

    try {
      ogw.startWalking(topNodes, nodeOutput);
    } catch (SemanticException ex) {
      throw new RuntimeException(ex);
    }

    // Filters are using an index which should match 3 rows
    Object result = nodeOutput.get(both);
    if ( result instanceof  List) {
      List results = (List) result;
      Assert.assertEquals(3, results.size());
      Assert.assertTrue("does not contain row1", results.contains(new Range("row1")));
      Assert.assertTrue("does not contain row2", results.contains(new Range("row2")));
      Assert.assertTrue("does not contain row3", results.contains(new Range("row3")));
    } else {
      Assert.fail("Results not a list");
    }
  }

  @Test
  public void testRangeOverBooleanIndexedField() throws Exception {
    // mgr == true
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.booleanTypeInfo, "mgr", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, true);
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqual(), children);
    assertNotNull(node);

    AccumuloRangeGenerator rangeGenerator = new AccumuloRangeGenerator(conf, handler, rowIdMapping, "rid");
    rangeGenerator.setIndexScanner(TestAccumuloDefaultIndexScanner.buildMockHandler(10));
    SemanticDispatcher disp = new DefaultRuleDispatcher(rangeGenerator,
        Collections.<SemanticRule, SemanticNodeProcessor> emptyMap(), null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(node);
    HashMap<Node,Object> nodeOutput = new HashMap<Node,Object>();

    try {
      ogw.startWalking(topNodes, nodeOutput);
    } catch (SemanticException ex) {
      throw new RuntimeException(ex);
    }

    // Filters are using an index which should match 2 rows
    Object result = nodeOutput.get(node);
    if ( result instanceof  List) {
      List results = (List) result;
      Assert.assertEquals(2, results.size());
      Assert.assertTrue("does not contain row1", results.contains( new Range( "row1")));
      Assert.assertTrue("does not contain row3", results.contains( new Range( "row3")));
    }
    else {
      Assert.fail("Results not a list");
    }
  }

}
