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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapper;
import org.apache.hadoop.hive.accumulo.predicate.compare.CompareOp;
import org.apache.hadoop.hive.accumulo.predicate.compare.DoubleCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.Equal;
import org.apache.hadoop.hive.accumulo.predicate.compare.GreaterThan;
import org.apache.hadoop.hive.accumulo.predicate.compare.GreaterThanOrEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.IntCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.LessThan;
import org.apache.hadoop.hive.accumulo.predicate.compare.LessThanOrEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.LongCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.NotEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.PrimitiveComparison;
import org.apache.hadoop.hive.accumulo.predicate.compare.StringCompare;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.accumulo.serde.TooManyAccumuloColumnsException;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class TestAccumuloPredicateHandler {
  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(TestAccumuloPredicateHandler.class);

  private AccumuloPredicateHandler handler = AccumuloPredicateHandler.getInstance();
  private JobConf conf;
  private ColumnMapper columnMapper;

  @Before
  public void setup() throws TooManyAccumuloColumnsException {
    FunctionRegistry.getFunctionNames();
    conf = new JobConf();
    List<String> columnNames = Arrays.asList("field1", "rid");
    List<TypeInfo> columnTypes = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo);
    conf.set(serdeConstants.LIST_COLUMNS, Joiner.on(',').join(columnNames));
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "string,string");

    String columnMappingStr = "cf:f1,:rowID";
    conf.set(AccumuloSerDeParameters.COLUMN_MAPPINGS, columnMappingStr);
    columnMapper = new ColumnMapper(columnMappingStr, ColumnEncoding.STRING.getName(), columnNames,
        columnTypes);
  }

  @Test
  public void testGetRowIDSearchCondition() {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "hi");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqual(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);

    List<IndexSearchCondition> sConditions = handler.getSearchConditions(conf);
    assertEquals(sConditions.size(), 1);
  }

  @Test()
  public void testRangeEqual() throws SerDeException {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqual(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);

    Collection<Range> ranges = handler.getRanges(conf, columnMapper);
    assertEquals(ranges.size(), 1);
    Range range = ranges.iterator().next();
    assertTrue(range.isStartKeyInclusive());
    assertFalse(range.isEndKeyInclusive());
    assertTrue(range.contains(new Key(new Text("aaa"))));
    assertTrue(range.afterEndKey(new Key(new Text("aab"))));
    assertTrue(range.beforeStartKey(new Key(new Text("aa"))));
  }

  @Test()
  public void testRangeGreaterThan() throws SerDeException {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPGreaterThan(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);

    Collection<Range> ranges = handler.getRanges(conf, columnMapper);
    assertEquals(ranges.size(), 1);
    Range range = ranges.iterator().next();
    assertTrue(range.isStartKeyInclusive());
    assertFalse(range.isEndKeyInclusive());
    assertFalse(range.contains(new Key(new Text("aaa"))));
    assertFalse(range.afterEndKey(new Key(new Text("ccccc"))));
    assertTrue(range.contains(new Key(new Text("aab"))));
    assertTrue(range.beforeStartKey(new Key(new Text("aa"))));
    assertTrue(range.beforeStartKey(new Key(new Text("aaa"))));
  }

  @Test
  public void rangeGreaterThanOrEqual() throws SerDeException {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);

    Collection<Range> ranges = handler.getRanges(conf, columnMapper);
    assertEquals(ranges.size(), 1);
    Range range = ranges.iterator().next();
    assertTrue(range.isStartKeyInclusive());
    assertFalse(range.isEndKeyInclusive());
    assertTrue(range.contains(new Key(new Text("aaa"))));
    assertFalse(range.afterEndKey(new Key(new Text("ccccc"))));
    assertTrue(range.contains(new Key(new Text("aab"))));
    assertTrue(range.beforeStartKey(new Key(new Text("aa"))));
  }

  @Test
  public void rangeLessThan() throws SerDeException {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPLessThan(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);

    Collection<Range> ranges = handler.getRanges(conf, columnMapper);
    assertEquals(ranges.size(), 1);
    Range range = ranges.iterator().next();
    assertTrue(range.isStartKeyInclusive());
    assertFalse(range.isEndKeyInclusive());
    assertFalse(range.contains(new Key(new Text("aaa"))));
    assertTrue(range.afterEndKey(new Key(new Text("ccccc"))));
    assertTrue(range.contains(new Key(new Text("aa"))));
    assertTrue(range.afterEndKey(new Key(new Text("aab"))));
    assertTrue(range.afterEndKey(new Key(new Text("aaa"))));
  }

  @Test
  public void rangeLessThanOrEqual() throws SerDeException {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrLessThan(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);

    Collection<Range> ranges = handler.getRanges(conf, columnMapper);
    assertEquals(ranges.size(), 1);
    Range range = ranges.iterator().next();
    assertTrue(range.isStartKeyInclusive());
    assertFalse(range.isEndKeyInclusive());
    assertTrue(range.contains(new Key(new Text("aaa"))));
    assertTrue(range.afterEndKey(new Key(new Text("ccccc"))));
    assertTrue(range.contains(new Key(new Text("aa"))));
    assertTrue(range.afterEndKey(new Key(new Text("aab"))));
    assertFalse(range.afterEndKey(new Key(new Text("aaa"))));
  }

  @Test
  public void testDisjointRanges() throws SerDeException {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrLessThan(), children);
    assertNotNull(node);

    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null,
        false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "bbb");
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPGreaterThan(), children2);
    assertNotNull(node2);

    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPAnd(), bothFilters);

    String filterExpr = Utilities.serializeExpression(both);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);

    Collection<Range> ranges = handler.getRanges(conf, columnMapper);

    // Impossible to get ranges for row <= 'aaa' and row >= 'bbb'
    assertEquals(0, ranges.size());
  }

  @Test
  public void testMultipleRanges() throws SerDeException {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), children);
    assertNotNull(node);

    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null,
        false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "bbb");
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPLessThan(), children2);
    assertNotNull(node2);

    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPAnd(), bothFilters);

    String filterExpr = Utilities.serializeExpression(both);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);

    List<Range> ranges = handler.getRanges(conf, columnMapper);
    assertEquals(1, ranges.size());
    Range range = ranges.get(0);
    assertEquals(new Range(new Key("aaa"), true, new Key("bbb"), false), range);
  }

  @Test
  public void testPushdownTuple() throws SerDeException, NoSuchPrimitiveComparisonException,
      NoSuchCompareOpException {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "field1", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 5);
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqual(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);

    List<IndexSearchCondition> sConditions = handler.getSearchConditions(conf);
    assertEquals(sConditions.size(), 1);
    IndexSearchCondition sc = sConditions.get(0);
    PushdownTuple tuple = new PushdownTuple(sConditions.get(0), handler.getPrimitiveComparison(sc
        .getColumnDesc().getTypeString(), sc), handler.getCompareOp(sc.getComparisonOp(), sc));
    byte[] expectedVal = new byte[4];
    ByteBuffer.wrap(expectedVal).putInt(5);
    assertArrayEquals(tuple.getConstVal(), expectedVal);
    assertEquals(tuple.getcOpt().getClass(), Equal.class);
    assertEquals(tuple.getpCompare().getClass(), IntCompare.class);
  }

  @Test(expected = NoSuchPrimitiveComparisonException.class)
  public void testPushdownColumnTypeNotSupported() throws SerDeException,
      NoSuchPrimitiveComparisonException, NoSuchCompareOpException {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "field1", null,
        false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.floatTypeInfo, 5.5f);
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqual(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    List<IndexSearchCondition> sConditions = handler.getSearchConditions(conf);
    assertEquals(sConditions.size(), 1);
    IndexSearchCondition sc = sConditions.get(0);

    handler.getPrimitiveComparison(sc.getColumnDesc().getTypeString(), sc);
  }

  @Test
  public void testPushdownComparisonOptNotSupported() {
    try {
      ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "field1", null,
          false);
      List<ExprNodeDesc> children = Lists.newArrayList();
      children.add(column);
      ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
          new GenericUDFOPNotNull(), children);
      assertNotNull(node);
      String filterExpr = Utilities.serializeExpression(node);
      conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
      List<IndexSearchCondition> sConditions = handler.getSearchConditions(conf);
      assertEquals(sConditions.size(), 1);
      IndexSearchCondition sc = sConditions.get(0);
      new PushdownTuple(sc, handler.getPrimitiveComparison(sc.getColumnDesc().getTypeString(), sc),
          handler.getCompareOp(sc.getComparisonOp(), sc));
      fail("Should fail: compare op not registered for index analyzer. Should leave undesirable residual predicate");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Unexpected residual predicate: field1 is not null"));
    } catch (Exception e) {
      fail(StringUtils.stringifyException(e));
    }
  }

  @Test
  public void testIteratorIgnoreRowIDFields() {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrLessThan(), children);
    assertNotNull(node);

    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null,
        false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "bbb");
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPGreaterThan(), children2);
    assertNotNull(node2);

    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPAnd(), bothFilters);

    String filterExpr = Utilities.serializeExpression(both);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    try {
      List<IteratorSetting> iterators = handler.getIterators(conf, columnMapper);
      assertEquals(iterators.size(), 0);
    } catch (SerDeException e) {
      StringUtils.stringifyException(e);
    }
  }

  @Test
  public void testIgnoreIteratorPushdown() throws TooManyAccumuloColumnsException {
    // Override what's placed in the Configuration by setup()
    conf = new JobConf();
    List<String> columnNames = Arrays.asList("field1", "field2", "rid");
    List<TypeInfo> columnTypes = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo);
    conf.set(serdeConstants.LIST_COLUMNS, Joiner.on(',').join(columnNames));
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "string,int,string");

    String columnMappingStr = "cf:f1,cf:f2,:rowID";
    conf.set(AccumuloSerDeParameters.COLUMN_MAPPINGS, columnMappingStr);
    columnMapper = new ColumnMapper(columnMappingStr, ColumnEncoding.STRING.getName(), columnNames,
        columnTypes);

    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "field1", null,
        false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrLessThan(), children);
    assertNotNull(node);

    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "field2", null,
        false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 5);
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPGreaterThan(), children2);
    assertNotNull(node2);

    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPAnd(), bothFilters);

    String filterExpr = Utilities.serializeExpression(both);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    conf.setBoolean(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY, false);
    try {
      List<IteratorSetting> iterators = handler.getIterators(conf, columnMapper);
      assertEquals(iterators.size(), 0);
    } catch (Exception e) {
      fail(StringUtils.stringifyException(e));
    }
  }

  @Test
  public void testCreateIteratorSettings() throws Exception {
    // Override what's placed in the Configuration by setup()
    conf = new JobConf();
    List<String> columnNames = Arrays.asList("field1", "field2", "rid");
    List<TypeInfo> columnTypes = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo);
    conf.set(serdeConstants.LIST_COLUMNS, Joiner.on(',').join(columnNames));
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "string,int,string");
    String columnMappingStr = "cf:f1,cf:f2,:rowID";
    conf.set(AccumuloSerDeParameters.COLUMN_MAPPINGS, columnMappingStr);
    columnMapper = new ColumnMapper(columnMappingStr, ColumnEncoding.STRING.getName(), columnNames,
        columnTypes);

    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "field1", null,
        false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPEqualOrLessThan(), children);
    assertNotNull(node);

    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "field2", null,
        false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 5);
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPGreaterThan(), children2);
    assertNotNull(node2);

    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPAnd(), bothFilters);

    String filterExpr = Utilities.serializeExpression(both);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    List<IteratorSetting> iterators = handler.getIterators(conf, columnMapper);
    assertEquals(iterators.size(), 2);
    IteratorSetting is1 = iterators.get(0);
    IteratorSetting is2 = iterators.get(1);

    boolean foundQual = false;
    boolean foundPCompare = false;
    boolean foundCOpt = false;
    boolean foundConst = false;
    for (Map.Entry<String,String> option : is1.getOptions().entrySet()) {
      String optKey = option.getKey();
      if (optKey.equals(PrimitiveComparisonFilter.COLUMN)) {
        foundQual = true;
        assertEquals(option.getValue(), "cf:f1");
      } else if (optKey.equals(PrimitiveComparisonFilter.CONST_VAL)) {
        foundConst = true;
        assertEquals(option.getValue(), new String(Base64.encodeBase64("aaa".getBytes())));
      } else if (optKey.equals(PrimitiveComparisonFilter.COMPARE_OPT_CLASS)) {
        foundCOpt = true;
        assertEquals(option.getValue(), LessThanOrEqual.class.getName());
      } else if (optKey.equals(PrimitiveComparisonFilter.P_COMPARE_CLASS)) {
        foundPCompare = true;
        assertEquals(option.getValue(), StringCompare.class.getName());
      }

    }
    assertTrue(foundConst & foundCOpt & foundPCompare & foundQual);

    foundQual = false;
    foundPCompare = false;
    foundCOpt = false;
    foundConst = false;
    for (Map.Entry<String,String> option : is2.getOptions().entrySet()) {
      String optKey = option.getKey();
      if (optKey.equals(PrimitiveComparisonFilter.COLUMN)) {
        foundQual = true;
        assertEquals(option.getValue(), "cf:f2");
      } else if (optKey.equals(PrimitiveComparisonFilter.CONST_VAL)) {
        foundConst = true;
        byte[] intVal = new byte[4];
        ByteBuffer.wrap(intVal).putInt(5);
        assertEquals(option.getValue(), new String(Base64.encodeBase64(intVal)));
      } else if (optKey.equals(PrimitiveComparisonFilter.COMPARE_OPT_CLASS)) {
        foundCOpt = true;
        assertEquals(option.getValue(), GreaterThan.class.getName());
      } else if (optKey.equals(PrimitiveComparisonFilter.P_COMPARE_CLASS)) {
        foundPCompare = true;
        assertEquals(option.getValue(), IntCompare.class.getName());
      }

    }
    assertTrue(foundConst & foundCOpt & foundPCompare & foundQual);
  }

  @Test
  public void testBasicOptLookup() throws NoSuchCompareOpException {
    boolean foundEqual = false;
    boolean foundNotEqual = false;
    boolean foundGreaterThanOrEqual = false;
    boolean foundGreaterThan = false;
    boolean foundLessThanOrEqual = false;
    boolean foundLessThan = false;
    for (String opt : handler.cOpKeyset()) {
      Class<? extends CompareOp> compOpt = handler.getCompareOpClass(opt);
      if (compOpt.getName().equals(Equal.class.getName())) {
        foundEqual = true;
      } else if (compOpt.getName().equals(NotEqual.class.getName())) {
        foundNotEqual = true;
      } else if (compOpt.getName().equals(GreaterThan.class.getName())) {
        foundGreaterThan = true;
      } else if (compOpt.getName().equals(GreaterThanOrEqual.class.getName())) {
        foundGreaterThanOrEqual = true;
      } else if (compOpt.getName().equals(LessThan.class.getName())) {
        foundLessThan = true;
      } else if (compOpt.getName().equals(LessThanOrEqual.class.getName())) {
        foundLessThanOrEqual = true;
      }
    }
    assertTrue("Did not find Equal comparison op", foundEqual);
    assertTrue("Did not find NotEqual comparison op", foundNotEqual);
    assertTrue("Did not find GreaterThan comparison op", foundGreaterThan);
    assertTrue("Did not find GreaterThanOrEqual comparison op", foundGreaterThanOrEqual);
    assertTrue("Did not find LessThan comparison op", foundLessThan);
    assertTrue("Did not find LessThanOrEqual comparison op", foundLessThanOrEqual);
  }

  @Test(expected = NoSuchCompareOpException.class)
  public void testNoOptFound() throws NoSuchCompareOpException {
    handler.getCompareOpClass("blah");
  }

  @Test
  public void testPrimitiveComparsionLookup() throws NoSuchPrimitiveComparisonException {
    boolean foundLong = false;
    boolean foundString = false;
    boolean foundInt = false;
    boolean foundDouble = false;
    for (String type : handler.pComparisonKeyset()) {
      Class<? extends PrimitiveComparison> pCompare = handler.getPrimitiveComparisonClass(type);
      if (pCompare.getName().equals(DoubleCompare.class.getName())) {
        foundDouble = true;
      } else if (pCompare.getName().equals(LongCompare.class.getName())) {
        foundLong = true;
      } else if (pCompare.getName().equals(IntCompare.class.getName())) {
        foundInt = true;
      } else if (pCompare.getName().equals(StringCompare.class.getName())) {
        foundString = true;
      }
    }
    assertTrue("Did not find DoubleCompare op", foundDouble);
    assertTrue("Did not find LongCompare op", foundLong);
    assertTrue("Did not find IntCompare op", foundInt);
    assertTrue("Did not find StringCompare op", foundString);
  }

  @Test
  public void testRowRangeIntersection() throws SerDeException {
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

    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPAnd(), bothFilters);

    String filterExpr = Utilities.serializeExpression(both);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);

    // Should make ['f', 'm\0')
    List<Range> ranges = handler.getRanges(conf, columnMapper);
    assertEquals(1, ranges.size());
    assertEquals(new Range(new Key("f"), true, new Key("m\0"), false), ranges.get(0));
  }

  @Test
  public void testRowRangeGeneration() throws SerDeException {
    List<String> columnNames = Arrays.asList("key", "column");
    List<TypeInfo> columnTypes = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo);
    conf.set(serdeConstants.LIST_COLUMNS, Joiner.on(',').join(columnNames));
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "string,string");

    String columnMappingStr = ":rowID,cf:f1";
    conf.set(AccumuloSerDeParameters.COLUMN_MAPPINGS, columnMappingStr);
    columnMapper = new ColumnMapper(columnMappingStr, ColumnEncoding.STRING.getName(), columnNames,
        columnTypes);

    // 100 < key
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "key", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 100);
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(constant);
    children.add(column);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPLessThan(), children);
    assertNotNull(node);

    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);

    // Should make (100, +inf)
    List<Range> ranges = handler.getRanges(conf, columnMapper);
    Assert.assertEquals(1, ranges.size());
    Assert.assertEquals(new Range(new Text("100"), false, null, false), ranges.get(0));
  }

  @Test
  public void testBinaryRangeGeneration() throws Exception {
    List<String> columnNames = Arrays.asList("key", "column");
    List<TypeInfo> columnTypes = Arrays.<TypeInfo> asList(TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.stringTypeInfo);
    conf.set(serdeConstants.LIST_COLUMNS, Joiner.on(',').join(columnNames));
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "int,string");

    String columnMappingStr = ":rowID#b,cf:f1";
    conf.set(AccumuloSerDeParameters.COLUMN_MAPPINGS, columnMappingStr);
    columnMapper = new ColumnMapper(columnMappingStr, ColumnEncoding.STRING.getName(), columnNames,
        columnTypes);

    int intValue = 100;

    // Make binary integer value in the bytearray
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JavaIntObjectInspector intOI = (JavaIntObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME));
    LazyUtils.writePrimitive(baos, intValue, intOI);

    // 100 < key
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "key", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, intValue);
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(constant);
    children.add(column);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFOPLessThan(), children);
    assertNotNull(node);

    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);

    // Should make (100, +inf)
    List<Range> ranges = handler.getRanges(conf, columnMapper);
    Assert.assertEquals(1, ranges.size());
    Assert.assertEquals(new Range(new Text(baos.toByteArray()), false, null, false), ranges.get(0));
  }

  @Test
  public void testNullRangeGeneratorOutput() throws SerDeException {
    // The AccumuloRangeGenerator produces an Object (due to the limitations of the
    // traversal interface) which requires interpretation of that Object into Ranges.
    // Changes in the return object from the AccumuloRangeGenerator must also represent
    // a change in the AccumuloPredicateHandler.
    AccumuloPredicateHandler mockHandler = Mockito.mock(AccumuloPredicateHandler.class);
    ExprNodeDesc root = Mockito.mock(ExprNodeDesc.class);
    String hiveRowIdColumnName = "rid";

    Mockito.when(mockHandler.getRanges(conf, columnMapper)).thenCallRealMethod();
    Mockito.when(mockHandler.generateRanges(columnMapper, hiveRowIdColumnName, root)).thenReturn(null);
    Mockito.when(mockHandler.getExpression(conf)).thenReturn(root);

    // A null result from AccumuloRangeGenerator is all ranges
    Assert.assertEquals(Arrays.asList(new Range()), mockHandler.getRanges(conf, columnMapper));
  }

  @Test
  public void testEmptyListRangeGeneratorOutput() throws SerDeException {
    // The AccumuloRangeGenerator produces an Object (due to the limitations of the
    // traversal interface) which requires interpretation of that Object into Ranges.
    // Changes in the return object from the AccumuloRangeGenerator must also represent
    // a change in the AccumuloPredicateHandler.
    AccumuloPredicateHandler mockHandler = Mockito.mock(AccumuloPredicateHandler.class);
    ExprNodeDesc root = Mockito.mock(ExprNodeDesc.class);
    String hiveRowIdColumnName = "rid";

    Mockito.when(mockHandler.getRanges(conf, columnMapper)).thenCallRealMethod();
    Mockito.when(mockHandler.generateRanges(columnMapper, hiveRowIdColumnName, root)).thenReturn(Collections.emptyList());
    Mockito.when(mockHandler.getExpression(conf)).thenReturn(root);

    // A null result from AccumuloRangeGenerator is all ranges
    Assert.assertEquals(Collections.emptyList(), mockHandler.getRanges(conf, columnMapper));
  }

  @Test
  public void testSingleRangeGeneratorOutput() throws SerDeException {
    // The AccumuloRangeGenerator produces an Object (due to the limitations of the
    // traversal interface) which requires interpretation of that Object into Ranges.
    // Changes in the return object from the AccumuloRangeGenerator must also represent
    // a change in the AccumuloPredicateHandler.
    AccumuloPredicateHandler mockHandler = Mockito.mock(AccumuloPredicateHandler.class);
    ExprNodeDesc root = Mockito.mock(ExprNodeDesc.class);
    String hiveRowIdColumnName = "rid";
    Range r = new Range("a");

    Mockito.when(mockHandler.getRanges(conf, columnMapper)).thenCallRealMethod();
    Mockito.when(mockHandler.generateRanges(columnMapper, hiveRowIdColumnName, root)).thenReturn(r);
    Mockito.when(mockHandler.getExpression(conf)).thenReturn(root);

    // A null result from AccumuloRangeGenerator is all ranges
    Assert.assertEquals(Collections.singletonList(r), mockHandler.getRanges(conf, columnMapper));
  }

  @Test
  public void testManyRangesGeneratorOutput() throws SerDeException {
    // The AccumuloRangeGenerator produces an Object (due to the limitations of the
    // traversal interface) which requires interpretation of that Object into Ranges.
    // Changes in the return object from the AccumuloRangeGenerator must also represent
    // a change in the AccumuloPredicateHandler.
    AccumuloPredicateHandler mockHandler = Mockito.mock(AccumuloPredicateHandler.class);
    ExprNodeDesc root = Mockito.mock(ExprNodeDesc.class);
    String hiveRowIdColumnName = "rid";
    Range r1 = new Range("a"), r2 = new Range("z");

    Mockito.when(mockHandler.getRanges(conf, columnMapper)).thenCallRealMethod();
    Mockito.when(mockHandler.generateRanges(columnMapper, hiveRowIdColumnName, root)).thenReturn(Arrays.asList(r1, r2));
    Mockito.when(mockHandler.getExpression(conf)).thenReturn(root);

    // A null result from AccumuloRangeGenerator is all ranges
    Assert.assertEquals(Arrays.asList(r1, r2), mockHandler.getRanges(conf, columnMapper));
  }
}
