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

package org.apache.hadoop.hive.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertNotNull;

/**
 * Test Class for Kafka Trimmer Class.
 */
public class KafkaScanTrimmerTest {
  private static final Path PATH = new Path("/tmp");

  private final ExprNodeDesc zeroInt = ConstantExprBuilder.build(0);
  private final ExprNodeDesc threeInt = ConstantExprBuilder.build(3);
  private final ExprNodeDesc thirtyLong = ConstantExprBuilder.build(30L);
  private final ExprNodeDesc thirtyFiveLong = ConstantExprBuilder.build(35L);
  private final ExprNodeDesc seventyFiveLong = ConstantExprBuilder.build(75L);
  private final ExprNodeDesc fortyLong = ConstantExprBuilder.build(40L);

  private ExprNodeDesc
      partitionColumn =
      new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo,
          KafkaStreamingUtils.MetadataColumn.PARTITION.getName(),
          null,
          false);
  private ExprNodeDesc
      offsetColumn =
      new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo,
          KafkaStreamingUtils.MetadataColumn.OFFSET.getName(),
          null,
          false);

  private String topic = "my_topic";
  private Map<TopicPartition, KafkaPullerInputSplit>
      fullHouse =
      ImmutableMap.of(new TopicPartition(topic, 0),
          new KafkaPullerInputSplit(topic, 0, 0, 45, PATH),
          new TopicPartition(topic, 1),
          new KafkaPullerInputSplit(topic, 1, 5, 1005, PATH),
          new TopicPartition(topic, 2),
          new KafkaPullerInputSplit(topic, 2, 9, 100, PATH),
          new TopicPartition(topic, 3),
          new KafkaPullerInputSplit(topic, 3, 0, 100, PATH));

  @Test public void computeOptimizedScanPartitionBinaryOpFilter() {
    KafkaScanTrimmer kafkaScanTrimmer = new KafkaScanTrimmer(fullHouse, null);
    int partitionId = 2;
    ExprNodeDesc constant = ConstantExprBuilder.build(partitionId);
    final List<ExprNodeDesc> children = Lists.newArrayList(partitionColumn, constant);

    ExprNodeGenericFuncDesc node = eq(children);
    assertNotNull(node);

    Map
        actual =
        kafkaScanTrimmer.computeOptimizedScan(SerializationUtilities
            .deserializeExpression(SerializationUtilities.serializeExpression(node)));
    Map expected = Maps.filterValues(fullHouse, tp -> Objects.requireNonNull(tp).getPartition() == partitionId);
    Assert.assertEquals(expected, actual);

    ExprNodeGenericFuncDesc lessNode = lessThan(children);
    assertNotNull(lessNode);
    actual =
        kafkaScanTrimmer.computeOptimizedScan(SerializationUtilities
            .deserializeExpression(SerializationUtilities.serializeExpression(lessNode)));
    expected = Maps.filterValues(fullHouse, tp -> Objects.requireNonNull(tp).getPartition() < partitionId);
    Assert.assertEquals(expected, actual);

    ExprNodeGenericFuncDesc lessEqNode = lessThanEq(children);

    assertNotNull(lessEqNode);
    actual =
        kafkaScanTrimmer.computeOptimizedScan(SerializationUtilities
            .deserializeExpression(SerializationUtilities.serializeExpression(lessEqNode)));
    expected = Maps.filterValues(fullHouse, tp -> Objects.requireNonNull(tp).getPartition() <= partitionId);
    Assert.assertEquals(expected, actual);

  }

  @Test public void computeOptimizedScanFalseFilter() {
    KafkaScanTrimmer kafkaScanTrimmer = new KafkaScanTrimmer(fullHouse, null);
    ExprNodeGenericFuncDesc
        falseFilter =
        and(Lists.newArrayList(eq(Lists.newArrayList(partitionColumn, zeroInt)),
            eq(Lists.newArrayList(partitionColumn, threeInt))));

    assertNotNull(falseFilter);
    Map
        actual =
        kafkaScanTrimmer.computeOptimizedScan(SerializationUtilities
            .deserializeExpression(SerializationUtilities.serializeExpression(falseFilter)));
    Assert.assertTrue(actual.isEmpty());

    ExprNodeGenericFuncDesc
        falseFilter2 =
        and(Lists.newArrayList(eq(Lists.newArrayList(offsetColumn, thirtyFiveLong)),
            eq(Lists.newArrayList(offsetColumn, fortyLong))));

    assertNotNull(falseFilter2);
    actual =
        kafkaScanTrimmer.computeOptimizedScan(SerializationUtilities
            .deserializeExpression(SerializationUtilities.serializeExpression(falseFilter2)));
    Assert.assertTrue(actual.isEmpty());

    ExprNodeGenericFuncDesc filter3 = or(Lists.newArrayList(falseFilter, falseFilter2));

    assertNotNull(filter3);
    actual =
        kafkaScanTrimmer.computeOptimizedScan(SerializationUtilities
            .deserializeExpression(SerializationUtilities.serializeExpression(filter3)));
    Assert.assertTrue(actual.isEmpty());

    ExprNodeGenericFuncDesc
        filter4 =
        and(Lists.newArrayList(filter3, eq(Lists.newArrayList(partitionColumn, zeroInt))));
    assertNotNull(filter4);
    actual =
        kafkaScanTrimmer.computeOptimizedScan(SerializationUtilities
            .deserializeExpression(SerializationUtilities.serializeExpression(filter4)));
    Assert.assertTrue(actual.isEmpty());
  }

  @Test public void computeOptimizedScanOrAndCombinedFilter() {
    KafkaScanTrimmer kafkaScanTrimmer = new KafkaScanTrimmer(fullHouse, null);
    // partition = 0 and 30 <= offset < 35 or partition = 3 and 35 <= offset < 75  or (partition = 0 and offset = 40)

    ExprNodeGenericFuncDesc
        part1 =
        and(Lists.newArrayList(greaterThanEq(Lists.newArrayList(offsetColumn, thirtyLong)),
            eq(Lists.newArrayList(partitionColumn, zeroInt)),
            lessThan(Lists.newArrayList(offsetColumn, thirtyFiveLong))));

    ExprNodeGenericFuncDesc
        part2 =
        and(Lists.newArrayList(greaterThanEq(Lists.newArrayList(offsetColumn, thirtyFiveLong)),
            eq(Lists.newArrayList(partitionColumn, threeInt)),
            lessThan(Lists.newArrayList(offsetColumn, seventyFiveLong))));

    ExprNodeGenericFuncDesc
        part3 =
        and(Lists.newArrayList(eq(Lists.newArrayList(offsetColumn, fortyLong)),
            eq(Lists.newArrayList(partitionColumn, zeroInt))));

    ExprNodeGenericFuncDesc orExpression = or(Lists.newArrayList(part1, part2, part3));

    assertNotNull(orExpression);
    Map
        actual =
        kafkaScanTrimmer.computeOptimizedScan(SerializationUtilities
            .deserializeExpression(SerializationUtilities.serializeExpression(orExpression)));
    TopicPartition tpZero = new TopicPartition(topic, 0);
    TopicPartition toThree = new TopicPartition(topic, 3);
    KafkaPullerInputSplit split1 = new KafkaPullerInputSplit(topic, 0, 30, 41, PATH);
    KafkaPullerInputSplit split2 = new KafkaPullerInputSplit(topic, 3, 35, 75, PATH);

    Map expected = ImmutableMap.of(tpZero, split1, toThree, split2);
    Assert.assertEquals(expected, actual);

  }

  @Test public void computeOptimizedScanPartitionOrAndCombinedFilter() {
    KafkaScanTrimmer kafkaScanTrimmer = new KafkaScanTrimmer(fullHouse, null);

    // partition = 1 or (partition >2 and <= 3)
    ExprNodeGenericFuncDesc eq = eq(Lists.newArrayList(partitionColumn, ConstantExprBuilder.build(1)));
    ExprNodeGenericFuncDesc lessEq = lessThanEq(Lists.newArrayList(partitionColumn, ConstantExprBuilder.build(3)));
    ExprNodeGenericFuncDesc greater = greaterThan(Lists.newArrayList(partitionColumn, ConstantExprBuilder.build(2)));
    ExprNodeGenericFuncDesc orNode = or(Lists.newArrayList(and(Lists.newArrayList(lessEq, greater)), eq));

    Map
        actual =
        kafkaScanTrimmer.computeOptimizedScan(SerializationUtilities
            .deserializeExpression(SerializationUtilities.serializeExpression(orNode)));
    Map
        expected =
        Maps.filterValues(fullHouse, tp -> Objects.requireNonNull(tp).getPartition() == 1 || tp.getPartition() == 3);
    Assert.assertEquals(expected, actual);
    assertNotNull(orNode);
  }

  @Test public void buildScanFormPartitionPredicateEq() {
    Map
        actual =
        KafkaScanTrimmer.buildScanFromPartitionPredicate(fullHouse, PredicateLeaf.Operator.EQUALS, 3, false, false);
    TopicPartition topicPartition = new TopicPartition(topic, 3);
    Assert.assertEquals(fullHouse.get(topicPartition), actual.get(topicPartition));
  }

  @Test public void buildScanFormPartitionPredicateLess() {
    // partitionConst < partitionColumn (flip true)
    int partitionConst = 2;
    Map
        actual =
        KafkaScanTrimmer.buildScanFromPartitionPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN,
            partitionConst,
            true,
            false);

    Map
        expected =
        Maps.filterEntries(fullHouse, entry -> Objects.requireNonNull(entry).getKey().partition() > partitionConst);
    Assert.assertEquals(expected, actual);
    Assert.assertFalse(actual.isEmpty());

    // partitionConst >= partitionColumn (flip true, negation true)
    actual =
        KafkaScanTrimmer.buildScanFromPartitionPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN,
            partitionConst,
            true,
            true);

    expected =
        Maps.filterEntries(fullHouse, entry -> partitionConst >= Objects.requireNonNull(entry).getKey().partition());
    Assert.assertEquals(expected, actual);
    Assert.assertFalse(actual.isEmpty());

    // partitionColumn >= partitionConst (negation true)
    actual =
        KafkaScanTrimmer.buildScanFromPartitionPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN,
            partitionConst,
            false,
            true);

    expected =
        Maps.filterEntries(fullHouse, entry -> Objects.requireNonNull(entry).getKey().partition() >= partitionConst);
    Assert.assertEquals(expected, actual);
    Assert.assertFalse(actual.isEmpty());

    // partitionColumn < partitionConst (negation true)
    actual =
        KafkaScanTrimmer.buildScanFromPartitionPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN,
            partitionConst,
            false,
            false);

    expected =
        Maps.filterEntries(fullHouse, entry -> Objects.requireNonNull(entry).getKey().partition() < partitionConst);
    Assert.assertEquals(expected, actual);
    Assert.assertFalse(actual.isEmpty());
  }

  @Test public void buildScanFormPartitionPredicateLessEq() {
    // partitionConst <= partitionColumn (flip true)
    int partitionConst = 2;
    Map
        actual =
        KafkaScanTrimmer.buildScanFromPartitionPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN_EQUALS,
            partitionConst,
            true,
            false);

    Map
        expected =
        Maps.filterEntries(fullHouse, entry -> Objects.requireNonNull(entry).getKey().partition() >= partitionConst);
    Assert.assertEquals(expected, actual);
    Assert.assertFalse(actual.isEmpty());

    // partitionConst > partitionColumn (flip true, negation true)
    actual =
        KafkaScanTrimmer.buildScanFromPartitionPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN_EQUALS,
            partitionConst,
            true,
            true);

    expected =
        Maps.filterEntries(fullHouse, entry -> partitionConst > Objects.requireNonNull(entry).getKey().partition());
    Assert.assertEquals(expected, actual);
    Assert.assertFalse(actual.isEmpty());

    // partitionColumn > partitionConst (negation true)
    actual =
        KafkaScanTrimmer.buildScanFromPartitionPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN_EQUALS,
            partitionConst,
            false,
            true);

    expected =
        Maps.filterEntries(fullHouse, entry -> Objects.requireNonNull(entry).getKey().partition() > partitionConst);
    Assert.assertEquals(expected, actual);
    Assert.assertFalse(actual.isEmpty());

    // partitionColumn <= partitionConst (negation true)
    actual =
        KafkaScanTrimmer.buildScanFromPartitionPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN_EQUALS,
            partitionConst,
            false,
            false);

    expected =
        Maps.filterEntries(fullHouse, entry -> Objects.requireNonNull(entry).getKey().partition() <= partitionConst);
    Assert.assertEquals(expected, actual);
    Assert.assertFalse(actual.isEmpty());
  }

  @Test public void buildScanFromOffsetPredicateEq() {
    long constantOffset = 30;
    Map
        actual =
        KafkaScanTrimmer.buildScanFromOffsetPredicate(fullHouse,
            PredicateLeaf.Operator.EQUALS,
            constantOffset,
            false,
            false);
    Map
        expected =
        Maps.transformValues(fullHouse,
            entry -> new KafkaPullerInputSplit(Objects.requireNonNull(entry).getTopic(),
                entry.getPartition(),
                constantOffset,
                constantOffset + 1,
                entry.getPath()));

    Assert.assertEquals(expected, actual);

    // seek to end if offset is out of reach
    actual =
        KafkaScanTrimmer.buildScanFromOffsetPredicate(fullHouse, PredicateLeaf.Operator.EQUALS, 3000000L, false, false);
    expected =
        Maps.transformValues(fullHouse,
            entry -> new KafkaPullerInputSplit(Objects.requireNonNull(entry).getTopic(),
                entry.getPartition(),
                entry.getEndOffset(),
                entry.getEndOffset(),
                entry.getPath()));
    Assert.assertEquals(expected, actual);

    // seek to end if offset is out of reach
    actual = KafkaScanTrimmer.buildScanFromOffsetPredicate(fullHouse, PredicateLeaf.Operator.EQUALS, 0L, false, false);

    expected =
        Maps.transformValues(fullHouse,
            entry -> new KafkaPullerInputSplit(Objects.requireNonNull(entry).getTopic(),
                entry.getPartition(),
                entry.getStartOffset() > 0 ? entry.getEndOffset() : 0,
                entry.getStartOffset() > 0 ? entry.getEndOffset() : 1,
                entry.getPath()));
    Assert.assertEquals(expected, actual);

  }

  @Test public void buildScanFromOffsetPredicateLess() {
    long constantOffset = 50;
    // columnOffset < constant
    Map
        actual =
        KafkaScanTrimmer.buildScanFromOffsetPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN,
            constantOffset,
            false,
            false);

    Map
        expected =
        Maps.transformValues(fullHouse,
            entry -> new KafkaPullerInputSplit(Objects.requireNonNull(entry).getTopic(),
                entry.getPartition(),
                entry.getStartOffset(),
                Math.min(constantOffset, entry.getEndOffset()),
                entry.getPath()));
    Assert.assertEquals(expected, actual);

    // columnOffset > constant
    actual =
        KafkaScanTrimmer.buildScanFromOffsetPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN,
            constantOffset,
            true,
            false);

    expected =
        Maps.transformValues(fullHouse,
            entry -> new KafkaPullerInputSplit(Objects.requireNonNull(entry).getTopic(),
                entry.getPartition(),
                Math.min(entry.getEndOffset(), Math.max(entry.getStartOffset(), constantOffset + 1)),
                entry.getEndOffset(),
                entry.getPath()));
    Assert.assertEquals(expected, actual);

    // columnOffset >= constant
    actual =
        KafkaScanTrimmer.buildScanFromOffsetPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN,
            constantOffset,
            false,
            true);

    expected =
        Maps.transformValues(fullHouse,
            entry -> new KafkaPullerInputSplit(Objects.requireNonNull(entry).getTopic(),
                entry.getPartition(),
                Math.min(entry.getEndOffset(), Math.max(entry.getStartOffset(), constantOffset)),
                entry.getEndOffset(),
                entry.getPath()));
    Assert.assertEquals(expected, actual);

    // columnOffset <= constant
    actual =
        KafkaScanTrimmer.buildScanFromOffsetPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN,
            constantOffset,
            true,
            true);

    expected =
        Maps.transformValues(fullHouse,
            entry -> new KafkaPullerInputSplit(Objects.requireNonNull(entry).getTopic(),
                entry.getPartition(),
                entry.getStartOffset(),
                Math.min(constantOffset + 1, entry.getEndOffset()),
                entry.getPath()));
    Assert.assertEquals(expected, actual);

  }

  @Test public void buildScanFromOffsetPredicateLessEq() {
    long constantOffset = 50;
    // columnOffset < constant
    Map
        actual =
        KafkaScanTrimmer.buildScanFromOffsetPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN_EQUALS,
            constantOffset,
            false,
            false);

    Map
        expected =
        Maps.transformValues(fullHouse,
            entry -> new KafkaPullerInputSplit(Objects.requireNonNull(entry).getTopic(),
                entry.getPartition(),
                entry.getStartOffset(),
                Math.min(constantOffset + 1, entry.getEndOffset()),
                entry.getPath()));
    Assert.assertEquals(expected, actual);

    // columnOffset >= constant
    actual =
        KafkaScanTrimmer.buildScanFromOffsetPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN_EQUALS,
            constantOffset,
            true,
            false);

    expected =
        Maps.transformValues(fullHouse,
            entry -> new KafkaPullerInputSplit(Objects.requireNonNull(entry).getTopic(),
                entry.getPartition(),
                Math.min(entry.getEndOffset(), Math.max(entry.getStartOffset(), constantOffset)),
                entry.getEndOffset(),
                entry.getPath()));
    Assert.assertEquals(expected, actual);

    // columnOffset > constant
    actual =
        KafkaScanTrimmer.buildScanFromOffsetPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN_EQUALS,
            constantOffset,
            false,
            true);

    expected =
        Maps.transformValues(fullHouse,
            entry -> new KafkaPullerInputSplit(Objects.requireNonNull(entry).getTopic(),
                entry.getPartition(),
                Math.min(entry.getEndOffset(), Math.max(entry.getStartOffset(), constantOffset + 1)),
                entry.getEndOffset(),
                entry.getPath()));
    Assert.assertEquals(expected, actual);

    // columnOffset < constant
    actual =
        KafkaScanTrimmer.buildScanFromOffsetPredicate(fullHouse,
            PredicateLeaf.Operator.LESS_THAN_EQUALS,
            constantOffset,
            true,
            true);

    expected =
        Maps.transformValues(fullHouse,
            entry -> new KafkaPullerInputSplit(Objects.requireNonNull(entry).getTopic(),
                entry.getPartition(),
                entry.getStartOffset(),
                Math.min(constantOffset, entry.getEndOffset()),
                entry.getPath()));
    Assert.assertEquals(expected, actual);
  }

  private static class ConstantExprBuilder {
    static ExprNodeDesc build(long constant) {
      return new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, constant);
    }

    static ExprNodeDesc build(int constant) {
      return new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, constant);
    }
  }

  private static ExprNodeGenericFuncDesc or(List<ExprNodeDesc> children) {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFOPOr(), children);
  }

  private static ExprNodeGenericFuncDesc and(List<ExprNodeDesc> children) {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFOPAnd(), children);
  }

  private static ExprNodeGenericFuncDesc eq(List<ExprNodeDesc> children) {
    return new ExprNodeGenericFuncDesc(children.get(0).getTypeInfo(), new GenericUDFOPEqual(), children);
  }

  private static ExprNodeGenericFuncDesc lessThan(List<ExprNodeDesc> children) {
    return new ExprNodeGenericFuncDesc(children.get(0).getTypeInfo(), new GenericUDFOPLessThan(), children);
  }

  private static ExprNodeGenericFuncDesc lessThanEq(List<ExprNodeDesc> children) {
    return new ExprNodeGenericFuncDesc(children.get(0).getTypeInfo(), new GenericUDFOPEqualOrLessThan(), children);
  }

  private static ExprNodeGenericFuncDesc greaterThan(List<ExprNodeDesc> children) {
    return new ExprNodeGenericFuncDesc(children.get(0).getTypeInfo(), new GenericUDFOPGreaterThan(), children);
  }

  private static ExprNodeGenericFuncDesc greaterThanEq(List<ExprNodeDesc> children) {
    return new ExprNodeGenericFuncDesc(children.get(0).getTypeInfo(), new GenericUDFOPEqualOrGreaterThan(), children);
  }
}
