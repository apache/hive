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

package org.apache.hadoop.hive.ql.udf.ptf;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.exec.BoundaryCache;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.OrderExpressionDef;
import org.apache.hadoop.io.IntWritable;

import com.google.common.collect.Lists;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order.ASC;
import static org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order.DESC;
import static org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec.UNBOUNDED_AMOUNT;
import static org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction.CURRENT;
import static org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction.FOLLOWING;
import static org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction.PRECEDING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests BoundaryCache used for RANGE windows in PTF functions.
 */
public class TestBoundaryCache {

  private static final Logger LOG = LoggerFactory.getLogger(TestBoundaryCache.class);
  private static final LinkedList<List<IntWritable>> TEST_PARTITION = new LinkedList<>();
  //Null for using no cache at all, 2 is minimum cache length, 5-9-15 for checking with smaller,
  // exactly equal and larger cache than needed.
  private static final List<Integer> CACHE_SIZES = Lists.newArrayList(null, 2, 5, 9, 15);
  private static final List<PTFInvocationSpec.Order> ORDERS = Lists.newArrayList(ASC, DESC);
  private static final int ORDER_BY_COL = 2;

  @BeforeClass
  public static void setupTests() throws Exception {
    //8 ranges, max cache content is 8+1=9 entries
    addRow(TEST_PARTITION, 1, 1, -7);
    addRow(TEST_PARTITION, 2, 1, -1);
    addRow(TEST_PARTITION, 3, 1, -1);
    addRow(TEST_PARTITION, 4, 1, 1);
    addRow(TEST_PARTITION, 5, 1, 1);
    addRow(TEST_PARTITION, 6, 1, 1);
    addRow(TEST_PARTITION, 7, 1, 1);
    addRow(TEST_PARTITION, 8, 1, 2);
    addRow(TEST_PARTITION, 9, 1, 2);
    addRow(TEST_PARTITION, 10, 1, 2);
    addRow(TEST_PARTITION, 11, 1, 2);
    addRow(TEST_PARTITION, 12, 1, 3);
    addRow(TEST_PARTITION, 13, 1, 5);
    addRow(TEST_PARTITION, 14, 1, 5);
    addRow(TEST_PARTITION, 15, 1, 5);
    addRow(TEST_PARTITION, 16, 1, 5);
    addRow(TEST_PARTITION, 17, 1, 6);
    addRow(TEST_PARTITION, 18, 1, 6);
    addRow(TEST_PARTITION, 19, 1, 9);
    addRow(TEST_PARTITION, 20, 1, null);
    addRow(TEST_PARTITION, 21, 1, null);

  }

  @Test
  public void testPrecedingUnboundedFollowingUnbounded() throws Exception {
    runTest(PRECEDING, UNBOUNDED_AMOUNT, FOLLOWING, UNBOUNDED_AMOUNT);
  }

  @Test
  public void testPrecedingUnboundedCurrentRow() throws Exception {
    runTest(PRECEDING, UNBOUNDED_AMOUNT, CURRENT, 0);
  }

  @Test
  public void testPrecedingUnboundedPreceding2() throws Exception {
    runTest(PRECEDING, UNBOUNDED_AMOUNT, PRECEDING, 2);
  }

  @Test
  public void testPreceding4Preceding1() throws Exception {
    runTest(PRECEDING, 4, PRECEDING, 1);
  }

  @Test
  public void testPreceding2CurrentRow() throws Exception {
    runTest(PRECEDING, 2, CURRENT, 0);
  }

  @Test
  public void testPreceding2Following100() throws Exception {
    runTest(PRECEDING, 1, FOLLOWING, 100);
  }

  @Test
  public void testCurrentRowFollowing3() throws Exception {
    runTest(CURRENT, 0, FOLLOWING, 3);
  }

  @Test
  public void testCurrentRowFFollowingUnbounded() throws Exception {
    runTest(CURRENT, 0, FOLLOWING, UNBOUNDED_AMOUNT);
  }

  @Test
  public void testFollowing2Following4() throws Exception {
    runTest(FOLLOWING, 2, FOLLOWING, 4);
  }

  @Test
  public void testFollowing2FollowingUnbounded() throws Exception {
    runTest(FOLLOWING, 2, FOLLOWING, UNBOUNDED_AMOUNT);
  }

  /**
   * Executes test on a given window definition. Such a test will be executed against the values set
   * in ORDERS and CACHE_SIZES, validating ORDERS X CACHE_SIZES test cases. Cache size of null will
   * be used to setup baseline.
   * @param startDirection
   * @param startAmount
   * @param endDirection
   * @param endAmount
   * @throws Exception
   */
  private void runTest(WindowingSpec.Direction startDirection, int startAmount,
                       WindowingSpec.Direction endDirection, int endAmount) throws Exception {

    BoundaryDef startBoundary = new BoundaryDef(startDirection, startAmount);
    BoundaryDef endBoundary = new BoundaryDef(endDirection, endAmount);
    AtomicInteger readCounter = new AtomicInteger(0);

    int[] expectedBoundaryStarts = new int[TEST_PARTITION.size()];
    int[] expectedBoundaryEnds = new int[TEST_PARTITION.size()];
    int expectedReadCountWithoutCache = -1;

    for (PTFInvocationSpec.Order order : ORDERS) {
      for (Integer cacheSize : CACHE_SIZES) {
        LOG.info(Thread.currentThread().getStackTrace()[2].getMethodName());
        LOG.info("Cache: " + cacheSize + " order: " + order);
        BoundaryCache cache = cacheSize == null ? null : new BoundaryCache(cacheSize);
        Pair<PTFPartition, ValueBoundaryScanner> mocks = setupMocks(TEST_PARTITION,
                ORDER_BY_COL, startBoundary, endBoundary, order, cache, readCounter);
        PTFPartition ptfPartition = mocks.getLeft();
        ValueBoundaryScanner scanner = mocks.getRight();
        for (int i = 0; i < TEST_PARTITION.size(); ++i) {
          scanner.handleCache(i, ptfPartition);
          int start = scanner.computeStart(i, ptfPartition);
          int end = scanner.computeEnd(i, ptfPartition) - 1;
          if (cache == null) {
            //Cache-less version should be baseline
            expectedBoundaryStarts[i] = start;
            expectedBoundaryEnds[i] = end;
          } else {
            assertEquals(expectedBoundaryStarts[i], start);
            assertEquals(expectedBoundaryEnds[i], end);
          }
          Integer col0 = ofNullable(TEST_PARTITION.get(i).get(0)).map(v -> v.get()).orElse(null);
          Integer col1 = ofNullable(TEST_PARTITION.get(i).get(1)).map(v -> v.get()).orElse(null);
          Integer col2 = ofNullable(TEST_PARTITION.get(i).get(2)).map(v -> v.get()).orElse(null);
          LOG.info(String.format("%d|\t%d\t%d\t%d\t|%d-%d", i, col0, col1, col2, start, end));
        }
        if (cache == null) {
          expectedReadCountWithoutCache = readCounter.get();
        } else {
          //Read count should be smaller with cache being used, but larger than the minimum of
          // reading every row once.
          assertTrue(expectedReadCountWithoutCache >= readCounter.get());
          if (startAmount != UNBOUNDED_AMOUNT || endAmount != UNBOUNDED_AMOUNT) {
            assertTrue(TEST_PARTITION.size() <= readCounter.get());
          }
        }
        readCounter.set(0);
      }
    }
  }

  /**
   * Sets up mock and spy objects used for testing.
   * @param partition The real partition containing row values.
   * @param orderByCol Index of column in the row used for separating ranges.
   * @param start Window definition.
   * @param end Window definition.
   * @param order Window definition.
   * @param cache BoundaryCache instance, it may come in various sizes.
   * @param readCounter counts how many times reading was invoked
   * @return Mocked PTFPartition instance and ValueBoundaryScanner spy.
   * @throws Exception
   */
  private static Pair<PTFPartition, ValueBoundaryScanner> setupMocks(
          List<List<IntWritable>> partition, int orderByCol, BoundaryDef start, BoundaryDef end,
          PTFInvocationSpec.Order order, BoundaryCache cache,
          AtomicInteger readCounter) throws Exception {
    PTFPartition partitionMock = mock(PTFPartition.class);
    doAnswer(invocationOnMock -> {
      int idx = invocationOnMock.getArgumentAt(0, Integer.class);
      return partition.get(idx);
    }).when(partitionMock).getAt(any(Integer.class));
    doAnswer(invocationOnMock -> {
      return partition.size();
    }).when(partitionMock).size();
    when(partitionMock.getBoundaryCache()).thenReturn(cache);

    OrderExpressionDef orderDef = mock(OrderExpressionDef.class);
    when(orderDef.getOrder()).thenReturn(order);

    ValueBoundaryScanner scan = new LongValueBoundaryScanner(start, end, orderDef, order == ASC);
    ValueBoundaryScanner scannerSpy = spy(scan);
    doAnswer(invocationOnMock -> {
      readCounter.incrementAndGet();
      List<IntWritable> row = invocationOnMock.getArgumentAt(0, List.class);
      return row.get(orderByCol);
    }).when(scannerSpy).computeValue(any(Object.class));
    doAnswer(invocationOnMock -> {
      IntWritable v1 = invocationOnMock.getArgumentAt(0, IntWritable.class);
      IntWritable v2 = invocationOnMock.getArgumentAt(1, IntWritable.class);
      return (v1 != null && v2 != null) ? v1.get() == v2.get() : v1 == null && v2 == null;
    }).when(scannerSpy).isEqual(any(Object.class), any(Object.class));
    doAnswer(invocationOnMock -> {
      IntWritable v1 = invocationOnMock.getArgumentAt(0, IntWritable.class);
      IntWritable v2 = invocationOnMock.getArgumentAt(1, IntWritable.class);
      Integer amt = invocationOnMock.getArgumentAt(2, Integer.class);
      return (v1 != null && v2 != null) ? (v1.get() - v2.get()) > amt :  v1 != null || v2 != null;
    }).when(scannerSpy).isDistanceGreater(any(Object.class), any(Object.class), any(Integer.class));

    setOrderOnTestPartitions(order);
    return new ImmutablePair<>(partitionMock, scannerSpy);

  }

  private static void addRow(List<List<IntWritable>> partition, Integer col0, Integer col1,
                             Integer col2) {
    partition.add(Lists.newArrayList(
            col0 != null ? new IntWritable(col0) : null,
            col1 != null ? new IntWritable(col1) : null,
            col2 != null ? new IntWritable(col2) : null
    ));
  }

  /**
   * Reverses order on actual data if needed, based on order parameter.
   * @param order
   */
  private static void setOrderOnTestPartitions(PTFInvocationSpec.Order order) {
    LinkedList<List<IntWritable>> notNulls = TEST_PARTITION.stream().filter(
        r -> r.get(ORDER_BY_COL) != null).collect(toCollection(LinkedList::new));
    List<List<IntWritable>> nulls = TEST_PARTITION.stream().filter(
        r -> r.get(ORDER_BY_COL) == null).collect(toList());

    boolean isAscCurrently = notNulls.getFirst().get(ORDER_BY_COL).get() <
            notNulls.getLast().get(ORDER_BY_COL).get();

    if ((ASC.equals(order) && !isAscCurrently) || (DESC.equals(order) && isAscCurrently)) {
      Collections.reverse(notNulls);
      TEST_PARTITION.clear();
      TEST_PARTITION.addAll(notNulls);
      TEST_PARTITION.addAll(nulls);
    }
  }

}
