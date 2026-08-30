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
package org.apache.hadoop.hive.ql.io.parquet.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.junit.Test;

/**
 * The gate deciding which bloom filters are worth reading, and so whether the data file is worth opening
 * at all. The block count alone cannot see an error here: Parquet answers a filter read for a column that
 * has none with null and leaves the row group standing, so a predicate collected too widely prunes exactly
 * as much while reading megabytes it did not need.
 */
public class TestVectorizedParquetBloomFilters {

  private static final ColumnPath INT_COL = ColumnPath.get("intCol");
  private static final ColumnPath OTHER_COL = ColumnPath.get("otherCol");
  private static final IntColumn INT_COLUMN = FilterApi.intColumn("intCol");
  private static final IntColumn OTHER_COLUMN = FilterApi.intColumn("otherCol");
  private static final ColumnPath THIRD_COL = ColumnPath.get("thirdCol");
  private static final IntColumn THIRD_COLUMN = FilterApi.intColumn("thirdCol");

  @Test
  public void testEqualityIsCollectedAndRangesAreNot() {
    assertEquals(Set.of(INT_COL), columnsRead(FilterApi.eq(INT_COLUMN, 51)));
    // a bloom filter only proves a value absent, so it says nothing about ranges
    assertEquals(Set.of(), columnsRead(FilterApi.lt(INT_COLUMN, 51)));
    assertEquals(Set.of(), columnsRead(FilterApi.gtEq(INT_COLUMN, 51)));
  }

  @Test
  public void testNullEqualityIsNotCollected() {
    // eq(col, null) asks for nulls, which a bloom filter says nothing about
    assertEquals(Set.of(), columnsRead(FilterApi.eq(INT_COLUMN, null)));
  }

  @Test
  public void testAndCollectsEitherSide() {
    // an AND drops a row group when either side proves absence, so both sides are worth reading
    assertEquals(Set.of(INT_COL, OTHER_COL),
        columnsRead(FilterApi.and(FilterApi.eq(INT_COLUMN, 51), FilterApi.eq(OTHER_COLUMN, 7))));
    assertEquals(Set.of(INT_COL),
        columnsRead(FilterApi.and(FilterApi.eq(INT_COLUMN, 51), FilterApi.lt(OTHER_COLUMN, 7))));
  }

  @Test
  public void testOrPrunesOnlyWhenBothSidesDo() {
    assertTrue(canPrune(FilterApi.or(FilterApi.eq(INT_COLUMN, 51), FilterApi.eq(OTHER_COLUMN, 7))));
    // one side a bloom filter cannot answer makes the OR itself unable to drop a row group
    assertFalse(canPrune(FilterApi.or(FilterApi.eq(INT_COLUMN, 51), FilterApi.lt(OTHER_COLUMN, 7))));
  }

  /**
   * Parquet reads the filter of a prunable side of an OR even when the OR cannot prune, so that column
   * still has to be fetched. Collecting only what can prune leaves that read unserved.
   */
  @Test
  public void testUnprunableOrStillContributesItsColumns() {
    FilterPredicate unprunableOr = FilterApi.or(FilterApi.eq(INT_COLUMN, 51), FilterApi.lt(OTHER_COLUMN, 7));
    assertEquals(Set.of(INT_COL), columnsRead(unprunableOr));

    // the AND can prune through its other side, so the level runs and the OR's column is read
    FilterPredicate predicate = FilterApi.and(unprunableOr, FilterApi.eq(THIRD_COLUMN, 3));
    assertTrue(canPrune(predicate));
    assertEquals(Set.of(INT_COL, THIRD_COL), columnsRead(predicate));
  }

  private static Set<ColumnPath> columnsRead(org.apache.parquet.filter2.predicate.FilterPredicate predicate) {
    return VectorizedParquetRecordReader.bloomFilterPlan(FilterCompat.get(predicate)).columnsRead();
  }

  private static boolean canPrune(org.apache.parquet.filter2.predicate.FilterPredicate predicate) {
    return VectorizedParquetRecordReader.bloomFilterPlan(FilterCompat.get(predicate)).canPrune();
  }

  @Test
  public void testAChunkStatingNoFilterLengthLeavesTheWholeFileToThePlainReader() {
    // a file written before the length was recorded states an offset it cannot say the extent of, and
    // the cache is served by extent, so none of the file's filters can be served from it
    assertNull("a chunk of unknown length gives up the file",
        VectorizedParquetRecordReader.bloomFilterRanges(Set.of(INT_COL),
            List.of(blockOf(chunk(INT_COL, 1024L, -1)))));
  }

  @Test
  public void testAChunkWithNoFilterIsPassedOver() {
    // no filter was written for this column, which is not a reason to give up the ones that were
    assertEquals("a chunk holding no filter contributes no range",
        Map.of(2048L, 64), VectorizedParquetRecordReader.bloomFilterRanges(Set.of(INT_COL, OTHER_COL),
            List.of(blockOf(chunk(INT_COL, -1L, -1), chunk(OTHER_COL, 2048L, 64)))));
  }

  @Test
  public void testOnlyTheColumnsAskedAboutContributeRanges() {
    assertEquals("a column the predicate never named is not read",
        Map.of(1024L, 32), VectorizedParquetRecordReader.bloomFilterRanges(Set.of(INT_COL),
            List.of(blockOf(chunk(INT_COL, 1024L, 32), chunk(OTHER_COL, 4096L, 64)))));
  }

  private static ColumnChunkMetaData chunk(ColumnPath path, long bloomOffset, int bloomLength) {
    ColumnChunkMetaData chunk = mock(ColumnChunkMetaData.class);
    when(chunk.getPath()).thenReturn(path);
    when(chunk.getBloomFilterOffset()).thenReturn(bloomOffset);
    when(chunk.getBloomFilterLength()).thenReturn(bloomLength);
    return chunk;
  }

  private static BlockMetaData blockOf(ColumnChunkMetaData... chunks) {
    BlockMetaData block = mock(BlockMetaData.class);
    when(block.getColumns()).thenReturn(List.of(chunks));
    return block;
  }
}
