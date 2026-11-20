/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.vector;

import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.Assert;
import org.junit.Test;

public class TestHiveDeleteFilter {
  private static final int CONTAINER = 1 << 16;

  private final Random random = new Random(42);

  @Test
  public void testRandomDeletes() {
    for (double density : new double[] { 0, 0.001, 0.07, 0.5, 1 }) {
      PositionDeleteIndex index = randomIndex(0, 5 * CONTAINER, density);
      for (boolean incomingSelection : new boolean[] { false, true }) {
        assertFilter(index, 0, 5 * CONTAINER, incomingSelection);
        assertFilter(index, random.nextInt(2 * CONTAINER), 5 * CONTAINER, incomingSelection);
      }
    }
  }

  @Test
  public void testDeletedRanges() {
    PositionDeleteIndex index = Deletes.toPositionIndex(CloseableIterable.empty());
    long start = 0;
    while (start < 5 * CONTAINER) {
      long end = start + 1 + random.nextInt(3000);
      if (random.nextBoolean()) {
        index.delete(start, end);
      }
      start = end;
    }
    assertFilter(index, 0, 5 * CONTAINER, false);
    assertFilter(index, 1000, 5 * CONTAINER, true);
  }

  @Test
  public void testPositionsBeyondIntRange() {
    long start = (1L << 32) - 3 * CONTAINER;
    PositionDeleteIndex index = randomIndex(start, start + 6 * CONTAINER, 0.07);
    assertFilter(index, start, start + 6 * CONTAINER, false);
    assertFilter(index, start + 5, start + 6 * CONTAINER, true);
  }

  @Test
  public void testUnknownRowPosition() {
    HiveDeleteFilter filter = new HiveDeleteFilter(randomIndex(0, 1024, 0.07));
    VectorizedRowBatch batch = new VectorizedRowBatch(1);

    batch.size = 1024;
    Assert.assertThrows(UnsupportedOperationException.class, () -> filter.filter(batch, Long.MIN_VALUE));
  }

  private List<Long> randomPositions(long from, long to, double density) {
    return LongStream.range(from, to).filter(pos -> random.nextDouble() < density).boxed().toList();
  }

  private PositionDeleteIndex randomIndex(long from, long to, double density) {
    return Deletes.toPositionIndex(CloseableIterable.withNoopClose(randomPositions(from, to, density)));
  }

  /**
   * Feeds the batches of a split through the filter and compares every batch with the rows of its incoming selection
   * that the index does not delete. Batches have random sizes, and random gaps between them stand for skipped row
   * groups. A batch is never empty: HiveBatchIterator ends the scan at an empty batch.
   */
  private void assertFilter(PositionDeleteIndex index, long splitStart, long fileEnd, boolean incomingSelection) {
    HiveDeleteFilter filter = new HiveDeleteFilter(index);
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    long offset = splitStart;
    while (offset < fileEnd) {
      int rows = (int) Math.min(fileEnd - offset, random.nextInt(4) == 0 ? 1 + random.nextInt(1024) : 1024);
      int[] selection = IntStream.range(0, rows).toArray();
      batch.selectedInUse = incomingSelection && random.nextBoolean();
      if (batch.selectedInUse) {
        do {
          selection = IntStream.range(0, rows).filter(row -> random.nextInt(3) > 0).toArray();
        } while (selection.length == 0);
        System.arraycopy(selection, 0, batch.selected, 0, selection.length);
      }
      batch.size = selection.length;
      long batchStart = offset;
      int[] expectedRows = IntStream.of(selection).filter(row -> !index.isDeleted(batchStart + row)).toArray();

      filter.filter(batch, offset);

      int[] actualRows = batch.selectedInUse ?
          IntStream.of(batch.selected).limit(batch.size).toArray() : IntStream.range(0, batch.size).toArray();
      Assert.assertArrayEquals("batch at " + offset, expectedRows, actualRows);

      offset += rows + (random.nextInt(8) == 0 ? random.nextInt(5000) : 0);
    }
  }
}
