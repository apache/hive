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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.data.CachingDeleteLoader;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestHiveDeleteFilter {
  private static final int CONTAINER = 1 << 16;
  private static final String DATA_FILE = "file:/tmp/data/a.parquet";
  private static final String OTHER_DATA_FILE = "file:/tmp/data/b.parquet";

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

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

    batch.size = 0;
    filter.filter(batch, Long.MIN_VALUE);

    batch.size = 1024;
    Assert.assertThrows(UnsupportedOperationException.class, () -> filter.filter(batch, Long.MIN_VALUE));
  }

  @Test
  public void testV2DeleteFiles() throws IOException {
    Table table = createTable(2);
    List<Long> positions = randomPositions(0, 4 * CONTAINER, 0.07);
    List<Long> otherPositions = randomPositions(0, 4 * CONTAINER, 0.02);
    int half = positions.size() / 2;
    DeleteFile first = writeDeletes(table, DATA_FILE, positions.subList(0, half), OTHER_DATA_FILE, otherPositions);
    DeleteFile second = writeDeletes(table, DATA_FILE, positions.subList(half, positions.size()), null, null);
    DeleteFile other = writeDeletes(table, OTHER_DATA_FILE, otherPositions, null, null);

    assertLoadedDeletes(table, ImmutableList.of(first), positions.subList(0, half));
    assertLoadedDeletes(table, ImmutableList.of(first, second, other), positions);
    assertLoadedDeletes(table, ImmutableList.of(other), ImmutableList.of());
  }

  @Test
  public void testLargeV2DeleteFile() throws IOException {
    Table table = createTable(2);
    List<Long> positions = randomPositions(0, 8 * CONTAINER, 0.3);
    Assert.assertTrue(positions.size() > 100_000);

    assertLoadedDeletes(table, ImmutableList.of(writeDeletes(table, DATA_FILE, positions, null, null)), positions);
  }

  @Test
  public void testDeletionVector() throws IOException {
    Table table = createTable(3);
    List<Long> positions = randomPositions(0, 4 * CONTAINER, 0.07);

    assertLoadedDeletes(table, ImmutableList.of(writeDeletes(table, DATA_FILE, positions, null, null)), positions);
  }

  /**
   * Loads the deletes of {@link #DATA_FILE} as the vectorized reader does, both with and without the per-query cache,
   * and filters the batches of a split starting mid-file.
   */
  private void assertLoadedDeletes(Table table, List<DeleteFile> deleteFiles, List<Long> expected) {
    PositionDeleteIndex expectedIndex = Deletes.toPositionIndex(CloseableIterable.withNoopClose(expected));
    for (String engine : new String[] { "mr", "tez" }) {
      Configuration conf = new Configuration();
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, engine);
      AtomicInteger cacheLookups = new AtomicInteger();
      CachingDeleteLoader loader = new CachingDeleteLoader(table.io()::newInputFile, conf) {
        @Override
        protected <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize) {
          cacheLookups.incrementAndGet();
          return super.getOrLoad(key, valueSupplier, valueSize);
        }
      };
      PositionDeleteIndex index = loader.loadPositionDeletes(deleteFiles, DATA_FILE);

      Assert.assertEquals(expected.size(), index.cardinality());
      boolean dv = deleteFiles.get(0).format() == FileFormat.PUFFIN;
      Assert.assertEquals(engine, !dv && engine.equals("mr"), cacheLookups.get() > 0);
      for (int split : new int[] { 0, 1 + random.nextInt(2 * CONTAINER) }) {
        assertFilter(index, expectedIndex, split, 4 * CONTAINER, false);
      }
    }
  }

  private Table createTable(int formatVersion) throws IOException {
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
    return new HadoopTables(new Configuration()).create(schema, PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)),
        temp.newFolder().getAbsolutePath());
  }

  private DeleteFile writeDeletes(Table table, String dataFile, List<Long> positions, String otherDataFile,
      List<Long> otherPositions) throws IOException {
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    positions.forEach(pos -> deletes.add(Pair.of(dataFile, pos)));
    if (otherDataFile != null) {
      otherPositions.forEach(pos -> deletes.add(Pair.of(otherDataFile, pos)));
    }
    File file = new File(temp.getRoot(), "deletes-" + random.nextLong() + ".parquet");
    return FileHelpers.writeDeleteFile(table, Files.localOutput(file), null, deletes,
        TableUtil.formatVersion(table)).first();
  }

  private List<Long> randomPositions(long from, long to, double density) {
    return LongStream.range(from, to).filter(pos -> random.nextDouble() < density).boxed().toList();
  }

  private PositionDeleteIndex randomIndex(long from, long to, double density) {
    return Deletes.toPositionIndex(CloseableIterable.withNoopClose(randomPositions(from, to, density)));
  }

  private void assertFilter(PositionDeleteIndex index, long splitStart, long fileEnd, boolean incomingSelection) {
    assertFilter(index, index, splitStart, fileEnd, incomingSelection);
  }

  /**
   * Feeds the batches of a split through the filter and compares every batch with the rows of its incoming selection
   * that the expected index does not delete. Batches have random sizes, and random gaps between them stand for
   * skipped row groups.
   */
  private void assertFilter(PositionDeleteIndex index, PositionDeleteIndex expected, long splitStart, long fileEnd,
      boolean incomingSelection) {
    HiveDeleteFilter filter = new HiveDeleteFilter(index);
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    long offset = splitStart;
    while (offset < fileEnd) {
      int rows = (int) Math.min(fileEnd - offset, random.nextInt(4) == 0 ? 1 + random.nextInt(1024) : 1024);
      int[] selection = IntStream.range(0, rows).toArray();
      batch.selectedInUse = incomingSelection && random.nextBoolean();
      if (batch.selectedInUse) {
        selection = IntStream.range(0, rows).filter(row -> random.nextInt(3) > 0).toArray();
        System.arraycopy(selection, 0, batch.selected, 0, selection.length);
      }
      batch.size = selection.length;
      long batchStart = offset;
      int[] expectedRows = IntStream.of(selection).filter(row -> !expected.isDeleted(batchStart + row)).toArray();

      filter.filter(batch, offset);

      int[] actualRows = batch.selectedInUse ?
          IntStream.of(batch.selected).limit(batch.size).toArray() : IntStream.range(0, batch.size).toArray();
      Assert.assertArrayEquals("batch at " + offset, expectedRows, actualRows);

      offset += rows + (random.nextInt(8) == 0 ? random.nextInt(5000) : 0);
    }
  }
}
