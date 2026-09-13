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
package org.apache.hadoop.hive.llap.io.encoded;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.DataCache.BooleanRef;
import org.apache.hadoop.hive.common.io.DataCache.DiskRangeListFactory;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.ParquetCacheLayout;
import org.apache.hadoop.hive.llap.cache.BuddyAllocator;
import org.apache.hadoop.hive.llap.cache.BufferUsageManager;
import org.apache.hadoop.hive.llap.cache.LlapAllocatorBuffer;
import org.apache.hadoop.hive.llap.cache.LlapDataBuffer;
import org.apache.hadoop.hive.llap.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheCounters;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheImpl;
import org.apache.hadoop.hive.llap.cache.LowLevelLrfuCachePolicy;
import org.apache.hadoop.hive.llap.cache.TestBuddyAllocatorForceEvict;
import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer.Includes;
import org.apache.hadoop.hive.llap.io.decode.ParquetEncodedDataConsumer;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonIOMetrics;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.encoded.CacheChunk;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.TypeDescription;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.apache.tez.common.counters.TezCounters;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Drives {@link ParquetEncodedDataReader} + {@link ParquetEncodedDataConsumer} over an in-process
 * BuddyAllocator/LowLevelCacheImpl against a real three-row-group Parquet file. The footer goes through
 * the real LLAP footer cache (LlapProxy in cache mode); the data cache is per test.
 */
public class TestParquetEncodedDataReader {

  private static final String COLUMNS = "id,big,dec,name,ratio,flag";
  private static final String TYPES = "int,bigint,decimal(7,2),string,double,boolean";
  private static final int ROWS_PER_GROUP = 1500;
  private static final int ROW_GROUPS = 3;
  private static final int ROWS = ROWS_PER_GROUP * ROW_GROUPS;
  // Smaller than every column chunk so each chunk spans several cache buffers.
  private static final int MAX_ALLOC = 4096;

  private static final MessageType SCHEMA = Types.buildMessage()
      .optional(PrimitiveTypeName.INT32).named("id")
      .optional(PrimitiveTypeName.INT64).named("big")
      .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(4)
          .as(LogicalTypeAnnotation.decimalType(2, 7)).named("dec")
      .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("name")
      .optional(PrimitiveTypeName.DOUBLE).named("ratio")
      .optional(PrimitiveTypeName.BOOLEAN).named("flag")
      .named("hive_schema");

  private static final LlapDaemonCacheMetrics CACHE_METRICS =
      LlapDaemonCacheMetrics.create("TestParquetEncodedDataReader-cache", "1");
  private static final LlapDaemonIOMetrics IO_METRICS =
      LlapDaemonIOMetrics.create("TestParquetEncodedDataReader-io", "1", null);

  private static HiveConf daemonConf;
  private static java.nio.file.Path tmpDir;
  private static Path file;
  private static long fileLength;
  private static ParquetMetadata footer;

  private LowLevelCacheImpl cache;
  private Ledger ledger;
  /** hive.llap.io.encode.alloc.size at its default, the grain these expectations assume. */
  private static final int FLOOR = 256 * 1024;

  private boolean failFirstRange;
  private boolean unslicedBuffers;
  private boolean stopAfterFirstBatch;
  private boolean failDecode;
  private ParquetEncodedDataReader reader;
  private int decoded;
  private final List<Integer> decodedAtRequest = new ArrayList<>();

  @BeforeClass
  public static void setUpClass() throws Exception {
    daemonConf = new HiveConf();
    HiveConf.setVar(daemonConf, ConfVars.LLAP_IO_MEMORY_MODE, "cache");
    HiveConf.setVar(daemonConf, ConfVars.LLAP_IO_MEMORY_MAX_SIZE, "64Mb");
    HiveConf.setBoolVar(daemonConf, ConfVars.LLAP_TRACK_CACHE_USAGE, false);
    HiveConf.setIntVar(daemonConf, ConfVars.LLAP_LRFU_BP_WRAPPER_SIZE, 1);

    tmpDir = Files.createTempDirectory("llap-parquet-native");
    file = new Path(tmpDir.toString(), "data.parquet");
    writeFile(file, daemonConf);
    FileSystem fs = file.getFileSystem(daemonConf);
    fileLength = fs.getFileStatus(file).getLen();
    footer = ParquetFileReader.readFooter(
        HadoopInputFile.fromPath(file, daemonConf), ParquetMetadataConverter.NO_FILTER);
    assertEquals(ROW_GROUPS, footer.getBlocks().size());
    for (BlockMetaData block : footer.getBlocks()) {
      assertEquals(ROWS_PER_GROUP, block.getRowCount());
    }

    LlapProxy.setDaemon(true);
    LlapProxy.initializeLlapIo(daemonConf);
  }

  @AfterClass
  public static void tearDownClass() throws IOException {
    LlapProxy.close();
    FileSystem.getLocal(daemonConf).delete(new Path(tmpDir.toString()), true);
  }

  @Before
  public void setUp() {
    LowLevelLrfuCachePolicy policy = new LowLevelLrfuCachePolicy(MAX_ALLOC, 64L << 20, daemonConf);
    BuddyAllocator allocator = TestBuddyAllocatorForceEvict.create(MAX_ALLOC, 2, 8 << 20, true, false);
    cache = new LowLevelCacheImpl(CACHE_METRICS, policy, allocator, true);
    ledger = new Ledger(cache);
    failFirstRange = false;
    unslicedBuffers = false;
    stopAfterFirstBatch = false;
    failDecode = false;
    decoded = 0;
    decodedAtRequest.clear();
  }

  @Test
  public void testFullReadAllColumns() throws Exception {
    Run run = read(jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5), wholeFile());

    run.assertClean();
    assertEquals(ROWS, run.rows.size());
    for (int i : new int[] {0, 1, 7, 11, 77, 1499, 1500, 2999, 3000, ROWS - 1}) {
      assertArrayEquals("row " + i, expectedRow(i), run.rows.get(i));
    }
    assertTrue(run.firstBatchCols[2] instanceof Decimal64ColumnVector);
    assertEquals(2, ((Decimal64ColumnVector) run.firstBatchCols[2]).scale);
    assertEquals(Arrays.asList(1024, 476, 1024, 476, 1024, 476), run.batchSizes);
    assertEquals(ROW_GROUPS, run.counter(LlapIOCounters.SELECTED_ROWGROUPS));
    assertEquals(ROWS, run.counter(LlapIOCounters.ROWS_EMITTED));
  }

  @Test
  public void testSplitCoveringSecondRowGroup() throws Exception {
    List<BlockMetaData> blocks = footer.getBlocks();
    long start = blocks.get(1).getStartingPos(), end = blocks.get(2).getStartingPos();
    Run run = read(jobConf(COLUMNS, TYPES, 0, 3), new FileSplit(file, start, end - start, (String[]) null));

    run.assertClean();
    assertEquals(1, run.counter(LlapIOCounters.SELECTED_ROWGROUPS));
    assertEquals(ROWS_PER_GROUP, run.rows.size());
    for (int i = 0; i < ROWS_PER_GROUP; ++i) {
      int row = ROWS_PER_GROUP + i;
      assertArrayEquals("row " + row, project(expectedRow(row), 0, 3), run.rows.get(i));
    }
  }

  @Test
  public void testDisjointSplitsEmitEveryRowOnce() throws Exception {
    long cut = footer.getBlocks().get(2).getStartingPos();
    Run first = read(jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5), new FileSplit(file, 0, cut, (String[]) null));
    Run second = read(jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5),
        new FileSplit(file, cut, fileLength - cut, (String[]) null));

    first.assertClean();
    second.assertClean();
    assertEquals(2, first.counter(LlapIOCounters.SELECTED_ROWGROUPS));
    assertEquals(1, second.counter(LlapIOCounters.SELECTED_ROWGROUPS));
    List<Object[]> all = new ArrayList<>(first.rows);
    all.addAll(second.rows);
    assertEquals(ROWS, all.size());
    for (int i = 0; i < ROWS; ++i) {
      assertArrayEquals("row " + i, expectedRow(i), all.get(i));
    }
  }

  @Test
  public void testProjectionOrderAndSubset() throws Exception {
    Run run = read(jobConf(COLUMNS, TYPES, 4, 0, 3), wholeFile());

    run.assertClean();
    assertEquals(ROWS, run.rows.size());
    assertEquals(3, run.firstBatchCols.length);
    assertTrue(run.firstBatchCols[0] instanceof DoubleColumnVector);
    assertTrue(run.firstBatchCols[1] instanceof LongColumnVector);
    assertTrue(run.firstBatchCols[2] instanceof BytesColumnVector);
    for (int i : new int[] {0, 7, 1500, ROWS - 1}) {
      assertArrayEquals("row " + i, project(expectedRow(i), 4, 0, 3), run.rows.get(i));
    }
  }

  @Test
  public void testCacheAllocationMatchesBytesRead() throws Exception {
    Run run = read(jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5), wholeFile());

    run.assertClean();
    long used = run.counter(LlapIOCounters.ALLOCATED_USED_BYTES);
    long allocated = run.counter(LlapIOCounters.ALLOCATED_BYTES);
    assertEquals(run.counter(LlapIOCounters.CACHE_MISS_BYTES), used);
    // Every buffer but the sub-floor remainder of each chunk is a power of two, so the rounding
    // overhead is bounded by one MAX_ALLOC per column chunk.
    long chunks = (long) ROW_GROUPS * 6;
    assertTrue("allocated " + allocated + " vs used " + used, allocated - used <= chunks * MAX_ALLOC);
  }

  @Test
  public void testAdjacentColumnChunksReadInOneRun() throws Exception {
    Run run = read(jobConf(COLUMNS, TYPES, 0, 1, 2), wholeFile());

    run.assertClean();
    // This fixture allocates in 4 Kb buffers, far below the cap, so what the loop below really
    // pins is that adjacent chunks merge -- the cap itself never binds here.
    int maxRun = new ParquetCacheLayout(MAX_ALLOC, FLOOR).maxRangeBytes();
    long previousEnd = -1;
    for (long[] read : run.reads) {
      assertTrue("read of " + read[1] + " exceeds the largest a range may be", read[1] <= maxRun);
      assertTrue("reads must move forward", read[0] > previousEnd - maxRun);
      previousEnd = read[0] + read[1];
    }
    // Chunks of id, big and dec sit back to back: within a row group every read starts where the
    // previous one ended, so the only jumps are the three row-group starts.
    int jumps = 0;
    previousEnd = -1;
    for (long[] read : run.reads) {
      if (read[0] != previousEnd) {
        ++jumps;
      }
      previousEnd = read[0] + read[1];
    }
    assertEquals(ROW_GROUPS, jumps);
    boolean crossesColumns = false;
    for (BlockMetaData block : footer.getBlocks()) {
      long bigStart = block.getColumns().get(1).getStartingPos();
      for (long[] read : run.reads) {
        crossesColumns |= read[0] < bigStart && read[0] + read[1] > bigStart;
      }
    }
    assertTrue("no read spans the id/big chunk boundary", crossesColumns);
    assertTrue("reads " + run.reads.size() + " should be far fewer than buffers " + run.buffers.size(),
        run.reads.size() * 3 <= run.buffers.size());
    for (int i : new int[] {0, 11, 1500, ROWS - 1}) {
      assertArrayEquals("row " + i, project(expectedRow(i), 0, 1, 2), run.rows.get(i));
    }
  }

  @Test
  public void testGapBetweenProjectedChunksSplitsTheRun() throws Exception {
    Run run = read(jobConf(COLUMNS, TYPES, 0, 2), wholeFile());

    run.assertClean();
    for (BlockMetaData block : footer.getBlocks()) {
      long bigStart = block.getColumns().get(1).getStartingPos();
      long bigEnd = bigStart + block.getColumns().get(1).getTotalSize();
      for (long[] read : run.reads) {
        long end = read[0] + read[1];
        assertFalse("read [" + read[0] + "," + end + ") crosses the unprojected chunk",
            read[0] < bigEnd && end > bigStart);
      }
    }
    for (int i : new int[] {0, 11, 1500, ROWS - 1}) {
      assertArrayEquals("row " + i, project(expectedRow(i), 0, 2), run.rows.get(i));
    }
  }

  @Test
  public void testCachedBuffersAreNotReread() throws Exception {
    read(jobConf(COLUMNS, TYPES, 1), wholeFile()).assertClean();
    Run second = read(jobConf(COLUMNS, TYPES, 0, 1, 2), wholeFile());

    second.assertClean();
    for (BlockMetaData block : footer.getBlocks()) {
      long bigStart = block.getColumns().get(1).getStartingPos();
      long bigEnd = bigStart + block.getColumns().get(1).getTotalSize();
      for (long[] read : second.reads) {
        long end = read[0] + read[1];
        assertFalse("read [" + read[0] + "," + end + ") re-read the cached chunk",
            read[0] < bigEnd && end > bigStart);
      }
    }
    assertTrue(second.counter(LlapIOCounters.CACHE_HIT_BYTES) > 0);
    for (int i : new int[] {0, 11, 1500, ROWS - 1}) {
      assertArrayEquals("row " + i, project(expectedRow(i), 0, 1, 2), second.rows.get(i));
    }
  }

  @Test
  public void testNextRowGroupIsRequestedBeforeCurrentDecodes() throws Exception {
    Run run = read(jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5), wholeFile());

    run.assertClean();
    // Row group 1's request goes out before row group 0 is decoded; row group 2's after it.
    assertEquals(Arrays.asList(0, 0, 1), decodedAtRequest);
  }

  @Test
  public void testPooledBuffersReadEveryRow() throws Exception {
    unslicedBuffers = true;
    Run run = read(jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5), wholeFile());

    run.assertClean();
    assertEquals(ROWS, run.rows.size());
    for (int i : new int[] {0, 1, 1499, 1500, ROWS - 1}) {
      assertArrayEquals("row " + i, expectedRow(i), run.rows.get(i));
    }
  }

  @Test
  public void testPooledBufferIsLimitedToItsRange() throws Exception {
    unslicedBuffers = true;
    // The second read of a run takes a pooled buffer that the first left longer than the range.
    read(jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5), wholeFile());
    Run run = read(jobConf(COLUMNS, TYPES, 3), wholeFile());

    run.assertClean();
    assertEquals(ROWS, run.rows.size());
    for (int i : new int[] {0, 1499, ROWS - 1}) {
      assertArrayEquals("row " + i, project(expectedRow(i), 3), run.rows.get(i));
    }
  }

  @Test
  public void testFailedRangeAbortsTheSplitWithoutLeaks() throws Exception {
    failFirstRange = true;
    Run run = read(jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5), wholeFile());

    assertNotNull("the failed range must surface", run.error);
    assertTrue(String.valueOf(run.error), String.valueOf(run.error).contains("boom"));
    assertEquals(0, run.rows.size());
    ledger.assertNothingLeaked();
  }

  @Test
  public void testStopWithNextRowGroupInFlightReleasesIt() throws Exception {
    stopAfterFirstBatch = true;
    Run run = read(jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5), wholeFile());

    assertTrue("stopped after the first row group", run.rows.size() < ROWS);
    assertTrue("row group 1 had been requested", run.reads.size() > 1);
    ledger.assertNothingLeaked();
  }

  @Test
  public void testDecodeFailureReleasesCachedBuffers() throws Exception {
    failDecode = true;
    Run run = read(jobConf(COLUMNS, TYPES, 0, 1, 2), wholeFile());

    assertNotNull(run.error);
    assertTrue(String.valueOf(run.error), String.valueOf(run.error).contains("decode failed"));
    ledger.assertNothingLeaked();
  }

  @Test
  public void testCollisionKeepsTheCachedCopyAndFreesOurs() throws Exception {
    ledger.collide = true;
    Run run = read(jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5), wholeFile());

    run.assertClean();
    assertEquals(ROWS, run.rows.size());
    for (int i : new int[] {0, 11, 1500, ROWS - 1}) {
      assertArrayEquals("row " + i, expectedRow(i), run.rows.get(i));
    }
    assertEquals("every buffer we allocated was refused and freed", ledger.allocated.size(), ledger.freed.size());
    assertTrue("nothing of ours reached the cache", ledger.accepted.isEmpty());
  }

  @Test
  public void testSargPrunesAllRowGroups() throws Exception {
    JobConf job = jobConf(COLUMNS, TYPES, 0, 3);
    setSarg(job, SearchArgumentFactory.newBuilder()
        .equals("id", PredicateLeaf.Type.LONG, (long) ROWS + 1).build());
    Run run = read(job, wholeFile());

    run.assertClean();
    assertEquals(0, run.counter(LlapIOCounters.SELECTED_ROWGROUPS));
    assertEquals(0, run.rows.size());
    assertEquals(0, run.counter(LlapIOCounters.ROWS_EMITTED));
  }

  @Test
  public void testSargKeepsMatchingRowGroup() throws Exception {
    JobConf job = jobConf(COLUMNS, TYPES, 0, 3);
    long lo = 2 * ROWS_PER_GROUP + 100, hi = lo + 50;
    setSarg(job, SearchArgumentFactory.newBuilder().between("id", PredicateLeaf.Type.LONG, lo, hi).build());
    Run run = read(job, wholeFile());

    run.assertClean();
    assertEquals(1, run.counter(LlapIOCounters.SELECTED_ROWGROUPS));
    assertEquals(ROWS_PER_GROUP, run.rows.size());
    assertArrayEquals(project(expectedRow(2 * ROWS_PER_GROUP), 0, 3), run.rows.get(0));
  }

  @Test
  public void testSecondReadHitsCache() throws Exception {
    long chunkBytes = projectedChunkBytes(1, 3);
    Run cold = read(jobConf(COLUMNS, TYPES, 1, 3), wholeFile());
    Run warm = read(jobConf(COLUMNS, TYPES, 1, 3), wholeFile());

    cold.assertClean();
    warm.assertClean();
    assertEquals(0, cold.counter(LlapIOCounters.CACHE_HIT_BYTES));
    assertEquals(chunkBytes, cold.counter(LlapIOCounters.CACHE_MISS_BYTES));
    assertEquals(chunkBytes, warm.counter(LlapIOCounters.CACHE_HIT_BYTES));
    assertEquals(0, warm.counter(LlapIOCounters.CACHE_MISS_BYTES));
    assertTrue(CACHE_METRICS.getCacheHitBytes() >= chunkBytes);

    assertEquals(cold.rows.size(), warm.rows.size());
    for (int i = 0; i < ROWS; ++i) {
      assertArrayEquals("row " + i, cold.rows.get(i), warm.rows.get(i));
    }
    assertEquals(new HashSet<>(cold.buffers), new HashSet<>(warm.buffers));
    assertTrue(cold.buffers.size() > 1);
    for (MemoryBuffer b : warm.buffers) {
      assertFalse(b.toString(), ((LlapAllocatorBuffer) b).isLocked());
    }
  }

  @Test
  public void testMissingTrailingColumnReadsAsNulls() throws Exception {
    Run run = read(jobConf(COLUMNS + ",extra", TYPES + ",string", 0, 6), wholeFile());

    run.assertClean();
    assertEquals(ROWS, run.rows.size());
    assertTrue(run.firstBatchCols[1] instanceof BytesColumnVector);
    for (int i : new int[] {0, 1500, ROWS - 1}) {
      assertArrayEquals("row " + i, new Object[] {(long) i, null}, run.rows.get(i));
    }
  }

  @Test
  public void testEvolvedColumnsReorderedDefaultedAndRecreated() throws Exception {
    // Hive order differs from the file (ratio before id), "added" is absent with an initial default,
    // and a recreated field carries the Iceberg placeholder name that no file column matches.
    String columns = "ratio,id,<<DUMMY_FOR_RECREATED_FIELD_IN_FILESCHEMA>>,added,name";
    Run run = read(jobConf(columns, "double,int,string,int,string", 0, 1, 2, 3, 4), wholeFile(),
        Map.of("added", 42));

    run.assertClean();
    assertEquals(ROWS, run.rows.size());
    assertTrue(run.firstBatchCols[2] instanceof BytesColumnVector);
    assertTrue(run.firstBatchCols[3] instanceof LongColumnVector);
    assertTrue(run.firstBatchCols[3].isRepeating);
    for (int i : new int[] {0, 7, 1500, ROWS - 1}) {
      assertArrayEquals("row " + i, new Object[] {ratio(i), (long) i, null, 42L, i % 7 == 0 ? null : "name-" + i},
          run.rows.get(i));
    }
  }

  @Test
  public void testStartRowInFileSkipsPrunedRowGroup() throws Exception {
    JobConf job = jobConf(COLUMNS, TYPES, 0, 3);
    setSarg(job, SearchArgumentFactory.newBuilder()
        .in("id", PredicateLeaf.Type.LONG, 100L, (long) 2 * ROWS_PER_GROUP + 1000).build());
    Run run = read(job, wholeFile());

    run.assertClean();
    assertEquals(2, run.counter(LlapIOCounters.SELECTED_ROWGROUPS));
    assertArrayEquals(project(expectedRow(3000), 0, 3), run.rows.get(ROWS_PER_GROUP));
  }

  @Test
  public void testCacheOnlyReadThrowsOnColdData() throws Exception {
    read(jobConf(COLUMNS, TYPES, 0, 3), wholeFile()).assertClean();
    JobConf job = jobConf(COLUMNS, TYPES, 1);
    HiveConf.setBoolVar(job, ConfVars.LLAP_IO_CACHE_ONLY, true);
    Run run = read(job, wholeFile());

    assertTrue(String.valueOf(run.error), run.error instanceof IOException);
    assertTrue(run.error.getMessage(), run.error.getMessage().contains("cache only"));
    assertFalse(run.done);
    assertEquals(0, run.rows.size());
  }

  @Test
  public void testNoFileKeyReadsUncached() throws Exception {
    HiveConf noKeyConf = new HiveConf(daemonConf);
    HiveConf.setBoolVar(noKeyConf, ConfVars.LLAP_CACHE_ALLOW_SYNTHETIC_FILEID, false);
    Run run = read(jobConf(COLUMNS, TYPES, 0, 2), wholeFile(), noKeyConf);

    run.assertClean();
    assertEquals(ROWS, run.rows.size());
    assertEquals(0, run.counter(LlapIOCounters.CACHE_HIT_BYTES));
    assertEquals(0, run.counter(LlapIOCounters.CACHE_MISS_BYTES));
    for (int i : new int[] {0, 11, ROWS - 1}) {
      assertArrayEquals("row " + i, project(expectedRow(i), 0, 2), run.rows.get(i));
    }
    for (MemoryBuffer b : run.buffers) {
      assertFalse(((LlapAllocatorBuffer) b).isLocked());
    }
  }

  // ---- fixture ----

  private static void writeFile(Path path, Configuration conf) throws IOException {
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(conf)
        .withType(SCHEMA)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withRowGroupSize(1024)
        .withMinRowCountForPageSizeCheck(ROWS_PER_GROUP)
        .withMaxRowCountForPageSizeCheck(ROWS_PER_GROUP)
        .build()) {
      SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
      for (int i = 0; i < ROWS; ++i) {
        Group g = factory.newGroup();
        g.append("id", i);
        g.append("big", bigValue(i));
        if (i % 11 != 0) {
          g.append("dec", Binary.fromConstantByteArray(ByteBuffer.allocate(4).putInt((int) decUnscaled(i)).array()));
        }
        if (i % 7 != 0) {
          g.append("name", "name-" + i);
        }
        g.append("ratio", ratio(i));
        g.append("flag", i % 3 == 0);
        writer.write(g);
      }
    }
  }

  private static long bigValue(int i) {
    return i * 1_000_003L;
  }

  private static long decUnscaled(int i) {
    return i * 125L + 1;
  }

  private static double ratio(int i) {
    return i / 8.0;
  }

  /** Values as the capturing consumer reads them back: longs for int/bigint/boolean/decimal64. */
  private static Object[] expectedRow(int i) {
    return new Object[] {
        (long) i,
        bigValue(i),
        i % 11 == 0 ? null : decUnscaled(i),
        i % 7 == 0 ? null : "name-" + i,
        ratio(i),
        i % 3 == 0 ? 1L : 0L};
  }

  private static Object[] project(Object[] row, int... cols) {
    Object[] out = new Object[cols.length];
    for (int i = 0; i < cols.length; ++i) {
      out[i] = row[cols[i]];
    }
    return out;
  }

  private static FileSplit wholeFile() {
    return new FileSplit(file, 0, fileLength, (String[]) null);
  }

  private static long projectedChunkBytes(int... fileCols) {
    long total = 0;
    for (BlockMetaData block : footer.getBlocks()) {
      for (int c : fileCols) {
        ColumnChunkMetaData ccm = block.getColumns().get(c);
        total += ccm.getTotalSize();
      }
    }
    return total;
  }

  private static JobConf jobConf(String columns, String types, int... readColumnIds) {
    JobConf job = new JobConf(daemonConf);
    job.set(IOConstants.COLUMNS, columns);
    job.set(IOConstants.COLUMNS_TYPES, types);
    List<Integer> ids = new ArrayList<>();
    for (int id : readColumnIds) {
      ids.add(id);
    }
    ColumnProjectionUtils.setReadColumns(job, ids);
    HiveConf.setVar(job, ConfVars.HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_ENABLED, "decimal_64");
    return job;
  }

  private static void setSarg(JobConf job, SearchArgument sarg) {
    job.set(ConvertAstToSearchArg.SARG_PUSHDOWN, ConvertAstToSearchArg.sargToKryo(sarg));
  }

  private Run read(JobConf job, FileSplit split) throws Exception {
    return read(job, split, daemonConf, null);
  }

  private Run read(JobConf job, FileSplit split, Map<String, Object> initialDefaults) throws Exception {
    return read(job, split, daemonConf, initialDefaults);
  }

  private Run read(JobConf job, FileSplit split, Configuration readerDaemonConf) throws Exception {
    return read(job, split, readerDaemonConf, null);
  }

  private Run read(JobConf job, FileSplit split, Configuration readerDaemonConf,
      Map<String, Object> initialDefaults) throws Exception {
    List<Integer> projection = ColumnProjectionUtils.getReadColumnIDs(job);
    TezCounters tezCounters = new TezCounters();
    QueryFragmentCounters counters = new QueryFragmentCounters(job, tezCounters);
    CapturingConsumer downstream = new CapturingConsumer();
    List<MemoryBuffer> buffers = new ArrayList<>();
    ParquetEncodedDataConsumer edc = new ParquetEncodedDataConsumer(
        downstream, includes(projection), counters, IO_METRICS, job) {
      @Override
      protected void decodeBatch(ParquetEncodedColumnBatch batch, Consumer<ColumnVectorBatch> consumer)
          throws InterruptedException {
        for (MemoryBuffer[] col : batch.columnBuffers) {
          Collections.addAll(buffers, col);
        }
        if (failDecode) {
          throw new IllegalStateException("decode failed");
        }
        super.decodeBatch(batch, consumer);
        if (++decoded == 1 && stopAfterFirstBatch) {
          reader.stop();
        }
      }
    };
    edc.setInitialDefaults(initialDefaults);
    List<long[]> reads = new ArrayList<>();
    ParquetEncodedDataReader reader = new ParquetEncodedDataReader(
        ledger, ledger, readerDaemonConf, job, split, includes(projection), edc, counters) {
      @Override
      FSDataInputStream openFile(FileSystem fs) throws IOException {
        return new RecordingStream(super.openFile(fs), reads);
      }
    };
    this.reader = reader;
    edc.init(reader, reader);
    reader.loadFooter();
    reader.call();
    return new Run(downstream, tezCounters, buffers, reads, ledger);
  }


  /**
   * Records every range of every vectored read as {offset, length} and how many batches had been
   * decoded when the request went out; can fail the first range of the first request.
   */
  private final class RecordingStream extends FSDataInputStream {
    private final List<long[]> reads;
    private boolean first = true;

    RecordingStream(FSDataInputStream delegate, List<long[]> reads) {
      super(delegate);
      this.reads = reads;
    }

    /** Reports the wrapped stream's capabilities, less the ones this test suppresses. */
    @Override
    public boolean hasCapability(String capability) {
      if (unslicedBuffers && StreamCapabilities.VECTOREDIO_BUFFERS_SLICED.equals(capability)) {
        return false;
      }
      return super.hasCapability(capability);
    }

    @Override
    public void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate)
        throws IOException {
      record(ranges);
      super.readVectored(ranges, allocate);
      injectFailure(ranges);
    }

    @Override
    public void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate,
        java.util.function.Consumer<ByteBuffer> release) throws IOException {
      record(ranges);
      super.readVectored(ranges, allocate, release);
      injectFailure(ranges);
    }

    private void record(List<? extends FileRange> ranges) {
      for (FileRange range : ranges) {
        reads.add(new long[] {range.getOffset(), range.getLength()});
      }
      decodedAtRequest.add(decoded);
    }

    private void injectFailure(List<? extends FileRange> ranges) {
      if (first && failFirstRange) {
        CompletableFuture<ByteBuffer> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IOException("boom"));
        ranges.get(0).setData(failed);
      }
      first = false;
    }
  }

  /**
   * Stands between the reader and the cache to account for every buffer: allocated by the
   * reader, freed by it, accepted by the cache, or handed out as a hit.
   */
  private static final class Ledger implements LowLevelCache, BufferUsageManager, Allocator {
    private final LowLevelCacheImpl cache;
    private final Allocator allocator;
    private final Set<MemoryBuffer> allocated = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<MemoryBuffer> freed = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<MemoryBuffer> accepted = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<MemoryBuffer> hits = Collections.newSetFromMap(new IdentityHashMap<>());
    /** Inserts its own copy of every range first, so the reader's put collides. */
    boolean collide;

    Ledger(LowLevelCacheImpl cache) {
      this.cache = cache;
      this.allocator = cache.getAllocator();
    }

    void assertNothingLeaked() {
      Set<MemoryBuffer> raw = Collections.newSetFromMap(new IdentityHashMap<>());
      raw.addAll(allocated);
      raw.removeAll(freed);
      raw.removeAll(accepted);
      assertTrue("raw allocations never freed nor cached: " + raw, raw.isEmpty());
      Set<MemoryBuffer> both = Collections.newSetFromMap(new IdentityHashMap<>());
      both.addAll(freed);
      both.retainAll(accepted);
      assertTrue("cache-owned buffers raw-freed: " + both, both.isEmpty());
      for (MemoryBuffer b : allocated) {
        assertFalse("still locked: " + b, ((LlapAllocatorBuffer) b).isLocked());
      }
      for (MemoryBuffer b : hits) {
        assertFalse("hit still locked: " + b, ((LlapAllocatorBuffer) b).isLocked());
      }
    }

    @Override
    public DiskRangeList getFileData(Object fileKey, DiskRangeList range, long baseOffset,
        DiskRangeListFactory factory, LowLevelCacheCounters qfCounters, BooleanRef gotAllData) {
      DiskRangeList result = cache.getFileData(fileKey, range, baseOffset, factory, qfCounters, gotAllData);
      for (DiskRangeList r = result; r != null; r = r.next) {
        if (r.hasData()) {
          hits.add(((CacheChunk) r).getBuffer());
        }
      }
      return result;
    }

    @Override
    public long[] putFileData(Object fileKey, DiskRange[] ranges, MemoryBuffer[] chunks, long baseOffset,
        Priority priority, LowLevelCacheCounters qfCounters, CacheTag tag) {
      if (collide) {
        MemoryBuffer[] copies = new MemoryBuffer[chunks.length];
        for (int i = 0; i < chunks.length; ++i) {
          MemoryBuffer[] one = new MemoryBuffer[1];
          allocator.allocateMultiple(one, ranges[i].getLength(), LlapDataBuffer::new);
          ByteBuffer src = chunks[i].getByteBufferRaw().duplicate();
          src.limit(src.position() + ranges[i].getLength());
          ByteBuffer dest = one[0].getByteBufferRaw();
          dest.limit(dest.position() + ranges[i].getLength());
          dest.duplicate().put(src);
          copies[i] = one[0];
        }
        cache.putFileData(fileKey, ranges, copies, baseOffset, priority, qfCounters, tag);
        cache.decRefBuffers(java.util.Arrays.asList(copies));
      }
      long[] result = cache.putFileData(fileKey, ranges, chunks, baseOffset, priority, qfCounters, tag);
      for (MemoryBuffer b : chunks) {
        if (allocated.contains(b)) {
          accepted.add(b);
        }
      }
      return result;
    }

    @Override
    public void notifyEvicted(MemoryBuffer buffer) {
      cache.notifyEvicted(buffer);
    }

    @Override
    public long markBuffersForProactiveEviction(Predicate<CacheTag> predicate, boolean isInstantDeallocation) {
      return cache.markBuffersForProactiveEviction(predicate, isInstantDeallocation);
    }

    @Override
    public Allocator getAllocator() {
      return this;
    }

    @Override
    public void decRefBuffer(MemoryBuffer buffer) {
      cache.decRefBuffer(buffer);
    }

    @Override
    public void decRefBuffers(List<MemoryBuffer> buffers) {
      cache.decRefBuffers(buffers);
    }

    @Override
    public boolean incRefBuffer(MemoryBuffer buffer) {
      // A raw allocation handed to the batch this way is freed by the cache on its last decref.
      if (allocated.contains(buffer)) {
        accepted.add(buffer);
      }
      return cache.incRefBuffer(buffer);
    }

    @Override
    public void allocateMultiple(MemoryBuffer[] dest, int size) {
      allocator.allocateMultiple(dest, size);
      Collections.addAll(allocated, dest);
    }

    @Override
    public void allocateMultiple(MemoryBuffer[] dest, int size, BufferObjectFactory factory) {
      allocator.allocateMultiple(dest, size, factory);
      Collections.addAll(allocated, dest);
    }

    @Override
    public MemoryBuffer createUnallocated() {
      return allocator.createUnallocated();
    }

    @Override
    public void deallocate(MemoryBuffer buffer) {
      freed.add(buffer);
      allocator.deallocate(buffer);
    }

    @Override
    public boolean isDirectAlloc() {
      return allocator.isDirectAlloc();
    }

    @Override
    public int getMaxAllocation() {
      return allocator.getMaxAllocation();
    }
  }

  private static final class Run {
    final List<Object[]> rows;
    final List<Integer> batchSizes;
    final ColumnVector[] firstBatchCols;
    final boolean done;
    final Throwable error;
    final TezCounters counters;
    final List<MemoryBuffer> buffers;
    final List<long[]> reads;
    final Ledger ledger;

    Run(CapturingConsumer c, TezCounters counters, List<MemoryBuffer> buffers, List<long[]> reads,
        Ledger ledger) {
      this.rows = c.rows;
      this.batchSizes = c.batchSizes;
      this.firstBatchCols = c.firstBatchCols;
      this.done = c.done;
      this.error = c.error;
      this.counters = counters;
      this.buffers = buffers;
      this.reads = reads;
      this.ledger = ledger;
    }

    long counter(LlapIOCounters counter) {
      return counters.findCounter(counter).getValue();
    }

    void assertClean() {
      assertNull(error == null ? null : error.toString(), error);
      assertTrue("setDone not reached", done);
      for (MemoryBuffer b : buffers) {
        assertFalse(b.toString(), ((LlapAllocatorBuffer) b).isLocked());
      }
      ledger.assertNothingLeaked();
    }
  }

  private static final class CapturingConsumer implements Consumer<ColumnVectorBatch> {
    final List<Object[]> rows = new ArrayList<>();
    final List<Integer> batchSizes = new ArrayList<>();
    ColumnVector[] firstBatchCols;
    boolean done;
    Throwable error;

    @Override
    public void consumeData(ColumnVectorBatch cvb) {
      if (firstBatchCols == null) {
        firstBatchCols = cvb.cols.clone();
      }
      batchSizes.add(cvb.size);
      for (int r = 0; r < cvb.size; ++r) {
        Object[] row = new Object[cvb.cols.length];
        for (int c = 0; c < cvb.cols.length; ++c) {
          row[c] = value(cvb.cols[c], r);
        }
        rows.add(row);
      }
    }

    private static Object value(ColumnVector cv, int r) {
      int ix = cv.isRepeating ? 0 : r;
      if (!cv.noNulls && cv.isNull[ix]) {
        return null;
      }
      if (cv instanceof LongColumnVector l) {
        return l.vector[ix];
      }
      if (cv instanceof DoubleColumnVector d) {
        return d.vector[ix];
      }
      if (cv instanceof BytesColumnVector b) {
        return b.toString(ix);
      }
      throw new AssertionError("Unexpected vector " + cv.getClass());
    }

    @Override
    public void setDone() {
      done = true;
    }

    @Override
    public void setError(Throwable t) {
      error = t;
    }
  }

  /** Only getPhysicalColumnIds is consulted by the Parquet pipeline. */
  private static Includes includes(List<Integer> physicalColumnIds) {
    return new Includes() {
      @Override
      public List<Integer> getPhysicalColumnIds() {
        return physicalColumnIds;
      }

      @Override
      public List<Integer> getReaderLogicalColumnIds() {
        return physicalColumnIds;
      }

      @Override
      public List<Integer> getLogicalOrderedColumnIds() {
        return physicalColumnIds;
      }

      @Override
      public boolean[] generateFileIncludes(TypeDescription fileSchema) {
        throw new UnsupportedOperationException();
      }

      @Override
      public TypeDescription[] getBatchReaderTypes(TypeDescription fileSchema) {
        throw new UnsupportedOperationException();
      }

      @Override
      public String[] getOriginalColumnNames(TypeDescription fileSchema) {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getQueryId() {
        return "test-query";
      }

      @Override
      public boolean isProbeDecodeEnabled() {
        return false;
      }

      @Override
      public byte getProbeMjSmallTablePos() {
        return -1;
      }

      @Override
      public String getProbeCacheKey() {
        return null;
      }

      @Override
      public String getProbeColName() {
        return null;
      }

      @Override
      public int getProbeColIdx() {
        return -1;
      }
    };
  }
}
