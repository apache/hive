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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
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
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.encoded.CacheChunk;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.NullWritable;
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
 *
 * <p>Each test asserts whatever it is about - read ranges, counters, buffer accounting - while
 * {@link #read} checks the rows themselves, against both the Java-computed {@link #expectedRow} and
 * the stock VectorizedParquetRecordReader. So the rows of every successful run are verified two ways
 * even in tests that never mention them, and a new test gets that for free.
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
  /** Opts out of the expectedRow(i) check, for tests whose Hive schema is not the fixture's. */
  private boolean skipExpectedRows;
  /** Opts out of the A/B check, for tests the stock reader cannot reproduce. */
  private boolean skipNonNativeParity;
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
    skipExpectedRows = false;
    skipNonNativeParity = false;
    decoded = 0;
    decodedAtRequest.clear();
  }

  @Test
  public void testFullReadAllColumns() throws Exception {
    JobConf job = jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5);
    FileSplit split = wholeFile();
    Run run = read(job, split);

    run.assertClean();
    assertEquals(ROWS, run.rows.size());
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
    JobConf job = jobConf(COLUMNS, TYPES, 0, 3);
    FileSplit split = new FileSplit(file, start, end - start, (String[]) null);
    Run run = read(job, split);

    run.assertClean();
    assertEquals(1, run.counter(LlapIOCounters.SELECTED_ROWGROUPS));
    assertEquals(ROWS_PER_GROUP, run.rows.size());
    // The split covers row group 1 only, so the first row emitted must be that group's first row.
    assertArrayEquals(project(expectedRow(ROWS_PER_GROUP), 0, 3), run.rows.get(0));
  }

  @Test
  public void testDisjointSplitsEmitEveryRowOnce() throws Exception {
    long cut = footer.getBlocks().get(2).getStartingPos();
    JobConf firstJob = jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5);
    FileSplit firstSplit = new FileSplit(file, 0, cut, (String[]) null);
    Run first = read(firstJob, firstSplit);
    JobConf secondJob = jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5);
    FileSplit secondSplit = new FileSplit(file, cut, fileLength - cut, (String[]) null);
    Run second = read(secondJob, secondSplit);

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
    JobConf job = jobConf(COLUMNS, TYPES, 4, 0, 3);
    FileSplit split = wholeFile();
    Run run = read(job, split);

    run.assertClean();
    assertEquals(ROWS, run.rows.size());
    assertEquals(3, run.firstBatchCols.length);
    assertTrue(run.firstBatchCols[0] instanceof DoubleColumnVector);
    assertTrue(run.firstBatchCols[1] instanceof LongColumnVector);
    assertTrue(run.firstBatchCols[2] instanceof BytesColumnVector);
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
  }

  @Test
  public void testFooterLookupBumpsMetadataCacheCounters() throws Exception {
    // Footer cache is a @BeforeClass singleton, so an earlier test may have already populated it.
    // We only assert on the *second* read here — its counters are order-independent because the
    // second read always finds the footer that the first read of this test put in (META_HIT is 1
    // and META_MISS is 0 regardless of prior state).
    read(jobConf(COLUMNS, TYPES, 0), wholeFile()).assertClean();

    Run second = read(jobConf(COLUMNS, TYPES, 0), wholeFile());
    second.assertClean();
    assertEquals(1, second.counter(LlapIOCounters.METADATA_CACHE_HIT));
    assertEquals(0, second.counter(LlapIOCounters.METADATA_CACHE_MISS));
  }

  @Test
  public void testFragmentCountersParityWithOrc() throws Exception {
    // The LLAP IO summary is emitted from QueryFragmentCounters#toString(). ORC populates FILE,
    // STRIPES, TOTAL_IO_TIME_NS and HDFS_TIME_NS for every fragment; the native Parquet reader
    // should populate the same fields so a Parquet fragment shows up in the summary the same way.
    // (TABLE is only set when LLAP_TRACK_CACHE_USAGE is on, which this test's fixture disables.)
    Run run = read(jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5), wholeFile());

    run.assertClean();
    assertEquals(ROW_GROUPS, run.counter(LlapIOCounters.SELECTED_ROWGROUPS));
    assertTrue("TOTAL_IO_TIME_NS not recorded",
        run.counter(LlapIOCounters.TOTAL_IO_TIME_NS) > 0);
    assertTrue("HDFS_TIME_NS not recorded",
        run.counter(LlapIOCounters.HDFS_TIME_NS) > 0);
    assertEquals("exactly one of metadata hit/miss should be bumped per fragment", 1,
        run.counter(LlapIOCounters.METADATA_CACHE_HIT)
            + run.counter(LlapIOCounters.METADATA_CACHE_MISS));

    String summary = run.fragmentCounters.toString();
    assertTrue("FILE descriptor missing from summary: " + summary,
        summary.contains(file.toString()));
    assertTrue("STRIPES descriptor missing from summary: " + summary,
        summary.contains("0," + ROW_GROUPS));
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
  }

  @Test
  public void testPooledBufferIsLimitedToItsRange() throws Exception {
    unslicedBuffers = true;
    // The second read of a run takes a pooled buffer that the first left longer than the range.
    read(jobConf(COLUMNS, TYPES, 0, 1, 2, 3, 4, 5), wholeFile());
    Run run = read(jobConf(COLUMNS, TYPES, 3), wholeFile());

    run.assertClean();
    assertEquals(ROWS, run.rows.size());
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
    // Pins which group survived: assertExpectedRows derives each block's offset from its own first
    // row, so it would be just as happy with a wrong-but-self-consistent group.
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
    // The Hive schema has a column the file does not, so expectedRow(i) does not describe these
    // rows; the stock reader resolves the missing column the same way, so the A/B check still holds.
    skipExpectedRows = true;
    Run run = read(jobConf(COLUMNS + ",extra", TYPES + ",string", 0, 6), wholeFile());

    run.assertClean();
    assertEquals(ROWS, run.rows.size());
    assertTrue(run.firstBatchCols[1] instanceof BytesColumnVector);
    for (int i : new int[] {0, 1500, ROWS - 1}) {
      assertArrayEquals("row " + i, new Object[] {(long) i, null}, run.rows.get(i));
    }
  }

  /**
   * The four ways an Iceberg Hive schema can disagree with the file's, in one read: an order of its
   * own (ratio before id, and three of the file's columns dropped), a column the file does not have
   * that an initial default fills in ("added" -> 42), a dropped-and-recreated column, which Iceberg
   * marks with the DUMMY_FOR_RECREATED_FIELD_IN_FILESCHEMA placeholder so that the stale bytes read
   * back as null instead of as the recreated column, and a column that merely moved ("name" is last
   * here, fourth in the file). Rows 7 and 1500 are in the sample because name is null every seventh
   * row, so a null that comes from the file still has to survive the evolution, and because the
   * column readers are rebuilt per row group, so a default filled in for the first group only fails
   * at the boundary.
   */
  @Test
  public void testEvolvedColumnsReorderedDefaultedAndRecreated() throws Exception {
    // Neither ground truth applies. expectedRow(i) is a row of the fixture's six file columns and
    // assertExpectedRows only picks indices out of it, but two of the five values below come from the
    // Hive schema rather than the file - the recreated column's null and the defaulted 42 - so no
    // projection of expectedRow(i) can produce this row. And the stock reader has no initial-defaults
    // plumbing, so it would emit null for "added" where the native path emits 42.
    skipExpectedRows = true;
    skipNonNativeParity = true;
    String columns = "ratio,id,<<DUMMY_FOR_RECREATED_FIELD_IN_FILESCHEMA>>,added,name";
    Run run = read(jobConf(columns, "double,int,string,int,string", 0, 1, 2, 3, 4), wholeFile(),
        Map.of("added", 42));

    run.assertClean();
    assertEquals(ROWS, run.rows.size());
    assertTrue(run.firstBatchCols[2] instanceof BytesColumnVector);
    assertTrue(run.firstBatchCols[3] instanceof LongColumnVector);
    assertTrue(run.firstBatchCols[3].isRepeating);
    // Values in Hive order: ratio, id, the recreated column, added, name.
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
    // Groups 0 and 2 are selected, so row group 2 follows row group 0 with nothing in between.
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
    for (MemoryBuffer b : run.buffers) {
      assertFalse(((LlapAllocatorBuffer) b).isLocked());
    }
  }

  @Test
  public void testProjectedLeavesSkipsPrecedingNestedGroup() {
    /*
     * File schema: struct nested { a, b }, x. Column chunks are in leaf order — nested.a,
     * nested.b, x — so x's top-level ordinal (1) diverges from its leaf ordinal (2); the
     * method must return the leaf ordinal.
     */
    MessageType fileSchema = Types.buildMessage()
        .requiredGroup()
            .required(PrimitiveTypeName.INT32).named("a")
            .required(PrimitiveTypeName.INT32).named("b")
        .named("nested")
        .required(PrimitiveTypeName.INT32).named("x")
        .named("file_schema");
    MessageType requested = Types.buildMessage()
        .required(PrimitiveTypeName.INT32).named("x")
        .named("requested");

    int[] leaves = ParquetEncodedDataReader.projectedLeaves(requested, fileSchema);

    // x sits at leaf index 2 (nested.a=0, nested.b=1, x=2), not at its top-level ordinal 1.
    assertArrayEquals(new int[] {2}, leaves);
  }

  @Test
  public void testProjectedLeavesMissingFieldSkipped() {
    // File schema: a, b. Requested: a, missing. The method must resolve a and drop the
    // unknown field silently rather than returning a placeholder or throwing.
    MessageType fileSchema = Types.buildMessage()
        .required(PrimitiveTypeName.INT32).named("a")
        .required(PrimitiveTypeName.INT32).named("b")
        .named("file_schema");
    MessageType requested = Types.buildMessage()
        .required(PrimitiveTypeName.INT32).named("a")
        .required(PrimitiveTypeName.INT32).named("missing")
        .named("requested");

    // Only a is present in the file schema (leaf 0); "missing" contributes nothing.
    assertArrayEquals(new int[] {0},
        ParquetEncodedDataReader.projectedLeaves(requested, fileSchema));
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
    HiveConf.setVar(job, ConfVars.HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_ENABLED,
        HiveConf.HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_DECIMAL_64);
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

  /**
   * Drives one split through the native reader and checks its rows against both ground truths -
   * the Java-computed {@link #expectedRow} and the stock VectorizedParquetRecordReader - so every
   * test that reads rows gets the full A/B comparison without having to ask for it. A run that
   * errored or was stopped emits a prefix of the split rather than nothing, so its rows are checked
   * too, and only the row count is let off. The sole opt-outs are the two tests whose Hive schema is
   * not the fixture's ({@link #skipExpectedRows} / {@link #skipNonNativeParity}).
   */
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
        for (MemoryBuffer[] col : batch.columnBuffers()) {
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
    ParquetEncodedDataReader parquetReader = new ParquetEncodedDataReader(
        ledger, ledger, readerDaemonConf, job, split, edc, counters) {
      @Override
      FSDataInputStream openFile(FileSystem fs) throws IOException {
        return new RecordingStream(super.openFile(fs), reads);
      }
    };
    this.reader = parquetReader;
    edc.init(parquetReader, parquetReader);
    parquetReader.loadFooter();
    parquetReader.call();
    Run run = new Run(downstream, counters, tezCounters, buffers, reads, ledger);
    boolean whole = run.error == null && !stopAfterFirstBatch;
    if (!skipExpectedRows) {
      assertExpectedRows(run, job, whole);
    }
    if (!skipNonNativeParity) {
      assertParityWithNonNative(run, job, split, whole);
    }
    return run;
  }

  /**
   * Checks every emitted row against {@link #expectedRow}. Rows arrive as whole row groups, so each
   * block of ROWS_PER_GROUP is located by matching its first row against the row-group starts; that
   * keeps the expectation independent of the reader's own split/SARG planning, which is what a test
   * asserting "the pruned group was skipped" needs. A run that was stopped or errored gets the same
   * treatment, less the whole-row-groups assertion, so that its rows are checked as far as they go.
   */
  private static void assertExpectedRows(Run run, JobConf job, boolean whole) {
    List<Integer> columnIds = ColumnProjectionUtils.getReadColumnIDs(job);
    int[] projection = columnIds.stream().mapToInt(Integer::intValue).toArray();
    if (whole) {
      assertEquals("rows should arrive as whole row groups", 0, run.rows.size() % ROWS_PER_GROUP);
    }
    for (int offset = 0; offset < run.rows.size(); offset += ROWS_PER_GROUP) {
      int firstRow = rowGroupStartOf(run.rows.get(offset), projection);
      int rows = Math.min(ROWS_PER_GROUP, run.rows.size() - offset);
      for (int i = 0; i < rows; ++i) {
        assertArrayEquals("row " + (firstRow + i),
            project(expectedRow(firstRow + i), projection), run.rows.get(offset + i));
      }
    }
  }

  private static int rowGroupStartOf(Object[] firstRow, int[] projection) {
    for (int group = 0; group < ROW_GROUPS; ++group) {
      int candidate = group * ROWS_PER_GROUP;
      if (Arrays.equals(project(expectedRow(candidate), projection), firstRow)) {
        return candidate;
      }
    }
    throw new AssertionError(
        "emitted block starts at no row group: " + Arrays.toString(firstRow));
  }

  // ---- parity: compare native rows against the stock VectorizedParquetRecordReader ----
  //
  // The second of the two ground truths read(...) checks every run against. expectedRow(i) is
  // computed in Java, so it catches a reader that returns the wrong values; this one is what parquet's
  // own decode (driven through VectorizedParquetRecordReader) returns for the same file, JobConf and
  // split, so it also catches the case where encode and decode drift together - and it covers the
  // row-group planning, since the stock reader applies the split bounds and the SARG itself.

  /** Reads the same (JobConf, FileSplit) via VectorizedParquetRecordReader, returning rows in projection order. */
  private static List<Object[]> readNonNative(JobConf job, FileSplit split) throws Exception {
    JobConf mrJob = new JobConf(job);
    // VectorizedParquetRecordReader looks up its rbCtx through Utilities.getMapWork, which requires
    // both HIVE_VECTORIZATION_ENABLED and a PLAN path set on the conf.
    HiveConf.setBoolVar(mrJob, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(mrJob, HiveConf.ConfVars.PLAN, "//tmp");
    List<TypeInfo> types = DataWritableReadSupport.getColumnTypes(mrJob.get(IOConstants.COLUMNS_TYPES));
    Utilities.setMapWork(mrJob, mapWorkFor(mrJob, types));
    List<Integer> colsToInclude = ColumnProjectionUtils.getReadColumnIDs(mrJob);
    List<Object[]> rows = new ArrayList<>();
    try (VectorizedParquetRecordReader mrReader = new VectorizedParquetRecordReader(split, mrJob)) {
      VectorizedRowBatch batch = mrReader.createValue();
      while (mrReader.next(NullWritable.get(), batch)) {
        for (int r = 0; r < batch.size; ++r) {
          Object[] row = new Object[colsToInclude.size()];
          for (int i = 0; i < colsToInclude.size(); ++i) {
            row[i] = CapturingConsumer.value(batch.cols[colsToInclude.get(i)], r);
          }
          rows.add(row);
        }
      }
    }
    return rows;
  }

  /**
   * Builds the MapWork/VectorizedRowBatchCtx pair VectorizedParquetRecordReader consults, matching
   * the physical variation the native path picks (DECIMAL_64 for decimal columns up to precision 18)
   * so both readers emit the same ColumnVector shape.
   */
  private static MapWork mapWorkFor(JobConf job, List<TypeInfo> types) throws Exception {
    List<String> names = DataWritableReadSupport.getColumnNames(job.get(IOConstants.COLUMNS));
    StructTypeInfo rowType = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(names, types);
    StructObjectInspector rowInspector = new ArrayWritableObjectInspector(rowType);
    DataTypePhysicalVariation[] variations = new DataTypePhysicalVariation[types.size()];
    for (int i = 0; i < types.size(); ++i) {
      TypeInfo t = types.get(i);
      variations[i] = (t instanceof DecimalTypeInfo dti
              && dti.precision() <= TypeDescription.MAX_DECIMAL64_PRECISION)
          ? DataTypePhysicalVariation.DECIMAL_64
          : DataTypePhysicalVariation.NONE;
    }
    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx();
    rbCtx.init(rowInspector, new String[0]);
    rbCtx.setRowDataTypePhysicalVariations(variations);
    MapWork mapWork = new MapWork();
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(rbCtx);
    return mapWork;
  }

  /**
   * Reads the same (job, split) with VectorizedParquetRecordReader and asserts row-by-row equality.
   * A run that was stopped or errored emits a prefix of the split, so only its row count is let off:
   * the rows it did emit still have to match parquet's, and there cannot be more of them.
   */
  private static void assertParityWithNonNative(Run run, JobConf job, FileSplit split, boolean whole)
      throws Exception {
    List<Object[]> stock = readNonNative(job, split);
    if (whole) {
      assertEquals("row count differs from parquet", stock.size(), run.rows.size());
    } else {
      assertTrue("more rows than parquet found: " + run.rows.size() + " > " + stock.size(),
          run.rows.size() <= stock.size());
    }
    for (int i = 0; i < run.rows.size(); ++i) {
      assertArrayEquals("row " + i + " differs from parquet", stock.get(i), run.rows.get(i));
    }
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
      recordRanges(ranges);
      super.readVectored(ranges, allocate);
      injectFailure(ranges);
    }

    @Override
    public void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate,
        java.util.function.Consumer<ByteBuffer> release) throws IOException {
      recordRanges(ranges);
      super.readVectored(ranges, allocate, release);
      injectFailure(ranges);
    }

    private void recordRanges(List<? extends FileRange> ranges) {
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
  }

  private static final class Run {
    final List<Object[]> rows;
    final List<Integer> batchSizes;
    final ColumnVector[] firstBatchCols;
    final boolean done;
    final Throwable error;
    final QueryFragmentCounters fragmentCounters;
    final TezCounters counters;
    final List<MemoryBuffer> buffers;
    final List<long[]> reads;
    final Ledger ledger;

    Run(CapturingConsumer c, QueryFragmentCounters fragmentCounters, TezCounters counters,
        List<MemoryBuffer> buffers, List<long[]> reads, Ledger ledger) {
      this.rows = c.rows;
      this.batchSizes = c.batchSizes;
      this.firstBatchCols = c.firstBatchCols;
      this.done = c.done;
      this.error = c.error;
      this.fragmentCounters = fragmentCounters;
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
