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

package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.cache.BuddyAllocator;
import org.apache.hadoop.hive.llap.cache.EvictionListener;
import org.apache.hadoop.hive.llap.cache.LlapAllocatorBuffer;
import org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheMemoryManager;
import org.apache.hadoop.hive.llap.cache.LowLevelLrfuCachePolicy;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache.LlapBufferOrBuffers;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.hive.DeleteCacheCounters;
import org.apache.iceberg.mr.mapred.MapredIcebergInputFormat;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.Pair;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.mapreduce.hadoop.mapred.MRReporter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;

public class TestCachingDeleteLoader {
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()));
  private static final String DATA_FILE_A = "file:/tmp/data/a.parquet";
  private static final String DATA_FILE_B = "file:/tmp/data/b.parquet";
  private static final String DATA_FILE_C = "file:/tmp/data/c.parquet";
  // Small enough that a DV with a few dozen positions spans several cache buffers.
  private static final int MAX_ALLOC = 64;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private final AtomicInteger opens = new AtomicInteger();
  private final List<LlapBufferOrBuffers> handedOut = new CopyOnWriteArrayList<>();
  private final TezCounters counters = new TezCounters();
  private JobConf conf;
  private Table table;
  private Table v2Table;
  private MetadataCache cache;

  @Before
  public void before() throws IOException {
    conf = new JobConf();
    table = new HadoopTables(conf).create(SCHEMA, PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"), temp.newFolder().toString());
    v2Table = new HadoopTables(conf).create(SCHEMA, PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"), temp.newFolder().toString());
    cache = trackingMetadataCache(conf, handedOut);
  }

  @Test
  public void testDVIsServedFromCacheAcrossLoaders() throws IOException {
    DeleteFile dv = writeDVs(ImmutableMap.of(DATA_FILE_A, new long[] { 0, 5, 7 })).get(0);

    for (int i = 0; i < 3; i++) {
      assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A), 0, 5, 7);
    }

    Assert.assertEquals("Only the first read may open the file", 1, opens.get());
    assertCounters(counters, 2, 1, 1, 0);
    Assert.assertEquals(dv.contentSizeInBytes().longValue(),
        counters.findCounter(DeleteCacheCounters.DELETE_CACHE_PUT_BYTES).getValue());
    // a DV has no directory
    Mockito.verify(cache, Mockito.times(1)).putFileMetadata(any(), any(ByteBuffer.class), any(), any());
    assertBuffersReleased();
  }

  @Test
  public void testDVSpanningSeveralCacheBuffers() throws IOException {
    long[] positions = LongStream.range(0, 50).map(i -> i * 3).toArray();
    DeleteFile dv = writeDVs(ImmutableMap.of(DATA_FILE_A, positions)).get(0);
    Assert.assertTrue(dv.contentSizeInBytes() > MAX_ALLOC);

    for (int i = 0; i < 3; i++) {
      assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A), positions);
    }

    Assert.assertEquals(1, opens.get());
    assertBuffersReleased();
  }

  @Test
  public void testDVsOfOnePuffinFileDoNotCollide() throws IOException {
    List<DeleteFile> dvs = writeDVs(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }, DATA_FILE_B, new long[] { 2, 3 }));
    DeleteFile dvA = dvs.stream().filter(dv -> DATA_FILE_A.equals(dv.referencedDataFile())).findFirst().get();
    DeleteFile dvB = dvs.stream().filter(dv -> DATA_FILE_B.equals(dv.referencedDataFile())).findFirst().get();
    Assert.assertEquals(dvA.location(), dvB.location());
    Assert.assertNotEquals(dvA.contentOffset(), dvB.contentOffset());

    for (int i = 0; i < 2; i++) {
      assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(dvA), DATA_FILE_A), 1);
      assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(dvB), DATA_FILE_B), 2, 3);
    }
    Assert.assertEquals(2, opens.get());
    assertBuffersReleased();
  }

  @Test
  public void testCacheLevelNoneBypassesCache() throws IOException {
    DeleteFile dv = writeDVs(ImmutableMap.of(DATA_FILE_A, new long[] { 4 })).get(0);
    // HiveConf validates the level case-insensitively and keeps the user's casing
    HiveConf.setVar(conf, HiveConf.ConfVars.LLAP_IO_CACHE_DELETEDELTAS, "NONE");

    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A), 4);
    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A), 4);

    Assert.assertEquals(2, opens.get());
    Mockito.verifyNoInteractions(cache);
  }

  @Test
  public void testCacheLevelMetadataCachesDVs() throws IOException {
    DeleteFile dv = writeDVs(ImmutableMap.of(DATA_FILE_A, new long[] { 4 })).get(0);
    HiveConf.setVar(conf, HiveConf.ConfVars.LLAP_IO_CACHE_DELETEDELTAS, "metadata");

    loader(cache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A);
    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A), 4);

    Assert.assertEquals(1, opens.get());
  }

  @Test
  public void testWithoutInjectedCache() throws IOException {
    DeleteFile dv = writeDVs(ImmutableMap.of(DATA_FILE_A, new long[] { 4, 9 })).get(0);

    assertDeleted(loader(null).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A), 4, 9);
    assertDeleted(loader(null).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A), 4, 9);

    Assert.assertEquals(2, opens.get());
  }

  @Test
  public void testPositionDeleteFileIsCachedPerDataFile() throws IOException {
    DeleteFile deletes =
        writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1, 2 }, DATA_FILE_B, new long[] { 5 }));

    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A), 1, 2);
    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_B), 5);
    PositionDeleteIndex cached = loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A);
    assertDeleted(cached, 1, 2);
    Assert.assertSame(deletes, Iterables.getOnlyElement(cached.deleteFiles()));

    // each read caches its own data file's entry; outside a query's object cache the sibling decodes the file again
    Assert.assertEquals(2, opens.get());
    assertBuffersReleased();
  }

  /**
   * A read caches the deletes of its data file only, not of every data file the delete file references: a daemon
   * caches the pairs it reads, and a writer's delete file covers the data files of a whole partition.
   */
  @Test
  public void testPositionDeleteFileCachesOnlyTheRequestedDataFile() throws IOException {
    DeleteFile deletes = writePosDeletes(ImmutableMap.of(
        DATA_FILE_A, new long[] { 1 }, DATA_FILE_B, new long[] { 2 }, DATA_FILE_C, new long[] { 3 }));

    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_B), 2);

    Mockito.verify(cache, Mockito.times(1)).putFileMetadata(pairKey(), any(ByteBuffer.class), any(), any());
    Mockito.verify(cache, Mockito.times(1)).putFileMetadata(directoryKey(), any(ByteBuffer.class), any(), any());
    assertBuffersReleased();
  }

  @Test
  public void testPositionDeleteFilesOfOneDataFileAreMerged() throws IOException {
    DeleteFile first = writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }));
    DeleteFile second =
        writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 2 }, DATA_FILE_B, new long[] { 3 }));

    for (int i = 0; i < 2; i++) {
      assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(first, second), DATA_FILE_A), 1, 2);
    }
    Assert.assertEquals(2, opens.get());
    assertBuffersReleased();
  }

  /**
   * A data file the delete file does not reference gets an empty index and no entry of its own; once the delete file's
   * directory is cached, it needs no decode either.
   */
  @Test
  public void testDataFileWithoutDeletesInPositionDeleteFileIsNotCached() throws IOException {
    // B is within the file_path bounds [A, C] of the delete file, but has no deletes in it
    DeleteFile deletes = writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }, DATA_FILE_C, new long[] { 2 }));

    for (int i = 0; i < 2; i++) {
      PositionDeleteIndex index = loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_B);
      assertDeleted(index);
      Assert.assertSame(deletes, Iterables.getOnlyElement(index.deleteFiles()));
    }
    Assert.assertEquals("the second read finds B missing from the directory", 1, opens.get());
    Mockito.verify(cache, Mockito.never()).putFileMetadata(pairKey(), any(ByteBuffer.class), any(), any());
    Mockito.verify(cache, Mockito.times(1)).putFileMetadata(directoryKey(), any(ByteBuffer.class), any(), any());
    assertCounters(counters, 0, 0, 0, 0);
    Assert.assertEquals(2, counters.findCounter(DeleteCacheCounters.DELETE_CACHE_UNREFERENCED).getValue());
    assertBuffersReleased();
  }

  /**
   * The directory outlives the query: a later query reads an unreferenced data file without opening the delete file.
   */
  @Test
  public void testUnreferencedDataFileOpensNothingInLaterQueries() throws IOException {
    DeleteFile deletes = writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }, DATA_FILE_C, new long[] { 3 }));
    LlapProxy.setDaemon(true);
    try {
      for (String query : List.of("query1", "query2")) {
        HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID, query);
        assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_B));
        ObjectCacheFactory.removeLlapQueryCache(query);
      }
    } finally {
      LlapProxy.setDaemon(false);
    }
    Assert.assertEquals("only the first query decodes the delete file", 1, opens.get());
    Assert.assertEquals(2, counters.findCounter(DeleteCacheCounters.DELETE_CACHE_UNREFERENCED).getValue());
    Mockito.verify(cache, Mockito.never()).putFileMetadata(pairKey(), any(ByteBuffer.class), any(), any());
    assertBuffersReleased();
  }

  @Test
  public void testDirectoryEvictedWhilePairsRemain() throws IOException {
    DeleteFile deletes = writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }, DATA_FILE_C, new long[] { 3 }));
    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A), 1);
    Mockito.doReturn(null).when(cache).getFileMetadata(directoryKey());

    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A), 1);
    Assert.assertEquals("the pair still serves", 1, opens.get());
    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_B));
    Assert.assertEquals("an unknown directory decodes the delete file", 2, opens.get());
  }

  @Test
  public void testPairsEvictedWhileDirectoryRemains() throws IOException {
    DeleteFile deletes = writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }, DATA_FILE_C, new long[] { 3 }));
    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A), 1);
    Mockito.doReturn(null).when(cache).getFileMetadata(pairKey());

    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_B));
    Assert.assertEquals("the directory answers for an unreferenced data file", 1, opens.get());
    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_C), 3);
    Assert.assertEquals("a referenced data file without its entry decodes the delete file", 2, opens.get());
  }

  @Test
  public void testFailedDirectoryPutIsCountedAndDecodesAgain() throws IOException {
    DeleteFile deletes = writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }, DATA_FILE_C, new long[] { 3 }));
    Mockito.doThrow(new RuntimeException("Cannot reserve memory"))
        .when(cache).putFileMetadata(directoryKey(), any(ByteBuffer.class), any(), any());

    for (int i = 0; i < 2; i++) {
      assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_B));
    }
    Assert.assertEquals("without a directory every read decodes the delete file", 2, opens.get());
    assertCounters(counters, 0, 0, 0, 2);
    Assert.assertEquals(2, counters.findCounter(DeleteCacheCounters.DELETE_CACHE_UNREFERENCED).getValue());
  }

  /**
   * A file-scoped delete file is attached to its one data file only: it has no directory.
   */
  @Test
  public void testFileScopedDeleteFileHasNoDirectory() throws IOException {
    DeleteFile deletes = writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }));
    Assert.assertNotNull(ContentFileUtil.referencedDataFile(deletes));

    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A), 1);

    Mockito.verify(cache, Mockito.never()).putFileMetadata(directoryKey(), any(ByteBuffer.class), any(), any());
    Mockito.verify(cache, Mockito.never()).getFileMetadata(directoryKey());
  }

  /**
   * A read that does not cover every row of the delete file publishes no directory: one that missed a data file the
   * delete file references would drop its deletes. Every lookup then decodes the delete file, with exact results.
   */
  @Test
  public void testShortReadPublishesNoDirectory() throws IOException {
    DeleteFile written =
        writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }, DATA_FILE_C, new long[] { 3 }));
    // the reader reads 2 rows of a file that claims 3, as a filtered or truncated read would
    DeleteFile deletes = FileMetadata.deleteFileBuilder(v2Table.spec()).copy(written)
        .withRecordCount(written.recordCount() + 1).build();

    for (int i = 0; i < 2; i++) {
      assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_B));
    }
    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_C), 3);

    Assert.assertEquals("without a directory every unreferenced lookup decodes", 3, opens.get());
    Mockito.verify(cache, Mockito.never()).putFileMetadata(directoryKey(), any(ByteBuffer.class), any(), any());
    Assert.assertEquals(2, counters.findCounter(DeleteCacheCounters.DELETE_CACHE_UNREFERENCED).getValue());
  }

  @Test
  public void testCacheLevelMetadataSkipsPositionDeleteFiles() throws IOException {
    DeleteFile deletes = writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }));
    HiveConf.setVar(conf, HiveConf.ConfVars.LLAP_IO_CACHE_DELETEDELTAS, "metadata");

    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A), 1);
    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A), 1);

    Assert.assertEquals(2, opens.get());
    Mockito.verifyNoInteractions(cache);
  }

  @Test
  public void testPositionDeleteFileIsDecodedOncePerQuery() throws IOException {
    // B is within the file_path bounds [A, C] of the delete file, but has no deletes in it
    DeleteFile deletes = writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }, DATA_FILE_C, new long[] { 3 }));
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID, "query1");
    LlapProxy.setDaemon(true);
    try {
      assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_B));
      assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_C), 3);
      assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A), 1);
    } finally {
      LlapProxy.setDaemon(false);
      ObjectCacheFactory.removeLlapQueryCache("query1");
    }
    Assert.assertEquals("Data files the delete file does not reference must not decode it again", 1, opens.get());
    // the data files the delete file references are cached, and its directory
    Mockito.verify(cache, Mockito.times(2)).putFileMetadata(pairKey(), any(ByteBuffer.class), any(), any());
    Mockito.verify(cache, Mockito.times(1)).putFileMetadata(directoryKey(), any(ByteBuffer.class), any(), any());
    assertCounters(counters, 0, 2, 2, 0);
    Assert.assertEquals(1, counters.findCounter(DeleteCacheCounters.DELETE_CACHE_UNREFERENCED).getValue());
    assertBuffersReleased();
  }

  @Test
  public void testFailedPutDoesNotFailRead() throws IOException {
    DeleteFile dv = writeDVs(ImmutableMap.of(DATA_FILE_A, new long[] { 4 })).get(0);
    DeleteFile deletes = writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }));
    Mockito.doThrow(new RuntimeException("Cannot reserve memory"))
        .when(cache).putFileMetadata(any(), any(ByteBuffer.class), any(), any());

    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A), 4);
    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A), 1);
    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_B));
  }

  @Test
  public void testCountersOfPositionDeleteFile() throws IOException {
    DeleteFile deletes =
        writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1, 2 }, DATA_FILE_B, new long[] { 5 }));

    loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A);
    loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_B);
    loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A);

    assertCounters(counters, 1, 2, 2, 0);
  }

  @Test
  public void testCountersOfFailedPuts() throws IOException {
    DeleteFile dv = writeDVs(ImmutableMap.of(DATA_FILE_A, new long[] { 4 })).get(0);
    Mockito.doThrow(new RuntimeException("Cannot reserve memory"))
        .when(cache).putFileMetadata(any(), any(ByteBuffer.class), any(), any());

    loader(cache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A);
    loader(cache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A);

    assertCounters(counters, 0, 2, 0, 2);
    Assert.assertEquals(2 * dv.contentSizeInBytes(),
        counters.findCounter(DeleteCacheCounters.DELETE_CACHE_PUT_FAILED_BYTES).getValue());
    Assert.assertEquals(0, counters.findCounter(DeleteCacheCounters.DELETE_CACHE_PUT_BYTES).getValue());
  }

  /** A DV larger than the whole cache: the put cannot evict enough and gives up instead of waiting. */
  @Test(timeout = 10_000)
  public void testPutThatCannotReserveMemoryDoesNotWait() throws IOException {
    long[] positions = LongStream.range(0, 400).map(i -> i * 3).toArray();
    DeleteFile dv = writeDVs(ImmutableMap.of(DATA_FILE_A, positions)).get(0);
    MetadataCache tinyCache = trackingMetadataCache(conf, handedOut, 512);

    assertDeleted(loader(tinyCache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A), positions);
    assertCounters(counters, 0, 1, 0, 1);
  }

  @Test
  public void testInvalidDVIsNotCached() throws IOException {
    DeleteFile dv = writeDVs(ImmutableMap.of(DATA_FILE_A, new long[] { 4 })).get(0);
    DeleteFile wrongCount = FileMetadata.deleteFileBuilder(table.spec()).copy(dv).withRecordCount(2).build();

    Assert.assertThrows(IllegalArgumentException.class,
        () -> loader(cache).loadPositionDeletes(ImmutableList.of(wrongCount), DATA_FILE_A));
    Mockito.verify(cache, Mockito.never()).putFileMetadata(any(), any(ByteBuffer.class), any(), any());
  }

  @Test
  public void testDVOfAnotherDataFileIsRejected() throws IOException {
    DeleteFile dv = writeDVs(ImmutableMap.of(DATA_FILE_A, new long[] { 4 })).get(0);

    IllegalArgumentException thrown = Assert.assertThrows(IllegalArgumentException.class,
        () -> loader(cache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_B));
    Assert.assertTrue(thrown.getMessage(),
        thrown.getMessage().startsWith("DV is expected to reference " + DATA_FILE_B));
    Mockito.verifyNoInteractions(cache);
  }

  /**
   * Concurrent tasks of concurrent queries on one daemon read deletes through the shared metadata cache: referenced and
   * unreferenced data files of two partition-scoped position delete files, merged, and a DV. Queries end meanwhile,
   * dropping their query caches. Every result is exact and every buffer is released.
   */
  @Test
  public void testConcurrentReadsAreExact() throws Exception {
    runConcurrentReads(cache);
    assertBuffersReleased();
  }

  /**
   * As above, with a metadata cache too small for the entries, so that they are evicted while other tasks read and put
   * them, and with every 7th put throwing and every 4th directory lookup finding nothing.
   */
  @Test
  public void testConcurrentReadsAreExactUnderEvictionAndFailedPuts() throws Exception {
    AtomicInteger evictions = new AtomicInteger();
    MetadataCache tinyCache = trackingMetadataCache(conf, handedOut, 512, evictions);
    AtomicInteger puts = new AtomicInteger();
    AtomicInteger directoryLookups = new AtomicInteger();
    Mockito.doAnswer(invocation -> invocation.getArgument(0) instanceof CachingDeleteLoader.DirectoryKey &&
            directoryLookups.incrementAndGet() % 4 == 0 ? null : track(handedOut, invocation.callRealMethod()))
        .when(tinyCache).getFileMetadata(any());
    Mockito.doAnswer(invocation -> {
      if (puts.incrementAndGet() % 7 == 0) {
        throw new RuntimeException("Injected put failure");
      }
      return track(handedOut, invocation.callRealMethod());
    }).when(tinyCache).putFileMetadata(any(), any(ByteBuffer.class), any(), any());

    TezCounters total = runConcurrentReads(tinyCache);

    Assert.assertTrue("entries must have been evicted", evictions.get() > 0);
    Assert.assertTrue("puts must have failed",
        total.findCounter(DeleteCacheCounters.DELETE_CACHE_PUT_FAILED).getValue() > 0);
    assertBuffersReleased();
  }

  private TezCounters runConcurrentReads(MetadataCache metadataCache) throws Exception {
    ConcurrentReads reads = new ConcurrentReads(metadataCache);
    List<TezCounters> taskCounters = Lists.newArrayList();
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(16);
    LlapProxy.setDaemon(true);
    try {
      List<Future<?>> tasks = Lists.newArrayList();
      for (int t = 0; t < 16; t++) {
        TezCounters taskCounter = new TezCounters();
        taskCounters.add(taskCounter);
        int seed = t;
        tasks.add(pool.submit(() -> {
          start.await();
          reads.run(new Random(seed), seed == 0, taskCounter);
          return null;
        }));
      }
      start.countDown();
      for (Future<?> task : tasks) {
        task.get(2, TimeUnit.MINUTES);
      }
    } finally {
      pool.shutdownNow();
      LlapProxy.setDaemon(false);
      reads.endQueries();
    }
    Assert.assertTrue("Wrong deletes: " + reads.wrong, reads.wrong.isEmpty());
    TezCounters total = new TezCounters();
    taskCounters.forEach(total::incrAllCounters);
    return total;
  }

  /**
   * Two partition-scoped position delete files, the first referencing d0..d11 and the second d6..d17, with u0..u7 in
   * neither, a DV of DATA_FILE_A, and eight queries whose tasks read them.
   */
  private final class ConcurrentReads {
    private final Map<String, long[]> first = Maps.newTreeMap();
    private final Map<String, long[]> second = Maps.newTreeMap();
    private final List<String> dataFiles = Lists.newArrayList();
    private final List<JobConf> queries = Lists.newArrayList();
    private final Queue<String> wrong = new ConcurrentLinkedQueue<>();
    private final long[] dvPositions = LongStream.range(0, 50).map(i -> i * 3).toArray();
    private final MetadataCache metadataCache;
    private final List<DeleteFile> posDeletes;
    private final DeleteFile dv;

    private ConcurrentReads(MetadataCache metadataCache) throws IOException {
      this.metadataCache = metadataCache;
      for (int i = 0; i < 18; i++) {
        String dataFile = "file:/tmp/data/d" + i + ".parquet";
        dataFiles.add(dataFile);
        if (i < 12) {
          first.put(dataFile, new long[] { i, 100 + i, 1000 + 7 * i });
        }
        if (i >= 6) {
          second.put(dataFile, new long[] { 5000 + i, 6000 + 3 * i });
        }
      }
      for (int i = 0; i < 8; i++) {
        dataFiles.add("file:/tmp/data/u" + i + ".parquet");
        JobConf query = new JobConf(conf);
        HiveConf.setVar(query, HiveConf.ConfVars.HIVE_QUERY_ID, "concurrent" + i);
        queries.add(query);
      }
      this.posDeletes = ImmutableList.of(writePosDeletes(first), writePosDeletes(second));
      posDeletes.forEach(deleteFile -> Assert.assertNull(ContentFileUtil.referencedDataFile(deleteFile)));
      this.dv = writeDVs(ImmutableMap.of(DATA_FILE_A, dvPositions)).get(0);
    }

    // 300 reads of one task, each by a loader of its own; the task that ends queries drops a query cache every 50
    private void run(Random random, boolean endsQueries, TezCounters taskCounter) {
      for (int i = 0; i < 300; i++) {
        if (endsQueries && i % 50 == 49) {
          ObjectCacheFactory.removeLlapQueryCache(queryId(queries.get(random.nextInt(queries.size()))));
        }
        JobConf query = queries.get(random.nextInt(queries.size()));
        CachingDeleteLoader loader = new CachingDeleteLoader(TestCachingDeleteLoader.this::countingInputFile,
            MapredIcebergInputFormat.newTaskAttemptContext(query, new MRReporter(taskCounter)), metadataCache, query);
        if (random.nextInt(10) == 0) {
          check(DATA_FILE_A, loader.loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A), dvPositions);
        } else {
          String dataFile = dataFiles.get(random.nextInt(dataFiles.size()));
          check(dataFile, loader.loadPositionDeletes(posDeletes, dataFile), LongStream.concat(
              LongStream.of(first.getOrDefault(dataFile, new long[0])),
              LongStream.of(second.getOrDefault(dataFile, new long[0]))).toArray());
        }
      }
    }

    private void check(String dataFile, PositionDeleteIndex index, long[] expected) {
      if (index.cardinality() != expected.length || !LongStream.of(expected).allMatch(index::isDeleted)) {
        wrong.add(dataFile + ": " + index + " instead of " + Arrays.toString(expected));
      }
    }

    private void endQueries() {
      queries.forEach(query -> ObjectCacheFactory.removeLlapQueryCache(queryId(query)));
    }
  }

  private static String queryId(JobConf query) {
    return HiveConf.getVar(query, HiveConf.ConfVars.HIVE_QUERY_ID);
  }

  private static Object directoryKey() {
    return argThat(key -> key instanceof CachingDeleteLoader.DirectoryKey);
  }

  private static Object pairKey() {
    return argThat(key -> !(key instanceof CachingDeleteLoader.DirectoryKey));
  }

  private CachingDeleteLoader loader(MetadataCache metadataCache) {
    return new CachingDeleteLoader(this::countingInputFile,
        MapredIcebergInputFormat.newTaskAttemptContext(conf, new MRReporter(counters)), metadataCache, conf);
  }

  public static void assertCounters(TezCounters counters, long hits, long misses, long puts, long failedPuts) {
    Assert.assertEquals("hits", hits, counters.findCounter(DeleteCacheCounters.DELETE_CACHE_HIT).getValue());
    Assert.assertEquals("misses", misses, counters.findCounter(DeleteCacheCounters.DELETE_CACHE_MISS).getValue());
    Assert.assertEquals("puts", puts, counters.findCounter(DeleteCacheCounters.DELETE_CACHE_PUT).getValue());
    Assert.assertEquals("failed puts", failedPuts,
        counters.findCounter(DeleteCacheCounters.DELETE_CACHE_PUT_FAILED).getValue());
  }

  // Counts opens where the loader asks for the file. A wrapped stream breaks Parquet, which unwraps it and leaves the
  // wrapper's finalizer to close the shared Hadoop stream.
  private InputFile countingInputFile(DeleteFile deleteFile) {
    opens.incrementAndGet();
    return table.io().newInputFile(deleteFile.location());
  }

  private List<DeleteFile> writeDVs(Map<String, long[]> positionsByDataFile) throws IOException {
    OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null);
    try (DVFileWriter closeableWriter = writer) {
      positionsByDataFile.forEach((dataFile, positions) -> {
        for (long position : positions) {
          closeableWriter.delete(dataFile, position, table.spec(), null);
        }
      });
    }
    return writer.result().deleteFiles();
  }

  private DeleteFile writePosDeletes(Map<String, long[]> positionsByDataFile) throws IOException {
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    positionsByDataFile.forEach((dataFile, positions) -> {
      for (long position : positions) {
        deletes.add(Pair.of(dataFile, position));
      }
    });
    OutputFile out = Files.localOutput(new File(temp.newFolder(), "deletes.parquet"));
    return FileHelpers.writeDeleteFile(v2Table, out, null, deletes, 2).first();
  }

  private void assertBuffersReleased() {
    assertReleased(handedOut);
  }

  public static void assertReleased(List<LlapBufferOrBuffers> handedOut) {
    Assert.assertFalse(handedOut.isEmpty());
    for (LlapBufferOrBuffers buffers : handedOut) {
      LlapAllocatorBuffer single = buffers.getSingleLlapBuffer();
      LlapAllocatorBuffer[] parts = single != null ? new LlapAllocatorBuffer[] { single } :
          buffers.getMultipleLlapBuffers();
      for (LlapAllocatorBuffer part : parts) {
        Assert.assertFalse("Buffer still locked: " + part, part.isLocked());
      }
    }
  }

  private static void assertDeleted(PositionDeleteIndex index, long... positions) {
    Assert.assertEquals(positions.length, index.cardinality());
    for (long position : positions) {
      Assert.assertTrue("Position " + position + " should be deleted", index.isDeleted(position));
    }
  }

  /**
   * A real LLAP metadata cache that records every buffer it hands out, so tests can check they got released.
   */
  public static MetadataCache trackingMetadataCache(Configuration conf, List<LlapBufferOrBuffers> handedOut) {
    return trackingMetadataCache(conf, handedOut, 1 << 20, new AtomicInteger());
  }

  private static MetadataCache trackingMetadataCache(Configuration conf, List<LlapBufferOrBuffers> handedOut,
      long maxSize) {
    return trackingMetadataCache(conf, handedOut, maxSize, new AtomicInteger());
  }

  /**
   * Evicted buffers are counted and freed, and their entries left to the cache: a lookup or put that cannot lock an
   * entry drops it. A daemon's EvictionDispatcher also calls MetadataCache.notifyEvicted, which removes the entry by
   * key alone and asserts that it held the evicted buffer; a key put again in between, after a lookup dropped its
   * evicted entry, fails that assertion. That costs a miss, not a wrong read, and is not what these tests cover.
   */
  private static MetadataCache trackingMetadataCache(Configuration conf, List<LlapBufferOrBuffers> handedOut,
      long maxSize, AtomicInteger evictions) {
    LlapDaemonCacheMetrics metrics = LlapDaemonCacheMetrics.create("", "");
    LowLevelLrfuCachePolicy policy = new LowLevelLrfuCachePolicy(8, maxSize, conf);
    LowLevelCacheMemoryManager memoryManager = new LowLevelCacheMemoryManager(maxSize, policy, metrics);
    BuddyAllocator allocator = new BuddyAllocator(false, false, 8, MAX_ALLOC, 1, maxSize, 0, null, memoryManager,
        metrics, null, true);
    MetadataCache cache = Mockito.spy(new MetadataCache(allocator, memoryManager, policy, false, metrics));
    policy.setEvictionListener(new EvictionListener() {
      @Override
      public void notifyEvicted(LlapCacheableBuffer buffer) {
        evictions.incrementAndGet();
        allocator.deallocateEvicted((MemoryBuffer) buffer);
      }

      @Override
      public void notifyProactivelyEvicted(LlapCacheableBuffer buffer) {
        evictions.incrementAndGet();
        allocator.deallocateProactivelyEvicted((MemoryBuffer) buffer);
      }
    });
    Mockito.doAnswer(invocation -> track(handedOut, invocation.callRealMethod()))
        .when(cache).getFileMetadata(any());
    Mockito.doAnswer(invocation -> track(handedOut, invocation.callRealMethod()))
        .when(cache).putFileMetadata(any(), any(ByteBuffer.class), any(), any());
    return cache;
  }

  private static Object track(List<LlapBufferOrBuffers> handedOut, Object buffers) {
    if (buffers != null) {
      handedOut.add((LlapBufferOrBuffers) buffers);
    }
    return buffers;
  }
}
