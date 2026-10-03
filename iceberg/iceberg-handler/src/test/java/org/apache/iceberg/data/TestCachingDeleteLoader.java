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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.cache.BuddyAllocator;
import org.apache.hadoop.hive.llap.cache.LlapAllocatorBuffer;
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
import org.apache.iceberg.types.Types;
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
    HiveConf.setVar(conf, HiveConf.ConfVars.LLAP_IO_CACHE_DELETES, "none");

    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A), 4);
    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A), 4);

    Assert.assertEquals(2, opens.get());
    Mockito.verifyNoInteractions(cache);
  }

  @Test
  public void testCacheLevelMetadataCachesDVs() throws IOException {
    DeleteFile dv = writeDVs(ImmutableMap.of(DATA_FILE_A, new long[] { 4 })).get(0);
    HiveConf.setVar(conf, HiveConf.ConfVars.LLAP_IO_CACHE_DELETES, "metadata");

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

    Mockito.verify(cache, Mockito.times(1)).putFileMetadata(any(), any(ByteBuffer.class), any(), any());
    assertCounters(counters, 0, 1, 1, 0);
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

  @Test
  public void testDataFileWithoutDeletesInPositionDeleteFileIsCached() throws IOException {
    DeleteFile deletes = writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }));

    for (int i = 0; i < 2; i++) {
      assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_B));
    }
    Assert.assertEquals(1, opens.get());
    assertBuffersReleased();
  }

  @Test
  public void testCacheLevelMetadataSkipsPositionDeleteFiles() throws IOException {
    DeleteFile deletes = writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }));
    HiveConf.setVar(conf, HiveConf.ConfVars.LLAP_IO_CACHE_DELETES, "metadata");

    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A), 1);
    assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A), 1);

    Assert.assertEquals(2, opens.get());
    Mockito.verifyNoInteractions(cache);
  }

  @Test
  public void testPositionDeleteFileIsDecodedOncePerQuery() throws IOException {
    DeleteFile deletes = writePosDeletes(ImmutableMap.of(DATA_FILE_A, new long[] { 1 }));
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID, "query1");
    LlapProxy.setDaemon(true);
    try {
      assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_B));
      assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_C));
      assertDeleted(loader(cache).loadPositionDeletes(ImmutableList.of(deletes), DATA_FILE_A), 1);
    } finally {
      LlapProxy.setDaemon(false);
      ObjectCacheFactory.removeLlapQueryCache("query1");
    }
    Assert.assertEquals("Data files the delete file does not reference must not decode it again", 1, opens.get());
    Mockito.verify(cache, Mockito.times(3)).putFileMetadata(any(), any(ByteBuffer.class), any(), any());
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
  public void testCountersOfDVs() throws IOException {
    DeleteFile dv = writeDVs(ImmutableMap.of(DATA_FILE_A, new long[] { 0, 5, 7 })).get(0);

    for (int i = 0; i < 3; i++) {
      loader(cache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A);
    }

    assertCounters(counters, 2, 1, 1, 0);
    Assert.assertEquals(dv.contentSizeInBytes().longValue(),
        counters.findCounter(DeleteCacheCounters.DELETE_CACHE_PUT_BYTES).getValue());
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

  @Test
  public void testPutThatCannotReserveMemoryEndsOnInterrupt() throws Exception {
    long[] positions = LongStream.range(0, 400).map(i -> i * 3).toArray();
    DeleteFile dv = writeDVs(ImmutableMap.of(DATA_FILE_A, positions)).get(0);
    MetadataCache tinyCache = trackingMetadataCache(conf, handedOut, 512);
    Assert.assertTrue(dv.contentSizeInBytes() > 512);

    AtomicReference<PositionDeleteIndex> result = new AtomicReference<>();
    Thread reader = new Thread(() ->
        result.set(loader(tinyCache).loadPositionDeletes(ImmutableList.of(dv), DATA_FILE_A)));
    reader.start();
    reader.join(1000);
    Assert.assertTrue("The put waits for memory to be evicted", reader.isAlive());
    reader.interrupt();
    reader.join(10_000);

    Assert.assertFalse(reader.isAlive());
    assertDeleted(result.get(), positions);
  }

  @Test
  public void testInvalidDVIsNotCached() throws IOException {
    DeleteFile dv = writeDVs(ImmutableMap.of(DATA_FILE_A, new long[] { 4 })).get(0);
    DeleteFile wrongCount = FileMetadata.deleteFileBuilder(table.spec()).copy(dv).withRecordCount(2).build();

    Assert.assertThrows(IllegalArgumentException.class,
        () -> loader(cache).loadPositionDeletes(ImmutableList.of(wrongCount), DATA_FILE_A));
    Mockito.verify(cache, Mockito.never()).putFileMetadata(any(), any(ByteBuffer.class), any(), any());
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
    return trackingMetadataCache(conf, handedOut, 1 << 20);
  }

  private static MetadataCache trackingMetadataCache(Configuration conf, List<LlapBufferOrBuffers> handedOut,
      long maxSize) {
    LlapDaemonCacheMetrics metrics = LlapDaemonCacheMetrics.create("", "");
    LowLevelLrfuCachePolicy policy = new LowLevelLrfuCachePolicy(8, maxSize, conf);
    LowLevelCacheMemoryManager memoryManager = new LowLevelCacheMemoryManager(maxSize, policy, metrics);
    BuddyAllocator allocator = new BuddyAllocator(false, false, 8, MAX_ALLOC, 1, maxSize, 0, null, memoryManager,
        metrics, null, true);
    MetadataCache cache = Mockito.spy(new MetadataCache(allocator, memoryManager, policy, false, metrics));
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
