/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.io.encoded;

import io.jsonwebtoken.lang.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.cache.BuddyAllocator;
import org.apache.hadoop.hive.llap.cache.LlapDataBuffer;
import org.apache.hadoop.hive.llap.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheImpl;
import org.apache.hadoop.hive.llap.cache.LowLevelLrfuCachePolicy;
import org.apache.hadoop.hive.llap.cache.TestBuddyAllocatorForceEvict;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.ql.io.SyntheticFileId;
import org.apache.hadoop.hive.ql.io.orc.encoded.CacheChunk;
import org.apache.hadoop.hive.ql.io.orc.encoded.IoTrace;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestLowLevelDataReader {

  private static final int ORC_PADDING = 3;
  private static final String TEST_PATH = "../data/files/orc_compressed";
  private static final String TEST_PATH_UNCOMPRESSED = "../data/files/orc_uncompressed";

  private Configuration conf;
  private BuddyAllocator buddyAllocator;
  private LlapDaemonCacheMetrics metrics;
  private MetadataCache metaCache;
  private FixedSizedObjectPool<IoTrace> tracePool;
  private LowLevelCacheImpl cache;
  private DataCache mockDataCache;
  private MockDiskRangeListFactory mockDiskRangeListFactory;

  @Before
  public void setUp() {
    conf = new Configuration();
    HiveConf.setIntVar(conf, HiveConf.ConfVars.LLAP_LRFU_BP_WRAPPER_SIZE, 1);
    LowLevelLrfuCachePolicy lrfu = new LowLevelLrfuCachePolicy(1, 39, conf);
    buddyAllocator =
        TestBuddyAllocatorForceEvict.create(2048, 2, 4096, false, true);
    metrics = LlapDaemonCacheMetrics.create("", "");
    metaCache = new MetadataCache(buddyAllocator, null, lrfu,
        false, metrics);
    tracePool = IoTrace.createTracePool(conf);
    cache = new LowLevelCacheImpl(metrics, lrfu, buddyAllocator, true);
    mockDataCache = new MockDataCache();
    mockDiskRangeListFactory = new MockDiskRangeListFactory();
  }

  @Test(expected = IOException.class)
  public void testWrongFileKey() throws IOException {
    LowLevelDataReader reader = new LowLevelDataReader(new Path(TEST_PATH),
        null, conf, null, null, null, null);
    reader.init();
  }

  @Test
  public void testReadFooter() throws IOException {
    Path path = new Path(TEST_PATH);
    Object key = fileId(path);
    LowLevelDataReader reader = new LowLevelDataReader(path, key, conf, null, metaCache,
        null, null);
    reader.init();

    reader.readFooter();

    MetadataCache.LlapBufferOrBuffers metadata = metaCache.getFileMetadata(key);
    Assert.notNull(metadata);
  }


  @Test
  public void testUncompressedReadRanges() throws IOException {
    Path path = new Path(TEST_PATH_UNCOMPRESSED);
    Object key = fileId(path);
    LowLevelDataReader reader = new LowLevelDataReader(path, key, conf, mockDataCache, metaCache,
        null, tracePool);
    reader.init();

    DiskRangeList range = new DiskRangeList(ORC_PADDING, 296);
    reader.read(range);

    DataCache.BooleanRef gotAllData = new DataCache.BooleanRef();
    DiskRangeList fileData = cache.getFileData(key, range, 0,
        mockDiskRangeListFactory, null, gotAllData);

    Assert.isTrue(gotAllData.value);
  }

  @Test
  public void testReadValidRanges() throws IOException {
    Path path = new Path(TEST_PATH);
    Object key = fileId(path);
    LowLevelDataReader reader = new LowLevelDataReader(path, key, conf, mockDataCache, metaCache,
        null, tracePool);
    reader.init();
    DiskRangeList range = new DiskRangeList(ORC_PADDING,38);
    reader.read(range);

    DataCache.BooleanRef gotAllData = new DataCache.BooleanRef();
    DiskRangeList fileData = cache.getFileData(key, range, 0,
        mockDiskRangeListFactory, null, gotAllData);

    Assert.isTrue(gotAllData.value);

  }

  @Test
  public void testReadBadlyEstimatedRanges() throws IOException {
    Path path = new Path(TEST_PATH);
    Object key = fileId(path);
    LowLevelDataReader reader = new LowLevelDataReader(path, key, conf, mockDataCache, metaCache,
        null, tracePool);
    reader.init();
    DiskRangeList range = new DiskRangeList(ORC_PADDING,40);
    reader.read(range);

    DataCache.BooleanRef gotAllData = new DataCache.BooleanRef();
    DiskRangeList fileData = cache.getFileData(key, range, 0,
        mockDiskRangeListFactory, null, gotAllData);

    Assert.isTrue(!gotAllData.value);

  }

  private SyntheticFileId fileId(Path path) throws IOException {
    FileStatus fs = path.getFileSystem(conf).getFileStatus(path);
    return new SyntheticFileId(path, fs.getLen(), fs.getModificationTime());
  }

  private class MockDataCache implements DataCache, Allocator.BufferObjectFactory {

    @Override
    public MemoryBuffer create() {
      return new LlapDataBuffer();
    }

    @Override
    public DiskRangeList getFileData(Object fileKey, DiskRangeList range, long baseOffset,
        DiskRangeListFactory factory, BooleanRef gotAllData) {
      return null;
    }

    @Override
    public long[] putFileData(Object fileKey, DiskRange[] ranges, MemoryBuffer[] data, long baseOffset) {
      return data != null ?
          cache.putFileData(fileKey,ranges, data, baseOffset, LowLevelCache.Priority.NORMAL, null, null) :
          null;
    }

    @Override
    public void releaseBuffer(MemoryBuffer buffer) {

    }

    @Override
    public void reuseBuffer(MemoryBuffer buffer) {

    }

    @Override
    public Allocator getAllocator() {
      return buddyAllocator;
    }

    @Override
    public Allocator.BufferObjectFactory getDataBufferFactory() {
      return this;
    }

    @Override
    public long[] putFileData(Object fileKey, DiskRange[] ranges, MemoryBuffer[] data, long baseOffset,
        CacheTag tag) {
      return data != null ?
          cache.putFileData(fileKey,ranges, data, baseOffset, LowLevelCache.Priority.NORMAL, null, tag) :
          null;
    }
  }

  private class MockDiskRangeListFactory implements DataCache.DiskRangeListFactory {

    @Override
    public DiskRangeList createCacheChunk(MemoryBuffer buffer, long startOffset, long endOffset) {
      return new CacheChunk(buffer, startOffset, endOffset);
    }
  }
}
