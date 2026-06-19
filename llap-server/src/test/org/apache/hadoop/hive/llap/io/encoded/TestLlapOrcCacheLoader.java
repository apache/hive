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
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.cache.BuddyAllocator;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheImpl;
import org.apache.hadoop.hive.llap.cache.LowLevelLrfuCachePolicy;
import org.apache.hadoop.hive.llap.cache.TestBuddyAllocatorForceEvict;
import org.apache.hadoop.hive.llap.io.LlapIoMocks;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.ql.io.SyntheticFileId;
import org.apache.hadoop.hive.ql.io.orc.encoded.IoTrace;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestLlapOrcCacheLoader {

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
  private LlapIoMocks.MockDiskRangeListFactory mockDiskRangeListFactory;

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
    mockDataCache = new LlapIoMocks.MockDataCache(cache, buddyAllocator, lrfu);
    mockDiskRangeListFactory = new LlapIoMocks.MockDiskRangeListFactory();
  }

  @Test(expected = IOException.class)
  public void testWrongFileKey() throws IOException {
    try(LlapOrcCacheLoader loader = new LlapOrcCacheLoader(new Path(TEST_PATH),
        null, conf, null, null, null, null)) {
      loader.init();
    }
  }

  @Test
  public void testLoadFooter() throws IOException {
    Path path = new Path(TEST_PATH);
    Object key = fileId(path);
    try(LlapOrcCacheLoader loader = new LlapOrcCacheLoader(path, key, conf, null, metaCache,
        null, null)) {
      loader.init();
      loader.loadFileFooter();
    }
    MetadataCache.LlapBufferOrBuffers metadata = metaCache.getFileMetadata(key);
    Assert.notNull(metadata);
  }


  @Test
  public void testLoadUncompressedRanges() throws IOException {
    Path path = new Path(TEST_PATH_UNCOMPRESSED);
    Object key = fileId(path);
    try(LlapOrcCacheLoader loader = new LlapOrcCacheLoader(path, key, conf, mockDataCache, metaCache,
        null, tracePool)) {
      loader.init();

      DiskRangeList range = new DiskRangeList(ORC_PADDING, 296);
      loader.loadRanges(range);

      DataCache.BooleanRef gotAllData = new DataCache.BooleanRef();
      cache.getFileData(key, range, 0,
          mockDiskRangeListFactory, null, gotAllData);

      Assert.isTrue(gotAllData.value);
    }
  }

  @Test
  public void testLoadValidRanges() throws IOException {
    Path path = new Path(TEST_PATH);
    Object key = fileId(path);
    DiskRangeList range = new DiskRangeList(ORC_PADDING,38);
    try(LlapOrcCacheLoader loader = new LlapOrcCacheLoader(path, key, conf, mockDataCache, metaCache,
        null, tracePool)) {
      loader.init();
      loader.loadRanges(range);
    }
    DataCache.BooleanRef gotAllData = new DataCache.BooleanRef();
    cache.getFileData(key, range, 0,
        mockDiskRangeListFactory, null, gotAllData);

    Assert.isTrue(gotAllData.value);
  }

  @Test
  public void testLoadAlreadyLoadedRange() throws IOException {
    Path path = new Path(TEST_PATH);
    Object key = fileId(path);
    DiskRangeList range = new DiskRangeList(ORC_PADDING,38);
    try(LlapOrcCacheLoader loader = new LlapOrcCacheLoader(path, key, conf, mockDataCache, metaCache,
        null, tracePool)) {
      loader.init();
      loader.loadRanges(range);
    }

    DataCache.BooleanRef gotAllData = new DataCache.BooleanRef();
    cache.getFileData(key, range, 0,
        mockDiskRangeListFactory, null, gotAllData);
    Assert.isTrue(gotAllData.value);

    DiskRangeList range2 = new DiskRangeList(ORC_PADDING,14);
    try(LlapOrcCacheLoader loader = new LlapOrcCacheLoader(path, key, conf, mockDataCache, metaCache,
        null, tracePool)) {
      loader.init();
      loader.loadRanges(range2);
    }
    gotAllData.value = false;
    cache.getFileData(key, range, 0,
        mockDiskRangeListFactory, null, gotAllData);
    Assert.isTrue(gotAllData.value);
  }

  @Test
  public void testLoadBadlyEstimatedRanges() throws IOException {
    Path path = new Path(TEST_PATH);
    Object key = fileId(path);
    DiskRangeList range = new DiskRangeList(ORC_PADDING,40);
    try(LlapOrcCacheLoader loader = new LlapOrcCacheLoader(path, key, conf, mockDataCache, metaCache,
        null, tracePool)) {
      loader.init();
      loader.loadRanges(range);
    }
    DataCache.BooleanRef gotAllData = new DataCache.BooleanRef();
    cache.getFileData(key, range, 0,
        mockDiskRangeListFactory, null, gotAllData);

    Assert.isTrue(!gotAllData.value);

  }

  private SyntheticFileId fileId(Path path) throws IOException {
    FileStatus fs = path.getFileSystem(conf).getFileStatus(path);
    return new SyntheticFileId(path, fs.getLen(), fs.getModificationTime());
  }
}
