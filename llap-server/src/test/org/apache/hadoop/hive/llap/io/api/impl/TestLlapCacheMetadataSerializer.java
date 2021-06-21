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
package org.apache.hadoop.hive.llap.io.api.impl;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.FileMetadataCache;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.cache.BuddyAllocator;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheImpl;
import org.apache.hadoop.hive.llap.cache.LowLevelCachePolicy;
import org.apache.hadoop.hive.llap.cache.LowLevelLrfuCachePolicy;
import org.apache.hadoop.hive.llap.cache.MemoryLimitedPathCache;
import org.apache.hadoop.hive.llap.cache.PathCache;
import org.apache.hadoop.hive.llap.cache.TestBuddyAllocatorForceEvict;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.io.LlapIoMocks;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.ql.io.SyntheticFileId;
import org.apache.hadoop.hive.ql.io.orc.encoded.IoTrace;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestLlapCacheMetadataSerializer {

  private static final String TEST_PATH = "../data/files/orc_compressed";

  private FileMetadataCache fileMetadataCache;
  private DataCache mockDataCache;
  private Configuration conf;
  private PathCache pathCache;
  private FixedSizedObjectPool<IoTrace> tracePool;
  private LowLevelCachePolicy cachePolicy;
  private LlapCacheMetadataSerializer serializer;

  @Before
  public void setUp() {
    conf = new Configuration();
    HiveConf.setIntVar(conf, HiveConf.ConfVars.LLAP_LRFU_BP_WRAPPER_SIZE, 1);
    HiveConf.setFloatVar(conf, HiveConf.ConfVars.LLAP_LRFU_HOTBUFFERS_PERCENTAGE, 1.0f);
    BuddyAllocator buddyAllocator = TestBuddyAllocatorForceEvict.create(16384, 2, 32768, false, true);
    LlapDaemonCacheMetrics metrics = LlapDaemonCacheMetrics.create("", "");
    cachePolicy = new LowLevelLrfuCachePolicy(1, 5, conf);
    LowLevelCacheImpl cache = new LowLevelCacheImpl(metrics, cachePolicy, buddyAllocator, true);
    fileMetadataCache = new MetadataCache(buddyAllocator, null, cachePolicy, false, metrics);
    tracePool = IoTrace.createTracePool(conf);
    mockDataCache = new LlapIoMocks.MockDataCache(cache, buddyAllocator, cachePolicy);
    pathCache = new MemoryLimitedPathCache(conf);
    tracePool = IoTrace.createTracePool(conf);
    serializer =
        new LlapCacheMetadataSerializer(fileMetadataCache, mockDataCache, conf, pathCache, tracePool, cachePolicy);
  }

  @Test
  public void testLoadData() throws IOException {
    LlapDaemonProtocolProtos.CacheEntryList dummyMetadata = createDummyMetadata();
    serializer.loadData(dummyMetadata);

    LlapDaemonProtocolProtos.CacheEntryList cachedMetadata = serializer.fetchCachedContentInfo();

    assertEquals(dummyMetadata, cachedMetadata);
  }

  @Test
  public void testEncodeDecodeLongFileKey() throws IOException {
    Long originalKey = new Long(12345678L);
    ByteString encodedKey = serializer.encodeFileKey(originalKey);
    Object decodedKey = serializer.decodeFileKey(encodedKey);
    assertEquals(originalKey, decodedKey);
  }

  @Test
  public void testEncodeDecodeSyntheticFileKey() throws IOException {
    SyntheticFileId originalKey = new SyntheticFileId(new Path("dummy"), 123L, 99999999L);
    ByteString encodedKey = serializer.encodeFileKey(originalKey);
    Object decodedKey = serializer.decodeFileKey(encodedKey);
    assertEquals(originalKey, decodedKey);
  }

  private LlapDaemonProtocolProtos.CacheEntryList createDummyMetadata() throws IOException {
    LlapDaemonProtocolProtos.CacheEntryRange re1 =
        LlapDaemonProtocolProtos.CacheEntryRange.newBuilder().setStart(3L).setEnd(14L).build();
    LlapDaemonProtocolProtos.CacheEntryRange re2 =
        LlapDaemonProtocolProtos.CacheEntryRange.newBuilder().setStart(14L).setEnd(38L).build();
    LlapDaemonProtocolProtos.CacheTag ct =
        LlapDaemonProtocolProtos.CacheTag.newBuilder().setTableName("dummyTable").build();

    Path path = new Path(TEST_PATH);
    SyntheticFileId syntheticFileId = fileId(path);
    pathCache.touch(syntheticFileId, path.toUri().toString());
    ByteString fileKey = serializer.encodeFileKey(syntheticFileId);

    LlapDaemonProtocolProtos.CacheEntry ce =
        LlapDaemonProtocolProtos.CacheEntry.newBuilder().setCacheTag(ct).setFilePath(TEST_PATH).setFileKey(fileKey)
            .addRanges(re2).addRanges(re1).build();
    LlapDaemonProtocolProtos.CacheEntryList cel =
        LlapDaemonProtocolProtos.CacheEntryList.newBuilder().addEntries(ce).build();
    return cel;
  }

  private SyntheticFileId fileId(Path path) throws IOException {
    FileStatus fs = path.getFileSystem(conf).getFileStatus(path);
    return new SyntheticFileId(path, fs.getLen(), fs.getModificationTime());
  }
}
