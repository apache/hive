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
package org.apache.hadoop.hive.llap.cache;

import com.google.common.io.Files;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestBasicLlapCacheHydration {

  private LlapIo mockIo;
  private BasicLlapCacheHydration hydr;

  @Before
  public void setUp() throws IOException {
    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    Configuration conf = new Configuration();
    HiveConf.setVar(conf, ConfVars.LLAP_CACHE_HYDRATION_SAVE_DIR, tempDir.getAbsolutePath());
    hydr = new BasicLlapCacheHydration();
    hydr.setConf(conf);
    mockIo = mock(LlapIo.class);
    hydr.llapIo = mockIo;
    hydr.initSavePath();
  }

  @Test
  public void testSaveAndLoad() throws IOException {
    LlapDaemonProtocolProtos.CacheEntryList entryList = createTestData();
    when(mockIo.fetchCachedContentInfo()).thenReturn(entryList);
    ArgumentCaptor<LlapDaemonProtocolProtos.CacheEntryList> captor =
        forClass(LlapDaemonProtocolProtos.CacheEntryList.class);

    hydr.save();
    hydr.load();

    verify(mockIo).loadDataIntoCache(captor.capture());
    LlapDaemonProtocolProtos.CacheEntryList res = captor.getValue();
    assertEquals(entryList, res);
  }

  private LlapDaemonProtocolProtos.CacheEntryList createTestData() throws IOException {
    LlapDaemonProtocolProtos.CacheEntryRange re1 =
        LlapDaemonProtocolProtos.CacheEntryRange.newBuilder().setStart(1L).setEnd(10L).build();
    LlapDaemonProtocolProtos.CacheEntryRange re2 =
        LlapDaemonProtocolProtos.CacheEntryRange.newBuilder().setStart(11L).setEnd(20L).build();
    LlapDaemonProtocolProtos.CacheTag ct =
        LlapDaemonProtocolProtos.CacheTag.newBuilder().setTableName("dummyTable").build();

    LlapDaemonProtocolProtos.CacheEntry ce =
        LlapDaemonProtocolProtos.CacheEntry.newBuilder().setCacheTag(ct).setFilePath("dummyPath")
            .setFileKey(ByteString.copyFromUtf8("dummyKey")).addRanges(re2).addRanges(re1).build();
    LlapDaemonProtocolProtos.CacheEntryList cel =
        LlapDaemonProtocolProtos.CacheEntryList.newBuilder().addEntries(ce).build();
    return cel;
  }
}
