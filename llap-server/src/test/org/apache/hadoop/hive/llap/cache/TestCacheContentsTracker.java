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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.common.io.CacheTag;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for TestCacheContentsTracker functions.
 */
public class TestCacheContentsTracker {

  private static CacheContentsTracker tracker;

  @BeforeClass
  public static void setup() {
    LowLevelCachePolicy lowLevelCachePolicyMock = mock(LowLevelCachePolicy.class);
    EvictionListener evictionListenerMock = mock(EvictionListener.class);
    tracker = new CacheContentsTracker(lowLevelCachePolicyMock);
    tracker.setEvictionListener(evictionListenerMock);
  }

  /**
   * Tests parent CacheTag generation by checking each step when traversing from 3rd level
   * partition to DB level.
   */
  @Test
  public void testParentCacheTagGeneration() {
    CacheTag db = cacheTagBuilder("dbname");
    CacheTag table = cacheTagBuilder("dbname.tablename");
    CacheTag p = cacheTagBuilder("dbname.tablename", "p=v1");
    CacheTag pp = cacheTagBuilder("dbname.tablename", "p=v1", "pp=vv1");
    CacheTag ppp = cacheTagBuilder("dbname.tablename", "p=v1", "pp=vv1", "ppp=vvv1");

    assertTrue(pp.compareTo(CacheTag.createParentCacheTag(ppp)) == 0);
    assertTrue(p.compareTo(CacheTag.createParentCacheTag(pp)) == 0);
    assertTrue(table.compareTo(CacheTag.createParentCacheTag(p)) == 0);
    assertTrue(db.compareTo(CacheTag.createParentCacheTag(table)) == 0);
    assertNull(CacheTag.createParentCacheTag(db));
  }

  /**
   * Caches some mock buffers and checks summary produced by CacheContentsTracker. Later this is
   * done again after some mock buffers were evicted.
   */
  @Test
  public void testAggregatedStatsGeneration() {
    cacheTestBuffers();
    StringBuilder sb = new StringBuilder();
    tracker.debugDumpShort(sb);
    assertEquals(EXPECTED_CACHE_STATE_WHEN_FULL, sb.toString());

    evictSomeTestBuffers();
    sb = new StringBuilder();
    tracker.debugDumpShort(sb);
    assertEquals(EXPECTED_CACHE_STATE_AFTER_EVICTION, sb.toString());
  }


  /**
   * Tests CacheTag.compareTo().
   */
  @Test
  public void testCacheTagComparison() {

    // Comparing with null
    compareViceVersa(1, cacheTagBuilder("dbname.tablename"), null);
    compareViceVersa(1, cacheTagBuilder("dbname.tablename", "p1=v1"), null);
    compareViceVersa(1, cacheTagBuilder("dbname.tablename", "p1=v1", "p2=v2"), null);

    // Comparing similar constructs
    compareViceVersa(0, cacheTagBuilder("dbname.tablename"),
        cacheTagBuilder("dbname.tablename"));
    compareViceVersa(0, cacheTagBuilder("dbname.tablename", "p1=v1"),
        cacheTagBuilder("dbname.tablename", "p1=v1"));
    compareViceVersa(0, cacheTagBuilder("dbname.tablename", "p1=v1", "p2=v2"),
        cacheTagBuilder("dbname.tablename", "p1=v1", "p2=v2"));

    // Comparing structs of different lengths
    compareViceVersa(1, cacheTagBuilder("dbname.tablename", "p1=v1", "p2=v2", "p3=v3"),
        cacheTagBuilder("dbname.tablename", "p1=v1", "p2=v2"));
    compareViceVersa(1, cacheTagBuilder("dbname.tablename", "p1=v1", "p2=v2"),
        cacheTagBuilder("dbname.tablename", "p1=v1"));
    compareViceVersa(1, cacheTagBuilder("dbname.tablename", "p1=v1"),
        cacheTagBuilder("dbname.tablename"));

    // Comparing different constructs with same length
    compareViceVersa(-1, cacheTagBuilder("dbname.tablename", "p1=v1"),
        cacheTagBuilder("dbname.tablenamf", "p1=v0"));
    compareViceVersa(-1, cacheTagBuilder("dbname.tablename", "p1=v1", "p2=v3"),
        cacheTagBuilder("dbname.tablenamf", "p1=v1", "p2=v2"));
    compareViceVersa(-25, cacheTagBuilder("dbname.tablename", "p1=v1", "p2=v2a"),
        cacheTagBuilder("dbname.tablename", "p1=v1", "p2=v2z"));
    compareViceVersa(-1, cacheTagBuilder("dbname.tablenameAA", "p1=v1", "p2=v2"),
        cacheTagBuilder("dbname.tablenameBB", "p1=v1", "p2=v2"));

  }

  @Test
  public void testEncodingDecoding() throws Exception {
    LinkedHashMap<String, String> partDescs = new LinkedHashMap<>();
    partDescs.put("pytha=goras", "a2+b2=c2");
    CacheTag tag = CacheTag.build("math.rules", partDescs);
    CacheTag.SinglePartitionCacheTag stag = ((CacheTag.SinglePartitionCacheTag)tag);
    assertEquals("pytha=goras=a2+b2=c2", stag.partitionDescToString());
    assertEquals(1, stag.getPartitionDescMap().size());
    assertEquals("a2+b2=c2", stag.getPartitionDescMap().get("pytha=goras"));

    partDescs.clear();
    partDescs.put("mutli=one", "one=/1");
    partDescs.put("mutli=two/", "two=2");
    tag = CacheTag.build("math.rules", partDescs);
    CacheTag.MultiPartitionCacheTag mtag = ((CacheTag.MultiPartitionCacheTag)tag);
    assertEquals("mutli=one=one=/1/mutli=two/=two=2", mtag.partitionDescToString());
    assertEquals(2, mtag.getPartitionDescMap().size());
    assertEquals("one=/1", mtag.getPartitionDescMap().get("mutli=one"));
    assertEquals("two=2", mtag.getPartitionDescMap().get("mutli=two/"));
  }

  private static void compareViceVersa(int expected, CacheTag a, CacheTag b) {
    if (a != null) {
      assertEquals(expected, a.compareTo(b));
    }
    if (b != null) {
      assertEquals(-1 * expected, b.compareTo(a));
    }
  }

  private static LlapCacheableBuffer createMockBuffer(long size, CacheTag cacheTag) {
    LlapCacheableBuffer llapCacheableBufferMock = mock(LlapCacheableBuffer.class);

    doAnswer(invocationOnMock -> {
      return size;
    }).when(llapCacheableBufferMock).getMemoryUsage();

    doAnswer(invocationOnMock -> {
      return cacheTag;
    }).when(llapCacheableBufferMock).getTag();

    return llapCacheableBufferMock;
  }

  public static CacheTag cacheTagBuilder(String dbAndTable, String... partitions) {
    if (partitions != null && partitions.length > 0) {
      LinkedHashMap<String, String> partDescs = new LinkedHashMap<>();
      for (String partition : partitions) {
        String[] partDesc = partition.split("=");
        partDescs.put(partDesc[0], partDesc[1]);
      }
      return CacheTag.build(dbAndTable, partDescs);
    } else {
      return CacheTag.build(dbAndTable);
    }
  }

  private static void cacheTestBuffers() {
    tracker.cache(createMockBuffer(4 * 1024L,
        cacheTagBuilder("default.testtable")), null);
    tracker.cache(createMockBuffer(2 * 1024L,
        cacheTagBuilder("otherdb.testtable", "p=v1", "pp=vv1")), null);
    tracker.cache(createMockBuffer(32 * 1024L,
        cacheTagBuilder("otherdb.testtable", "p=v1", "pp=vv1")), null);
    tracker.cache(createMockBuffer(64 * 1024L,
        cacheTagBuilder("otherdb.testtable", "p=v1", "pp=vv2")), null);
    tracker.cache(createMockBuffer(128 * 1024L,
        cacheTagBuilder("otherdb.testtable", "p=v2", "pp=vv1")), null);
    tracker.cache(createMockBuffer(256 * 1024L,
        cacheTagBuilder("otherdb.testtable2", "p=v3")), null);
    tracker.cache(createMockBuffer(512 * 1024 * 1024L,
        cacheTagBuilder("otherdb.testtable2", "p=v3")), null);
    tracker.cache(createMockBuffer(1024 * 1024 * 1024L,
        cacheTagBuilder("otherdb.testtable3")), null);
    tracker.cache(createMockBuffer(2 * 1024 * 1024L,
        cacheTagBuilder("default.testtable")), null);
  }

  private static void evictSomeTestBuffers() {
    tracker.notifyEvicted(createMockBuffer(32 * 1024L,
        cacheTagBuilder("otherdb.testtable", "p=v1", "pp=vv1")));
    tracker.notifyEvicted(createMockBuffer(512 * 1024 * 1024L,
        cacheTagBuilder("otherdb.testtable2", "p=v3")));
    tracker.notifyEvicted(createMockBuffer(2 * 1024 * 1024L,
        cacheTagBuilder("default.testtable")));
    tracker.notifyEvicted(createMockBuffer(4 * 1024L,
        cacheTagBuilder("default.testtable")));
  }

  private static final String EXPECTED_CACHE_STATE_WHEN_FULL =
      "\n" +
          "Cache state: \n" +
          "default : 2/2, 2101248/2101248\n" +
          "default.testtable : 2/2, 2101248/2101248\n" +
          "otherdb : 7/7, 1611106304/1611106304\n" +
          "otherdb.testtable : 4/4, 231424/231424\n" +
          "otherdb.testtable/p=v1 : 3/3, 100352/100352\n" +
          "otherdb.testtable/p=v1/pp=vv1 : 2/2, 34816/34816\n" +
          "otherdb.testtable/p=v1/pp=vv2 : 1/1, 65536/65536\n" +
          "otherdb.testtable/p=v2 : 1/1, 131072/131072\n" +
          "otherdb.testtable/p=v2/pp=vv1 : 1/1, 131072/131072\n" +
          "otherdb.testtable2 : 2/2, 537133056/537133056\n" +
          "otherdb.testtable2/p=v3 : 2/2, 537133056/537133056\n" +
          "otherdb.testtable3 : 1/1, 1073741824/1073741824";

  private static final String EXPECTED_CACHE_STATE_AFTER_EVICTION =
      "\n" +
          "Cache state: \n" +
          "default : 0/2, 0/2101248\n" +
          "default.testtable : 0/2, 0/2101248\n" +
          "otherdb : 5/7, 1074202624/1611106304\n" +
          "otherdb.testtable : 3/4, 198656/231424\n" +
          "otherdb.testtable/p=v1 : 2/3, 67584/100352\n" +
          "otherdb.testtable/p=v1/pp=vv1 : 1/2, 2048/34816\n" +
          "otherdb.testtable/p=v1/pp=vv2 : 1/1, 65536/65536\n" +
          "otherdb.testtable/p=v2 : 1/1, 131072/131072\n" +
          "otherdb.testtable/p=v2/pp=vv1 : 1/1, 131072/131072\n" +
          "otherdb.testtable2 : 1/2, 262144/537133056\n" +
          "otherdb.testtable2/p=v3 : 1/2, 262144/537133056\n" +
          "otherdb.testtable3 : 1/1, 1073741824/1073741824";

}
