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
package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

@Category(MetastoreUnitTest.class)
public class TestCacheAwareCompactor {

  @Test
  public void testCreateCache() {
    Configuration conf = new Configuration();
    CacheAwareCompactor.CompactorMetadataCache cache =
      CacheAwareCompactor.CompactorMetadataCache.createIfEnabled(conf);
    Assert.assertNotNull(cache);
  }

  @Test
  public void testDisableCache() {
    Configuration conf = new Configuration();
    MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.COMPACTOR_METADATA_CACHE_TIMEOUT, 0, TimeUnit.SECONDS);
    CacheAwareCompactor.CompactorMetadataCache cache =
      CacheAwareCompactor.CompactorMetadataCache.createIfEnabled(conf);
    Assert.assertNull(cache);
  }

  @Test
  public void testTrySetCache() {
    Configuration conf = new Configuration();
    CacheAwareCompactor.CompactorMetadataCache cache =
      CacheAwareCompactor.CompactorMetadataCache.createIfEnabled(conf);
    CacheAwareCompactor mock = Mockito.mock(CacheAwareCompactor.class);
    CacheAwareCompactor.trySetCache(mock, cache);
    Mockito.verify(mock).setCache(cache);
  }

  @Test
  public void testTableCache() {
    Configuration conf = new Configuration();
    CacheAwareCompactor.CompactorMetadataCache cache =
      CacheAwareCompactor.CompactorMetadataCache.createIfEnabled(conf);
    Table table = Mockito.mock(Table.class);
    CompactionInfo ci = new CompactionInfo("testdb", "testtbl", null, null);
    Table res = cache.resolveTable(ci, () -> table);
    Assert.assertEquals(table, res);
    res = cache.resolveTable(ci, () -> null);
    // It should return the cached value
    Assert.assertEquals(table, res);
  }

  @Test
  public void testPartitionCache() {
    Configuration conf = new Configuration();
    CacheAwareCompactor.CompactorMetadataCache cache =
      CacheAwareCompactor.CompactorMetadataCache.createIfEnabled(conf);
    Partition part = Mockito.mock(Partition.class);
    CompactionInfo ci = new CompactionInfo("testdb", "testtbl", "testpart", null);
    Partition res = cache.resolvePartition(ci, () -> part);
    Assert.assertEquals(part, res);
    res = cache.resolvePartition(ci, () -> new Partition());
    // It should return the cached value
    Assert.assertEquals(part, res);
  }

  @Test
  public void testNoPartitionNameIsSet() {
    Configuration conf = new Configuration();
    CacheAwareCompactor.CompactorMetadataCache cache =
      CacheAwareCompactor.CompactorMetadataCache.createIfEnabled(conf);
    CompactionInfo ci = new CompactionInfo("testdb", "testtbl", null, null);
    Partition res = cache.resolvePartition(ci, () -> new Partition());
    Assert.assertNull(res);
  }

  @Test
  public void testTableCacheTimeout() throws Exception {
    CacheAwareCompactor.CompactorMetadataCache cache =
      new CacheAwareCompactor.CompactorMetadataCache(1, TimeUnit.MILLISECONDS);
    Table old = Mockito.mock(Table.class);
    CompactionInfo ci = new CompactionInfo("testdb", "testtbl", null, null);
    Table res = cache.resolveTable(ci, () -> old);
    Assert.assertEquals(old, res);
    Thread.sleep(2);

    Table recent = Mockito.mock(Table.class);
    res = cache.resolveTable(ci, () -> recent);
    // It should return the recent value
    Assert.assertEquals(recent, res);
  }

  @Test
  public void testPartitionCacheTimeout() throws Exception {
    CacheAwareCompactor.CompactorMetadataCache cache =
      new CacheAwareCompactor.CompactorMetadataCache(1, TimeUnit.MILLISECONDS);
    Partition old = Mockito.mock(Partition.class);
    CompactionInfo ci = new CompactionInfo("testdb", "testtbl", "testpart", null);
    Partition res = cache.resolvePartition(ci, () -> old);
    Assert.assertEquals(old, res);
    Thread.sleep(2);

    Partition recent = Mockito.mock(Partition.class);
    res = cache.resolvePartition(ci, () -> recent);
    // It should return the recent value
    Assert.assertEquals(recent, res);
  }

}
