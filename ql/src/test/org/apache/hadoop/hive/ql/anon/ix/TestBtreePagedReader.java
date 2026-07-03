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

package org.apache.hadoop.hive.ql.anon.ix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.anon.btree.LocatorSchemaItem;
import org.apache.hadoop.hive.ql.anon.btree.StructValueList;
import org.apache.hadoop.hive.ql.anon.btree.ValueItem;
import org.apache.hadoop.hive.ql.anon.index.BtreeIndexReader;
import org.apache.hadoop.hive.ql.anon.index.PageManager;
import org.apache.hadoop.hive.ql.anon.index.api.BtreeDataEntry;
import org.apache.hadoop.hive.ql.anon.index.impl.BtreeDataEntryImpl;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.*;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBtreePagedReader {

  private static final int PAGE_SIZE = 256;
  private static final int BUFFER_SIZE = 32 * PAGE_SIZE;
  private static final int N = 20000;
  private static final int CACHE_PAGES = 16;

  private Configuration conf() {
    final Configuration c = new Configuration();
    c.setInt(BTREE_CONF_PAGE_SIZE, PAGE_SIZE);
    c.setInt(BTREE_CONF_BUFFER_SIZE, BUFFER_SIZE);
    c.setInt(BTREE_CONF_PAGE_HEADER_SIZE, 64);
    c.set(INDEX_KEY_TYPE, ANON_INDEX_TEST_KEY_TYPE);
    c.set(INDEX_VALUE_TYPES, ANON_INDEX_TEST_VALUE_TYPES);
    c.set(INDEX_ADDR_TYPE, ANON_INDEX_TEST_POINTER_TYPE);
    return c;
  }

  private void buildIndex(final Configuration c, final String file) throws IOException {
    final FileSystem fs = FileSystem.get(c);
    final PageManager pm = new PageManager(c);
    for (int i = 1; i < N; i++) {
      final StructValueList valueList = new StructValueList();
      final ValueItem item = new ValueItem();
      item.filePath = new Text("f");
      final LocatorSchemaItem schemaItem = new LocatorSchemaItem();
      schemaItem.rowLocator = new LongWritable(i);
      schemaItem.schemaId = new IntWritable(i);
      item.add(schemaItem);
      valueList.add(item);
      final BtreeDataEntry entry = new BtreeDataEntryImpl(new IntWritable(i), valueList);
      pm.addDataEntry(entry);
    }
    try (FSDataOutputStream os = fs.create(new Path(file))) {
      pm.save(os);
      os.flush();
    }
  }

  @Test
  public void pagedMatchesEagerAndTransfersFarLessThanWholeFile(@TempDir final java.nio.file.Path tmp)
      throws IOException {
    final String file = tmp.resolve("paged.bt").toString();
    buildIndex(conf(), file);

    final FileSystem fs = FileSystem.get(conf());
    final long fileSize = fs.getFileStatus(new Path(file)).getLen();
    final long pageCount = (fileSize - 64) / PAGE_SIZE;

    final String[] expected = new String[N];
    final BtreeIndexReader eager = new BtreeIndexReader(conf(), file);
    for (int i = 1; i < N; i++) {
      final Writable w = eager.seek(new IntWritable(i));
      assertNotNull(w, "eager seek missed key " + i);
      expected[i] = w.toString();
    }

    final Configuration pc = conf();
    pc.setBoolean(BTREE_CONF_PAGED_READ, true);
    pc.setInt(BTREE_CONF_PAGE_CACHE_PAGES, CACHE_PAGES);
    long bytesAll;
    try (BtreeIndexReader paged = new BtreeIndexReader(pc, file)) {
      assertTrue(paged.getPagesFetched() >= 1, "root must be fetched in paged mode");
      for (int i = 1; i < N; i++) {
        final Writable w = paged.seek(new IntWritable(i));
        assertNotNull(w, "paged seek missed key " + i);
        assertEquals(expected[i], w.toString(), "paged != eager for key " + i);
      }
      bytesAll = paged.getBytesFetched();
    }

    final int k = 200;
    final Configuration kc = conf();
    kc.setBoolean(BTREE_CONF_PAGED_READ, true);
    kc.setInt(BTREE_CONF_PAGE_CACHE_PAGES, CACHE_PAGES);
    long bytesK;
    long pagesK;
    try (BtreeIndexReader paged = new BtreeIndexReader(kc, file)) {
      final int stride = (N - 1) / k;
      for (int j = 0; j < k; j++) {
        final int key = 1 + j * stride;
        assertNotNull(paged.seek(new IntWritable(key)), "paged seek missed spread key " + key);
      }
      bytesK = paged.getBytesFetched();
      pagesK = paged.getPagesFetched();
    }

    final double coldPerLookup = pagesK / (double) k;
    final long crossoverK = Math.round(pageCount / Math.max(1.0, coldPerLookup));

    System.out.println("==== paged b-tree reader: transfer model ====");
    System.out.printf("  entries .................. %d%n", N - 1);
    System.out.printf("  page size ................ %d B%n", PAGE_SIZE);
    System.out.printf("  index file ............... %d B (%d pages)%n", fileSize, pageCount);
    System.out.printf("  LRU cache ................ %d pages (%d B)%n", CACHE_PAGES, (long) CACHE_PAGES * PAGE_SIZE);
    System.out.printf("  eager read transfers ..... %d B (whole file, once)%n", fileSize);
    System.out.printf("  paged, K=%d lookups ...... %d B in %d page reads (%.2f cold pages/lookup)%n",
        k, bytesK, pagesK, coldPerLookup);
    System.out.printf("  paged transfer / file .... %.1f%%%n", 100.0 * bytesK / fileSize);
    System.out.printf("  paged, all %d lookups .... %d B (%.1f%% of file)%n",
        N - 1, bytesAll, 100.0 * bytesAll / fileSize);
    System.out.printf("  crossover K (paged==file)  ~%d lookups%n", crossoverK);

    assertTrue(bytesK < fileSize / 4,
        "expected a K=" + k + " batch to transfer < 25% of the file; got " + bytesK + " of " + fileSize);
    assertTrue(coldPerLookup < 12.0, "cold pages/lookup unexpectedly high: " + coldPerLookup);
  }
}
