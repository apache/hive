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
import org.apache.hadoop.hive.ql.anon.index.RawDataEntry;
import org.apache.hadoop.hive.ql.anon.index.dir.DirectoryIndex;
import org.apache.hadoop.hive.ql.anon.index.dir.DirectoryIndexReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_INDEX_TEST_KEY_TYPE;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_INDEX_TEST_POINTER_TYPE;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_INDEX_TEST_VALUE_TYPES;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.DIRECTORY_CONF_PAGED_READ;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.INDEX_KEY_TYPE;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.INDEX_VALUE_TYPES;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.INDEX_ADDR_TYPE;
import static org.apache.hadoop.hive.ql.anon.index.Converters.convert;
import static org.apache.hadoop.hive.ql.anon.index.Converters.convertWritableToBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDirectoryPagedReader {

  private static final int N = 20000;
  private static final String PAD;
  static {
    final StringBuilder b = new StringBuilder();
    for (int i = 0; i < 150; i++) {
      b.append('x');
    }
    PAD = b.toString();
  }

  private Configuration conf() {
    final Configuration c = new Configuration();
    c.set(INDEX_KEY_TYPE, ANON_INDEX_TEST_KEY_TYPE);
    c.set(INDEX_VALUE_TYPES, ANON_INDEX_TEST_VALUE_TYPES);
    c.set(INDEX_ADDR_TYPE, ANON_INDEX_TEST_POINTER_TYPE);
    return c;
  }

  private void buildIndex(final Configuration c, final String file) throws IOException {
    final DirectoryIndex index = new DirectoryIndex(c);
    for (int i = 1; i <= N; i++) {
      final IntWritable key = new IntWritable(i);
      final StructValueList bvl = new StructValueList(ANON_INDEX_TEST_VALUE_TYPES);
      final ValueItem item = new ValueItem(ANON_INDEX_TEST_VALUE_TYPES);
      item.filePath = new Text(PAD);
      final LocatorSchemaItem si = new LocatorSchemaItem();
      si.rowLocator = new LongWritable(i);
      si.schemaId = new IntWritable(i);
      item.add(si);
      bvl.add(item);
      index.addEntry(new RawDataEntry(convertWritableToBytes(key), convert(bvl)));
    }
    final FileSystem fs = FileSystem.get(c);
    try (FSDataOutputStream os = fs.create(new Path(file), true)) {
      index.save(os);
    }
  }

  @Test
  public void pagedMatchesEagerAndReadsOnlyMatchedValues(@TempDir final java.nio.file.Path tmp) throws IOException {
    final String file = tmp.resolve("paged.dir").toString();
    buildIndex(conf(), file);
    final FileSystem fs = FileSystem.get(conf());
    final long fileSize = fs.getFileStatus(new Path(file)).getLen();

    final String[] expected = new String[N + 1];
    final DirectoryIndexReader eager = new DirectoryIndexReader(conf(), file);
    for (int i = 1; i <= N; i++) {
      final Writable w = eager.seek(new IntWritable(i));
      assertNotNull(w, "eager miss key " + i);
      expected[i] = w.toString();
    }

    final Configuration pc = conf();
    pc.setBoolean(DIRECTORY_CONF_PAGED_READ, true);
    try (DirectoryIndexReader paged = new DirectoryIndexReader(pc, file)) {
      for (int i = 1; i <= N; i++) {
        final Writable w = paged.seek(new IntWritable(i));
        assertNotNull(w, "paged miss key " + i);
        assertEquals(expected[i], w.toString(), "paged != eager for key " + i);
      }
      assertEquals(N, paged.getValuesFetched(), "exactly one value positioned-read per lookup");
    }

    final int k = 200;
    final Configuration kc = conf();
    kc.setBoolean(DIRECTORY_CONF_PAGED_READ, true);
    try (DirectoryIndexReader paged = new DirectoryIndexReader(kc, file)) {
      final int stride = N / k;
      for (int j = 0; j < k; j++) {
        assertNotNull(paged.seek(new IntWritable(1 + j * stride)), "paged miss spread key");
      }
      final long valueBytes = paged.getBytesFetched();
      final long valuesFetched = paged.getValuesFetched();
      System.out.println("==== directory paged reader ====");
      System.out.printf("  entries .......... %d%n", N);
      System.out.printf("  index file ....... %,d B%n", fileSize);
      System.out.printf("  K lookups ........ %d -> %d values positioned-read, %,d value bytes (%.2f%% of file)%n",
          k, valuesFetched, valueBytes, 100.0 * valueBytes / fileSize);
      assertEquals(k, valuesFetched, "one value read per lookup");
      assertTrue(valueBytes < fileSize / 4,
          "K=" + k + " batch should read << file; got " + valueBytes + " of " + fileSize);
    }
  }
}
