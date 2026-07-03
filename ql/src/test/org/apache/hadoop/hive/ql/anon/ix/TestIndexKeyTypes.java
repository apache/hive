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
import org.apache.hadoop.hive.ql.anon.index.tab.TabularEntry;
import org.apache.hadoop.hive.ql.anon.index.tab.TabularIndex;
import org.apache.hadoop.hive.ql.anon.index.tab.TabularIndexReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_INDEX_TEST_VALUE_TYPES;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.DIRECTORY_CONF_PAGED_READ;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.INDEX_ADDR_TYPE;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.INDEX_KEY_TYPE;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.INDEX_VALUE_TYPES;
import static org.apache.hadoop.hive.ql.anon.index.Converters.convert;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestIndexKeyTypes {

  private static final int ENTRY_COUNT = 50;
  private static final long LONG_BASE = 5_000_000_000L;

  private interface KeyGen {
    WritableComparable key(int i);
  }

  private static final KeyGen INT_KEYS = i -> new IntWritable(i + 1);
  private static final KeyGen LONG_KEYS = i -> new LongWritable(LONG_BASE + i);
  private static final KeyGen STRING_KEYS = i -> new Text("user-" + (i + 1));

  private Configuration writeConf(final String keyType) {
    final Configuration c = new Configuration();
    c.set(INDEX_KEY_TYPE, keyType);
    c.set(INDEX_VALUE_TYPES, ANON_INDEX_TEST_VALUE_TYPES);
    c.set(INDEX_ADDR_TYPE, "I");
    return c;
  }

  private StructValueList value(final int i) {
    final StructValueList bvl = new StructValueList(ANON_INDEX_TEST_VALUE_TYPES);
    final ValueItem item = new ValueItem(ANON_INDEX_TEST_VALUE_TYPES);
    item.filePath = new Text("file-" + i);
    final LocatorSchemaItem si = new LocatorSchemaItem();
    si.rowLocator = new LongWritable(100_000_000L + i);
    si.schemaId = new IntWritable(1000 + i);
    item.add(si);
    bvl.add(item);
    return bvl;
  }

  private void writeDirectory(final String keyType, final KeyGen keys, final String file) throws IOException {
    final Configuration c = writeConf(keyType);
    final DirectoryIndex index = new DirectoryIndex(c);
    for (int i = 0; i < ENTRY_COUNT; i++) {
      index.addEntry(new RawDataEntry(convert(keys.key(i)), convert(value(i))));
    }
    final FileSystem fs = FileSystem.get(c);
    try (FSDataOutputStream os = fs.create(new Path(file), true)) {
      index.save(os);
    }
  }

  private void writeTabular(final String keyType, final KeyGen keys, final String file) throws IOException {
    final Configuration c = writeConf(keyType);
    final TabularIndex index = new TabularIndex(c);
    for (int i = 0; i < ENTRY_COUNT; i++) {
      index.addEntry(new TabularEntry(keys.key(i), value(i)));
    }
    final FileSystem fs = FileSystem.get(c);
    try (FSDataOutputStream os = fs.create(new Path(file), true)) {
      index.save(os);
    }
  }

  private void assertLocators(final Writable result, final int i) {
    assertNotNull(result, "miss for entry " + i);
    final StructValueList bvl = (StructValueList) result;
    final List<ValueItem> items = bvl.getItems();
    assertEquals(1, items.size(), "one value item per key");
    final ValueItem item = items.get(0);
    assertEquals("file-" + i, item.filePath.toString());
    final List<LocatorSchemaItem> locators = item.getItemList();
    assertEquals(1, locators.size(), "one locator per value item");
    assertEquals(100_000_000L + i, ((LongWritable) locators.get(0).rowLocator).get());
    assertEquals(1000 + i, ((IntWritable) locators.get(0).schemaId).get());
  }

  private void probeDirectory(final Configuration readConf, final String file,
                              final KeyGen keys, final WritableComparable missKey) throws IOException {
    try (DirectoryIndexReader reader = new DirectoryIndexReader(readConf, file)) {
      for (int i = 0; i < ENTRY_COUNT; i++) {
        assertLocators(reader.seek(keys.key(i)), i);
      }
      assertNull(reader.seek(missKey), "unexpected hit for absent key");
    }
  }

  private void roundTripDirectory(final String keyType, final KeyGen keys,
                                  final WritableComparable missKey, final java.nio.file.Path tmp) throws IOException {
    final String file = tmp.resolve("keytype-" + keyType + ".dir").toString();
    writeDirectory(keyType, keys, file);
    probeDirectory(new Configuration(), file, keys, missKey);
    final Configuration paged = new Configuration();
    paged.setBoolean(DIRECTORY_CONF_PAGED_READ, true);
    probeDirectory(paged, file, keys, missKey);
  }

  private void roundTripTabular(final String keyType, final KeyGen keys,
                                final WritableComparable missKey, final java.nio.file.Path tmp) throws IOException {
    final String file = tmp.resolve("keytype-" + keyType + ".tab").toString();
    writeTabular(keyType, keys, file);
    final TabularIndexReader reader = new TabularIndexReader(new Configuration(), file);
    for (int i = 0; i < ENTRY_COUNT; i++) {
      assertLocators(reader.seek(keys.key(i)), i);
    }
    assertNull(reader.seek(missKey), "unexpected hit for absent key");
  }

  @Test
  public void directoryLongKeys(@TempDir final java.nio.file.Path tmp) throws IOException {
    roundTripDirectory("L", LONG_KEYS, new LongWritable(LONG_BASE - 1), tmp);
  }

  @Test
  public void directoryStringKeys(@TempDir final java.nio.file.Path tmp) throws IOException {
    roundTripDirectory("T", STRING_KEYS, new Text("user-none"), tmp);
  }

  @Test
  public void directoryIntKeysHeaderDriven(@TempDir final java.nio.file.Path tmp) throws IOException {
    roundTripDirectory("I", INT_KEYS, new IntWritable(ENTRY_COUNT + 1), tmp);
  }

  @Test
  public void tabularLongKeys(@TempDir final java.nio.file.Path tmp) throws IOException {
    roundTripTabular("L", LONG_KEYS, new LongWritable(LONG_BASE - 1), tmp);
  }

  @Test
  public void tabularStringKeys(@TempDir final java.nio.file.Path tmp) throws IOException {
    roundTripTabular("T", STRING_KEYS, new Text("user-none"), tmp);
  }

  @Test
  public void tabularIntKeysHeaderDriven(@TempDir final java.nio.file.Path tmp) throws IOException {
    roundTripTabular("I", INT_KEYS, new IntWritable(ENTRY_COUNT + 1), tmp);
  }
}
