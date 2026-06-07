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
import org.apache.hadoop.hive.ql.anon.index.tab.TabularEntry;
import org.apache.hadoop.hive.ql.anon.index.tab.TabularIndex;
import org.apache.hadoop.hive.ql.anon.index.tab.TabularIndexReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.bouncycastle.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_INDEX_TEST_KEY_TYPE;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_INDEX_TEST_VALUE_TYPES;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.INDEX_KEY_TYPE;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.INDEX_VALUE_TYPES;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestTabular {

  private static final Logger LOG = LoggerFactory.getLogger(TestTabular.class);
  private final String fileName = System.getProperty("java.io.tmpdir") + "/warehouse4/test.tab";
  private final int entryCount = 10;
  private final Configuration conf = new Configuration();

  @Test
  @Order(1)
  public void testWrite() throws IOException {
    conf.set(INDEX_KEY_TYPE, ANON_INDEX_TEST_KEY_TYPE);
    conf.set(INDEX_VALUE_TYPES, ANON_INDEX_TEST_VALUE_TYPES);
    final TabularIndex index = new TabularIndex(conf);

    for (int i = 0; i < entryCount; i++) {
      final IntWritable wcKey = new IntWritable(i + 1);
      final StructValueList bvl = new StructValueList(ANON_INDEX_TEST_VALUE_TYPES);
      final ValueItem valueItem = new ValueItem(ANON_INDEX_TEST_VALUE_TYPES);
      valueItem.filePath = new Text("dummy" + i);

      final LocatorSchemaItem schemaItem = new LocatorSchemaItem();
      schemaItem.rowLocator = new LongWritable(100_000_000 + i);
      schemaItem.schemaId = new IntWritable(10 + i);

      valueItem.add(schemaItem);

      bvl.add(valueItem);

      final TabularEntry entry = new TabularEntry(wcKey, bvl);
      index.addEntry(entry);
    }

    final FileSystem fs = FileSystem.get(conf);
    final FSDataOutputStream os = fs.create(new Path(fileName));
    index.save(os);
    os.flush();
    os.close();
  }

  @Test
  @Order(2)
  public void testRead() throws IOException {
    final TabularIndexReader indexReader = new TabularIndexReader(conf, fileName);
    final IntWritable iwKey = new IntWritable();

    final int[] keys = new int[entryCount];
    for (int i = 0; i < entryCount; i++) {
      keys[i] = i + 1;
    }

    final int[] keysReversed = Arrays.reverse(keys);

    for (int key : keys) {
      iwKey.set(key);
      final Writable ret = indexReader.seek(iwKey);
      Assertions.assertNotNull(ret);
    }

    for (int key : keysReversed) {
      iwKey.set(key);
      final Writable ret = indexReader.seek(iwKey);
      Assertions.assertNotNull(ret);
    }
  }

}
