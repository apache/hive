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
import org.apache.hadoop.hive.ql.anon.btree.KeyValueStruct;
import org.apache.hadoop.hive.ql.anon.btree.LocatorSchemaItem;
import org.apache.hadoop.hive.ql.anon.btree.StructValueList;
import org.apache.hadoop.hive.ql.anon.btree.ValueItem;
import org.apache.hadoop.hive.ql.anon.index.BtreeIndexReader;
import org.apache.hadoop.hive.ql.anon.index.PageManager;
import org.apache.hadoop.hive.ql.anon.index.api.BtreeDataEntry;
import org.apache.hadoop.hive.ql.anon.index.impl.BtreeDataEntryImpl;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.*;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestBtree {

  private final String fileName = System.getProperty("java.io.tmpdir") + "/warehouse4/test.bt";
  private final int entryCount = 10;
  private final Configuration conf = new Configuration();

  @Test
  @Order(1)
  public void testWrite() throws CommandProcessorException, IOException {
    final int pageSize = 110;
    final int bufferSize = 10 * pageSize;
    final int headerSize = 64;
    conf.setInt(BTREE_CONF_BUFFER_SIZE, bufferSize);
    conf.setInt(BTREE_CONF_PAGE_SIZE, pageSize);
    conf.setInt(BTREE_CONF_PAGE_HEADER_SIZE, headerSize);
    conf.set(INDEX_KEY_TYPE, ANON_INDEX_TEST_KEY_TYPE);
    conf.set(INDEX_VALUE_TYPES, ANON_INDEX_TEST_VALUE_TYPES);
    conf.set(INDEX_ADDR_TYPE, ANON_INDEX_TEST_POINTER_TYPE);

    final FileSystem fs = FileSystem.get(conf);
    final PageManager pageManager = new PageManager(conf);

    for (int i = 1; i < entryCount; i++) {
      final KeyValueStruct struct = new KeyValueStruct();
      struct.setKey(new IntWritable(i));
      StructValueList valueList = new StructValueList();
      for (int j = 0; j < 3; j++) {
        final ValueItem valueItem = new ValueItem();

        final String fn = "dummy-" + i * 1000 + j;
        valueItem.filePath = new Text(fn);

        final LocatorSchemaItem schemaItem = new LocatorSchemaItem();
        schemaItem.rowLocator = new LongWritable(i * 1000 + j);
        schemaItem.schemaId = new IntWritable(i * 1000 + j);
        valueItem.add(schemaItem);

        valueList.add(valueItem);
        struct.setValue(valueList);
      }
      final BtreeDataEntry dataEntry = new BtreeDataEntryImpl(struct.getKey(), struct.getValue());
      pageManager.addDataEntry(dataEntry);
    }

    final FSDataOutputStream os = fs.create(new Path(fileName));
    pageManager.save(os);
    os.flush();
    os.close();
  }

  @Test
  @Order(2)
  public void testRead() throws CommandProcessorException, IOException {
    final int pageSize = 110;
    final int bufferSize = 10 * pageSize;
    final int headerSize = 64;
    conf.setInt(BTREE_CONF_BUFFER_SIZE, bufferSize);
    conf.setInt(BTREE_CONF_PAGE_SIZE, pageSize);
    conf.set(INDEX_ADDR_TYPE, ANON_INDEX_TEST_POINTER_TYPE);
    conf.set(INDEX_KEY_TYPE, ANON_INDEX_TEST_KEY_TYPE);
    conf.set(INDEX_VALUE_TYPES, ANON_INDEX_TEST_VALUE_TYPES);

    final BtreeIndexReader indexReader = new BtreeIndexReader(conf, fileName);
    final IntWritable key = new IntWritable();

    final int max = entryCount;
    for (int i = 1; i < max; i++) {
      key.set(i);
      final Writable w = indexReader.seek(key);
      final String msg = "key: " + i;
      Assertions.assertNotNull(w, msg);
    }

    for (int i = max; i < 2 * max; i++) {
      key.set(i);
      final Writable w = indexReader.seek(key);
      final String msg = "key: " + i;
      Assertions.assertNull(w, msg);
    }
  }
}
