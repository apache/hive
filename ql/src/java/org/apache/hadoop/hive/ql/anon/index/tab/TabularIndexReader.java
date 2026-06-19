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

package org.apache.hadoop.hive.ql.anon.index.tab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.anon.ex.AnonIndexException;
import org.apache.hadoop.hive.ql.anon.index.IndexReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_INDEX_TEST_KEY_TYPE;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_INDEX_TEST_VALUE_TYPES;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;
import static org.apache.hadoop.hive.ql.anon.utils.Utils.headerType;
import static org.apache.hadoop.hive.ql.anon.utils.Utils.headerTypes;

public class TabularIndexReader implements IndexReader {

  private Configuration conf;

  private String keyType;
  private String valuesTypes;

  private int rawKeysLength = 0;
  private int _1MB = 1024 * 1024;

  private int keyCount = 0;
  private byte[] baEntries;
  private ByteArrayInputStream baisEntries;
  private DataInputStream disEntries;

  public TabularIndexReader(final Configuration conf, final String fileName) throws IOException {
    this.conf = conf;
    final Path path = new Path(fileName);
    FileSystem fs = path.getFileSystem(conf);
    FSDataInputStream is = fs.open(path);
    load(is);
  }

  @Override
  public void load(final InputStream is) throws IOException {
    final DataInputStream din = new DataInputStream(is);
    final byte[] header = new byte[INDEX_HEADER_SIZE];
    din.readFully(header);
    final ByteBuffer bb = ByteBuffer.wrap(header);

    final short magic = bb.getShort();
    if (magic != TABULAR_CONF_FILE_MAGIC_VALUE) {
      throw new AnonIndexException("header magic mismatch!");
    }
    final byte keyTypeByte = bb.get();
    final byte[] valueTypeBytes = {bb.get(), bb.get(), bb.get()};
    keyType = headerType(keyTypeByte, conf.get(INDEX_KEY_TYPE, ANON_INDEX_TEST_KEY_TYPE));
    valuesTypes = headerTypes(valueTypeBytes, conf.get(INDEX_VALUE_TYPES, ANON_INDEX_TEST_VALUE_TYPES));

    keyCount = bb.getInt();
    final int baKeysLength = bb.getInt();

    baEntries = new byte[baKeysLength];
    din.readFully(baEntries);

    baisEntries = new ByteArrayInputStream(baEntries);
    disEntries = new DataInputStream(baisEntries);
  }

  @Override
  public Writable seek(final WritableComparable key) throws IOException {
    disEntries.reset();

    TabularEntry entry = new TabularEntry(keyType, valuesTypes);
    for (int i = 0; i < keyCount; i++) {
      entry.readFields(disEntries);
      if (entry.getKey().equals(key)) {
        return entry.getValue();
      }
      entry.clear();
    }

    return null;
  }

  @Override
  public String getType() {
    return "TB";
  }
}
