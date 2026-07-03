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

package org.apache.hadoop.hive.ql.anon.index.dir;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.anon.ex.AnonIndexException;
import org.apache.hadoop.hive.ql.anon.btree.StructValueList;
import org.apache.hadoop.hive.ql.anon.index.IndexReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_INDEX_TEST_KEY_TYPE;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_INDEX_TEST_VALUE_TYPES;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.INDEX_HEADER_SIZE;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.INDEX_KEY_TYPE;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.INDEX_VALUE_TYPES;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.DIRECTORY_CONF_FILE_MAGIC_VALUE;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.DIRECTORY_CONF_PAGED_READ;
import static org.apache.hadoop.hive.ql.anon.utils.Utils.createWritable;
import static org.apache.hadoop.hive.ql.anon.utils.Utils.headerType;
import static org.apache.hadoop.hive.ql.anon.utils.Utils.headerTypes;

public class DirectoryIndexReader implements IndexReader, Closeable {

  private Configuration conf;

  private String keyType;
  private String pointerType;
  private String valuesTypes;

  private int rawKeysLength = 0;
  private int rawPointersLength = 0;
  private int pointerOffset = 0;
  private int valueOffset = 0;
  private int _1MB = 1024 * 1024;

  private int keyCount = 0;
  private byte[] baKeys;
  private byte[] baPointers;
  private byte[] baValues;
  private ByteArrayInputStream baisKeys;
  private ByteArrayInputStream baisPointers;
  private ByteArrayInputStream baisValues;
  private DataInputStream disKeys;
  private DataInputStream disPointers;
  private DataInputStream disValues;

  private FSDataInputStream pagedIn;
  private long valuesRegionOffset;
  private int valuesLength;
  private int[] pointers;
  private long bytesFetched;
  private long valuesFetched;

  public DirectoryIndexReader(final Configuration conf, final String fileName) throws IOException {
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
    if (magic != DIRECTORY_CONF_FILE_MAGIC_VALUE) {
      throw new AnonIndexException("header magic mismatch!");
    }
    final byte keyTypeByte = bb.get();
    bb.get();
    final byte[] valueTypeBytes = {bb.get(), bb.get(), bb.get()};
    keyType = headerType(keyTypeByte, conf.get(INDEX_KEY_TYPE, ANON_INDEX_TEST_KEY_TYPE));
    valuesTypes = headerTypes(valueTypeBytes, conf.get(INDEX_VALUE_TYPES, ANON_INDEX_TEST_VALUE_TYPES));

    keyCount = bb.getInt();
    final int baKeysLength = bb.getInt();
    final int baPointersLength = bb.getInt();
    final int baValuesLength = bb.getInt();

    baKeys = new byte[baKeysLength];
    din.readFully(baKeys);
    baPointers = new byte[baPointersLength];
    din.readFully(baPointers);

    baisKeys = new ByteArrayInputStream(baKeys);
    disKeys = new DataInputStream(baisKeys);

    final boolean paged = conf.getBoolean(DIRECTORY_CONF_PAGED_READ, false);
    if (paged && is instanceof FSDataInputStream) {
      pointers = new int[keyCount];
      final DataInputStream dp = new DataInputStream(new ByteArrayInputStream(baPointers));
      for (int i = 0; i < keyCount; i++) {
        pointers[i] = dp.readInt();
      }
      valuesLength = baValuesLength;
      valuesRegionOffset = (long) INDEX_HEADER_SIZE + baKeysLength + baPointersLength;
      pagedIn = (FSDataInputStream) is;
    } else {
      baValues = new byte[baValuesLength];
      din.readFully(baValues);
      baisPointers = new ByteArrayInputStream(baPointers);
      baisValues = new ByteArrayInputStream(baValues);
      disPointers = new DataInputStream(baisPointers);
      disValues = new DataInputStream(baisValues);
    }
  }

  @Override
  public Writable seek(final WritableComparable key) throws IOException {
    disKeys.reset();
    final WritableComparable tmpKey = (WritableComparable) createWritable(keyType.charAt(0));
    int ix = -1;
    for (int i = 0; i < keyCount; i++) {
      tmpKey.readFields(disKeys);
      if (tmpKey.equals(key)) {
        ix = i;
        break;
      }
    }

    if (ix == -1) {
      return null;
    }

    if (pagedIn != null) {
      final int start = pointers[ix];
      final int end = (ix + 1 < keyCount) ? pointers[ix + 1] : valuesLength;
      final int len = end - start;
      final byte[] vbuf = new byte[len];
      pagedIn.readFully(valuesRegionOffset + start, vbuf, 0, len);
      bytesFetched += len;
      valuesFetched++;
      final StructValueList bvl = new StructValueList(valuesTypes);
      bvl.readFields(new DataInputStream(new ByteArrayInputStream(vbuf)));
      return bvl;
    }

    disPointers.reset();
    disValues.reset();
    final long r = disPointers.skip(ix * 4L);
    final IntWritable pointer = new IntWritable();
    pointer.readFields(disPointers);

    final long r2 = disValues.skip(pointer.get());
    final StructValueList bvl = new StructValueList(valuesTypes);
    bvl.readFields(disValues);

    return bvl;
  }

  @Override
  public void close() throws IOException {
    if (pagedIn != null) {
      pagedIn.close();
      pagedIn = null;
    }
  }

  public long getBytesFetched() {
    return bytesFetched;
  }

  public long getValuesFetched() {
    return valuesFetched;
  }

  @Override
  public String getType() {
    return "DR";
  }
}
