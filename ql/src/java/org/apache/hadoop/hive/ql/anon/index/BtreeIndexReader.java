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

package org.apache.hadoop.hive.ql.anon.index;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.anon.btree.KeyValueStruct;
import org.apache.hadoop.hive.ql.anon.ex.AnonIndexException;
import org.apache.hadoop.hive.ql.anon.ex.BtreeException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;
public class BtreeIndexReader implements IndexReader, Closeable {

  private static final int HEADER_SIZE = 64;
  private final Configuration conf;
  private BufferPool bufferPool;
  private final int pageSize;
  private final List<Page> indexPages = new ArrayList<>();

  public BtreeIndexReader(final Configuration conf, final String fileName) throws IOException {
    this.conf = conf;
    this.pageSize = conf.getInt(BTREE_CONF_PAGE_SIZE, BTREE_INVALID);

    final Path path = new Path(fileName);
    FileSystem fs = path.getFileSystem(conf);
    FSDataInputStream is = fs.open(path);
    load(is);
  }

  @Override
  public void load(final InputStream is) throws IOException {
    final byte[] header = new byte[HEADER_SIZE];
    try {
      new DataInputStream(is).readFully(header);
    } catch (final EOFException eof) {
      throw new BtreeException("header read err!");
    }
    final ByteBuffer bb = ByteBuffer.wrap(header);
    final short magic = bb.getShort();
    if (magic != BTREE_CONF_FILE_MAGIC_VALUE) {
      throw new AnonIndexException("header magic mismatch!");
    }
    final int pgSize = bb.getInt();
    final int pgCount = bb.getInt();
    final int bufSize = bb.getInt();
    final int bufCount = bb.getInt();
    final int rootPageId = bb.getInt();

    if (pgSize != pageSize) {
      throw new BtreeException("page size mismatch!");
    }

    final byte keyFieldCount = bb.get();
    final List<Byte> keyFieldTypes = new ArrayList<>();
    for (int i = 0; i < keyFieldCount; i++) {
      final byte b = bb.get();
      keyFieldTypes.add(b);
    }

    final byte valueFieldCount = bb.get();
    final List<Byte> valueFieldTypes = new ArrayList<>();
    for (int i = 0; i < valueFieldCount; i++) {
      final byte b = bb.get();
      valueFieldTypes.add(b);
    }


    bufferPool = new BufferPool(conf);
    final String headerKeyType = typesToString(keyFieldTypes);
    final String headerValueTypes = typesToString(valueFieldTypes);
    if (headerKeyType != null && headerValueTypes != null) {
      bufferPool.overrideTypes(headerKeyType, headerValueTypes);
    }
    final boolean paged = conf.getBoolean(BTREE_CONF_PAGED_READ, false);
    if (paged && is instanceof FSDataInputStream) {
      final int cachePages = conf.getInt(BTREE_CONF_PAGE_CACHE_PAGES,
          Math.max(1, bufSize / pgSize));
      bufferPool.enablePagedMode((FSDataInputStream) is, HEADER_SIZE, pgCount, cachePages);
    } else {
      bufferPool.load(is, bufSize, bufCount, pgCount);
    }

    restoreTree(rootPageId);
  }

  private static String typesToString(final List<Byte> types) {
    if (types.isEmpty()) {
      return null;
    }
    final StringBuilder sb = new StringBuilder(types.size());
    for (final byte b : types) {
      if (b <= 0) {
        return null;
      }
      sb.append((char) b);
    }
    return sb.toString();
  }

  private void restoreTree(final int rootPageId) {
    final Page rootPage = getPage(rootPageId);
    indexPages.add(rootPage);
  }

  public Page getPage(final int pageId) {
    return bufferPool.getPage(pageId);
  }

  @Override
  public Writable seek(WritableComparable searchKey) throws IOException {
    Page page = indexPages.get(0);
    PageInfo pageInfo = page.getInfo();
    List<Writable> e1 = pageInfo.getEntries();
    Writable value = nav(searchKey, e1);
    return value;
  }

  private Writable nav(WritableComparable searchKey, List<Writable> entries) throws IOException {
    int ix = -1;
    int entryCount = entries.size();
    for (int i = 0; i < entryCount; i++) {
      Writable entry = entries.get(i);
      if (!(entry instanceof IndexStruct)) {
        throw new BtreeException("entry is not a IndexStruct");
      }
      IndexStruct indexStruct = (IndexStruct) entry;
      int cmp = searchKey.compareTo(indexStruct.getKey());
      if (cmp >= 0) {
        ix = i;
      }
    }

    if (ix >= 0) {
      IndexStruct indexStruct = (IndexStruct) entries.get(ix);
      IntWritable pageAddress = (IntWritable) indexStruct.getValue();
      Page tmp = bufferPool.getPage(pageAddress.get());
      PageInfo tmpInfo = tmp.getInfo();
      List<Writable> e2 = tmpInfo.getEntries();
      if (tmpInfo.getType() == PageType.INDEX) {
        return nav(searchKey, e2);
      } else {
        return navData(searchKey, e2);
      }
    }

    return null;
  }

  private Writable navData(WritableComparable key, List<Writable> e1) throws IOException {
    for (Writable entry : e1) {
      if (!(entry instanceof KeyValueStruct)) {
        throw new RuntimeException("not a BtreeStruct");
      }
      KeyValueStruct struct = (KeyValueStruct) entry;
      if (key.compareTo(struct.getKey()) == 0) {
        Writable value = struct.getValue();
        return value;
      }
    }
    return null;
  }

  @Override
  public String getType() {
    return "BT";
  }

  @Override
  public void close() throws IOException {
    bufferPool.closePaged();
  }

  public long getPagesFetched() {
    return bufferPool.getPagesFetched();
  }

  public long getBytesFetched() {
    return bufferPool.getBytesFetched();
  }
}
