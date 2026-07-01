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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.anon.ex.BtreeException;
import org.apache.hadoop.hive.ql.anon.index.api.BtreeDataEntry;
import org.apache.hadoop.hive.ql.anon.index.api.BtreeIndexEntry;
import org.apache.hadoop.hive.ql.anon.index.impl.BtreeIndexEntryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;
public final class PageManager {

  private static final Logger LOG = LoggerFactory.getLogger(PageManager.class);
  private final Configuration conf;
  private final int pageSize;
  private final BufferPool bufferPool;
  private Page dataPage;
  private final List<Page> indexPages = new ArrayList<>();
  private final Converter converter;



  public PageManager(final Configuration configuration) {
    conf = configuration;
    pageSize = conf.getInt(BTREE_CONF_PAGE_SIZE, BTREE_INVALID);

    bufferPool = new BufferPool(conf);
    converter = new SimpleConverter(conf);
  }

  public void addDataEntry(final BtreeDataEntry dataEntry) throws IOException {
    final RawDataEntry rawDataEntry = converter.convert(dataEntry);
    if (indexPages.isEmpty()) {
      dataPage = bufferPool.allocateDataPage();
      dataPage.addEntry(rawDataEntry);

      indexPages.add(bufferPool.allocateIndexPage());
      addIndexEntry(rawDataEntry);
    } else {
      if (dataPage.addEntry(rawDataEntry)) {
        return;
      }

      dataPage = bufferPool.allocateDataPage();

      final boolean ret = dataPage.addEntry(rawDataEntry);
      validateRet(ret);

      addIndexEntry(rawDataEntry);
    }
  }

  private void addIndexEntry(final RawDataEntry dataEntry) throws IOException {
    final BtreeIndexEntry indexEntry = new BtreeIndexEntryImpl(dataEntry.rawKey, dataPage.getPageId());
    final RawIndexEntry rawIndexEntry = Converters.convert(indexEntry);

    final Page currentIndexPage = indexPages.get(0);
    if (currentIndexPage.addEntry(rawIndexEntry)) {
      return;
    }

    final Page newIndexPage = bufferPool.allocateIndexPage();
    final boolean ret = newIndexPage.addEntry(rawIndexEntry);
    validateRet(ret);

    indexPages.set(0, newIndexPage);
    updateIndexPage(1, currentIndexPage, newIndexPage);
  }

  private void updateIndexPage(final int level, final Page oldPage, final Page newPage) throws IOException {
    if (indexPages.size() <= level) {

      final Page newLevelPage = bufferPool.allocateIndexPage();
      final BtreeIndexEntry indexEntry1 = new BtreeIndexEntryImpl(oldPage.getMinKey(), oldPage.getPageId());
      final RawIndexEntry rawIndexEntry1 = Converters.convert(indexEntry1);

      final BtreeIndexEntry indexEntry2 = new BtreeIndexEntryImpl(newPage.getMinKey(), newPage.getPageId());
      final RawIndexEntry rawIndexEntry2 = Converters.convert(indexEntry2);

      final boolean ret1 = newLevelPage.addEntry(rawIndexEntry1);
      validateRet(ret1);

      final boolean ret2 = newLevelPage.addEntry(rawIndexEntry2);
      validateRet(ret2);

      indexPages.add(newLevelPage);
    } else {
      final Page currentIndexPageAtLevel = indexPages.get(level);
      final BtreeIndexEntry indexEntry = new BtreeIndexEntryImpl(newPage.getMinKey(), newPage.getPageId());
      final RawIndexEntry rawIndexEntry = Converters.convert(indexEntry);
      if (currentIndexPageAtLevel.addEntry(rawIndexEntry)) {
        return;
      }

      final Page newIndexPageAtLevel = bufferPool.allocateIndexPage();
      final boolean ret = newIndexPageAtLevel.addEntry(rawIndexEntry);
      validateRet(ret);

      indexPages.set(level, newIndexPageAtLevel);
      updateIndexPage(level + 1, currentIndexPageAtLevel, newIndexPageAtLevel);
    }
  }

  private void validateRet(final boolean ret) {
    if (!ret) {
      throw new BtreeException("panic!");
    }
  }

  public void dump() {
    final int bytesPerRow = conf.getInt(BTREE_CONF_DUMP_BYTES_PER_ROW, BTREE_INVALID);
    LOG.info("pg sz: {}", pageSize);
    Page rootPage = getRootPage();
    LOG.info("root page:");
    rootPage.dumpPage(bytesPerRow);
    bufferPool.dumpPool(bytesPerRow);
    LOG.info("page count: {}", bufferPool.getPageCount());
  }

  public void save(final OutputStream os) throws IOException {
    final byte[] header = prepareHeader();
    os.write(header);
    bufferPool.save(os);
  }

  private byte[] prepareHeader() {
    final ByteBuffer bb = ByteBuffer.allocate(INDEX_HEADER_SIZE);
    bb.putShort(BTREE_CONF_FILE_MAGIC_VALUE);
    bb.putInt(pageSize);
    bb.putInt(bufferPool.getPageCount());
    bb.putInt(bufferPool.getBufferSize());
    bb.putInt(bufferPool.getBufferCount());

    final int rootPageId = getRootPage().getPageId();
    bb.putInt(rootPageId);

    if (converter.keyFieldTypes().isEmpty()) {
      throw new BtreeException("key field types cannot be empty");
    }
    if (converter.valueFieldTypes().isEmpty()) {
      throw new BtreeException("value field types cannot be empty");
    }
    bb.put((byte) converter.keyFieldTypes().size());
    for (final byte b : converter.keyFieldTypes()) {
      bb.put(b);
    }
    bb.put((byte) converter.valueFieldTypes().size());
    for (final byte b : converter.valueFieldTypes()) {
      bb.put(b);
    }

    return bb.array();
  }

  public void load(final InputStream is) throws IOException {
    final byte[] header = new byte[INDEX_HEADER_SIZE];
    try {
      new DataInputStream(is).readFully(header);
    } catch (final EOFException eof) {
      throw new BtreeException("header read err!");
    }
    final ByteBuffer bb = ByteBuffer.wrap(header);
    final short magic = bb.getShort();
    if (magic != BTREE_CONF_FILE_MAGIC_VALUE) {
      throw new BtreeException("no magic!");
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

    validateConverter(keyFieldTypes, valueFieldTypes);

    bufferPool.load(is, bufSize, bufCount, pgCount);

    restoreTree(rootPageId);
  }

  private void validateConverter(final List<Byte> keyFieldTypes, final List<Byte> valueFieldTypes) {
    if (!converter.keyFieldTypes().equals(keyFieldTypes)) {
      throw new BtreeException("key types mismatch!");
    }

    if (!converter.valueFieldTypes().equals(valueFieldTypes)) {
      throw new BtreeException("value types mismatch!");
    }
  }

  private void restoreTree(final int rootPageId) {
    final Page rootPage = getPage(rootPageId);
    indexPages.add(rootPage);
  }

  public Page getRootPage() {
    if (!indexPages.isEmpty()) {
      int level = indexPages.size() - 1;
      LOG.info("root page level: {}", level);
      return indexPages.get(level);
    }
    throw new BtreeException("tree incomplete!");
  }

  public Page getPage(final int pageId) {
    return bufferPool.getPage(pageId);
  }
}
