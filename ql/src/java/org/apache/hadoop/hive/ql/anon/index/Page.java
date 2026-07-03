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

import org.apache.hadoop.hive.ql.anon.ex.BtreeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public final class Page {
  private static final Logger LOG = LoggerFactory.getLogger(Page.class);

  private final int pageSize;
  private final ByteBuffer bb;
  private final int startPosition;
  private final int limit;
  private final int headerSize;

  private final int maxPayloadSize;

  private static final int ENTRY_COUNT_OFFSET = 4;
  private static final int PAGE_TYPE_SIZE = 1;
  private static final int PAGE_ID_SIZE = 4;
  private static final int ENTRY_COUNT_SIZE = 4;

  private int entryCount = 0;
  private final int pageId;
  private byte[] minKey;

  private final String keyType;
  private final String addrType;
  private final String valueTypes;

  public Page(final ByteBuffer bb, final int pageSize, final int pageId, final String keyType, final String addrType, final String valueTypes) {
    this.bb = bb;
    this.pageSize = pageSize;
    this.pageId = pageId;

    this.startPosition = bb.position();
    this.limit = bb.limit();

    headerSize = PAGE_TYPE_SIZE + PAGE_ID_SIZE + ENTRY_COUNT_SIZE;
    this.maxPayloadSize = this.pageSize - (headerSize);
    this.bb.position(this.startPosition + headerSize);

    this.keyType = keyType;
    this.addrType = addrType;
    this.valueTypes = valueTypes;
  }

  private int getFreeSpace() {
    int freeSpace = limit - bb.position();
    if (freeSpace < 0) {
      throw new BtreeException("space");
    }
    return freeSpace;
  }

  public boolean addEntry(final RawEntry entry) {
    final int entrySize = entry.getSize();
    if (entrySize > maxPayloadSize) {
      throw new BtreeException("entry size overflow! delta: " + (maxPayloadSize - entrySize));
    }
    final int dataPageFreeSpace = getFreeSpace();
    if (entrySize > dataPageFreeSpace) {
      return false;
    }

    bb.put(entry.rawKey);
    bb.put(entry.rawValue);
    entryCount++;
    if (entryCount == 1) {
      minKey = entry.rawKey;
    }

    writeEntryCount();
    return true;
  }

  private void writeEntryCount() {
    final int oldPos = bb.position();
    bb.position(this.startPosition + PAGE_TYPE_SIZE + ENTRY_COUNT_OFFSET);
    bb.putInt(entryCount);
    bb.position(oldPos);
  }

  public int getPageId() {
    return pageId;
  }

  public byte[] getMinKey() {
    if (minKey == null) {
      throw new BtreeException("no min key!");
    }
    return minKey;
  }

  public int getHeaderSize() {
    return headerSize;
  }

  public void writeTest(final int pos, final byte[] bytes) {
    bb.position(pos);
    bb.put(bytes);
  }

  public void writeTest2(final int offset, int numBytes, byte b) {
    bb.position(startPosition + offset);
    byte[] bytes = new byte[numBytes];
    Arrays.fill(bytes, b);
    bb.put(bytes);
  }

  public void dumpPage(final int bytesPerRow) {
    int oldPos = bb.position();
    bb.position(startPosition + PAGE_TYPE_SIZE);
    final int readPageId = bb.getInt();
    bb.position(oldPos);

    LOG.info("pageId: {}", String.format("%08X", readPageId));
    LOG.info("start pos: {}, limit: {}", this.startPosition, this.limit);
    LOG.info("bb pos: {}", this.bb.position());

    Utils.dump(copyRawBytes(), bytesPerRow);
  }

  public byte[] copyRawBytes() {
    final byte[] pageBytes = new byte[pageSize];
    System.arraycopy(bb.array(), startPosition, pageBytes, 0, pageSize);
    return pageBytes;
  }

  public ByteBuffer copyByteBuffer() {
    return ByteBuffer.wrap(copyRawBytes());
  }

  public PageInfo getInfo() throws IOException {
    return new PageInfo(copyByteBuffer(), headerSize, keyType, addrType, valueTypes);
  }

}
