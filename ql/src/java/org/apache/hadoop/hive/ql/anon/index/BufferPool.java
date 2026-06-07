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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hive.ql.anon.ex.BtreeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;
public class BufferPool {

  private static final Logger LOG = LoggerFactory.getLogger(BufferPool.class);

  private final List<byte[]> buffers = new ArrayList<>();
  private final int pageSize;
  private final int bufferSize;
  private final int maxPagesPerBuffer;
  private String keyType;
  private final String addrType;
  private String valueTypes;

  private int pageCount = 0;

  private FSDataInputStream pagedIn;
  private long pagedDataOffset;
  private LinkedHashMap<Integer, byte[]> pageCache;
  private long pagesFetched;
  private long bytesFetched;

  public BufferPool(final Configuration conf) {

    this.pageSize = conf.getInt(BTREE_CONF_PAGE_SIZE, BTREE_INVALID);
    this.bufferSize = conf.getInt(BTREE_CONF_BUFFER_SIZE, BTREE_INVALID);

    validateSizes();

    maxPagesPerBuffer = bufferSize / pageSize;
    buffers.add(new byte[bufferSize]);

    keyType = conf.get(INDEX_KEY_TYPE);
    addrType = conf.get(INDEX_ADDR_TYPE);
    valueTypes = conf.get(INDEX_VALUE_TYPES);

    if (addrType == null || addrType.isEmpty()) {
      throw new BtreeException("addrType is empty");
    }
  }

  public void overrideTypes(final String keyType, final String valueTypes) {
    this.keyType = keyType;
    this.valueTypes = valueTypes;
  }

  public Page getPage(final int pageId) {
    final ByteBuffer bb = getPageRawBuffer(pageId);
    return new Page(bb, pageSize, pageId, keyType, addrType, valueTypes);
  }

  public byte[] getPageBytes(final int pageId) {
    if (pagedIn != null) {
      return fetchPage(pageId).clone();
    }
    final int bufferIx = pageId / maxPagesPerBuffer;
    final byte[] buffer = buffers.get(bufferIx);
    final int startIx = pageId % maxPagesPerBuffer;
    final int from = startIx * pageSize;
    final int to = (startIx + 1) * pageSize;
    return Arrays.copyOfRange(buffer, from, to);
  }

  private Page allocatePage(PageType type) {
    final int pageId = pageCount;
    final ByteBuffer bb = getPageRawBuffer(pageId);

    final int pos = bb.position();
    LOG.debug("allocation position: {}", pos);
    bb.put(type.getType());
    bb.putInt(pageId);
    bb.position(pos);
    pageCount++;
    return new Page(bb, pageSize, pageId, keyType, addrType, valueTypes);
  }

  public Page allocateDataPage() {
    return allocatePage(PageType.DATA);
  }

  public Page allocateIndexPage() {
    return allocatePage(PageType.INDEX);
  }

  private ByteBuffer getPageRawBuffer(final int pageId) {
    if (pagedIn != null) {
      return ByteBuffer.wrap(fetchPage(pageId));
    }
    LOG.debug("raw page: {}", pageId);
    final int bufferIx = pageId / maxPagesPerBuffer;
    if (bufferIx >= buffers.size()) {
      LOG.debug("increasing pool size...");
      buffers.add(new byte[bufferSize]);
    }
    final byte[] buffer = buffers.get(bufferIx);
    final int startIx = pageId % maxPagesPerBuffer;
    final int startPosition = startIx * pageSize;
    return ByteBuffer.wrap(buffer, startPosition, pageSize);
  }

  public void dumpPage(final int pageId, final int bytesPerRow) {
    LOG.info("pageId: {}", pageId);
    final byte[] pageBytes = getPageBytes(pageId);
    Utils.dump(pageBytes, bytesPerRow);
  }

  public void dumpPool(final int bytesPerRow) {
    LOG.info("pool:");
    for (byte[] buffer : buffers) {
      LOG.info("buffer:");
      Utils.dump(buffer, bytesPerRow);
    }
  }

  private void validateSizes() {
    if (bufferSize % pageSize != 0) {
      throw new BtreeException("buffer size must be a multiple of " + pageSize);
    }

    if (pageSize == BTREE_INVALID) {
      throw new BtreeException("invalid page size size: " + pageSize);
    }

    if (bufferSize == 0 || bufferSize == BTREE_INVALID) {
      throw new BtreeException("invalid buffer size:! " + bufferSize);
    }
  }

  public int getPageCount() {
    return pageCount;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public int getBufferCount() {
    return buffers.size();
  }

  public void save(final OutputStream os) throws IOException {
    for (final byte[] buffer : buffers) {
      os.write(buffer);
    }
  }

  public void load(final InputStream is, final int bufSize, final int bufCount, final int pgCount) throws IOException {
    if (bufSize != bufferSize) {
      throw new BtreeException("buffer size mismatch: " + bufSize + " != " + bufferSize);
    }

    buffers.clear();
    pageCount = pgCount;

    LOG.info("buffer count: {}", bufCount);
    LOG.info("page count: {}", pgCount);

    for (int i = 0; i < bufCount; i++) {
      final byte[] buf = new byte[bufSize];

      int totalRead = 0;
      while (totalRead < bufSize) {
        final int read = is.read(buf, totalRead, bufSize - totalRead);
        if (read < 0) {
          throw new EOFException(
              "truncated index buffer: read " + totalRead + " of " + bufSize + " bytes");
        }
        totalRead += read;
      }

      buffers.add(buf);
    }
  }

  public void enablePagedMode(final FSDataInputStream in, final long dataOffset,
                              final int pgCount, final int cachePages) {
    this.pagedIn = in;
    this.pagedDataOffset = dataOffset;
    this.pageCount = pgCount;
    final int cap = Math.max(1, cachePages);
    this.pageCache = new LinkedHashMap<Integer, byte[]>(cap * 2, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(final Map.Entry<Integer, byte[]> eldest) {
        return size() > cap;
      }
    };
    LOG.info("paged read mode: dataOffset={}, pages={}, cache={} pages", dataOffset, pgCount, cap);
  }

  public boolean isPaged() {
    return pagedIn != null;
  }

  private byte[] fetchPage(final int pageId) {
    final byte[] cached = pageCache.get(pageId);
    if (cached != null) {
      return cached;
    }
    final byte[] page = new byte[pageSize];
    final long pos = pagedDataOffset + (long) pageId * pageSize;
    try {
      pagedIn.readFully(pos, page, 0, pageSize);
    } catch (final IOException e) {
      throw new BtreeException("paged read failed for page " + pageId + " at offset " + pos + ": " + e.getMessage());
    }
    pagesFetched++;
    bytesFetched += pageSize;
    pageCache.put(pageId, page);
    return page;
  }

  public long getPagesFetched() {
    return pagesFetched;
  }

  public long getBytesFetched() {
    return bytesFetched;
  }

  public void closePaged() throws IOException {
    if (pagedIn != null) {
      pagedIn.close();
      pagedIn = null;
    }
  }
}
