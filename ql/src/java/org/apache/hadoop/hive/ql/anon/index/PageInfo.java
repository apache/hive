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

import org.apache.hadoop.hive.ql.anon.btree.KeyValueStruct;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PageInfo {

  private static final Logger LOG = LoggerFactory.getLogger(PageInfo.class);

  private final ByteBuffer bb;
  private final PageType type;
  private final int pageId;
  private final int entryCount;
  private final ByteArrayInputStream bais;
  private final List<Writable> entries = new ArrayList<>();

  public PageInfo(final ByteBuffer buffer, final int headerSize, final String keyType, final String addrType, final String valueTypes) throws IOException {
    bb = buffer;

    type = PageType.parse(bb.get());
    LOG.debug("type: {}", type);

    pageId = bb.getInt();
    LOG.debug("pg id: {}", pageId);

    entryCount = bb.getInt();
    LOG.debug("entries: {}", entryCount);

    LOG.debug("hdr sz: {}", headerSize);
    bb.position(headerSize);
    int pos = bb.position();

    byte[] ba = new byte[bb.array().length - pos];
    System.arraycopy(bb.array(), pos, ba, 0, ba.length);
    bais = new ByteArrayInputStream(ba);
    DataInputStream dis = new DataInputStream(bais);

    for (int i = 0; i < entryCount; i++) {
      if (type == PageType.DATA) {
        KeyValueStruct struct = new KeyValueStruct(keyType, valueTypes);
        struct.readFields(dis);
        entries.add(struct);
      } else if (type == PageType.INDEX) {
        IndexStruct struct = new IndexStruct(keyType, addrType);
        struct.readFields(dis);
        entries.add(struct);
      } else {
        throw new RuntimeException("Unrecognized PageType: " + type);
      }
    }

  }

  public int getPageId() {
    return pageId;
  }

  public PageType getType() {
    return type;
  }

  public int getEntryCount() {
    return entryCount;
  }

  public List<Writable> getEntries() {
    return entries;
  }
}
