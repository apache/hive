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

package org.apache.hadoop.hive.ql.anon.consts;

public final class BtreeConst {

  private BtreeConst() {
  }

  public static final short BTREE_CONF_FILE_MAGIC_VALUE = 0x4254;
  public static final short DIRECTORY_CONF_FILE_MAGIC_VALUE = 0x4452;
  public static final short TABULAR_CONF_FILE_MAGIC_VALUE = 0x5442;

  public static final short BTREE_INVALID = -1;

  public static final String BTREE_CONF_PAGE_SIZE = "btree.page.size";
  public static final String BTREE_CONF_PAGE_HEADER_SIZE = "btree.page.header.size";
  public static final String BTREE_CONF_FILE_HEADER_SIZE = "btree.file.header.size";
  public static final String BTREE_CONF_BUFFER_SIZE = "btree.buffer.size";
  public static final String BTREE_CONF_BUFFER_NUM = "btree.buffer.number";

  public static final String BTREE_CONF_PAGED_READ = "btree.paged.read";
  public static final String BTREE_CONF_PAGE_CACHE_PAGES = "btree.page.cache.pages";

  public static final String DIRECTORY_CONF_PAGED_READ = "directory.paged.read";

  public static final String BTREE_CONF_DUMP_BYTES_PER_ROW = "btree.dump.bytes.per.row";

  public static final byte BTREE_PAGES_SECTION_START = 'P';
  public static final byte BTREE_BYTE_TYPE = 'B';
  public static final byte BTREE_SHORT_TYPE = 'S';
  public static final byte BTREE_INT_TYPE = 'I';
  public static final byte BTREE_LONG_TYPE = 'L';
  public static final byte BTREE_TEXT_TYPE = 'T';
  public static final byte BTREE_BINARY_TYPE = 'Y';

  public static final int INDEX_HEADER_SIZE = 64;

  public static final byte BTREE_PAGE_MARK_DATA = 'D';
  public static final byte BTREE_PAGE_MARK_INDEX = 'I';

  public static final String INDEX_KEY_TYPE = "index.key.type";
  public static final String INDEX_ADDR_TYPE = "index.addr.type";
  public static final String INDEX_VALUE_TYPES = "index.value.types";

  public static short genMagic(String str) {
    if (str == null || str.length() != 2) {
      throw new IllegalArgumentException("invalid string");
    }

    return (short) (str.charAt(0) << 8 | str.charAt(1));
  }
}
