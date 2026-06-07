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

import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.BTREE_PAGE_MARK_DATA;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.BTREE_PAGE_MARK_INDEX;

public enum PageType {
  DATA(BTREE_PAGE_MARK_DATA),
  INDEX(BTREE_PAGE_MARK_INDEX);

  private final byte type;

  PageType(int pm) {
    type = (byte) pm;
  }

  public byte getType() {
    return type;
  }

  public static PageType parse(byte b) {
    switch (b) {
      case BTREE_PAGE_MARK_DATA:
        return PageType.DATA;
      case BTREE_PAGE_MARK_INDEX:
        return PageType.INDEX;
      default:
        throw new BtreeException("unknown page type!");
    }
  }
}
