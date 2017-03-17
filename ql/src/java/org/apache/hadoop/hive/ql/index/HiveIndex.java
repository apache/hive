/**
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
package org.apache.hadoop.hive.ql.index;

import org.apache.hadoop.hive.ql.index.bitmap.BitmapIndexHandler;
import org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds index related constants
 */
public class HiveIndex {
  public static final Logger l4j = LoggerFactory.getLogger("HiveIndex");
  public static final String INDEX_TABLE_CREATETIME = "hive.index.basetbl.dfs.lastModifiedTime";

  public static enum IndexType {
    AGGREGATE_TABLE("aggregate",  AggregateIndexHandler.class.getName()),
    COMPACT_SUMMARY_TABLE("compact", CompactIndexHandler.class.getName()),
    BITMAP_TABLE("bitmap", BitmapIndexHandler.class.getName());

    private IndexType(String indexType, String className) {
      indexTypeName = indexType;
      this.handlerClsName = className;
    }

    private final String indexTypeName;
    private final String handlerClsName;

    public String getName() {
      return indexTypeName;
    }

    public String getHandlerClsName() {
      return handlerClsName;
    }
  }

  public static IndexType getIndexType(String name) {
    IndexType[] types = IndexType.values();
    for (IndexType type : types) {
      if(type.getName().equals(name.toLowerCase())) {
        return type;
      }
    }
    return null;
  }

  public static IndexType getIndexTypeByClassName(String className) {
    IndexType[] types = IndexType.values();
    for (IndexType type : types) {
      if(type.getHandlerClsName().equals(className)) {
        return type;
      }
    }
    return null;
  }

}

