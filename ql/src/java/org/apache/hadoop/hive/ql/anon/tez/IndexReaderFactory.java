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

package org.apache.hadoop.hive.ql.anon.tez;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.IndexType;
import org.apache.hadoop.hive.ql.anon.index.BtreeIndexReader;
import org.apache.hadoop.hive.ql.anon.index.IndexReader;
import org.apache.hadoop.hive.ql.anon.index.dir.DirectoryIndexReader;
import org.apache.hadoop.hive.ql.anon.index.tab.TabularIndexReader;

import java.io.IOException;

public final class IndexReaderFactory {

  public static IndexReader getIndexReader(final IndexType indexType, final Configuration conf, final String indexPath) throws IOException {
    switch (indexType) {
      case BTREE:
        return new BtreeIndexReader(conf, indexPath);
      case DIRECTORY:
        return new DirectoryIndexReader(conf, indexPath);
      case TABULAR:
        return new TabularIndexReader(conf, indexPath);
      default:
        throw new RuntimeException("Unknown index type: " + indexType);
    }
  }
}
