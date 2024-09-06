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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.metastore.api.CompactionType;

class CompactionQueryBuilderFactory {
  public CompactionQueryBuilder getCompactionQueryBuilder(CompactionType compactionType, boolean insertOnly) {

    if (compactionType == null) {
      throw new IllegalArgumentException("CompactionQueryBuilder.CompactionType cannot be null");
    }
    if (insertOnly) {
      return new CompactionQueryBuilderForInsertOnly(compactionType);
    } else {
      switch (compactionType) {
      case MAJOR:
        return new CompactionQueryBuilderForMajor();
      case MINOR:
        return new CompactionQueryBuilderForMinor();
      case REBALANCE:
        return new CompactionQueryBuilderForRebalance();
      }
    }
    throw new IllegalArgumentException(
        String.format("Compaction cannot be created with type %s", compactionType));
  }

}
