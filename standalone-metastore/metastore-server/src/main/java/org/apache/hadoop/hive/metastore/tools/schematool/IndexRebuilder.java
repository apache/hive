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
package org.apache.hadoop.hive.metastore.tools.schematool;

import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaException;

/**
 * Interface for rebuilding indexes in the HMS backend database.
 * Implementations should extend {@link AbstractIndexRebuilder}.
 */
public interface IndexRebuilder {

  /** Returns all B-tree indexes in the current HMS schema. */
  List<IndexInfo> loadIndexes() throws HiveMetaException;

  /**
   * Returns duplicate key-group count for the index. Rebuild must be blocked when this is
   * greater than zero. Always returns zero for non-unique indexes.
   */
  long findDuplicates(IndexInfo index) throws HiveMetaException;

  void rebuildIndex(IndexInfo index) throws HiveMetaException;

  /** Returns a description of the DDL that would be executed to rebuild the index. */
  String describeRebuildDDL(IndexInfo index);
}
