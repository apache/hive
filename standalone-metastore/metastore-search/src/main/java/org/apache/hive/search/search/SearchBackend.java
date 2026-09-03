/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.search;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hive.search.exception.IndexNotHealthyException;
import org.apache.hive.search.exception.InitializeException;
import org.apache.hive.search.exception.SearchException;

/**
 * Pluggable search storage and query engine. Phase 1 uses embedded Lucene; Phase 3 may add
 * OpenSearch or Elasticsearch without changing the mapping contract or ingest path.
 */
public interface SearchBackend extends Closeable {

  void initialize(Configuration configuration) throws InitializeException, IOException;

  /** Whether this instance can serve search (index opened and bootstrap completed). */
  boolean isReady() throws IndexNotHealthyException;

  TableSearchResult search(SearchQuery query)
      throws SearchException, IOException;

  /** Loads stored tables by name */
  TableSearchResult loadTables(List<TableName> names)
      throws SearchException, IOException;

}
