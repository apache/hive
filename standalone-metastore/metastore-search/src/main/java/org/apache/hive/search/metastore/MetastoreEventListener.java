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

package org.apache.hive.search.metastore;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.common.DatabaseName;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.search.exception.IndexNotHealthyException;

public interface MetastoreEventListener {

  /**
   * Apply a coalesced batch of index mutations. Implementations must either complete all
   * mutations or throw; partial success must remain safe to retry idempotently.
   */
  default void notifyIndexTask(IndexTask task) throws java.io.IOException {

  }

  default void notifyIndexState(boolean healthy, IndexNotHealthyException... e) {

  }

  class IndexTask {
    public long firstEventId;
    public long lastEventId;
    public Set<TableName> tablesToDrop = new LinkedHashSet<>();
    public Set<DatabaseName> databasesToDrop = new LinkedHashSet<>();
    public Map<TableName, Table> tablesToAdd = new LinkedHashMap<>();
  }
}
