/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;

public class SmallTableCache {
  private static final Logger LOG = LoggerFactory.getLogger(SmallTableCache.class.getName());

  private static final ConcurrentHashMap<Path, MapJoinTableContainer>
    tableContainerMap = new ConcurrentHashMap<Path, MapJoinTableContainer>();
  private static volatile String queryId;

  /**
   * Check if this is a new query. If so, clean up the cache
   * that is for the previous query, and reset the current query id.
   */
  public static void initialize(Configuration conf) {
    String currentQueryId = conf.get(HiveConf.ConfVars.HIVEQUERYID.varname);
    if (!currentQueryId.equals(queryId)) {
      if (!tableContainerMap.isEmpty()) {
        synchronized (tableContainerMap) {
          if (!currentQueryId.equals(queryId) && !tableContainerMap.isEmpty()) {
            for (MapJoinTableContainer tableContainer: tableContainerMap.values()) {
              tableContainer.clear();
            }
            tableContainerMap.clear();
            if (LOG.isDebugEnabled()) {
              LOG.debug("Cleaned up small table cache for query " + queryId);
            }
          }
        }
      }
      queryId = currentQueryId;
    }
  }

  public static void cache(Path path, MapJoinTableContainer tableContainer) {
    if (tableContainerMap.putIfAbsent(path, tableContainer) == null && LOG.isDebugEnabled()) {
      LOG.debug("Cached small table file " + path + " for query " + queryId);
    }
  }

  public static MapJoinTableContainer get(Path path) {
    MapJoinTableContainer tableContainer = tableContainerMap.get(path);
    if (tableContainer != null && LOG.isDebugEnabled()) {
      LOG.debug("Loaded small table file " + path + " from cache for query " + queryId);
    }
    return tableContainer;
  }
}
