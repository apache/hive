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
package org.apache.hive.service.cli.operation;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hive.service.cli.OperationHandle;

/**
 * Cache some SQLOperation information for WebUI
 */
public class QueryInfoCache {

  private static final Logger LOG = LoggerFactory.getLogger(QueryInfoCache.class);

  //Following fields for displaying queries on WebUI
  private Object webuiLock = new Object();

  private HistoricalQueryInfos historicalQueryInfos;

  private Map<String, QueryInfo> liveQueryInfos = new LinkedHashMap<>();

  QueryInfoCache(HiveConf hiveConf) {
    if (hiveConf.isWebUiQueryInfoCacheEnabled()) {
      historicalQueryInfos = new HistoricalQueryInfos(
          hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_MAX_HISTORIC_QUERIES));
    }
  }

  /**
   * Add the live operation's query info into the cache
   * @param operation the live operation
   */
  public void addLiveQueryInfo(Operation operation) {
    if (operation instanceof SQLOperation) {
      synchronized (webuiLock) {
        liveQueryInfos.put(operation.getHandle().getHandleIdentifier().toString(),
            ((SQLOperation) operation).getQueryInfo());
      }
    }
  }

  /**
   * @return displays representing live SQLOperations
   */
  public List<QueryInfo> getLiveQueryInfos() {
    List<QueryInfo> result = new LinkedList<>();
    synchronized (webuiLock) {
      result.addAll(liveQueryInfos.values());
    }
    return result;
  }

  /**
   * Remove the live operation's query info from the {@link #liveQueryInfos},
   * and push the query info to the historic query cache if enabled.
   * @param operation the to remove operation
   */
  public void removeLiveQueryInfo(Operation operation) {
    if (operation instanceof SQLOperation) {
      OperationHandle operationHandle = operation.getHandle();
      synchronized (webuiLock) {
        String opKey = operationHandle.getHandleIdentifier().toString();
        // remove from list of live operations
        QueryInfo display = liveQueryInfos.remove(opKey);
        if (display == null) {
          LOG.debug("Unexpected display object value of null for operation {}",
              opKey);
        } else if (historicalQueryInfos != null) {
          // add to list of saved historic operations
          historicalQueryInfos.put(opKey, display);
        }
      }
    }
  }

  /**
   * @param handle handle of SQLOperation.
   * @return display representing a particular SQLOperation.
   */
  public QueryInfo getQueryInfo(String handle) {
    synchronized (webuiLock) {
      QueryInfo result = liveQueryInfos.get(handle);
      if (result != null) {
        return result;
      }
      if (historicalQueryInfos == null) {
        return null;
      }
      return historicalQueryInfos.get(handle);
    }
  }

  /**
   * @return displays representing a number of historical SQLOperations, at max number of
   * hive.server2.webui.max.historic.queries. Newest items will be first.
   */
  public List<QueryInfo> getHistoricalQueryInfos() {
    List<QueryInfo> result = new LinkedList<>();
    synchronized (webuiLock) {
      if (historicalQueryInfos != null) {
        result.addAll(historicalQueryInfos.values());
        Collections.reverse(result);
      }
    }
    return result;
  }

  public Set<String> getAllQueryIds() {
    List<QueryInfo> queryInfos = new LinkedList<>();
    synchronized (webuiLock) {
      queryInfos.addAll(liveQueryInfos.values());
      if (historicalQueryInfos != null) {
        queryInfos.addAll(historicalQueryInfos.values());
      }
    }

    Set<String> results = new HashSet<>();
    for (QueryInfo queryInfo : queryInfos) {
      results.add(queryInfo.getQueryDisplay().getQueryId());
    }
    return results;
  }

  private static class HistoricalQueryInfos extends LinkedHashMap<String, QueryInfo> {
    private final int capacity;

    public HistoricalQueryInfos(int capacity) {
      super(capacity + 1, 1.1f, false);
      this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
      return size() > capacity;
    }

  }

}
