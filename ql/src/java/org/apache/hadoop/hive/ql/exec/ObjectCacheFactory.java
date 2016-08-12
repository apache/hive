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

package org.apache.hadoop.hive.ql.exec;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.exec.tez.LlapObjectCache;

/**
 * ObjectCacheFactory returns the appropriate cache depending on settings in
 * the hive conf.
 */
public class ObjectCacheFactory {
  private static final ConcurrentHashMap<String, ObjectCache> llapQueryCaches =
      new ConcurrentHashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(ObjectCacheFactory.class);

  private ObjectCacheFactory() {
    // avoid instantiation
  }

  /**
   * Returns the appropriate cache
   */
  public static ObjectCache getCache(Configuration conf, String queryId, boolean isPlanCache) {
    if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      if (LlapProxy.isDaemon()) { // daemon
        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_OBJECT_CACHE_ENABLED)
            && !isPlanCache) {
          // LLAP object cache, unlike others, does not use globals. Thus, get the existing one.
          return getLlapObjectCache(queryId);
        } else { // no cache
          return new ObjectCacheWrapper(
              new org.apache.hadoop.hive.ql.exec.mr.ObjectCache(), queryId);
        }
      } else { // container
        return new ObjectCacheWrapper(
            new org.apache.hadoop.hive.ql.exec.tez.ObjectCache(), queryId);
      }
    } else { // mr or spark
      return new ObjectCacheWrapper(
          new  org.apache.hadoop.hive.ql.exec.mr.ObjectCache(), queryId);
    }
  }

  private static ObjectCache getLlapObjectCache(String queryId) {
    // If order of events (i.e. dagstart and fragmentstart) was guaranteed, we could just
    // create the cache when dag starts, and blindly return it to execution here.
    if (queryId == null) throw new RuntimeException("Query ID cannot be null");
    ObjectCache result = llapQueryCaches.get(queryId);
    if (result != null) return result;
    result = new LlapObjectCache();
    ObjectCache old = llapQueryCaches.putIfAbsent(queryId, result);
    if (old == null && LOG.isInfoEnabled()) {
      LOG.info("Created object cache for " + queryId);
    }
    return (old != null) ? old : result;
  }

  public static void removeLlapQueryCache(String queryId) {
    if (LOG.isInfoEnabled()) {
      LOG.info("Removing object cache for " + queryId);
    }
    llapQueryCaches.remove(queryId);
  }
}
