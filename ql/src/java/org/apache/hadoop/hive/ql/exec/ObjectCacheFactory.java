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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapIoProxy;
import org.apache.hadoop.hive.llap.io.api.LlapIoProxy;
import org.apache.hadoop.hive.ql.exec.tez.LlapObjectCache;

/**
 * ObjectCacheFactory returns the appropriate cache depending on settings in
 * the hive conf.
 */
public class ObjectCacheFactory {
  private static final ConcurrentHashMap<String, ObjectCache> llapQueryCaches =
      new ConcurrentHashMap<>();
  private static final Log LOG = LogFactory.getLog(ObjectCacheFactory.class);

  private ObjectCacheFactory() {
    // avoid instantiation
  }

  /**
   * Returns the appropriate cache
   */
  public static ObjectCache getCache(Configuration conf, String queryId) {
    if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      if (LlapIoProxy.isDaemon()) { // daemon
        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_OBJECT_CACHE_ENABLED)) {
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
    ObjectCache result = llapQueryCaches.get(queryId);
    if (result != null) return result;
    result = new LlapObjectCache();
    ObjectCache old = llapQueryCaches.putIfAbsent(queryId, result);
    if (old == null && LOG.isDebugEnabled()) {
      LOG.debug("Created object cache for " + queryId);
    }
    return (old != null) ? old : result;
  }

  public static void removeLlapQueryCache(String queryId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing object cache for " + queryId);
    }
    llapQueryCaches.remove(queryId);
  }
}
