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

package org.apache.hadoop.hive.ql.exec.tez;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;

/**
 * Access to the Object cache from Tez, along with utility methods for accessing specific Keys.
 */
public class TezCacheAccess {

  private TezCacheAccess(ObjectCache cache) {
    this.cache = cache;
  }

  private ObjectCache cache;

  public static TezCacheAccess createInstance(Configuration conf) {
    ObjectCache cache = ObjectCacheFactory.getCache(conf);
    return new TezCacheAccess(cache);
  }

  private static final String CACHED_INPUT_KEY = "CACHED_INPUTS";
  
  private final ReentrantLock cachedInputLock = new ReentrantLock();

  public boolean isInputCached(String inputName) {
    this.cachedInputLock.lock();
    try {
      @SuppressWarnings("unchecked")
      Set<String> cachedInputs = (Set<String>) cache.retrieve(CACHED_INPUT_KEY);
      if (cachedInputs == null) {
        return false;
      } else {
        return cachedInputs.contains(inputName);
      }
    } finally {
      this.cachedInputLock.unlock();
    }
  }

  public void registerCachedInput(String inputName) {
    this.cachedInputLock.lock();
    try {
      @SuppressWarnings("unchecked")
      Set<String> cachedInputs = (Set<String>) cache.retrieve(CACHED_INPUT_KEY);
      if (cachedInputs == null) {
        cachedInputs = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        cache.cache(CACHED_INPUT_KEY, cachedInputs);
      }
      cachedInputs.add(inputName);
    } finally {
      this.cachedInputLock.unlock();
    }
  }

}
