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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * LlapObjectCache. Llap implementation for the shared object cache.
 *
 */
public class LlapObjectCache implements org.apache.hadoop.hive.ql.exec.ObjectCache {

  private static final Logger LOG = LoggerFactory.getLogger(LlapObjectCache.class.getName());

  private static ExecutorService staticPool = Executors.newCachedThreadPool();

  private static final boolean isLogDebugEnabled = LOG.isDebugEnabled();

  private final Cache<String, Object> registry = CacheBuilder.newBuilder().softValues().build();

  private final Map<String, ReentrantLock> locks = new HashMap<String, ReentrantLock>();

  private final ReentrantLock lock = new ReentrantLock();


  @Override
  public void release(String key) {
    // nothing to do, soft references will clean themselves up
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T retrieve(String key) throws HiveException {

    T value = null;

    lock.lock();
    try {
      value = (T) registry.getIfPresent(key);
      if (value != null && isLogDebugEnabled) {
        LOG.debug("Found " + key + " in cache");
      }
      return value;
    } finally {
      lock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T retrieve(String key, Callable<T> fn) throws HiveException {

    T value = null;
    ReentrantLock objectLock = null;

    lock.lock();
    try {
      value = (T) registry.getIfPresent(key);
      if (value != null) {
        if (isLogDebugEnabled) {
          LOG.debug("Found " + key + " in cache");
        }
        return value;
      }

      if (locks.containsKey(key)) {
        objectLock = locks.get(key);
      } else {
        objectLock = new ReentrantLock();
        locks.put(key, objectLock);
      }
    } finally {
      lock.unlock();
    }

    objectLock.lock();
    try{
      lock.lock();
      try {
        value = (T) registry.getIfPresent(key);
        if (value != null) {
          if (isLogDebugEnabled) {
            LOG.debug("Found " + key + " in cache");
          }
          return value;
        }
      } finally {
        lock.unlock();
      }

      try {
        value = fn.call();
      } catch (Exception e) {
        throw new HiveException(e);
      }

      lock.lock();
      try {
        if (isLogDebugEnabled) {
          LOG.debug("Caching new object for key: " + key);
        }

        registry.put(key, value);
        locks.remove(key);
      } finally {
        lock.unlock();
      }
    } finally {
      objectLock.unlock();
    }
    return value;
  }

  @Override
  public <T> Future<T> retrieveAsync(final String key, final Callable<T> fn) throws HiveException {
    return staticPool.submit(new Callable<T>() {
      @Override
      public T call() throws Exception {
        return retrieve(key, fn);
      }
    });
  }

  @Override
  public void remove(String key) {
    if (isLogDebugEnabled) {
      LOG.debug("Removing key: " + key);
    }
    registry.invalidate(key);
  }
}
