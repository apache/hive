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

package org.apache.hadoop.hive.ql.exec.mr;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * ObjectCache. Simple implementation on MR we don't have a means to reuse
 * Objects between runs of the same task, this acts as a local cache.
 *
 */
public class ObjectCache implements org.apache.hadoop.hive.ql.exec.ObjectCache {

  private static final Logger LOG = LoggerFactory.getLogger(ObjectCache.class.getName());

  private final Map<String, Object> cache = new ConcurrentHashMap<>();

  @Override
  public void release(String key) {
    LOG.debug("{} no longer needed", key);
    cache.remove(key);
  }

  @Override
  public <T> T retrieve(String key) throws HiveException {
    return (T) cache.get(key);
  }

  @Override
  public <T> T retrieve(String key, Callable<T> fn) throws HiveException {
    T value = (T) cache.get(key);
    if (value != null || fn == null) {
      return value;
    }
    try {
      LOG.debug("Creating {}", key);
      value = fn.call();
    } catch (Exception e) {
      throw new HiveException(e);
    }
    T previous = (T) cache.putIfAbsent(key, value);
    return previous != null ? previous : value;
  }

  @Override
  public <T> Future<T> retrieveAsync(String key, Callable<T> fn) throws HiveException {
    final T value = retrieve(key, fn);

    return new Future<T>() {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return true;
      }

      @Override
      public T get() throws InterruptedException, ExecutionException {
        return value;
      }

      @Override
      public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
          TimeoutException {
        return value;
      }
    };
  }

  @Override
  public void remove(String key) {
    cache.remove(key);
  }
}
