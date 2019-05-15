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

package org.apache.hadoop.hive.ql.exec.tez;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.tez.runtime.api.ObjectRegistry;

import com.google.common.base.Preconditions;

/**
 * ObjectCache. Tez implementation based on the tez object registry.
 *
 */
public class ObjectCache implements org.apache.hadoop.hive.ql.exec.ObjectCache {

  private static final Logger LOG = LoggerFactory.getLogger(ObjectCache.class.getName());

  // ObjectRegistry is available via the Input/Output/ProcessorContext.
  // This is setup as part of the Tez Processor construction, so that it is available whenever an
  // instance of the ObjectCache is created. The assumption is that Tez will initialize the Processor
  // before anything else.
  private volatile static ObjectRegistry staticRegistry;

  private static ExecutorService staticPool;

  private final ObjectRegistry registry;

  public ObjectCache() {
    Preconditions.checkNotNull(staticRegistry,
        "Object registry not setup yet. This should have been setup by the TezProcessor");
    registry = staticRegistry;
  }

  public static boolean isObjectRegistryConfigured() {
    return (staticRegistry != null);
  }


  public static void setupObjectRegistry(ObjectRegistry objectRegistry) {
    staticRegistry = objectRegistry;
    staticPool = Executors.newCachedThreadPool();
  }

  @Override
  public void release(String key) {
    // nothing to do
    LOG.info("Releasing key: " + key);
  }


  @SuppressWarnings("unchecked")
  @Override
  public <T> T retrieve(String key) throws HiveException {
    T value = null;
    try {
      value = (T) registry.get(key);
      if ( value != null) {
        LOG.info("Found " + key + " in cache with value: " + value);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T retrieve(String key, Callable<T> fn) throws HiveException {
    T value;
    try {
      value = (T) registry.get(key);
      if (value == null) {
        value = fn.call();
        LOG.info("Caching key: " + key);
        registry.cacheForVertex(key, value);
      } else {
        LOG.info("Found " + key + " in cache with value: " + value);
      }
    } catch (Exception e) {
      throw new HiveException(e);
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
    LOG.info("Removing key: " + key);
    registry.delete(key);
  }
}
