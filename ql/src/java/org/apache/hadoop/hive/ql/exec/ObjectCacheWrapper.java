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
package org.apache.hadoop.hive.ql.exec;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public class ObjectCacheWrapper implements ObjectCache {
  private final String queryId;
  private final ObjectCache globalCache;
  public ObjectCacheWrapper(ObjectCache globalCache, String queryId) {
    this.queryId = queryId;
    this.globalCache = globalCache;
  }

  @Override
  public void release(String key) {
    globalCache.release(makeKey(key));
  }

  @Override
  public <T> T retrieve(String key) throws HiveException {
    return globalCache.retrieve(makeKey(key));
  }

  @Override
  public <T> T retrieve(String key, Callable<T> fn) throws HiveException {
    return globalCache.retrieve(makeKey(key), fn);
  }

  @Override
  public <T> Future<T> retrieveAsync(String key, Callable<T> fn)
      throws HiveException {
    return globalCache.retrieveAsync(makeKey(key), fn);
  }

  @Override
  public void remove(String key) {
    globalCache.remove(makeKey(key));
  }

  private String makeKey(String key) {
    return queryId + "_" + key;
  }
}
