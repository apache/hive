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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.cache;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * Warms up a metadata cache from the backing database before the cache starts serving.
 * Implementations own the resources they need (worker RawStores, thread pools) and release them
 * in {@link #close()}, which is expected to be called before the cache reports itself warm.
 */
public interface MetaCachePreWarm extends AutoCloseable, Configurable {

  /**
   * Creates the resources needed to prewarm the cache, e.g. worker stores and thread pools.
   */
  void initialize() throws MetaException;

  /**
   * Populates the cache from the backing database.
   * @return true if all metadata was cached; false if prewarm stopped early (the cache memory
   *         limit was reached or the thread was interrupted) with only part of the metadata cached
   */
  boolean preWarm() throws MetaException;

  /**
   * Moves the given tables, when they are still pending prewarm, to the front of the prewarm
   * queue, so that a table a client is asking for right now becomes available in the cache as
   * soon as possible.
   */
  void prioritizeTableForPrewarm(TableName... tableNames);

  /**
   * Releases the prewarm resources, waiting for any still running workers to terminate so that
   * nothing keeps mutating the cache after prewarm reports completion.
   */
  @Override void close();
}
