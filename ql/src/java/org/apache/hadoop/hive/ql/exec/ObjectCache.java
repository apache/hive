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

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * ObjectCache. Interface for maintaining objects associated with a task.
 */
public interface ObjectCache {

  /**
   * @param key
   */
  public void release(String key);

  /**
   * Retrieve object from cache.
   *
   * @param <T>
   * @param key
   * @param fn
   *          function to generate the object if it's not there
   * @return the last cached object with the key, null if none.
   */
  public <T> T retrieve(String key, Callable<T> fn) throws HiveException;

  /**
   * Retrieve object from cache asynchronously.
   *
   * @param <T>
   * @param key
   * @param fn
   *          function to generate the object if it's not there
   * @return the last cached object with the key, null if none.
   */
  public <T> Future<T> retrieveAsync(String key, Callable<T> fn) throws HiveException;

  /**
   * Removes the specified key from the object cache.
   *
   * @param key - key to be removed
   */
  public void remove(String key);
}
