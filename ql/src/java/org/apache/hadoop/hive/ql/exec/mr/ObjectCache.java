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

package org.apache.hadoop.hive.ql.exec.mr;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * ObjectCache. No-op implementation on MR we don't have a means to reuse
 * Objects between runs of the same task.
 *
 */
public class ObjectCache implements org.apache.hadoop.hive.ql.exec.ObjectCache {

  private static final Log LOG = LogFactory.getLog(ObjectCache.class.getName());
  private static final boolean isInfoEnabled = LOG.isInfoEnabled();

  @Override
  public void release(String key) {
    // nothing to do
    if (isInfoEnabled) {
      LOG.info(key + " no longer needed");
    }
  }

  @Override
  public <T> T retrieve(String key, Callable<T> fn) throws HiveException {
    try {
      if (isInfoEnabled) {
        LOG.info("Creating " + key);
      }
      return fn.call();
    } catch (Exception e) {
      throw new HiveException(e);
    }
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
    // nothing to do
  }
}
