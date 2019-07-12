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
package org.apache.hadoop.hive.upgrade.acid;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has similar functionality as java.lang.ThreadLocal.
 * Plus it provides a close function to clean up unmanaged resources in all threads where the resource was initialized.
 * @param <T> - type of resource
 */
public class CloseableThreadLocal<T> {

  private static final Logger LOG = LoggerFactory.getLogger(CloseableThreadLocal.class);

  private final ConcurrentHashMap<Thread, T> threadLocalMap;
  private final Supplier<T> initialValue;
  private final Consumer<T> closeFunction;

  public CloseableThreadLocal(Supplier<T> initialValue, Consumer<T> closeFunction, int poolSize) {
    this.initialValue = initialValue;
    threadLocalMap = new ConcurrentHashMap<>(poolSize);
    this.closeFunction = closeFunction;
  }

  public T get() {
    return threadLocalMap.computeIfAbsent(Thread.currentThread(), thread -> initialValue.get());
  }

  public void close() {
    threadLocalMap.values().forEach(this::closeQuietly);
  }

  private void closeQuietly(T resource) {
    try {
      closeFunction.accept(resource);
    } catch (Exception e) {
      LOG.warn("Error while closing resource.", e);
    }
  }
}
