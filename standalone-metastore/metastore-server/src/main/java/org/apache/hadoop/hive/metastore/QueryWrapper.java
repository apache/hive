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

package org.apache.hadoop.hive.metastore;

import javax.jdo.Query;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A wrapper around Query objects where the {@link AutoCloseable#close()} method
 * is delegated to the wrapped object's {@link Query#closeAll()} method.
 * This way the users of the wrapper can use try-with-resources without exception handling.
 */
public class QueryWrapper implements AutoCloseable {
  private final Query delegate;

  public QueryWrapper(Query query) {
    requireNonNull(query, "query is null");
    this.delegate = query;
  }

  /**
   * Delegates the method to {@link Query#closeAll()}, so no exception is thrown.
   */
  @Override
  public void close() {
    delegate.closeAll();
  }

  public void setClass(Class cls) {
    delegate.setClass(cls);
  }

  public void setFilter(String filter) {
    delegate.setFilter(filter);
  }

  public void declareParameters(String parameters) {
    delegate.declareParameters(parameters);
  }

  public void setOrdering(String ordering) {
    delegate.setOrdering(ordering);
  }

  public Object execute() {
    return delegate.execute();
  }

  public Object execute(Object p1) {
    return delegate.execute(p1);
  }

  public Object execute(Object p1, Object p2) {
    return delegate.execute(p1, p2);
  }

  public Object execute(Object p1, Object p2, Object p3) {
    return delegate.execute(p1, p2, p3);
  }

  public Object executeWithMap(Map parameters) {
    return delegate.executeWithMap(parameters);
  }

  public Object executeWithArray(Object... parameters) {
    return delegate.executeWithArray(parameters);
  }

  public void setUnique(boolean unique) {
    delegate.setUnique(unique);
  }

  public void setResult(String data) {
    delegate.setResult(data);
  }

  public void setResultClass(Class cls) {
    delegate.setResultClass(cls);
  }

  public void setRange(long fromIncl, long toExcl) {
    delegate.setRange(fromIncl, toExcl);
  }

  public long deletePersistentAll(Object... parameters) {
    return delegate.deletePersistentAll(parameters);
  }

  public long deletePersistentAll(Map parameters) {
    return delegate.deletePersistentAll(parameters);
  }

  public Query getInnerQuery() {
    return delegate;
  }
}
