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

import javax.jdo.Extent;
import javax.jdo.FetchPlan;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A wrapper around Query objects where the {@link AutoCloseable#close()} method
 * is delegated to the wrapped object's {@link Query#closeAll()} method.
 * This way the users of the wrapper can use try-with-resources without exception handling.
 */
public class QueryWrapper implements Query {

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

  // ======================= Interfaces of Query ===========================
  @Override
  public void setClass(Class cls) {
    delegate.setClass(cls);
  }

  @Override
  public void setCandidates(Extent pcs) {
    delegate.setCandidates(pcs);
  }

  @Override
  public void setCandidates(Collection pcs) {
    delegate.setCandidates(pcs);
  }

  @Override
  public void setFilter(String filter) {
    delegate.setFilter(filter);
  }

  @Override
  public void declareImports(String imports) {
    delegate.declareImports(imports);
  }

  @Override
  public void declareParameters(String parameters) {
    delegate.declareParameters(parameters);
  }

  @Override
  public void declareVariables(String variables) {
    delegate.declareVariables(variables);
  }

  @Override
  public void setOrdering(String ordering) {
    delegate.setOrdering(ordering);
  }

  @Override
  public void setIgnoreCache(boolean ignoreCache) {
    delegate.setIgnoreCache(ignoreCache);
  }

  @Override
  public boolean getIgnoreCache() {
    return delegate.getIgnoreCache();
  }

  @Override
  public void compile() {
    delegate.compile();
  }

  @Override
  public Object execute() {
    return delegate.execute();
  }

  @Override
  public Object execute(Object p1) {
    return delegate.execute(p1);
  }

  @Override
  public Object execute(Object p1, Object p2) {
    return delegate.execute(p1, p2);
  }

  @Override
  public Object execute(Object p1, Object p2, Object p3) {
    return delegate.execute(p1, p2, p3);
  }

  @Override
  public Object executeWithMap(Map parameters) {
    return delegate.executeWithMap(parameters);
  }

  @Override
  public Object executeWithArray(Object... parameters) {
    return delegate.executeWithArray(parameters);
  }

  @Override
  public PersistenceManager getPersistenceManager() {
    return delegate.getPersistenceManager();
  }

  @Override
  public void close(Object queryResult) {
    delegate.close(queryResult);
  }

  @Override
  public void closeAll() {
    delegate.closeAll();
  }

  @Override
  public void setGrouping(String group) {
    delegate.setGrouping(group);
  }

  @Override
  public void setUnique(boolean unique) {
    delegate.setUnique(unique);
  }

  @Override
  public void setResult(String data) {
    delegate.setResult(data);
  }

  @Override
  public void setResultClass(Class cls) {
    delegate.setResultClass(cls);
  }

  @Override
  public void setRange(long fromIncl, long toExcl) {
    delegate.setRange(fromIncl, toExcl);
  }

  @Override
  public void setRange(String fromInclToExcl) {
    delegate.setRange(fromInclToExcl);
  }

  @Override
  public void addExtension(String key, Object value) {
    delegate.addExtension(key, value);
  }

  @Override
  public void setExtensions(Map extensions) {
    delegate.setExtensions(extensions);
  }

  @Override
  public FetchPlan getFetchPlan() {
    return delegate.getFetchPlan();
  }

  @Override
  public long deletePersistentAll(Object... parameters) {
    return delegate.deletePersistentAll(parameters);
  }

  @Override
  public long deletePersistentAll(Map parameters) {
    return delegate.deletePersistentAll(parameters);
  }

  @Override
  public long deletePersistentAll() {
    return delegate.deletePersistentAll();
  }

  @Override
  public void setUnmodifiable() {
    delegate.setUnmodifiable();
  }

  @Override
  public boolean isUnmodifiable() {
    return delegate.isUnmodifiable();
  }

  @Override
  public void addSubquery(Query sub, String variableDeclaration, String candidateCollectionExpression) {
    delegate.addSubquery(sub, variableDeclaration, candidateCollectionExpression);
  }

  @Override
  public void addSubquery(Query sub, String variableDeclaration, String candidateCollectionExpression, String parameter) {
    delegate.addSubquery(sub, variableDeclaration, candidateCollectionExpression, parameter);
  }

  @Override
  public void addSubquery(Query sub, String variableDeclaration, String candidateCollectionExpression, String... parameters) {
    delegate.addSubquery(sub, variableDeclaration, candidateCollectionExpression, parameters);
  }

  @Override
  public void addSubquery(Query sub, String variableDeclaration, String candidateCollectionExpression, Map parameters) {
    delegate.addSubquery(sub, variableDeclaration, candidateCollectionExpression, parameters);
  }

  @Override
  public void setDatastoreReadTimeoutMillis(Integer interval) {
    delegate.setDatastoreReadTimeoutMillis(interval);
  }

  @Override
  public Integer getDatastoreReadTimeoutMillis() {
    return delegate.getDatastoreReadTimeoutMillis();
  }

  @Override
  public void setDatastoreWriteTimeoutMillis(Integer interval) {
    delegate.setDatastoreWriteTimeoutMillis(interval);
  }

  @Override
  public Integer getDatastoreWriteTimeoutMillis() {
    return delegate.getDatastoreWriteTimeoutMillis();
  }

  @Override
  public void cancelAll() {
    delegate.cancelAll();
  }

  @Override
  public void cancel(Thread thread) {
    delegate.cancel(thread);
  }

  @Override
  public void setSerializeRead(Boolean serialize) {
    delegate.setSerializeRead(serialize);
  }

  @Override
  public Boolean getSerializeRead() {
    return delegate.getSerializeRead();
  }

  @Override
  public Query saveAsNamedQuery(String name) {
    return delegate.saveAsNamedQuery(name);
  }

  @Override
  public Query filter(String filter) {
    return delegate.filter(filter);
  }

  @Override
  public Query orderBy(String ordering) {
    return delegate.orderBy(ordering);
  }

  @Override
  public Query groupBy(String group) {
    return delegate.groupBy(group);
  }

  @Override
  public Query result(String result) {
    return delegate.result(result);
  }

  @Override
  public Query range(long fromIncl, long toExcl) {
    return delegate.range(fromIncl, toExcl);
  }

  @Override
  public Query range(String fromInclToExcl) {
    return delegate.range(fromInclToExcl);
  }

  @Override
  public Query subquery(Query sub, String variableDeclaration, String candidateCollectionExpression) {
    return delegate.subquery(sub, variableDeclaration, candidateCollectionExpression);
  }

  @Override
  public Query subquery(Query sub, String variableDeclaration, String candidateCollectionExpression, String parameter) {
    return delegate.subquery(sub, variableDeclaration, candidateCollectionExpression, parameter);
  }

  @Override
  public Query subquery(Query sub, String variableDeclaration, String candidateCollectionExpression, String... parameters) {
    return delegate.subquery(sub, variableDeclaration, candidateCollectionExpression, parameters);
  }

  @Override
  public Query subquery(Query sub, String variableDeclaration, String candidateCollectionExpression, Map parameters) {
    return delegate.subquery(sub, variableDeclaration, candidateCollectionExpression, parameters);
  }

  @Override
  public Query imports(String imports) {
    return delegate.imports(imports);
  }

  @Override
  public Query parameters(String parameters) {
    return delegate.parameters(parameters);
  }

  @Override
  public Query variables(String variables) {
    return delegate.variables(variables);
  }

  @Override
  public Query datastoreReadTimeoutMillis(Integer interval) {
    return delegate.datastoreReadTimeoutMillis(interval);
  }

  @Override
  public Query datastoreWriteTimeoutMillis(Integer interval) {
    return delegate.datastoreWriteTimeoutMillis(interval);
  }

  @Override
  public Query serializeRead(Boolean serialize) {
    return delegate.serializeRead(serialize);
  }

  @Override
  public Query unmodifiable() {
    return delegate.unmodifiable();
  }

  @Override
  public Query ignoreCache(boolean flag) {
    return delegate.ignoreCache(flag);
  }

  @Override
  public Query extension(String key, Object value) {
    return delegate.extension(key, value);
  }

  @Override
  public Query extensions(Map values) {
    return delegate.extensions(values);
  }

  @Override
  public Query setNamedParameters(Map namedParamMap) {
    return delegate.setNamedParameters(namedParamMap);
  }

  @Override
  public Query setParameters(Object... paramValues) {
    return delegate.setParameters(paramValues);
  }

  @Override
  public List executeList() {
    return delegate.executeList();
  }

  @Override
  public Object executeUnique() {
    return delegate.executeUnique();
  }

  @Override
  public List executeResultList(Class resultCls) {
    return delegate.executeResultList(resultCls);
  }

  @Override
  public Object executeResultUnique(Class resultCls) {
    return delegate.executeResultUnique(resultCls);
  }

  @Override
  public List<Object> executeResultList() {
    return delegate.executeResultList();
  }

  @Override
  public Object executeResultUnique() {
    return delegate.executeResultUnique();
  }
  // ======================= END ===========================
}
