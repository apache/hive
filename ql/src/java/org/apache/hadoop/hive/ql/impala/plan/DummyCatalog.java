// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.hadoop.hive.ql.impala.plan;

import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDataSource;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsCachePool;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TGetPartitionStatsResponse;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.PatternMatcher;

import java.util.List;
import java.util.Set;

public class DummyCatalog implements FeCatalog {

  @Override
  public List<? extends FeDb> getDbs(PatternMatcher matcher) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public List<String> getTableNames(String dbName, PatternMatcher matcher)
      throws DatabaseNotFoundException {
    throw new RuntimeException("not implemented");
  }

  @Override
  public FeTable getTable(String dbName, String tableName)
      throws DatabaseNotFoundException {
    throw new RuntimeException("not implemented");
  }

  @Override
  public FeTable getTableNoThrow(String dbName, String tableName) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public FeTable getTableIfCached(String dbName, String tableName)
      throws DatabaseNotFoundException {
    throw new RuntimeException("not implemented");
  }

  @Override
  public FeTable getTableIfCachedNoThrow(String dbName, String tableName) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public TCatalogObject getTCatalogObject(TCatalogObject objectDesc)
      throws CatalogException {
    throw new RuntimeException("not implemented");
  }

  @Override
  public FeDb getDb(String db) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public FeFsPartition getHdfsPartition(String db, String tbl,
      List<TPartitionKeyValue> partitionSpec) throws CatalogException {
    throw new RuntimeException("not implemented");
  }

  @Override
  public List<? extends FeDataSource> getDataSources(PatternMatcher createHivePatternMatcher) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public FeDataSource getDataSource(String dataSourceName) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public Function getFunction(Function desc, Function.CompareMode mode) {
    return BuiltinsDb.getInstance(true).getFunction(desc, mode);
  }

  @Override
  public HdfsCachePool getHdfsCachePool(String poolName) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public void prioritizeLoad(Set<TableName> tableNames) throws InternalException {
    throw new RuntimeException("not implemented");
  }

  @Override
  public TGetPartitionStatsResponse getPartitionStats(TableName table) throws InternalException {
    throw new RuntimeException("not implemented");
  }

  @Override
  public  void waitForCatalogUpdate(long timeoutMs) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public TUniqueId getCatalogServiceId() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public AuthorizationPolicy getAuthPolicy() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public String getDefaultKuduMasterHosts() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public boolean isReady() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public void setIsReady(boolean isReady) {
    throw new RuntimeException("not implemented");
  }

}

