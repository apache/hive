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

package org.apache.hadoop.hive.ql.metadata.client;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.CacheI;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.CacheKey;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.KeyType;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.Supplier;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Query-level cache of HMS requests/responses, owned by the QueryState and used by SessionHiveMetaStoreClient
 * while the query compiles.
 */
public class HMSQueryCache implements CacheI {
  private static final Logger LOG = LoggerFactory.getLogger(HMSQueryCache.class);

  private final Map<Object, Object> cache = new HashMap<>();

  @Override
  public void put(Object k, Object v) {
    cache.put(k, v);
  }

  @Override
  public Object get(Object k) {
    return cache.get(k);
  }

  public void clear() {
    cache.clear();
  }

  /**
   * Returns the cached value of the key, or loads and caches it.
   * @param method the client method, for logging
   */
  @SuppressWarnings("unchecked")
  public <T> T getOrLoad(String method, CacheKey key, Supplier<T, TException> loader) throws TException {
    T v = (T) cache.get(key);
    if (v == null) {
      v = loader.get();
      cache.put(key, v);
    } else {
      LOG.debug("Query level HMS cache: method={}, key={}", method, key);
    }
    return v;
  }

  /**
   * Answers getTable(GetTableRequest): sets the cached table id on the request and caches the table also by name.
   * A table missing from the metastore stays missing for the rest of the compilation; Executor drops these
   * entries before the tasks run (see {@link #dropTablesNotFound()}).
   * @param catName the catalog of the request, the default one if the request has none
   */
  public Table getTable(GetTableRequest req, String catName, Supplier<Table, TException> loader) throws TException {
    throwIfTableNotFound(catName, req.getDbName(), req.getTblName());
    CacheKey tableIdKey = new CacheKey(KeyType.TABLE_ID, catName, StringUtils.lowerCase(req.getDbName()),
        StringUtils.lowerCase(req.getTblName()));
    Long cachedId = (Long) cache.get(tableIdKey);
    req.setId(cachedId != null ? cachedId : -1);
    Table table = (Table) cache.get(new CacheKey(KeyType.TABLE, req));
    if (table == null) {
      table = loadTable(req, catName, loader);
      if (cachedId == null) {
        cache.put(tableIdKey, table.getId());
        req.setId(table.getId());
      }
      putTable(req, catName, table);
    }
    return table;
  }

  private void throwIfTableNotFound(String catName, String dbName, String tblName) throws NoSuchObjectException {
    String notFound = (String) cache.get(tableNotFoundKey(catName, dbName, tblName));
    if (notFound != null) {
      LOG.debug("Query level HMS cache: method=getTable, table not found, dbName={}, tblName={}", dbName, tblName);
      throw new NoSuchObjectException(notFound);
    }
  }

  private Table loadTable(GetTableRequest req, String catName, Supplier<Table, TException> loader) throws TException {
    try {
      return loader.get();
    } catch (NoSuchObjectException e) {
      cache.put(tableNotFoundKey(catName, req.getDbName(), req.getTblName()), String.valueOf(e.getMessage()));
      throw e;
    }
  }

  private void putTable(GetTableRequest req, String catName, Table table) {
    // key on a copy: the caller owns req and may change it after this call
    cache.put(new CacheKey(KeyType.TABLE, new GetTableRequest(req)), table);
    cache.putIfAbsent(MetaStoreClientCacheUtils.tableByNameKey(catName, req.getDbName(), req.getTblName()), table);
  }

  /**
   * Returns a table cached by name, or loads and caches it.
   */
  public Table getTableByName(String catName, String dbName, String tblName, Supplier<Table, TException> loader)
      throws TException {
    return getOrLoad("getTableByName", MetaStoreClientCacheUtils.tableByNameKey(catName, dbName, tblName), loader);
  }

  /**
   * Forgets a table cached as not found, e.g. once it is created.
   */
  public void invalidateTableNotFound(String catName, String dbName, String tblName) {
    cache.remove(tableNotFoundKey(catName, dbName, tblName));
  }

  /**
   * Drops the tables cached as not found. They hold only for the compilation: the tasks may create the table
   * (CTAS, CREATE TABLE) while the cache is disabled, so the post-execution lookups must reach the metastore.
   */
  public void dropTablesNotFound() {
    cache.keySet().removeIf(k -> k instanceof CacheKey cacheKey && cacheKey.IDENTIFIER == KeyType.TABLE_NOT_FOUND);
  }

  private static CacheKey tableNotFoundKey(String catName, String dbName, String tblName) {
    return new CacheKey(KeyType.TABLE_NOT_FOUND, catName, StringUtils.lowerCase(dbName),
        StringUtils.lowerCase(tblName));
  }

  /**
   * Answers getValidWriteIds(List, String): the tables in the cache come from it, the rest from the loader.
   * The result keeps the order of the request.
   */
  public List<TableValidWriteIds> getValidWriteIds(GetValidWriteIdsRequest rqst,
      ValidWriteIdsLoader loader) throws TException {
    // 1) Retrieve from the cache those ids present, gather the rest
    Pair<List<TableValidWriteIds>, List<String>> p = getValidWriteIdsCache(rqst);
    List<String> fullTableNamesMissing = p.getRight();
    List<TableValidWriteIds> tblValidWriteIds = p.getLeft();
    // 2) If they were all present in the cache, return
    if (fullTableNamesMissing.isEmpty()) {
      return tblValidWriteIds;
    }
    // 3) If they were not, gather the remaining
    List<TableValidWriteIds> r = loader.load(fullTableNamesMissing);
    // 4) Populate the cache
    List<TableValidWriteIds> newTblValidWriteIds = loadValidWriteIdsCache(r, rqst);
    // 5) Sort result (in case there is any assumption) and return
    return computeValidWriteIdsFinal(rqst, tblValidWriteIds, newTblValidWriteIds);
  }

  /**
   * Loads the valid write ids of the tables missing from the cache.
   */
  @FunctionalInterface
  public interface ValidWriteIdsLoader {
    List<TableValidWriteIds> load(List<String> fullTableNames) throws TException;
  }

  private Pair<List<TableValidWriteIds>, List<String>> getValidWriteIdsCache(GetValidWriteIdsRequest rqst) {
    List<String> fullTableNamesMissing = new ArrayList<>();
    List<TableValidWriteIds> tblValidWriteIds = new ArrayList<>();
    for (String fullTableName : rqst.getFullTableNames()) {
      CacheKey cacheKey =
          new CacheKey(KeyType.VALID_WRITE_IDS, fullTableName, rqst.getValidTxnList(), rqst.getWriteId());
      TableValidWriteIds v = (TableValidWriteIds) cache.get(cacheKey);
      if (v == null) {
        fullTableNamesMissing.add(fullTableName);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getValidWriteIdsInternal, fullTableName={}",
            fullTableName);
        tblValidWriteIds.add(v);
      }
    }
    return Pair.of(tblValidWriteIds, fullTableNamesMissing);
  }

  private List<TableValidWriteIds> loadValidWriteIdsCache(List<TableValidWriteIds> r,
      GetValidWriteIdsRequest rqst) {
    List<TableValidWriteIds> newTblValidWriteIds = new ArrayList<>();
    for (TableValidWriteIds tableValidWriteIds : r) {
      newTblValidWriteIds.add(tableValidWriteIds);
      // Add to the cache
      CacheKey cacheKey = new CacheKey(KeyType.VALID_WRITE_IDS,
          tableValidWriteIds.getFullTableName(), rqst.getValidTxnList(), rqst.getWriteId());
      cache.put(cacheKey, tableValidWriteIds);
    }
    return newTblValidWriteIds;
  }

  private static List<TableValidWriteIds> computeValidWriteIdsFinal(GetValidWriteIdsRequest rqst,
      List<TableValidWriteIds> tblValidWriteIds, List<TableValidWriteIds> newTblValidWriteIds) {
    List<TableValidWriteIds> result = new ArrayList<>();
    int i = 0;
    int j = 0;

    for (String fullTableName : rqst.getFullTableNames()) {
      if (i >= tblValidWriteIds.size() || j >= newTblValidWriteIds.size()) {
        break;
      }
      if (tblValidWriteIds.get(i).getFullTableName().equals(fullTableName)) {
        result.add(tblValidWriteIds.get(i));
        i++;
      } else if (newTblValidWriteIds.get(j).getFullTableName().equals(fullTableName)) {
        result.add(newTblValidWriteIds.get(j));
        j++;
      }
    }
    while (i < tblValidWriteIds.size()) {
      result.add(tblValidWriteIds.get(i));
      i++;
    }
    while (j < newTblValidWriteIds.size()) {
      result.add(newTblValidWriteIds.get(j));
      j++;
    }
    return result;
  }
}
