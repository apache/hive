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

package org.apache.hadoop.hive.ql.parse;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to help populate the cache at the beginning of query analysis. We would like
 * to minimize the number of calls to fetch validWriteIdLists from the metastore. HMS
 * has an API to request this object for multiple tables within one call, and this class
 * uses that API.
 *
 * The sole purpose of this class is to help populate the HMS query cache. Nothing is returned
 * from the public methods. In this way, if another method attempts to fetch a validWriteIdList,
 * the SessionHiveMetaStoreClient query cache will contain the information.
 *
 * Because this class is only responsible for cache population, it is not a requirement for
 * a caller to supply all the tables necessary for the query. It is also not a requirement
 * for the tables to be part of the query. Of course, the query qill benefit if those
 * conditions were true, but if the table is not in the cache, a later call fetching the writeids
 * will hit the HMS server and will not fail.
 *
 * One tricky aspect to this class is that if a view is passed in, we want to fetch the
 * validWriteIdLists for the underlying tables. At the beginning of the query, it is impossible
 * to know the underlying tables without contacting HMS.
 *
 * In order to handle the underlying tables to the views, we keep a cache that holds our
 * best guess. If we see a view in any query, we set up a server-wide cache that tracks
 * the underlying tables to the view. If the view doesn't change, then this information
 * will be accurate and allow us to fetch the underlying tables on our next query. If the
 * view does change and the underlying tables are different, our fetch won't retrieve the
 * correct information. But that's ok...remember what was said earlier that it is not
 * a requirement for the tables to be part of the query. Later on in the query, this class
 * will be called on the view level via the populateCacheForView call. At that point, if
 * something changed, it will populate the cache with the newly detected tables. It will also
 * change the underying table information for the view to optimize the next query using
 * the view.
 */
public class CacheTableHelper {
  protected static final Logger LOG = LoggerFactory.getLogger(CacheTableHelper.class);

  // Server wide cache used to hold what we currently think are the underlying tables
  // for a view (which is the key). This information can go stale, but that's ok. The only
  // repercussion of a stale view is that we will have to make an additional HMS call
  // to retrieve the validWriteIdList for the changed tables.
  // Will hold 10000 objects, can't imagine that being more than a couple of M, tops.
  private static final Cache<String, Set<String>> underlyingTableHints = Caffeine.newBuilder()
      .maximumSize(10000)
      .build();

  /**
   * Populates the cache for the given table pairs. The tables passed in are a Pair
   * containing the dbname (null if not given) and the table name.
   */
  public void populateCache(List<Pair<String, String>> tables, HiveConf conf,
      HiveTxnManager txnMgr) {
    // if there is no transaction, then we don't need to fetch the validWriteIds.
    if (txnMgr == null || !txnMgr.isTxnOpen()) {
      return;
    }

    List<String> fullTableNamesList = new ArrayList<>(getAllUniqueViewsAndTables(tables));
    LOG.debug("Populating query cache");
    for (String s : fullTableNamesList) {
      LOG.debug("Populating table " + s);
    }
    String validTxnList = conf.get(ValidTxnList.VALID_TXNS_KEY);
    try {
      txnMgr.getValidWriteIds(fullTableNamesList, validTxnList);
    } catch (Exception e) {
      LOG.info("Population of valid write id list cache failed, will be done later in query.");
    }
  }

  /**
   * Populates the cache for the given table pairs associated with a viewName.
   * If the table names provided match the table names that we think are associated
   * with the view, we just return, because we presume that they have already been
   * popuated via the "populateCache" method. If they are different, we populate the cache
   * with the new associated tables and change our associated tables for the view.
   */
  public void populateCacheForView(List<Pair<String, String>> tables, HiveConf conf,
      HiveTxnManager txnMgr, String dbName, String viewName) {
    if (!conf.getBoolVar(ConfVars.HIVE_OPTIMIZE_VIEW_CACHE_ENABLED)) {
      return;
    }

    String completeViewName = dbName + "." + viewName;
    LOG.debug("Found view while parsing: " + completeViewName);
    Set<String> underlyingTablesAndViews = getUniqueNames(tables);
    // If the tables passed in match our cache, assume the cache has already
    // been populated.
    if (underlyingTablesAndViews.equals(underlyingTableHints.getIfPresent(completeViewName))) {
      LOG.debug("View already cached.");
      return;
    }
    // populate the metastore cache for this view.
    populateCache(tables, conf, txnMgr);
    // cache the names of the tables for the given view in our server-wide cache.
    underlyingTableHints.put(completeViewName, underlyingTablesAndViews);
  }

  /**
   * Return all the unique views and tables given a list of tables. We iterate
   * through all the tables passed in. If it is a table in the list, we add the
   * table to our unique list. If it is a view, we add what we think are the
   * tables and views used by the view and iterate through those.
   *
   * In order to generate a unique list, we need to add the dbname (from the SessionState)
   * if the db was not provided in the query from the user.
   */
  private Set<String> getAllUniqueViewsAndTables(List<Pair<String, String>> tables) {
    Set<String> fullTableNames = new HashSet<>();
    Queue<Pair<String, String>> queue = new LinkedList<>(tables);
    LOG.debug("Getting all tables.");
    while (queue.peek() != null) {
      String tableOrView = getTableName(queue.remove());
      LOG.debug("Getting table " + tableOrView);
      Set<String> underlyingTables = underlyingTableHints.getIfPresent(tableOrView);
      if (underlyingTables != null) {
        // don't process the same table twice.
        if (fullTableNames.contains(tableOrView)) {
          continue;
        }
        // it's a view.
        LOG.debug("View in cache, adding its tables to queue.");
        for (String viewTable : underlyingTables) {
          LOG.debug("View table is " + viewTable);
          String[] dbTableArray = viewTable.split("\\.");
          Preconditions.checkNotNull(dbTableArray[0]);
          Preconditions.checkNotNull(dbTableArray[1]);
          queue.offer(new ImmutablePair<String, String>(dbTableArray[0], dbTableArray[1]));
        }
      }
      // We'll fetch validWriteIdList information whether it's a view or a table.
      fullTableNames.add(tableOrView);
    }
    return fullTableNames;
  }

  /**
   * Get the unique names from a table list. The list may contain some cases where
   * both the dbname and tablename are provided and some cases where the dbname is
   * null, in which case we need to grab the dbname from the SessionState.
   */
  private Set<String> getUniqueNames(List<Pair<String, String>> tables) {
    Set<String> names = new HashSet<>();
    for (Pair<String, String> table : tables) {
      names.add(getTableName(table));
    }
    return names;
  }

  /**
   * Get the table name from a dbname/tablename pair. If the dbname is
   * null, use the SessionState to provide the dbname.
   */
  private String getTableName(Pair<String, String> dbTablePair) {
    String dbName = dbTablePair.getLeft() == null
        ? SessionState.get().getCurrentDatabase() : dbTablePair.getLeft();
    return dbName + "." + dbTablePair.getRight();
  }
}
