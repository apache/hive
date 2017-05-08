/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.hive.common.ValidCompactorTxnList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.metastore.api.TxnState;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TxnUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TxnUtils.class);

  /**
   * Transform a {@link org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse} to a
   * {@link org.apache.hadoop.hive.common.ValidTxnList}.  This assumes that the caller intends to
   * read the files, and thus treats both open and aborted transactions as invalid.
   * @param txns txn list from the metastore
   * @param currentTxn Current transaction that the user has open.  If this is greater than 0 it
   *                   will be removed from the exceptions list so that the user sees his own
   *                   transaction as valid.
   * @return a valid txn list.
   */
  public static ValidTxnList createValidReadTxnList(GetOpenTxnsResponse txns, long currentTxn) {
    long highWater = txns.getTxn_high_water_mark();
    Set<Long> open = txns.getOpen_txns();
    long[] exceptions = new long[open.size() - (currentTxn > 0 ? 1 : 0)];
    int i = 0;
    for(long txn: open) {
      if (currentTxn > 0 && currentTxn == txn) continue;
      exceptions[i++] = txn;
    }
    if(txns.isSetMin_open_txn()) {
      return new ValidReadTxnList(exceptions, highWater, txns.getMin_open_txn());
    }
    else {
      return new ValidReadTxnList(exceptions, highWater);
    }
  }

  /**
   * Transform a {@link org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse} to a
   * {@link org.apache.hadoop.hive.common.ValidTxnList}.  This assumes that the caller intends to
   * compact the files, and thus treats only open transactions as invalid.  Additionally any
   * txnId > highestOpenTxnId is also invalid.  This is to avoid creating something like
   * delta_17_120 where txnId 80, for example, is still open.
   * @param txns txn list from the metastore
   * @return a valid txn list.
   */
  public static ValidTxnList createValidCompactTxnList(GetOpenTxnsInfoResponse txns) {
    long highWater = txns.getTxn_high_water_mark();
    long minOpenTxn = Long.MAX_VALUE;
    long[] exceptions = new long[txns.getOpen_txnsSize()];
    int i = 0;
    for (TxnInfo txn : txns.getOpen_txns()) {
      if (txn.getState() == TxnState.OPEN) {
        minOpenTxn = Math.min(minOpenTxn, txn.getId());
      }
      else {
        //only need aborted since we don't consider anything above minOpenTxn
        exceptions[i++] = txn.getId();
      }
    }
    if(i < exceptions.length) {
      exceptions = Arrays.copyOf(exceptions, i);
    }
    highWater = minOpenTxn == Long.MAX_VALUE ? highWater : minOpenTxn - 1;
    return new ValidCompactorTxnList(exceptions, highWater);
  }

  /**
   * Get an instance of the TxnStore that is appropriate for this store
   * @param conf configuration
   * @return txn store
   */
  public static TxnStore getTxnStore(HiveConf conf) {
    String className = conf.getVar(HiveConf.ConfVars.METASTORE_TXN_STORE_IMPL);
    try {
      TxnStore handler = ((Class<? extends TxnHandler>) MetaStoreUtils.getClass(
        className)).newInstance();
      handler.setConf(conf);
      return handler;
    } catch (Exception e) {
      LOG.error("Unable to instantiate raw store directly in fastpath mode", e);
      throw new RuntimeException(e);
    }
  }

  /** Checks if a table is a valid ACID table.
   * Note, users are responsible for using the correct TxnManager. We do not look at
   * SessionState.get().getTxnMgr().supportsAcid() here
   * @param table table
   * @return true if table is a legit ACID table, false otherwise
   */
  public static boolean isAcidTable(Table table) {
    if (table == null) {
      return false;
    }
    Map<String, String> parameters = table.getParameters();
    String tableIsTransactional = parameters.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    return tableIsTransactional != null && tableIsTransactional.equalsIgnoreCase("true");
  }

  /**
   * Build a query (or queries if one query is too big) with specified "prefix" and "suffix",
   * while populating the IN list into multiple OR clauses, e.g. id in (1,2,3) OR id in (4,5,6)
   * For NOT IN case, NOT IN list is broken into multiple AND clauses.
   * @param queries array of complete query strings
   * @param prefix part of the query that comes before IN list
   * @param suffix part of the query that comes after IN list
   * @param inList the list containing IN list values
   * @param inColumn column name of IN list operator
   * @param addParens add a pair of parenthesis outside the IN lists
   *                  e.g. ( id in (1,2,3) OR id in (4,5,6) )
   * @param notIn clause to be broken up is NOT IN
   */
  public static void buildQueryWithINClause(HiveConf conf, List<String> queries, StringBuilder prefix,
                                            StringBuilder suffix, List<Long> inList,
                                            String inColumn, boolean addParens, boolean notIn) {
    if (inList == null || inList.size() == 0) {
      throw new IllegalArgumentException("The IN list is empty!");
    }
    int batchSize = conf.getIntVar(HiveConf.ConfVars.METASTORE_DIRECT_SQL_MAX_ELEMENTS_IN_CLAUSE);
    int numWholeBatches = inList.size() / batchSize;
    StringBuilder buf = new StringBuilder();
    buf.append(prefix);
    if (addParens) {
      buf.append("(");
    }
    buf.append(inColumn);
    if (notIn) {
      buf.append(" not in (");
    } else {
      buf.append(" in (");
    }

    for (int i = 0; i <= numWholeBatches; i++) {
      if (i * batchSize == inList.size()) {
        // At this point we just realized we don't need another query
        break;
      }

      if (needNewQuery(conf, buf)) {
        // Wrap up current query string
        if (addParens) {
          buf.append(")");
        }
        buf.append(suffix);
        queries.add(buf.toString());

        // Prepare a new query string
        buf.setLength(0);
      }

      if (i > 0) {
        if (notIn) {
          if (buf.length() == 0) {
            buf.append(prefix);
            if (addParens) {
              buf.append("(");
            }
          } else {
            buf.append(" and ");
          }
          buf.append(inColumn);
          buf.append(" not in (");
        } else {
          if (buf.length() == 0) {
            buf.append(prefix);
            if (addParens) {
              buf.append("(");
            }
          } else {
            buf.append(" or ");
          }
          buf.append(inColumn);
          buf.append(" in (");
        }
      }

      for (int j = i * batchSize; j < (i + 1) * batchSize && j < inList.size(); j++) {
        buf.append(inList.get(j)).append(",");
      }
      buf.setCharAt(buf.length() - 1, ')');
    }

    if (addParens) {
      buf.append(")");
    }
    buf.append(suffix);
    queries.add(buf.toString());
  }

  /** Estimate if the size of a string will exceed certain limit */
  private static boolean needNewQuery(HiveConf conf, StringBuilder sb) {
    int queryMemoryLimit = conf.getIntVar(HiveConf.ConfVars.METASTORE_DIRECT_SQL_MAX_QUERY_LENGTH);
    // http://www.javamex.com/tutorials/memory/string_memory_usage.shtml
    long sizeInBytes = 8 * (((sb.length() * 2) + 45) / 8);
    return sizeInBytes / 1024 > queryMemoryLimit;
  }
}
