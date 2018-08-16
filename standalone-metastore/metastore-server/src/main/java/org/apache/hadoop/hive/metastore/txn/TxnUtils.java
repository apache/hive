/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.TransactionalValidationListener;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class TxnUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TxnUtils.class);

  // Transactional stats states
  static final public char STAT_OPEN = 'o';
  static final public char STAT_INVALID = 'i';
  static final public char STAT_COMMITTED = 'c';
  static final public char STAT_OBSOLETE = 's';

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
    /*
     * The highWaterMark should be min(currentTxn,txns.getTxn_high_water_mark()) assuming currentTxn>0
     * otherwise if currentTxn=7 and 8 commits before 7, then 7 will see result of 8 which
     * doesn't make sense for Snapshot Isolation. Of course for Read Committed, the list should
     * include the latest committed set.
     */
    long highWaterMark = (currentTxn > 0) ? Math.min(currentTxn, txns.getTxn_high_water_mark())
                                          : txns.getTxn_high_water_mark();

    // Open txns are already sorted in ascending order. This list may or may not include HWM
    // but it is guaranteed that list won't have txn > HWM. But, if we overwrite the HWM with currentTxn
    // then need to truncate the exceptions list accordingly.
    List<Long> openTxns = txns.getOpen_txns();

    // We care only about open/aborted txns below currentTxn and hence the size should be determined
    // for the exceptions list. The currentTxn will be missing in openTxns list only in rare case like
    // txn is aborted by AcidHouseKeeperService and compactor actually cleans up the aborted txns.
    // So, for such cases, we get negative value for sizeToHwm with found position for currentTxn, and so,
    // we just negate it to get the size.
    int sizeToHwm = (currentTxn > 0) ? Collections.binarySearch(openTxns, currentTxn) : openTxns.size();
    sizeToHwm = (sizeToHwm < 0) ? (-sizeToHwm) : sizeToHwm;
    long[] exceptions = new long[sizeToHwm];
    BitSet inAbortedBits = BitSet.valueOf(txns.getAbortedBits());
    BitSet outAbortedBits = new BitSet();
    long minOpenTxnId = Long.MAX_VALUE;
    int i = 0;
    for (long txn : openTxns) {
      // For snapshot isolation, we don't care about txns greater than current txn and so stop here.
      // Also, we need not include current txn to exceptions list.
      if ((currentTxn > 0) && (txn >= currentTxn)) {
        break;
      }
      if (inAbortedBits.get(i)) {
        outAbortedBits.set(i);
      } else if (minOpenTxnId == Long.MAX_VALUE) {
        minOpenTxnId = txn;
      }
      exceptions[i++] = txn;
    }
    return new ValidReadTxnList(exceptions, outAbortedBits, highWaterMark, minOpenTxnId);
  }

  /**
   * Transform a {@link org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse} to a
   * {@link org.apache.hadoop.hive.common.ValidTxnWriteIdList}.  This assumes that the caller intends to
   * read the files, and thus treats both open and aborted transactions as invalid.
   * @param currentTxnId current txn ID for which we get the valid write ids list
   * @param list valid write ids list from the metastore
   * @return a valid write IDs list for the whole transaction.
   */
  public static ValidTxnWriteIdList createValidTxnWriteIdList(Long currentTxnId,
                                                              List<TableValidWriteIds> validIds) {
    ValidTxnWriteIdList validTxnWriteIdList = new ValidTxnWriteIdList(currentTxnId);
    for (TableValidWriteIds tableWriteIds : validIds) {
      validTxnWriteIdList.addTableValidWriteIdList(createValidReaderWriteIdList(tableWriteIds));
    }
    return validTxnWriteIdList;
  }

  /**
   * Transform a {@link org.apache.hadoop.hive.metastore.api.TableValidWriteIds} to a
   * {@link org.apache.hadoop.hive.common.ValidReaderWriteIdList}.  This assumes that the caller intends to
   * read the files, and thus treats both open and aborted write ids as invalid.
   * @param tableWriteIds valid write ids for the given table from the metastore
   * @return a valid write IDs list for the input table
   */
  public static ValidReaderWriteIdList createValidReaderWriteIdList(TableValidWriteIds tableWriteIds) {
    String fullTableName = tableWriteIds.getFullTableName();
    long highWater = tableWriteIds.getWriteIdHighWaterMark();
    List<Long> invalids = tableWriteIds.getInvalidWriteIds();
    BitSet abortedBits = BitSet.valueOf(tableWriteIds.getAbortedBits());
    long[] exceptions = new long[invalids.size()];
    int i = 0;
    for (long writeId : invalids) {
      exceptions[i++] = writeId;
    }
    if (tableWriteIds.isSetMinOpenWriteId()) {
      return new ValidReaderWriteIdList(fullTableName, exceptions, abortedBits, highWater,
                                        tableWriteIds.getMinOpenWriteId());
    } else {
      return new ValidReaderWriteIdList(fullTableName, exceptions, abortedBits, highWater);
    }
  }

  /**
   * Transform a {@link org.apache.hadoop.hive.metastore.api.TableValidWriteIds} to a
   * {@link org.apache.hadoop.hive.common.ValidCompactorWriteIdList}.  This assumes that the caller intends to
   * compact the files, and thus treats only open transactions/write ids as invalid.  Additionally any
   * writeId &gt; highestOpenWriteId is also invalid.  This is to avoid creating something like
   * delta_17_120 where writeId 80, for example, is still open.
   * @param tableValidWriteIds table write id list from the metastore
   * @return a valid write id list.
   */
  public static ValidCompactorWriteIdList createValidCompactWriteIdList(TableValidWriteIds tableValidWriteIds) {
    String fullTableName = tableValidWriteIds.getFullTableName();
    long highWater = tableValidWriteIds.getWriteIdHighWaterMark();
    long minOpenWriteId = Long.MAX_VALUE;
    List<Long> invalids = tableValidWriteIds.getInvalidWriteIds();
    BitSet abortedBits = BitSet.valueOf(tableValidWriteIds.getAbortedBits());
    long[] exceptions = new long[invalids.size()];
    int i = 0;
    for (long writeId : invalids) {
      if (abortedBits.get(i)) {
        // Only need aborted since we don't consider anything above minOpenWriteId
        exceptions[i++] = writeId;
      } else {
        minOpenWriteId = Math.min(minOpenWriteId, writeId);
      }
    }
    if(i < exceptions.length) {
      exceptions = Arrays.copyOf(exceptions, i);
    }
    highWater = minOpenWriteId == Long.MAX_VALUE ? highWater : minOpenWriteId - 1;
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0, exceptions.length); // for ValidCompactorWriteIdList, everything in exceptions are aborted
    if (minOpenWriteId == Long.MAX_VALUE) {
      return new ValidCompactorWriteIdList(fullTableName, exceptions, bitSet, highWater);
    } else {
      return new ValidCompactorWriteIdList(fullTableName, exceptions, bitSet, highWater, minOpenWriteId);
    }
  }

  public static ValidReaderWriteIdList updateForCompactionQuery(ValidReaderWriteIdList ids) {
    // This is based on the existing valid write ID list that was built for a select query;
    // therefore we assume all the aborted txns, etc. were already accounted for.
    // All we do is adjust the high watermark to only include contiguous txns.
    Long minOpenWriteId = ids.getMinOpenWriteId();
    if (minOpenWriteId != null && minOpenWriteId != Long.MAX_VALUE) {
      return ids.updateHighWatermark(ids.getMinOpenWriteId() - 1);
    }
    return ids;
  }

  /**
   * Get an instance of the TxnStore that is appropriate for this store
   * @param conf configuration
   * @return txn store
   */
  public static TxnStore getTxnStore(Configuration conf) {
    String className = MetastoreConf.getVar(conf, ConfVars.TXN_STORE_IMPL);
    try {
      TxnStore handler = JavaUtils.getClass(className, TxnStore.class).newInstance();
      handler.setConf(conf);
      return handler;
    } catch (Exception e) {
      LOG.error("Unable to instantiate raw store directly in fastpath mode", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Note, users are responsible for using the correct TxnManager. We do not look at
   * SessionState.get().getTxnMgr().supportsAcid() here
   * Should produce the same result as
   * {@link org.apache.hadoop.hive.ql.io.AcidUtils#isTransactionalTable(org.apache.hadoop.hive.ql.metadata.Table)}.
   * @return true if table is a transactional table, false otherwise
   */
  public static boolean isTransactionalTable(Table table) {
    if (table == null) {
      return false;
    }
    Map<String, String> parameters = table.getParameters();
    if (parameters == null) return false;
    String tableIsTransactional = parameters.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    return tableIsTransactional != null && tableIsTransactional.equalsIgnoreCase("true");
  }

  public static boolean isTransactionalTable(Map<String, String> parameters) {
    if (parameters == null) {
      return false;
    }
    String tableIsTransactional = parameters.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    return tableIsTransactional != null && tableIsTransactional.equalsIgnoreCase("true");
  }

  /**
   * Should produce the same result as
   * {@link org.apache.hadoop.hive.ql.io.AcidUtils#isAcidTable(org.apache.hadoop.hive.ql.metadata.Table)}.
   */
  public static boolean isAcidTable(Table table) {
    return TxnUtils.isTransactionalTable(table) &&
      TransactionalValidationListener.DEFAULT_TRANSACTIONAL_PROPERTY.equals(table.getParameters()
      .get(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES));
  }

  /**
   * Should produce the result as <dbName>.<tableName>.
   */
  public static String getFullTableName(String dbName, String tableName) {
    return dbName.toLowerCase() + "." + tableName.toLowerCase();
  }

  public static String[] getDbTableName(String fullTableName) {
    return fullTableName.split("\\.");
  }



  /**
   * Build a query (or queries if one query is too big but only for the case of 'IN'
   * composite clause. For the case of 'NOT IN' clauses, multiple queries change
   * the semantics of the intended query.
   * E.g., Let's assume that input "inList" parameter has [5, 6] and that
   * _DIRECT_SQL_MAX_QUERY_LENGTH_ configuration parameter only allows one value in a 'NOT IN' clause,
   * Then having two delete statements changes the semantics of the inteneded SQL statement.
   * I.e. 'delete from T where a not in (5)' and 'delete from T where a not in (6)' sequence
   * is not equal to 'delete from T where a not in (5, 6)'.)
   * with one or multiple 'IN' or 'NOT IN' clauses with the given input parameters.
   *
   * Note that this method currently support only single column for
   * IN/NOT IN clauses and that only covers OR-based composite 'IN' clause and
   * AND-based composite 'NOT IN' clause.
   * For example, for 'IN' clause case, the method will build a query with OR.
   * E.g., "id in (1,2,3) OR id in (4,5,6)".
   * For 'NOT IN' case, NOT IN list is broken into multiple 'NOT IN" clauses connected by AND.
   *
   * Note that, in this method, "a composite 'IN' clause" is defined as "a list of multiple 'IN'
   * clauses in a query".
   *
   * @param queries   OUT: Array of query strings
   * @param prefix    IN:  Part of the query that comes before IN list
   * @param suffix    IN:  Part of the query that comes after IN list
   * @param inList    IN:  the list with IN list values
   * @param inColumn  IN:  single column name of IN list operator
   * @param addParens IN:  add a pair of parenthesis outside the IN lists
   *                       e.g. "(id in (1,2,3) OR id in (4,5,6))"
   * @param notIn     IN:  is this for building a 'NOT IN' composite clause?
   * @return          OUT: a list of the count of IN list values that are in each of the corresponding queries
   */
  public static List<Integer> buildQueryWithINClause(Configuration conf,
                                            List<String> queries,
                                            StringBuilder prefix,
                                            StringBuilder suffix,
                                            List<Long> inList,
                                            String inColumn,
                                            boolean addParens,
                                            boolean notIn) {
    List<String> inListStrings = new ArrayList<>(inList.size());
    for (Long aLong : inList) {
      inListStrings.add(aLong.toString());
    }
    return buildQueryWithINClauseStrings(conf, queries, prefix, suffix,
        inListStrings, inColumn, addParens, notIn);

  }
  /**
   * Build a query (or queries if one query is too big but only for the case of 'IN'
   * composite clause. For the case of 'NOT IN' clauses, multiple queries change
   * the semantics of the intended query.
   * E.g., Let's assume that input "inList" parameter has [5, 6] and that
   * _DIRECT_SQL_MAX_QUERY_LENGTH_ configuration parameter only allows one value in a 'NOT IN' clause,
   * Then having two delete statements changes the semantics of the inteneded SQL statement.
   * I.e. 'delete from T where a not in (5)' and 'delete from T where a not in (6)' sequence
   * is not equal to 'delete from T where a not in (5, 6)'.)
   * with one or multiple 'IN' or 'NOT IN' clauses with the given input parameters.
   *
   * Note that this method currently support only single column for
   * IN/NOT IN clauses and that only covers OR-based composite 'IN' clause and
   * AND-based composite 'NOT IN' clause.
   * For example, for 'IN' clause case, the method will build a query with OR.
   * E.g., "id in (1,2,3) OR id in (4,5,6)".
   * For 'NOT IN' case, NOT IN list is broken into multiple 'NOT IN" clauses connected by AND.
   *
   * Note that, in this method, "a composite 'IN' clause" is defined as "a list of multiple 'IN'
   * clauses in a query".
   *
   * @param queries   OUT: Array of query strings
   * @param prefix    IN:  Part of the query that comes before IN list
   * @param suffix    IN:  Part of the query that comes after IN list
   * @param inList    IN:  the list with IN list values
   * @param inColumn  IN:  single column name of IN list operator
   * @param addParens IN:  add a pair of parenthesis outside the IN lists
   *                       e.g. "(id in (1,2,3) OR id in (4,5,6))"
   * @param notIn     IN:  is this for building a 'NOT IN' composite clause?
   * @return          OUT: a list of the count of IN list values that are in each of the corresponding queries
   */
  public static List<Integer> buildQueryWithINClauseStrings(Configuration conf, List<String> queries, StringBuilder prefix,
      StringBuilder suffix, List<String> inList, String inColumn, boolean addParens, boolean notIn) {
    // Get configuration parameters
    int maxQueryLength = MetastoreConf.getIntVar(conf, ConfVars.DIRECT_SQL_MAX_QUERY_LENGTH);
    int batchSize = MetastoreConf.getIntVar(conf, ConfVars.DIRECT_SQL_MAX_ELEMENTS_IN_CLAUSE);

    // Check parameter set validity as a public method.
    if (inList == null || inList.size() == 0 || maxQueryLength <= 0 || batchSize <= 0) {
      throw new IllegalArgumentException("The IN list is empty!");
    }

    // Define constants and local variables.
    int inListSize = inList.size();
    StringBuilder buf = new StringBuilder();

    int cursor4InListArray = 0,  // cursor for the "inList" array.
        cursor4InClauseElements = 0,  // cursor for an element list per an 'IN'/'NOT IN'-clause.
        cursor4queryOfInClauses = 0;  // cursor for in-clause lists per a query.
    boolean nextItemNeeded = true;
    boolean newInclausePrefixJustAppended = false;
    StringBuilder nextValue = new StringBuilder("");
    StringBuilder newInclausePrefix =
      new StringBuilder(notIn ? " and " + inColumn + " not in (":
	                        " or " + inColumn + " in (");
    List<Integer> ret = new ArrayList<>();
    int currentCount = 0;

    // Loop over the given inList elements.
    while( cursor4InListArray < inListSize || !nextItemNeeded) {
      if (cursor4queryOfInClauses == 0) {
        // Append prefix
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
        cursor4queryOfInClauses++;
        newInclausePrefixJustAppended = false;
      }

      // Get the next "inList" value element if needed.
      if (nextItemNeeded) {
        nextValue.setLength(0);
        nextValue.append(String.valueOf(inList.get(cursor4InListArray++)));
        nextItemNeeded = false;
      }

      // Compute the size of a query when the 'nextValue' is added to the current query.
      int querySize = querySizeExpected(buf.length(), nextValue.length(), suffix.length(), addParens);

      if (querySize > maxQueryLength * 1024) {
        // Check an edge case where the DIRECT_SQL_MAX_QUERY_LENGTH does not allow one 'IN' clause with single value.
        if (cursor4queryOfInClauses == 1 && cursor4InClauseElements == 0) {
          throw new IllegalArgumentException("The current " + ConfVars.DIRECT_SQL_MAX_QUERY_LENGTH.getVarname() + " is set too small to have one IN clause with single value!");
        }

        // Check en edge case to throw Exception if we can not build a single query for 'NOT IN' clause cases as mentioned at the method comments.
        if (notIn) {
          throw new IllegalArgumentException("The NOT IN list has too many elements for the current " + ConfVars.DIRECT_SQL_MAX_QUERY_LENGTH.getVarname() + "!");
        }

        // Wrap up the current query string since we can not add another "inList" element value.
        if (newInclausePrefixJustAppended) {
          buf.delete(buf.length()-newInclausePrefix.length(), buf.length());
        }

        buf.setCharAt(buf.length() - 1, ')'); // replace the "commar" to finish a 'IN' clause string.

        if (addParens) {
          buf.append(")");
        }

        buf.append(suffix);
        queries.add(buf.toString());
        ret.add(currentCount);

        // Prepare a new query string.
        buf.setLength(0);
        currentCount = 0;
        cursor4queryOfInClauses = cursor4InClauseElements = 0;
        querySize = 0;
        newInclausePrefixJustAppended = false;
        continue;
      } else if (cursor4InClauseElements >= batchSize-1 && cursor4InClauseElements != 0) {
        // Finish the current 'IN'/'NOT IN' clause and start a new clause.
        buf.setCharAt(buf.length() - 1, ')'); // replace the "commar".
        buf.append(newInclausePrefix.toString());

        newInclausePrefixJustAppended = true;

        // increment cursor for per-query IN-clause list
        cursor4queryOfInClauses++;
        cursor4InClauseElements = 0;
      } else {
        buf.append(nextValue.toString()).append(",");
        currentCount++;
        nextItemNeeded = true;
        newInclausePrefixJustAppended = false;
        // increment cursor for elements per 'IN'/'NOT IN' clause.
        cursor4InClauseElements++;
      }
    }

    // Finish the last query.
    if (newInclausePrefixJustAppended) {
        buf.delete(buf.length()-newInclausePrefix.length(), buf.length());
      }
    buf.setCharAt(buf.length() - 1, ')'); // replace the commar.
    if (addParens) {
      buf.append(")");
    }
    buf.append(suffix);
    queries.add(buf.toString());
    ret.add(currentCount);
    return ret;
  }

  /**
   * Compute and return the size of a query statement with the given parameters as input variables.
   *
   * @param sizeSoFar     size of the current contents of the buf
   * @param sizeNextItem      size of the next 'IN' clause element value.
   * @param suffixSize    size of the suffix for a quey statement
   * @param addParens     Do we add an additional parenthesis?
   */
  private static int querySizeExpected(int sizeSoFar,
                                       int sizeNextItem,
                                       int suffixSize,
                                       boolean addParens) {

    int size = sizeSoFar + sizeNextItem + suffixSize;

    if (addParens) {
       size++;
    }

    return size;
  }
}
