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

import com.google.common.base.Splitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE;
import static org.apache.hadoop.hive.metastore.DatabaseProduct.determineDatabaseProduct;
import static org.apache.hadoop.hive.metastore.TransactionalValidationListener.DEFAULT_TRANSACTIONAL_PROPERTY;
import static org.apache.hadoop.hive.metastore.TransactionalValidationListener.INSERTONLY_TRANSACTIONAL_PROPERTY;

public class TxnUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TxnUtils.class);

  /**
   * Returns a valid txn list for cleaner.
   * @param txns Response containing open txns list.
   * @param minOpenTxn Minimum open txn which is min open write txn on the table in the case of abort cleanup.
   * @param isAbortCleanup Whether the request is for abort cleanup.
   * @return a valid txn list
   */
  public static ValidTxnList createValidTxnListForCleaner(GetOpenTxnsResponse txns, long minOpenTxn, boolean isAbortCleanup) {
    long highWatermark = minOpenTxn - 1;
    long[] exceptions = new long[txns.getOpen_txnsSize()];
    BitSet abortedBits = BitSet.valueOf(txns.getAbortedBits());
    int i = 0;
    for (long txnId : txns.getOpen_txns()) {
      if (txnId > highWatermark) {
        break;
      }
      if (abortedBits.get(i)) {
        exceptions[i] = txnId;
      } else {
        if (isAbortCleanup) {
          exceptions[i] = txnId;
        } else {
          assert false : JavaUtils.txnIdToString(txnId) + " is open and <= hwm:" + highWatermark;
        }
      }
      ++i;
    }
    exceptions = Arrays.copyOf(exceptions, i);
    if (!isAbortCleanup) {
      BitSet bitSet = new BitSet(exceptions.length);
      bitSet.set(0, exceptions.length);
      //add ValidCleanerTxnList? - could be problematic for all the places that read it from
      // string as they'd have to know which object to instantiate
      return new ValidReadTxnList(exceptions, bitSet, highWatermark, Long.MAX_VALUE);
    } else {
      return new ValidReadTxnList(exceptions, abortedBits, highWatermark, Long.MAX_VALUE);
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
    List<Long> invalids = tableValidWriteIds.getInvalidWriteIds();
    BitSet abortedBits = BitSet.valueOf(tableValidWriteIds.getAbortedBits());
    long[] exceptions = new long[invalids.size()];
    int i = 0;
    for (long writeId : invalids) {
      if (abortedBits.get(i)) {
        // Only need aborted since we don't consider anything above minOpenWriteId
        exceptions[i++] = writeId;
      } 
    }
    if (i < exceptions.length) {
      exceptions = Arrays.copyOf(exceptions, i);
    }
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0, exceptions.length); // for ValidCompactorWriteIdList, everything in exceptions are aborted
    if (tableValidWriteIds.isSetMinOpenWriteId()) {
      long minOpenWriteId = tableValidWriteIds.getMinOpenWriteId();
      return new ValidCompactorWriteIdList(fullTableName, exceptions, bitSet, minOpenWriteId - 1, minOpenWriteId);
    } else {
      return new ValidCompactorWriteIdList(fullTableName, exceptions, bitSet, tableValidWriteIds.getWriteIdHighWaterMark());
    }
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
      handler = TransactionalRetryProxy.getProxy(handler.getRetryHandler(), handler.getJdbcResourceHolder(), handler);
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
   * org.apache.hadoop.hive.ql.io.AcidUtils#isTransactionalTable.
   * @return true if table is a transactional table, false otherwise
   */
  public static boolean isTransactionalTable(Table table) {
    return table != null && isTransactionalTable(table.getParameters());
  }

  public static boolean isTransactionalTable(Map<String, String> parameters) {
    if (parameters == null) {
      return false;
    }
    String tableIsTransactional = parameters.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    return Boolean.parseBoolean(tableIsTransactional);
  }

  /**
   * Should produce the same result as
   * org.apache.hadoop.hive.ql.io.AcidUtils#isAcidTable.
   */
  public static boolean isAcidTable(Table table) {
    return table != null && isAcidTable(table.getParameters());
  }

  public static boolean isAcidTable(Map<String, String> parameters) {
    return isTransactionalTable(parameters) && DEFAULT_TRANSACTIONAL_PROPERTY.equalsIgnoreCase(
        parameters.get(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES));
  }

  public static boolean isInsertOnlyTable(Table table) {
    return TxnUtils.isTransactionalTable(table) && INSERTONLY_TRANSACTIONAL_PROPERTY.equalsIgnoreCase(
        table.getParameters().get(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES));
  }

  public static boolean isTableSoftDeleteEnabled(Table table, boolean isSoftDelete) {
    return isSoftDelete && TxnUtils.isTransactionalTable(table)
      && Boolean.parseBoolean(table.getParameters().get(SOFT_DELETE_TABLE));
  }
  
  /**
   * Should produce the result as &lt;dbName&gt;.&lt;tableName&gt;.
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
   * E.g., Let's assume that input "inValues" parameter has [5, 6] and that
   * _DIRECT_SQL_MAX_QUERY_LENGTH_ configuration parameter only allows one value in a 'NOT IN' clause,
   * Then having two delete statements changes the semantics of the intended SQL statement.
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
   * @param inValues  IN:  Collection containing IN clause values
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
                                            Collection<Long> inValues,
                                            String inColumn,
                                            boolean addParens,
                                            boolean notIn) {
    List<String> inValueStrings = inValues.stream()
            .map(Object::toString)
            .collect(Collectors.toList());

    return buildQueryWithINClauseStrings(conf, queries, prefix, suffix,
            inValueStrings, inColumn, addParens, notIn);
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
   * @param queries   IN-OUT: Array of query strings
   * @param prefix    IN:     Part of the query that comes before IN list
   * @param suffix    IN:     Part of the query that comes after IN list
   * @param inList    IN:     the list with IN list values
   * @param inColumn  IN:     single column name of IN list operator
   * @param addParens IN:     add a pair of parenthesis outside the IN lists
   *                          e.g. "(id in (1,2,3) OR id in (4,5,6))"
   * @param notIn     IN:     is this for building a 'NOT IN' composite clause?
   * @return          OUT:    a list of the count of IN list values that are in each of the corresponding queries
   */
  public static List<Integer> buildQueryWithINClauseStrings(Configuration conf, List<String> queries, StringBuilder prefix,
      StringBuilder suffix, List<String> inList, String inColumn, boolean addParens, boolean notIn) {
    // Get configuration parameters
    int maxQueryLength = MetastoreConf.getIntVar(conf, ConfVars.DIRECT_SQL_MAX_QUERY_LENGTH);
    int batchSize = MetastoreConf.getIntVar(conf, ConfVars.DIRECT_SQL_MAX_ELEMENTS_IN_CLAUSE);
    int maxParameters = MetastoreConf.getIntVar(conf, ConfVars.DIRECT_SQL_MAX_PARAMETERS);

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

      if ((querySize > maxQueryLength * 1024) || (currentCount >= maxParameters)) {
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
        buf.setCharAt(buf.length() - 1, ')'); // replace the "comma".
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

  /**
   * Get database specific function which returns the milliseconds value after the epoch.
   * @param dbProduct The type of the db which is used
   * @throws MetaException For unknown database type.
   */
  public static String getEpochFn(DatabaseProduct dbProduct) throws MetaException {
    return dbProduct.getMillisAfterEpochFn();
  }

  /**
   * Calls queries in batch, but does not return affected row numbers. Same as executeQueriesInBatch,
   * with the only difference when the db is Oracle. In this case it is called as an anonymous stored
   * procedure instead of batching, since batching is not optimized. See:
   * https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28752
   * @param dbProduct The type of the db which is used
   * @param stmt Statement which will be used for batching and execution.
   * @param queries List of sql queries to execute in a Statement batch.
   * @param batchSize maximum number of queries in a single batch
   * @throws SQLException Thrown if an execution error occurs.
   */
  public static void executeQueriesInBatchNoCount(DatabaseProduct dbProduct, Statement stmt, List<String> queries, int batchSize) throws SQLException {
    if (dbProduct.isORACLE()) {
      int queryCounter = 0;
      StringBuilder sb = new StringBuilder();
      sb.append("begin ");
      for (String query : queries) {
        LOG.debug("Adding query to batch: <" + query + ">");
        queryCounter++;
        sb.append(query).append(";");
        if (queryCounter % batchSize == 0) {
          sb.append("end;");
          String batch = sb.toString();
          LOG.debug("Going to execute queries in oracle anonymous statement. {}", batch);
          stmt.execute(batch);
          sb.setLength(0);
          sb.append("begin ");
        }
      }
      if (queryCounter % batchSize != 0) {
        sb.append("end;");
        String batch = sb.toString();
        LOG.debug("Going to execute queries in oracle anonymous statement. {}", batch);
        stmt.execute(batch);
      }
    } else {
      executeQueriesInBatch(stmt, queries, batchSize);
    }
  }

  /**
   * @param stmt Statement which will be used for batching and execution.
   * @param queries List of sql queries to execute in a Statement batch.
   * @param batchSize maximum number of queries in a single batch
   * @return A list with the number of rows affected by each query in queries.
   * @throws SQLException Thrown if an execution error occurs.
   */
  public static List<Integer> executeQueriesInBatch(Statement stmt, List<String> queries, int batchSize) throws SQLException {
    List<Integer> affectedRowsByQuery = new ArrayList<>();
    int queryCounter = 0;
    for (String query : queries) {
      LOG.debug("Adding query to batch: <{}>", query);
      queryCounter++;
      stmt.addBatch(query);
      if (queryCounter % batchSize == 0) {
        LOG.debug("Going to execute queries in batch. Batch size: {}", batchSize);
        int[] affectedRecordsByQuery = stmt.executeBatch();
        Arrays.stream(affectedRecordsByQuery).forEach(affectedRowsByQuery::add);
      }
    }
    if (queryCounter % batchSize != 0) {
      LOG.debug("Going to execute queries in batch. Batch size: {}", queryCounter % batchSize);
      int[] affectedRecordsByQuery = stmt.executeBatch();
      Arrays.stream(affectedRecordsByQuery).forEach(affectedRowsByQuery::add);
    }
    return affectedRowsByQuery;
  }

  /**
   * Restarts the txnId sequence with the given seed value.
   * It is the responsibility of the caller to not set the sequence backward.
   * @param conn database connection
   * @param stmt sql statement
   * @param seedTxnId the seed value for the sequence
   * @throws SQLException ex
   */
  public static void seedTxnSequence(Connection conn, Configuration conf, Statement stmt, long seedTxnId) throws SQLException {
    String dbProduct = conn.getMetaData().getDatabaseProductName();
    DatabaseProduct databaseProduct = determineDatabaseProduct(dbProduct, conf);
    stmt.execute(databaseProduct.getTxnSeedFn(seedTxnId));
  }

  /**
   * Determine which user to run an operation as. If metastore.compactor.run.as.user is set, that user will be
   * returned; if not: the the owner of the directory to be compacted.
   * It is asserted that either the user running the hive metastore or the table
   * owner must be able to stat the directory and determine the owner.
   * @param location directory that will be read or written to.
   * @param t metastore table object
   * @return metastore.compactor.run.as.user value; or if that is not set: username of the owner of the location.
   * @throws java.io.IOException if neither the hive metastore user nor the table owner can stat
   * the location.
   */
  public static String findUserToRunAs(String location, Table t, Configuration conf) 
      throws IOException, InterruptedException {
    LOG.debug("Determining who to run the job as.");

    // check if a specific user is set in config
    String runUserAs = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.COMPACTOR_RUN_AS_USER);
    if (runUserAs != null && !"".equals(runUserAs)) {
      return runUserAs;
    }
    // get table directory owner
    Path p = new Path(location);
    FileSystem fs = p.getFileSystem(conf);

    try {
      FileStatus stat = fs.getFileStatus(p);
      LOG.debug("Running job as {}", stat.getOwner());
      return stat.getOwner();
    } catch (AccessControlException e) {
      // TODO not sure this is the right exception
      LOG.debug("Unable to stat file as current user, trying as table owner");

      // Now, try it as the table owner and see if we get better luck.
      List<String> wrapper = new ArrayList<>(1);
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(t.getOwner(),
        UserGroupInformation.getLoginUser());

      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        // need to use a new filesystem object here to have the correct ugi
        FileSystem proxyFs = p.getFileSystem(conf);
        FileStatus stat = proxyFs.getFileStatus(p);
        wrapper.add(stat.getOwner());
        return null;
      });

      try {
        FileSystem.closeAllForUGI(ugi);
      } catch (IOException exception) {
        LOG.error("Could not clean up file-system handles for UGI: " + ugi, exception);
      }
      if (wrapper.size() == 1) {
        LOG.debug("Running job as {}", wrapper.get(0));
        return wrapper.get(0);
      }
    }
    LOG.error("Unable to stat file {} as either current user({}) or table owner({}), giving up", p,
        UserGroupInformation.getLoginUser(), t.getOwner());
    throw new IOException("Unable to stat file: " + p);
  }

  public static CompactionType dbCompactionType2ThriftType(char dbValue) throws SQLException {
    switch (dbValue) {
      case TxnStore.MAJOR_TYPE:
        return CompactionType.MAJOR;
      case TxnStore.MINOR_TYPE:
        return CompactionType.MINOR;
      case TxnStore.REBALANCE_TYPE:
        return CompactionType.REBALANCE;
      case TxnStore.ABORT_TXN_CLEANUP_TYPE:
        return CompactionType.ABORT_TXN_CLEANUP;
      default:
        throw new SQLException("Unexpected compaction type " + dbValue);
    }
  }

  public static Character thriftCompactionType2DbType(CompactionType ct) throws MetaException {
    switch (ct) {
      case MAJOR:
        return TxnStore.MAJOR_TYPE;
      case MINOR:
        return TxnStore.MINOR_TYPE;
      case REBALANCE:
        return TxnStore.REBALANCE_TYPE;
      case ABORT_TXN_CLEANUP:
        return TxnStore.ABORT_TXN_CLEANUP_TYPE;
      default:
        throw new MetaException("Unexpected compaction type " + ct);
    }
  }

  /**
   * A helper method to return SQL's 'IS NULL'
   * clause whenever input is NULL.
   * @param input A string to be compared to null.
   * @return String
   */
  public static String nvl(String input) {
    return input != null ? " = ? " : " IS NULL ";
  }

  public static String normalizePartitionCase(String s) {
    if (s == null) {
      return null;
    }
    Map<String, String> map = Splitter.on(Path.SEPARATOR).withKeyValueSeparator('=').split(s);
    return FileUtils.makePartName(new ArrayList<>(map.keySet()), new ArrayList<>(map.values()));
  }

  @SuppressWarnings("squid:S2245")
  public static long generateTemporaryId() {
    return -1 * ThreadLocalRandom.current().nextLong();
  }

  public static boolean isValidTxn(long txnId) {
    return txnId != 0;
  }

  /**
   * Used to raise an informative error when the caller expected a txn in a particular TxnStatus
   * but found it in some other status
   */
  public static void raiseTxnUnexpectedState(TxnStatus actualStatus, long txnid)
      throws NoSuchTxnException, TxnAbortedException {
    switch (actualStatus) {
      case ABORTED:
        throw new TxnAbortedException("Transaction " + JavaUtils.txnIdToString(txnid) + " already aborted");
      case COMMITTED:
        throw new NoSuchTxnException("Transaction " + JavaUtils.txnIdToString(txnid) + " is already committed.");
      case UNKNOWN:
        throw new NoSuchTxnException("No such transaction " + JavaUtils.txnIdToString(txnid));
      case OPEN:
        throw new NoSuchTxnException(JavaUtils.txnIdToString(txnid) + " is " + TxnStatus.OPEN);
      default:
        throw new IllegalArgumentException("Unknown TxnStatus " + actualStatus);
    }
  }

  /**
   * Checks is the passed exception, or any of the root (cause) exceptions are an instance of {@link SQLException}.
   * Returns with the found {@link SQLException} or throws an {@link IllegalArgumentException} if no {@link SQLException}
   * found in the chain.
   * @param ex The exception to check
   * @return Returns with the {@link SQLException} found in the exception chain. 
   * @throws IllegalArgumentException Thrown if there is no {@link SQLException} in the exception chain
   */
  public static SQLException getSqlException(Throwable ex) throws IllegalArgumentException {
    while (ex != null && !(ex instanceof SQLException) ) {
      ex = ex.getCause();
    }
    if (ex == null) {
      throw new IllegalArgumentException("No SQLException found in the exception chain!");
    }
    return (SQLException)ex;
  }

  public static String createUpdatePreparedStmt(String tableName, List<String> columnNames, List<String> conditionKeys) {
    StringBuilder sb = new StringBuilder();
    sb.append("update " + tableName + " set ");
    sb.append(columnNames.stream().map(col -> col + "=?").collect(Collectors.joining(",")));
    sb.append(" where " + conditionKeys.stream().map(cond -> cond + "=?").collect(Collectors.joining(" and ")));
    return sb.toString();
  }

  public static String createInsertPreparedStmt(String tableName, List<String> columnNames) {
    StringBuilder sb = new StringBuilder();
    sb.append("insert into " + tableName + "(");
    sb.append(columnNames.stream().collect(Collectors.joining(",")));
    String placeholder = columnNames.stream().map(col -> "?").collect(Collectors.joining(","));
    sb.append(") values (" + placeholder + ")");
    return sb.toString();
  }
}
