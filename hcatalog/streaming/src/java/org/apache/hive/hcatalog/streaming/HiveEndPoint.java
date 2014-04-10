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

package org.apache.hive.hcatalog.streaming;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Information about the hive end point (i.e. table or partition) to write to.
 * A light weight object that does NOT internally hold on to resources such as
 * network connections. It can be stored in Hashed containers such as sets and hash tables.
 */
public class HiveEndPoint {
  public final String metaStoreUri;
  public final String database;
  public final String table;
  public final ArrayList<String> partitionVals;


  static final private Log LOG = LogFactory.getLog(HiveEndPoint.class.getName());

  /**
   *
   * @param metaStoreUri   URI of the metastore to connect to eg: thrift://localhost:9083
   * @param database       Name of the Hive database
   * @param table          Name of table to stream to
   * @param partitionVals  Indicates the specific partition to stream to. Can be null or empty List
   *                       if streaming to a table without partitions. The order of values in this
   *                       list must correspond exactly to the order of partition columns specified
   *                       during the table creation. E.g. For a table partitioned by
   *                       (continent string, country string), partitionVals could be the list
   *                       ("Asia", "India").
   */
  public HiveEndPoint(String metaStoreUri
          , String database, String table, List<String> partitionVals) {
    this.metaStoreUri = metaStoreUri;
    if (database==null) {
      throw new IllegalArgumentException("Database cannot be null for HiveEndPoint");
    }
    this.database = database;
    this.table = table;
    if (table==null) {
      throw new IllegalArgumentException("Table cannot be null for HiveEndPoint");
    }
    this.partitionVals = partitionVals==null ? new ArrayList<String>()
                                             : new ArrayList<String>( partitionVals );
  }


  /**
   * Acquire a new connection to MetaStore for streaming
   * @param createPartIfNotExists If true, the partition specified in the endpoint
   *                              will be auto created if it does not exist
   * @return
   * @throws ConnectionError if problem connecting
   * @throws InvalidPartition  if specified partition is not valid (createPartIfNotExists = false)
   * @throws ImpersonationFailed  if not able to impersonate 'proxyUser'
   * @throws IOException  if there was an I/O error when acquiring connection
   * @throws PartitionCreationFailed if failed to create partition
   * @throws InterruptedException
   */
  public StreamingConnection newConnection(final boolean createPartIfNotExists)
          throws ConnectionError, InvalidPartition, InvalidTable, PartitionCreationFailed
          , ImpersonationFailed , InterruptedException {
    return newConnection(null, createPartIfNotExists, null);
  }

  /**
   * Acquire a new connection to MetaStore for streaming
   * @param createPartIfNotExists If true, the partition specified in the endpoint
   *                              will be auto created if it does not exist
   * @param conf HiveConf object, set it to null if not using advanced hive settings.
   * @return
   * @throws ConnectionError if problem connecting
   * @throws InvalidPartition  if specified partition is not valid (createPartIfNotExists = false)
   * @throws ImpersonationFailed  if not able to impersonate 'proxyUser'
   * @throws IOException  if there was an I/O error when acquiring connection
   * @throws PartitionCreationFailed if failed to create partition
   * @throws InterruptedException
   */
  public StreamingConnection newConnection(final boolean createPartIfNotExists, HiveConf conf)
          throws ConnectionError, InvalidPartition, InvalidTable, PartitionCreationFailed
          , ImpersonationFailed , InterruptedException {
    return newConnection(null, createPartIfNotExists, conf);
  }

  /**
   * Acquire a new connection to MetaStore for streaming
   * @param proxyUser User on whose behalf all hdfs and hive operations will be
   *                  performed on this connection. Set it to null or empty string
   *                  to connect as user of current process without impersonation.
   *                  Currently this argument is not supported and must be null
   * @param createPartIfNotExists If true, the partition specified in the endpoint
   *                              will be auto created if it does not exist
   * @return
   * @throws ConnectionError if problem connecting
   * @throws InvalidPartition  if specified partition is not valid (createPartIfNotExists = false)
   * @throws ImpersonationFailed  if not able to impersonate 'proxyUser'
   * @throws IOException  if there was an I/O error when acquiring connection
   * @throws PartitionCreationFailed if failed to create partition
   * @throws InterruptedException
   */
  private StreamingConnection newConnection(final String proxyUser,
                                            final boolean createPartIfNotExists, final HiveConf conf)
          throws ConnectionError, InvalidPartition,
               InvalidTable, PartitionCreationFailed, ImpersonationFailed , InterruptedException {
    if (proxyUser ==null || proxyUser.trim().isEmpty() ) {
      return newConnectionImpl(System.getProperty("user.name"), null, createPartIfNotExists, conf);
    }
    final UserGroupInformation ugi = getUserGroupInfo(proxyUser);
    try {
      return ugi.doAs (
              new PrivilegedExceptionAction<StreamingConnection>() {
                @Override
                public StreamingConnection run()
                        throws ConnectionError, InvalidPartition, InvalidTable
                        , PartitionCreationFailed {
                  return newConnectionImpl(proxyUser, ugi, createPartIfNotExists, conf);
                }
              }
      );
    } catch (IOException e) {
      throw new ImpersonationFailed("Failed to impersonate '" + proxyUser +
              "' when acquiring connection", e);
    }
  }



  private StreamingConnection newConnectionImpl(String proxyUser, UserGroupInformation ugi,
                                               boolean createPartIfNotExists, HiveConf conf)
          throws ConnectionError, InvalidPartition, InvalidTable
          , PartitionCreationFailed {
    return new ConnectionImpl(this, proxyUser, ugi, conf, createPartIfNotExists);
  }

  private static UserGroupInformation getUserGroupInfo(String proxyUser)
          throws ImpersonationFailed {
    try {
      return UserGroupInformation.createProxyUser(
              proxyUser, UserGroupInformation.getLoginUser());
    } catch (IOException e) {
      LOG.error("Unable to login as proxy user. Exception follows.", e);
      throw new ImpersonationFailed(proxyUser,e);
    }
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HiveEndPoint endPoint = (HiveEndPoint) o;

    if (database != null
            ? !database.equals(endPoint.database)
            : endPoint.database != null ) {
      return false;
    }
    if (metaStoreUri != null
            ? !metaStoreUri.equals(endPoint.metaStoreUri)
            : endPoint.metaStoreUri != null ) {
      return false;
    }
    if (!partitionVals.equals(endPoint.partitionVals)) {
      return false;
    }
    if (table != null ? !table.equals(endPoint.table) : endPoint.table != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = metaStoreUri != null ? metaStoreUri.hashCode() : 0;
    result = 31 * result + (database != null ? database.hashCode() : 0);
    result = 31 * result + (table != null ? table.hashCode() : 0);
    result = 31 * result + partitionVals.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "{" +
            "metaStoreUri='" + metaStoreUri + '\'' +
            ", database='" + database + '\'' +
            ", table='" + table + '\'' +
            ", partitionVals=" + partitionVals + " }";
  }


  private static class ConnectionImpl implements StreamingConnection {
    private final IMetaStoreClient msClient;
    private final HiveEndPoint endPt;
    private final String proxyUser;
    private final UserGroupInformation ugi;

    /**
     *
     * @param endPoint end point to connect to
     * @param proxyUser  can be null
     * @param ugi of prody user. If ugi is null, impersonation of proxy user will be disabled
     * @param conf HiveConf object
     * @param createPart create the partition if it does not exist
     * @throws ConnectionError if there is trouble connecting
     * @throws InvalidPartition if specified partition does not exist (and createPart=false)
     * @throws InvalidTable if specified table does not exist
     * @throws PartitionCreationFailed if createPart=true and not able to create partition
     */
    private ConnectionImpl(HiveEndPoint endPoint, String proxyUser, UserGroupInformation ugi,
                           HiveConf conf, boolean createPart)
            throws ConnectionError, InvalidPartition, InvalidTable
                   , PartitionCreationFailed {
      this.proxyUser = proxyUser;
      this.endPt = endPoint;
      this.ugi = ugi;
      if (conf==null) {
        conf = HiveEndPoint.createHiveConf(this.getClass(),endPoint.metaStoreUri);
      }
      this.msClient = getMetaStoreClient(endPoint, conf);
      if (createPart  &&  !endPoint.partitionVals.isEmpty()) {
        createPartitionIfNotExists(endPoint, msClient, conf);
      }
    }

    /**
     * Close connection
     */
    @Override
    public void close() {
      if (ugi==null) {
        msClient.close();
        return;
      }
      try {
        ugi.doAs (
            new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                msClient.close();
                return null;
              }
            } );
      } catch (IOException e) {
        LOG.error("Error closing connection to " + endPt, e);
      } catch (InterruptedException e) {
        LOG.error("Interrupted when closing connection to " + endPt, e);
      }
    }


    /**
     * Acquires a new batch of transactions from Hive.
     *
     * @param numTransactions is a hint from client indicating how many transactions client needs.
     * @param recordWriter  Used to write record. The same writer instance can
     *                      be shared with another TransactionBatch (to the same endpoint)
     *                      only after the first TransactionBatch has been closed.
     *                      Writer will be closed when the TransactionBatch is closed.
     * @return
     * @throws StreamingIOFailure if failed to create new RecordUpdater for batch
     * @throws TransactionBatchUnAvailable if failed to acquire a new Transaction batch
     * @throws ImpersonationFailed failed to run command as proxyUser
     * @throws InterruptedException
     */
    public TransactionBatch fetchTransactionBatch(final int numTransactions,
                                                      final RecordWriter recordWriter)
            throws StreamingException, TransactionBatchUnAvailable, ImpersonationFailed
                  , InterruptedException {
      if (ugi==null) {
        return fetchTransactionBatchImpl(numTransactions, recordWriter);
      }
      try {
        return ugi.doAs (
                new PrivilegedExceptionAction<TransactionBatch>() {
                  @Override
                  public TransactionBatch run() throws StreamingException {
                    return fetchTransactionBatchImpl(numTransactions, recordWriter);
                  }
                }
        );
      } catch (IOException e) {
        throw new ImpersonationFailed("Failed impersonating proxy user '" + proxyUser +
                "' when acquiring Transaction Batch on endPoint " + endPt, e);
      }
    }

    private TransactionBatch fetchTransactionBatchImpl(int numTransactions,
                                                  RecordWriter recordWriter)
            throws StreamingException, TransactionBatchUnAvailable {
      return new TransactionBatchImpl(proxyUser, ugi, endPt, numTransactions, msClient
              , recordWriter);
    }


    private static void createPartitionIfNotExists(HiveEndPoint ep,
                                                   IMetaStoreClient msClient, HiveConf conf)
            throws InvalidTable, PartitionCreationFailed {
      if (ep.partitionVals.isEmpty()) {
        return;
      }
      SessionState state = SessionState.start(new CliSessionState(conf));
      Driver driver = new Driver(conf);

      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Attempting to create partition (if not existent) " + ep);
        }

        List<FieldSchema> partKeys = msClient.getTable(ep.database, ep.table)
                .getPartitionKeys();
        runDDL(driver, "use " + ep.database);
        String query = "alter table " + ep.table + " add if not exists partition "
                + partSpecStr(partKeys, ep.partitionVals);
        runDDL(driver, query);
      } catch (MetaException e) {
        LOG.error("Failed to create partition : " + ep, e);
        throw new PartitionCreationFailed(ep, e);
      } catch (NoSuchObjectException e) {
        LOG.error("Failed to create partition : " + ep, e);
        throw new InvalidTable(ep.database, ep.table);
      } catch (TException e) {
        LOG.error("Failed to create partition : " + ep, e);
        throw new PartitionCreationFailed(ep, e);
      } catch (QueryFailedException e) {
        LOG.error("Failed to create partition : " + ep, e);
        throw new PartitionCreationFailed(ep, e);
      } finally {
        driver.close();
        try {
          state.close();
        } catch (IOException e) {
          LOG.warn("Error closing SessionState used to run Hive DDL.");
        }
      }
    }

    private static boolean runDDL(Driver driver, String sql) throws QueryFailedException {
      int retryCount = 1; // # of times to retry if first attempt fails
      for (int attempt=0; attempt<=retryCount; ++attempt) {
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Running Hive Query: "+ sql);
          }
          driver.run(sql);
          return true;
        } catch (CommandNeedRetryException e) {
          if (attempt==retryCount) {
            throw new QueryFailedException(sql, e);
          }
          continue;
        }
      } // for
      return false;
    }

    private static String partSpecStr(List<FieldSchema> partKeys, ArrayList<String> partVals) {
      if (partKeys.size()!=partVals.size()) {
        throw new IllegalArgumentException("Partition values:" + partVals +
                ", does not match the partition Keys in table :" + partKeys );
      }
      StringBuffer buff = new StringBuffer(partKeys.size()*20);
      buff.append(" ( ");
      int i=0;
      for (FieldSchema schema : partKeys) {
        buff.append(schema.getName());
        buff.append("='");
        buff.append(partVals.get(i));
        buff.append("'");
        if (i!=partKeys.size()-1) {
          buff.append(",");
        }
        ++i;
      }
      buff.append(" )");
      return buff.toString();
    }

    private static IMetaStoreClient getMetaStoreClient(HiveEndPoint endPoint, HiveConf conf)
            throws ConnectionError {

      if (endPoint.metaStoreUri!= null) {
        conf.setVar(HiveConf.ConfVars.METASTOREURIS, endPoint.metaStoreUri);
      }

      try {
        return new HiveMetaStoreClient(conf);
      } catch (MetaException e) {
        throw new ConnectionError("Error connecting to Hive Metastore URI: "
                + endPoint.metaStoreUri, e);
      }
    }


  } // class ConnectionImpl

  private static class TransactionBatchImpl implements TransactionBatch {
    private final String proxyUser;
    private final UserGroupInformation ugi;
    private final HiveEndPoint endPt;
    private final IMetaStoreClient msClient;
    private final RecordWriter recordWriter;
    private final List<Long> txnIds;

    private int currentTxnIndex;
    private final String partNameForLock;

    private TxnState state;
    private LockRequest lockRequest = null;

    /**
     * Represents a batch of transactions acquired from MetaStore
     *
     * @param proxyUser
     * @param ugi
     * @param endPt
     * @param numTxns
     * @param msClient
     * @param recordWriter
     * @throws StreamingException if failed to create new RecordUpdater for batch
     * @throws TransactionBatchUnAvailable if failed to acquire a new Transaction batch
     */
    private TransactionBatchImpl(String proxyUser, UserGroupInformation ugi, HiveEndPoint endPt
              , int numTxns, IMetaStoreClient msClient, RecordWriter recordWriter)
            throws StreamingException, TransactionBatchUnAvailable {
      try {
        if ( endPt.partitionVals!=null   &&   !endPt.partitionVals.isEmpty() ) {
          Table tableObj = msClient.getTable(endPt.database, endPt.table);
          List<FieldSchema> partKeys = tableObj.getPartitionKeys();
          partNameForLock = Warehouse.makePartName(partKeys, endPt.partitionVals);
        } else {
          partNameForLock = null;
        }
        this.proxyUser = proxyUser;
        this.ugi = ugi;
        this.endPt = endPt;
        this.msClient = msClient;
        this.recordWriter = recordWriter;
        this.txnIds = msClient.openTxns(proxyUser, numTxns).getTxn_ids();
        this.currentTxnIndex = -1;
        this.state = TxnState.INACTIVE;
        recordWriter.newBatch(txnIds.get(0), txnIds.get(txnIds.size()-1));
      } catch (TException e) {
        throw new TransactionBatchUnAvailable(endPt, e);
      }
    }

    @Override
    public String toString() {
      if (txnIds==null || txnIds.isEmpty()) {
        return "{}";
      }
      return "TxnIds=[" + txnIds.get(0) + "src/gen/thrift" + txnIds.get(txnIds.size()-1)
              + "] on endPoint= " + endPt;
    }

    /**
     * Activate the next available transaction in the current transaction batch
     * @throws TransactionError failed to switch to next transaction
     */
    @Override
    public void beginNextTransaction() throws TransactionError, ImpersonationFailed,
            InterruptedException {
      if (ugi==null) {
        beginNextTransactionImpl();
        return;
      }
      try {
        ugi.doAs (
              new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws TransactionError {
                  beginNextTransactionImpl();
                  return null;
                }
              }
        );
      } catch (IOException e) {
        throw new ImpersonationFailed("Failed impersonating proxyUser '" + proxyUser +
                "' when switch to next Transaction for endPoint :" + endPt, e);
      }
    }

    private void beginNextTransactionImpl() throws TransactionError {
      if ( currentTxnIndex >= txnIds.size() )
        throw new InvalidTrasactionState("No more transactions available in" +
                " current batch for end point : " + endPt);
      ++currentTxnIndex;
      lockRequest = createLockRequest(endPt, partNameForLock, proxyUser, getCurrentTxnId());
      try {
        LockResponse res = msClient.lock(lockRequest);
        if (res.getState() != LockState.ACQUIRED) {
          throw new TransactionError("Unable to acquire lock on " + endPt);
        }
      } catch (TException e) {
        throw new TransactionError("Unable to acquire lock on " + endPt, e);
      }

      state = TxnState.OPEN;
    }

    /**
     * Get Id of currently open transaction
     * @return
     */
    @Override
    public Long getCurrentTxnId() {
      return txnIds.get(currentTxnIndex);
    }

    /**
     * get state of current tramsaction
     * @return
     */
    @Override
    public TxnState getCurrentTransactionState() {
      return state;
    }

    /**
     * Remaining transactions are the ones that are not committed or aborted or active.
     * Active transaction is not considered part of remaining txns.
     * @return number of transactions remaining this batch.
     */
    @Override
    public int remainingTransactions() {
      if (currentTxnIndex>=0) {
        return txnIds.size() - currentTxnIndex -1;
      }
      return txnIds.size();
    }


    /**
     *  Write record using RecordWriter
     * @param record  the data to be written
     * @throws StreamingIOFailure I/O failure
     * @throws SerializationError  serialization error
     * @throws ImpersonationFailed error writing on behalf of proxyUser
     * @throws InterruptedException
     */
    @Override
    public void write(final byte[] record)
            throws StreamingException, InterruptedException,
            ImpersonationFailed {
      if (ugi==null) {
        recordWriter.write(getCurrentTxnId(), record);
        return;
      }
      try {
        ugi.doAs (
            new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws StreamingException {
                recordWriter.write(getCurrentTxnId(), record);
                return null;
              }
            }
        );
      } catch (IOException e) {
        throw new ImpersonationFailed("Failed impersonating proxy user '" + proxyUser +
                "' when writing to endPoint :" + endPt + ". Transaction Id: "
                + getCurrentTxnId(), e);
      }
    }


    /**
     *  Write records using RecordWriter
     * @param records collection of rows to be written
     * @throws StreamingException  serialization error
     * @throws ImpersonationFailed error writing on behalf of proxyUser
     * @throws InterruptedException
     */
    @Override
    public void write(final Collection<byte[]> records)
            throws StreamingException, InterruptedException,
            ImpersonationFailed {
      if (ugi==null) {
        writeImpl(records);
        return;
      }
      try {
        ugi.doAs (
                new PrivilegedExceptionAction<Void>() {
                  @Override
                  public Void run() throws StreamingException {
                    writeImpl(records);
                    return null;
                  }
                }
        );
      } catch (IOException e) {
        throw new ImpersonationFailed("Failed impersonating proxyUser '" + proxyUser +
                "' when writing to endPoint :" + endPt + ". Transaction Id: "
                + getCurrentTxnId(), e);
      }
    }

    private void writeImpl(Collection<byte[]> records)
            throws StreamingException {
      for (byte[] record : records) {
        recordWriter.write(getCurrentTxnId(), record);
      }
    }


    /**
     * Commit the currently open transaction
     * @throws TransactionError
     * @throws StreamingIOFailure  if flushing records failed
     * @throws ImpersonationFailed if
     * @throws InterruptedException
     */
    @Override
    public void commit()  throws TransactionError, StreamingException,
           ImpersonationFailed, InterruptedException {
      if (ugi==null) {
        commitImpl();
        return;
      }
      try {
        ugi.doAs (
              new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws StreamingException {
                  commitImpl();
                  return null;
                }
              }
        );
      } catch (IOException e) {
        throw new ImpersonationFailed("Failed impersonating proxy user '" + proxyUser +
                "' when committing Txn on endPoint :" + endPt + ". Transaction Id: "
                + getCurrentTxnId(), e);
      }

    }

    private void commitImpl() throws TransactionError, StreamingException {
      try {
        recordWriter.flush();
        msClient.commitTxn(txnIds.get(currentTxnIndex));
        state = TxnState.COMMITTED;
      } catch (NoSuchTxnException e) {
        throw new TransactionError("Invalid transaction id : "
                + getCurrentTxnId(), e);
      } catch (TxnAbortedException e) {
        throw new TransactionError("Aborted transaction cannot be committed"
                , e);
      } catch (TException e) {
        throw new TransactionError("Unable to commit transaction"
                + getCurrentTxnId(), e);
      }
    }

    /**
     * Abort the currently open transaction
     * @throws TransactionError
     */
    @Override
    public void abort() throws TransactionError, StreamingException
                      , ImpersonationFailed, InterruptedException {
      if (ugi==null) {
        abortImpl();
        return;
      }
      try {
        ugi.doAs (
                new PrivilegedExceptionAction<Void>() {
                  @Override
                  public Void run() throws StreamingException {
                    abortImpl();
                    return null;
                  }
                }
        );
      } catch (IOException e) {
        throw new ImpersonationFailed("Failed impersonating proxy user '" + proxyUser +
                "' when aborting Txn on endPoint :" + endPt + ". Transaction Id: "
                + getCurrentTxnId(), e);
      }
    }

    private void abortImpl() throws TransactionError, StreamingException {
      try {
        recordWriter.clear();
        msClient.rollbackTxn(getCurrentTxnId());
        state = TxnState.ABORTED;
      } catch (NoSuchTxnException e) {
        throw new TransactionError("Unable to abort invalid transaction id : "
                + getCurrentTxnId(), e);
      } catch (TException e) {
        throw new TransactionError("Unable to abort transaction id : "
                + getCurrentTxnId(), e);
      }
    }

    @Override
    public void heartbeat() throws StreamingException, HeartBeatFailure {
      Long first = txnIds.get(currentTxnIndex);
      Long last = txnIds.get(txnIds.size()-1);
      try {
        HeartbeatTxnRangeResponse resp = msClient.heartbeatTxnRange(first, last);
        if (!resp.getAborted().isEmpty() || !resp.getNosuch().isEmpty()) {
          throw new HeartBeatFailure(resp.getAborted(), resp.getNosuch());
        }
      } catch (TException e) {
        throw new StreamingException("Failure to heartbeat on ids (" + first + "src/gen/thrift"
                + last + ") on end point : " + endPt );
      }
    }

    /**
     * Close the TransactionBatch
     * @throws StreamingIOFailure I/O failure when closing transaction batch
     */
    @Override
    public void close() throws StreamingException, ImpersonationFailed, InterruptedException {
      if (ugi==null) {
        state = TxnState.INACTIVE;
        recordWriter.closeBatch();
        return;
      }
      try {
        ugi.doAs (
                new PrivilegedExceptionAction<Void>() {
                  @Override
                  public Void run() throws StreamingException {
                    state = TxnState.INACTIVE;
                    recordWriter.closeBatch();
                    return null;
                  }
                }
        );
      } catch (IOException e) {
        throw new ImpersonationFailed("Failed impersonating proxy user '" + proxyUser +
                "' when closing Txn Batch on  endPoint :" + endPt, e);
      }
    }

    private static LockRequest createLockRequest(final HiveEndPoint hiveEndPoint,
            String partNameForLock, String user, long txnId)  {
      LockRequestBuilder rqstBuilder = new LockRequestBuilder();
      rqstBuilder.setUser(user);
      rqstBuilder.setTransactionId(txnId);

      LockComponentBuilder lockCompBuilder = new LockComponentBuilder()
              .setDbName(hiveEndPoint.database)
              .setTableName(hiveEndPoint.table)
              .setShared();
      if (partNameForLock!=null && !partNameForLock.isEmpty() ) {
          lockCompBuilder.setPartitionName(partNameForLock);
      }
      rqstBuilder.addLockComponent(lockCompBuilder.build());

      return rqstBuilder.build();
    }
  } // class TransactionBatchImpl

  static HiveConf createHiveConf(Class<?> clazz, String metaStoreUri) {
    HiveConf conf = new HiveConf(clazz);
    conf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER,
            "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, true);
    if (metaStoreUri!= null) {
      conf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUri);
    }
    return conf;
  }

}  // class HiveEndPoint
