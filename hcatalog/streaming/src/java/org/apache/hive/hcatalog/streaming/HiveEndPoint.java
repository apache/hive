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

package org.apache.hive.hcatalog.streaming;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
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
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.common.HCatUtil;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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


  static final private Logger LOG = LoggerFactory.getLogger(HiveEndPoint.class.getName());

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
   * @deprecated As of release 1.3/2.1.  Replaced by {@link #newConnection(boolean, String)}
   */
  @Deprecated
  public StreamingConnection newConnection(final boolean createPartIfNotExists)
    throws ConnectionError, InvalidPartition, InvalidTable, PartitionCreationFailed
    , ImpersonationFailed , InterruptedException {
    return newConnection(createPartIfNotExists, null, null, null);
  }
  /**
   * @deprecated As of release 1.3/2.1.  Replaced by {@link #newConnection(boolean, HiveConf, String)}
   */
  @Deprecated
  public StreamingConnection newConnection(final boolean createPartIfNotExists, HiveConf conf)
    throws ConnectionError, InvalidPartition, InvalidTable, PartitionCreationFailed
    , ImpersonationFailed , InterruptedException {
    return newConnection(createPartIfNotExists, conf, null, null);
  }
  /**
   * @deprecated As of release 1.3/2.1.  Replaced by {@link #newConnection(boolean, HiveConf, UserGroupInformation, String)}
   */
  @Deprecated
  public StreamingConnection newConnection(final boolean createPartIfNotExists, final HiveConf conf,
                                           final UserGroupInformation authenticatedUser)
    throws ConnectionError, InvalidPartition,
    InvalidTable, PartitionCreationFailed, ImpersonationFailed , InterruptedException {
    return newConnection(createPartIfNotExists, conf, authenticatedUser, null);
  }
  /**
   * Acquire a new connection to MetaStore for streaming
   * @param createPartIfNotExists If true, the partition specified in the endpoint
   *                              will be auto created if it does not exist
   * @param agentInfo should uniquely identify the process/entity that is using this batch.  This
   *                  should be something that can be correlated with calling application log files
   *                  and/or monitoring consoles.
   * @return
   * @throws ConnectionError if problem connecting
   * @throws InvalidPartition  if specified partition is not valid (createPartIfNotExists = false)
   * @throws ImpersonationFailed  if not able to impersonate 'proxyUser'
   * @throws PartitionCreationFailed if failed to create partition
   * @throws InterruptedException
   */
  public StreamingConnection newConnection(final boolean createPartIfNotExists, String agentInfo)
    throws ConnectionError, InvalidPartition, InvalidTable, PartitionCreationFailed
    , ImpersonationFailed , InterruptedException {
    return newConnection(createPartIfNotExists, null, null, agentInfo);
  }

  /**
   * Acquire a new connection to MetaStore for streaming
   * @param createPartIfNotExists If true, the partition specified in the endpoint
   *                              will be auto created if it does not exist
   * @param conf HiveConf object, set it to null if not using advanced hive settings.
   * @param agentInfo should uniquely identify the process/entity that is using this batch.  This
   *                  should be something that can be correlated with calling application log files
   *                  and/or monitoring consoles.
   * @return
   * @throws ConnectionError if problem connecting
   * @throws InvalidPartition  if specified partition is not valid (createPartIfNotExists = false)
   * @throws ImpersonationFailed  if not able to impersonate 'proxyUser'
   * @throws PartitionCreationFailed if failed to create partition
   * @throws InterruptedException
   */
  public StreamingConnection newConnection(final boolean createPartIfNotExists, HiveConf conf, String agentInfo)
          throws ConnectionError, InvalidPartition, InvalidTable, PartitionCreationFailed
          , ImpersonationFailed , InterruptedException {
    return newConnection(createPartIfNotExists, conf, null, agentInfo);
  }

  /**
   * Acquire a new connection to MetaStore for streaming. To connect using Kerberos,
   *   'authenticatedUser' argument should have been used to do a kerberos login.  Additionally the
   *   'hive.metastore.kerberos.principal' setting should be set correctly either in hive-site.xml or
   *    in the 'conf' argument (if not null). If using hive-site.xml, it should be in classpath.
   *
   * @param createPartIfNotExists If true, the partition specified in the endpoint
   *                              will be auto created if it does not exist
   * @param conf               HiveConf object to be used for the connection. Can be null.
   * @param authenticatedUser  UserGroupInformation object obtained from successful authentication.
   *                           Uses non-secure mode if this argument is null.
   * @param agentInfo should uniquely identify the process/entity that is using this batch.  This
   *                  should be something that can be correlated with calling application log files
   *                  and/or monitoring consoles.
   * @return
   * @throws ConnectionError if there is a connection problem
   * @throws InvalidPartition  if specified partition is not valid (createPartIfNotExists = false)
   * @throws ImpersonationFailed  if not able to impersonate 'username'
   * @throws PartitionCreationFailed if failed to create partition
   * @throws InterruptedException
   */
  public StreamingConnection newConnection(final boolean createPartIfNotExists, final HiveConf conf,
                                           final UserGroupInformation authenticatedUser, final String agentInfo)
          throws ConnectionError, InvalidPartition,
               InvalidTable, PartitionCreationFailed, ImpersonationFailed , InterruptedException {

    if( authenticatedUser==null ) {
      return newConnectionImpl(authenticatedUser, createPartIfNotExists, conf, agentInfo);
    }

    try {
      return authenticatedUser.doAs (
             new PrivilegedExceptionAction<StreamingConnection>() {
                @Override
                public StreamingConnection run()
                        throws ConnectionError, InvalidPartition, InvalidTable
                        , PartitionCreationFailed {
                  return newConnectionImpl(authenticatedUser, createPartIfNotExists, conf, agentInfo);
                }
             }
      );
    } catch (IOException e) {
      throw new ConnectionError("Failed to connect as : " + authenticatedUser.getShortUserName(), e);
    }
  }

  private StreamingConnection newConnectionImpl(UserGroupInformation ugi,
                                               boolean createPartIfNotExists, HiveConf conf, String agentInfo)
          throws ConnectionError, InvalidPartition, InvalidTable
          , PartitionCreationFailed {
    return new ConnectionImpl(this, ugi, conf, createPartIfNotExists, agentInfo);
  }

  private static UserGroupInformation getUserGroupInfo(String user)
          throws ImpersonationFailed {
    try {
      return UserGroupInformation.createProxyUser(
              user, UserGroupInformation.getLoginUser());
    } catch (IOException e) {
      LOG.error("Unable to get UserGroupInfo for user : " + user, e);
      throw new ImpersonationFailed(user,e);
    }
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
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
    private final IMetaStoreClient heartbeaterMSClient;
    private final HiveEndPoint endPt;
    private final UserGroupInformation ugi;
    private final String username;
    private final boolean secureMode;
    private final String agentInfo;

    /**
     * @param endPoint end point to connect to
     * @param ugi on behalf of whom streaming is done. cannot be null
     * @param conf HiveConf object
     * @param createPart create the partition if it does not exist
     * @throws ConnectionError if there is trouble connecting
     * @throws InvalidPartition if specified partition does not exist (and createPart=false)
     * @throws InvalidTable if specified table does not exist
     * @throws PartitionCreationFailed if createPart=true and not able to create partition
     */
    private ConnectionImpl(HiveEndPoint endPoint, UserGroupInformation ugi,
                           HiveConf conf, boolean createPart, String agentInfo)
            throws ConnectionError, InvalidPartition, InvalidTable
                   , PartitionCreationFailed {
      this.endPt = endPoint;
      this.ugi = ugi;
      this.agentInfo = agentInfo;
      this.username = ugi==null ? System.getProperty("user.name") : ugi.getShortUserName();
      if (conf==null) {
        conf = HiveEndPoint.createHiveConf(this.getClass(), endPoint.metaStoreUri);
      }
      else {
          overrideConfSettings(conf);
      }
      this.secureMode = ugi==null ? false : ugi.hasKerberosCredentials();
      this.msClient = getMetaStoreClient(endPoint, conf, secureMode);
      // We use a separate metastore client for heartbeat calls to ensure heartbeat RPC calls are
      // isolated from the other transaction related RPC calls.
      this.heartbeaterMSClient = getMetaStoreClient(endPoint, conf, secureMode);
      checkEndPoint(endPoint, msClient);
      if (createPart  &&  !endPoint.partitionVals.isEmpty()) {
        createPartitionIfNotExists(endPoint, msClient, conf);
      }
    }

    /**
     * Checks the validity of endpoint
     * @param endPoint the HiveEndPoint to be checked
     * @param msClient the metastore client
     * @throws InvalidTable
     */
    private void checkEndPoint(HiveEndPoint endPoint, IMetaStoreClient msClient)
        throws InvalidTable, ConnectionError {
      Table t;
      try {
        t = msClient.getTable(endPoint.database, endPoint.table);
      } catch (Exception e) {
        LOG.warn("Unable to check the endPoint: " + endPoint, e);
        throw new InvalidTable(endPoint.database, endPoint.table, e);
      }
      // 1 - check that the table is Acid
      if (!AcidUtils.isFullAcidTable(t)) {
        LOG.error("HiveEndPoint " + endPoint + " must use an acid table");
        throw new InvalidTable(endPoint.database, endPoint.table, "is not an Acid table");
      }

      // 2 - check if partitionvals are legitimate
      if (t.getPartitionKeys() != null && !t.getPartitionKeys().isEmpty()
          && endPoint.partitionVals.isEmpty()) {
        // Invalid if table is partitioned, but endPoint's partitionVals is empty
        String errMsg = "HiveEndPoint " + endPoint + " doesn't specify any partitions for " +
            "partitioned table";
        LOG.error(errMsg);
        throw new ConnectionError(errMsg);
      }
      if ((t.getPartitionKeys() == null || t.getPartitionKeys().isEmpty())
          && !endPoint.partitionVals.isEmpty()) {
        // Invalid if table is not partitioned, but endPoint's partitionVals is not empty
        String errMsg = "HiveEndPoint" + endPoint + " specifies partitions for unpartitioned table";
        LOG.error(errMsg);
        throw new ConnectionError(errMsg);
      }
    }

    /**
     * Close connection
     */
    @Override
    public void close() {
      if (ugi==null) {
        msClient.close();
        heartbeaterMSClient.close();
        return;
      }
      try {
        ugi.doAs (
            new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                msClient.close();
                heartbeaterMSClient.close();
                return null;
              }
            } );
        try {
          FileSystem.closeAllForUGI(ugi);
        } catch (IOException exception) {
          LOG.error("Could not clean up file-system handles for UGI: " + ugi, exception);
        }
      } catch (IOException e) {
        LOG.error("Error closing connection to " + endPt, e);
      } catch (InterruptedException e) {
        LOG.error("Interrupted when closing connection to " + endPt, e);
      }
    }

    @Override
    public UserGroupInformation getUserGroupInformation() {
      return ugi;
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
    @Override
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
                  public TransactionBatch run() throws StreamingException, InterruptedException {
                    return fetchTransactionBatchImpl(numTransactions, recordWriter);
                  }
                }
        );
      } catch (IOException e) {
        throw new ImpersonationFailed("Failed to fetch Txn Batch as user '" + ugi.getShortUserName()
                + "' when acquiring Transaction Batch on endPoint " + endPt, e);
      }
    }

    private TransactionBatch fetchTransactionBatchImpl(int numTransactions,
                                                  RecordWriter recordWriter)
            throws StreamingException, TransactionBatchUnAvailable, InterruptedException {
      return new TransactionBatchImpl(username, ugi, endPt, numTransactions, msClient,
          heartbeaterMSClient, recordWriter, agentInfo);
    }


    private static void createPartitionIfNotExists(HiveEndPoint ep,
                                                   IMetaStoreClient msClient, HiveConf conf)
            throws InvalidTable, PartitionCreationFailed {
      if (ep.partitionVals.isEmpty()) {
        return;
      }
      SessionState localSession = null;
      if(SessionState.get() == null) {
        localSession = SessionState.start(new CliSessionState(conf));
      }
      IDriver driver = DriverFactory.newDriver(conf);

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
          if(localSession != null) {
            localSession.close();
          }
        } catch (IOException e) {
          LOG.warn("Error closing SessionState used to run Hive DDL.");
        }
      }
    }

    private static boolean runDDL(IDriver driver, String sql) throws QueryFailedException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Running Hive Query: " + sql);
      }
      driver.run(sql);
      return true;
    }

    private static String partSpecStr(List<FieldSchema> partKeys, ArrayList<String> partVals) {
      if (partKeys.size()!=partVals.size()) {
        throw new IllegalArgumentException("Partition values:" + partVals +
                ", does not match the partition Keys in table :" + partKeys );
      }
      StringBuilder buff = new StringBuilder(partKeys.size()*20);
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

    private static IMetaStoreClient getMetaStoreClient(HiveEndPoint endPoint, HiveConf conf, boolean secureMode)
            throws ConnectionError {

      if (endPoint.metaStoreUri!= null) {
        conf.setVar(HiveConf.ConfVars.METASTOREURIS, endPoint.metaStoreUri);
      }
      if(secureMode) {
        conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL,true);
      }
      try {
        return HCatUtil.getHiveMetastoreClient(conf);
      } catch (MetaException e) {
        throw new ConnectionError("Error connecting to Hive Metastore URI: "
                + endPoint.metaStoreUri + ". " + e.getMessage(), e);
      } catch (IOException e) {
        throw new ConnectionError("Error connecting to Hive Metastore URI: "
            + endPoint.metaStoreUri + ". " + e.getMessage(), e);
      }
    }
  } // class ConnectionImpl

  private static class TransactionBatchImpl implements TransactionBatch {
    private final String username;
    private final UserGroupInformation ugi;
    private final HiveEndPoint endPt;
    private final IMetaStoreClient msClient;
    private final IMetaStoreClient heartbeaterMSClient;
    private final RecordWriter recordWriter;
    private final List<Long> txnIds;

    //volatile because heartbeat() may be in a "different" thread; updates of this are "piggybacking"
    private volatile int currentTxnIndex = -1;
    private final String partNameForLock;
    //volatile because heartbeat() may be in a "different" thread
    private volatile TxnState state;
    private LockRequest lockRequest = null;
    /**
     * once any operation on this batch encounters a system exception
     * (e.g. IOException on write) it's safest to assume that we can't write to the
     * file backing this batch any more.  This guards important public methods
     */
    private volatile boolean isClosed = false;
    private final String agentInfo;
    /**
     * Tracks the state of each transaction
     */
    private final TxnState[] txnStatus;
    /**
     * ID of the last txn used by {@link #beginNextTransactionImpl()}
     */
    private long lastTxnUsed;

    /**
     * Represents a batch of transactions acquired from MetaStore
     *
     * @throws StreamingException if failed to create new RecordUpdater for batch
     * @throws TransactionBatchUnAvailable if failed to acquire a new Transaction batch
     */
    private TransactionBatchImpl(final String user, UserGroupInformation ugi, HiveEndPoint endPt,
        final int numTxns, final IMetaStoreClient msClient,
        final IMetaStoreClient heartbeaterMSClient, RecordWriter recordWriter, String agentInfo)
        throws StreamingException, TransactionBatchUnAvailable, InterruptedException {
      boolean success = false;
      try {
        if ( endPt.partitionVals!=null   &&   !endPt.partitionVals.isEmpty() ) {
          Table tableObj = msClient.getTable(endPt.database, endPt.table);
          List<FieldSchema> partKeys = tableObj.getPartitionKeys();
          partNameForLock = Warehouse.makePartName(partKeys, endPt.partitionVals);
        } else {
          partNameForLock = null;
        }
        this.username = user;
        this.ugi = ugi;
        this.endPt = endPt;
        this.msClient = msClient;
        this.heartbeaterMSClient = heartbeaterMSClient;
        this.recordWriter = recordWriter;
        this.agentInfo = agentInfo;

        txnIds = openTxnImpl(msClient, user, numTxns, ugi);
        txnStatus = new TxnState[numTxns];
        for(int i = 0; i < txnStatus.length; i++) {
          txnStatus[i] = TxnState.OPEN;//Open matches Metastore state
        }

        this.state = TxnState.INACTIVE;
        recordWriter.newBatch(txnIds.get(0), txnIds.get(txnIds.size()-1));
        success = true;
      } catch (TException e) {
        throw new TransactionBatchUnAvailable(endPt, e);
      } catch (IOException e) {
        throw new TransactionBatchUnAvailable(endPt, e);
      }
      finally {
        //clean up if above throws
        markDead(success);
      }
    }

    private List<Long> openTxnImpl(final IMetaStoreClient msClient, final String user, final int numTxns, UserGroupInformation ugi)
            throws IOException, TException,  InterruptedException {
      if(ugi==null) {
        return  msClient.openTxns(user, numTxns).getTxn_ids();
      }
      return (List<Long>) ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          return msClient.openTxns(user, numTxns).getTxn_ids();
        }
      }) ;
    }

    @Override
    public String toString() {
      if (txnIds==null || txnIds.isEmpty()) {
        return "{}";
      }
      StringBuilder sb = new StringBuilder(" TxnStatus[");
      for(TxnState state : txnStatus) {
        //'state' should not be null - future proofing
        sb.append(state == null ? "N" : state);
      }
      sb.append("] LastUsed ").append(JavaUtils.txnIdToString(lastTxnUsed));
      return "TxnIds=[" + txnIds.get(0) + "..." + txnIds.get(txnIds.size()-1)
              + "] on endPoint = " + endPt + "; " + sb;
    }

    /**
     * Activate the next available transaction in the current transaction batch
     * @throws TransactionError failed to switch to next transaction
     */
    @Override
    public void beginNextTransaction() throws TransactionError, ImpersonationFailed,
            InterruptedException {
      checkIsClosed();
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
        throw new ImpersonationFailed("Failed switching to next Txn as user '" + username +
                "' in Txn batch :" + this, e);
      }
    }

    private void beginNextTransactionImpl() throws TransactionError {
      state = TxnState.INACTIVE;//clear state from previous txn
      if ( currentTxnIndex + 1 >= txnIds.size() ) {
        throw new InvalidTrasactionState("No more transactions available in" +
                " current batch for end point : " + endPt);
      }
      ++currentTxnIndex;
      state = TxnState.OPEN;
      lastTxnUsed = getCurrentTxnId();
      lockRequest = createLockRequest(endPt, partNameForLock, username, getCurrentTxnId(), agentInfo);
      try {
        LockResponse res = msClient.lock(lockRequest);
        if (res.getState() != LockState.ACQUIRED) {
          throw new TransactionError("Unable to acquire lock on " + endPt);
        }
      } catch (TException e) {
        throw new TransactionError("Unable to acquire lock on " + endPt, e);
      }
    }

    /**
     * Get Id of currently open transaction
     * @return -1 if there is no open TX
     */
    @Override
    public Long getCurrentTxnId() {
      if(currentTxnIndex >= 0) {
        return txnIds.get(currentTxnIndex);
      }
      return -1L;
    }

    /**
     * get state of current transaction
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
            throws StreamingException, InterruptedException {
      write(Collections.singletonList(record));
    }
    private void checkIsClosed() throws IllegalStateException {
      if(isClosed) {
        throw new IllegalStateException("TransactionBatch " + toString() + " has been closed()");
      }
    }
    /**
     * A transaction batch opens a single HDFS file and writes multiple transaction to it.  If there is any issue
     * with the write, we can't continue to write to the same file any as it may be corrupted now (at the tail).
     * This ensures that a client can't ignore these failures and continue to write.
     */
    private void markDead(boolean success) {
      if(success) {
        return;
      }
      isClosed = true;//also ensures that heartbeat() is no-op since client is likely doing it async
      try {
        abort(true);//abort all remaining txns
      }
      catch(Exception ex) {
        LOG.error("Fatal error on " + toString() + "; cause " + ex.getMessage(), ex);
      }
      try {
        closeImpl();
      }
      catch (Exception ex) {
        LOG.error("Fatal error on " + toString() + "; cause " + ex.getMessage(), ex);
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
      checkIsClosed();
      boolean success = false;
      try {
        if (ugi == null) {
          writeImpl(records);
        } else {
          ugi.doAs(
            new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws StreamingException {
                writeImpl(records);
                return null;
              }
            }
          );
        }
        success = true;
      } catch(SerializationError ex) {
        //this exception indicates that a {@code record} could not be parsed and the
        //caller can decide whether to drop it or send it to dead letter queue.
        //rolling back the txn and retrying won't help since the tuple will be exactly the same
        //when it's replayed.
        success = true;
        throw ex;
      } catch(IOException e){
        throw new ImpersonationFailed("Failed writing as user '" + username +
          "' to endPoint :" + endPt + ". Transaction Id: "
          + getCurrentTxnId(), e);
      }
      finally {
        markDead(success);
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
      checkIsClosed();
      boolean success = false;
      try {
        if (ugi == null) {
          commitImpl();
        }
        else {
          ugi.doAs(
            new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws StreamingException {
                commitImpl();
                return null;
              }
            }
          );
        }
        success = true;
      } catch (IOException e) {
        throw new ImpersonationFailed("Failed committing Txn ID " + getCurrentTxnId() + " as user '"
                + username + "'on endPoint :" + endPt + ". Transaction Id: ", e);
      }
      finally {
        markDead(success);
      }
    }

    private void commitImpl() throws TransactionError, StreamingException {
      try {
        recordWriter.flush();
        msClient.commitTxn(txnIds.get(currentTxnIndex));
        state = TxnState.COMMITTED;
        txnStatus[currentTxnIndex] = TxnState.COMMITTED;
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
      if(isClosed) {
        /**
         * isDead is only set internally by this class.  {@link #markDead(boolean)} will abort all
         * remaining txns, so make this no-op to make sure that a well-behaved client that calls abort()
         * error doesn't get misleading errors
         */
        return;
      }
      abort(false);
    }
    private void abort(final boolean abortAllRemaining) throws TransactionError, StreamingException
        , ImpersonationFailed, InterruptedException {
      if (ugi==null) {
        abortImpl(abortAllRemaining);
        return;
      }
      try {
        ugi.doAs (
                new PrivilegedExceptionAction<Void>() {
                  @Override
                  public Void run() throws StreamingException {
                    abortImpl(abortAllRemaining);
                    return null;
                  }
                }
        );
      } catch (IOException e) {
        throw new ImpersonationFailed("Failed aborting Txn " + getCurrentTxnId()  + " as user '"
                + username + "' on endPoint :" + endPt, e);
      }
    }

    private void abortImpl(boolean abortAllRemaining) throws TransactionError, StreamingException {
      try {
        if(abortAllRemaining) {
          //when last txn finished (abort/commit) the currentTxnIndex is pointing at that txn
          //so we need to start from next one, if any.  Also if batch was created but
          //fetchTransactionBatch() was never called, we want to start with first txn
          int minOpenTxnIndex = Math.max(currentTxnIndex +
            (state == TxnState.ABORTED || state == TxnState.COMMITTED ? 1 : 0), 0);
          for(currentTxnIndex = minOpenTxnIndex;
              currentTxnIndex < txnIds.size(); currentTxnIndex++) {
            msClient.rollbackTxn(txnIds.get(currentTxnIndex));
            txnStatus[currentTxnIndex] = TxnState.ABORTED;
          }
          currentTxnIndex--;//since the loop left it == txnId.size()
        }
        else {
          if (getCurrentTxnId() > 0) {
            msClient.rollbackTxn(getCurrentTxnId());
            txnStatus[currentTxnIndex] = TxnState.ABORTED;
          }
        }
        state = TxnState.ABORTED;
        recordWriter.clear();
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
      if(isClosed) {
        return;
      }
      if(state != TxnState.OPEN && currentTxnIndex >= txnIds.size() - 1) {
        //here means last txn in the batch is resolved but the close() hasn't been called yet so
        //there is nothing to heartbeat
        return;
      }
      //if here after commit()/abort() but before next beginNextTransaction(), currentTxnIndex still
      //points at the last txn which we don't want to heartbeat
      Long first = txnIds.get(state == TxnState.OPEN ? currentTxnIndex : currentTxnIndex + 1);
      Long last = txnIds.get(txnIds.size()-1);
      try {
        HeartbeatTxnRangeResponse resp = heartbeaterMSClient.heartbeatTxnRange(first, last);
        if (!resp.getAborted().isEmpty() || !resp.getNosuch().isEmpty()) {
          throw new HeartBeatFailure(resp.getAborted(), resp.getNosuch());
        }
      } catch (TException e) {
        throw new StreamingException("Failure to heartbeat on ids (" + first + "src/gen/thrift"
                + last + ") on end point : " + endPt );
      }
    }

    @Override
    public boolean isClosed() {
      return isClosed;
    }
    /**
     * Close the TransactionBatch.  This will abort any still open txns in this batch.
     * @throws StreamingIOFailure I/O failure when closing transaction batch
     */
    @Override
    public void close() throws StreamingException, ImpersonationFailed, InterruptedException {
      if(isClosed) {
        return;
      }
      isClosed = true;
      abortImpl(true);//abort proactively so that we don't wait for timeout
      closeImpl();//perhaps we should add a version of RecordWriter.closeBatch(boolean abort) which
      //will call RecordUpdater.close(boolean abort)
    }
    private void closeImpl() throws StreamingException, InterruptedException{
      state = TxnState.INACTIVE;
      if(ugi == null) {
        recordWriter.closeBatch();
        return;
      }
      try {
        ugi.doAs (
                new PrivilegedExceptionAction<Void>() {
                  @Override
                  public Void run() throws StreamingException {
                    recordWriter.closeBatch();
                    return null;
                  }
                }
        );
        try {
          FileSystem.closeAllForUGI(ugi);
        } catch (IOException exception) {
          LOG.error("Could not clean up file-system handles for UGI: " + ugi, exception);
        }
      } catch (IOException e) {
        throw new ImpersonationFailed("Failed closing Txn Batch as user '" + username +
                "' on  endPoint :" + endPt, e);
      }
    }

    private static LockRequest createLockRequest(final HiveEndPoint hiveEndPoint,
            String partNameForLock, String user, long txnId, String agentInfo)  {
      LockRequestBuilder rqstBuilder = agentInfo == null ?
        new LockRequestBuilder() : new LockRequestBuilder(agentInfo);
      rqstBuilder.setUser(user);
      rqstBuilder.setTransactionId(txnId);

      LockComponentBuilder lockCompBuilder = new LockComponentBuilder()
              .setDbName(hiveEndPoint.database)
              .setTableName(hiveEndPoint.table)
              .setShared()
              .setOperationType(DataOperationType.INSERT);
      if (partNameForLock!=null && !partNameForLock.isEmpty() ) {
          lockCompBuilder.setPartitionName(partNameForLock);
      }
      rqstBuilder.addLockComponent(lockCompBuilder.build());

      return rqstBuilder.build();
    }
  } // class TransactionBatchImpl

  static HiveConf createHiveConf(Class<?> clazz, String metaStoreUri) {
    HiveConf conf = new HiveConf(clazz);
    if (metaStoreUri!= null) {
      setHiveConf(conf, HiveConf.ConfVars.METASTOREURIS, metaStoreUri);
    }
    HiveEndPoint.overrideConfSettings(conf);
    return conf;
  }

  private static void overrideConfSettings(HiveConf conf) {
    setHiveConf(conf, HiveConf.ConfVars.HIVE_TXN_MANAGER,
            "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    setHiveConf(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    setHiveConf(conf, HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, true);
    // Avoids creating Tez Client sessions internally as it takes much longer currently
    setHiveConf(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
  }

  private static void setHiveConf(HiveConf conf, HiveConf.ConfVars var, String value) {
    if( LOG.isDebugEnabled() ) {
      LOG.debug("Overriding HiveConf setting : " + var + " = " + value);
    }
    conf.setVar(var, value);
  }

  private static void setHiveConf(HiveConf conf, HiveConf.ConfVars var, boolean value) {
    if( LOG.isDebugEnabled() ) {
      LOG.debug("Overriding HiveConf setting : " + var + " = " + value);
    }
    conf.setBoolVar(var, value);
  }

}  // class HiveEndPoint
