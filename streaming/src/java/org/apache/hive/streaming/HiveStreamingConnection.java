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

package org.apache.hive.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Streaming connection implementation for hive. To create a streaming connection, use the builder API
 * to create record writer first followed by the connection itself. Once connection is created, clients can
 * begin a transaction, keep writing using the connection, commit the transaction and close connection when done.
 * To bind to the correct metastore, HiveConf object has to be created from hive-site.xml or HIVE_CONF_DIR.
 * If hive conf is manually created, metastore uri has to be set correctly. If hive conf object is not specified,
 * "thrift://localhost:9083" will be used as default.
 * <br/><br/>
 * NOTE: The streaming connection APIs and record writer APIs are not thread-safe. Streaming connection creation,
 * begin/commit/abort transactions, write and close has to be called in the same thread. If close() or
 * abortTransaction() has to be triggered from a separate thread it has to be co-ordinated via external variables or
 * synchronization mechanism
 * <br/><br/>
 * Example usage:
 * <pre>{@code
 * // create delimited record writer whose schema exactly matches table schema
 * StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
 *                                      .withFieldDelimiter(',')
 *                                      .build();
 * // create and open streaming connection (default.src table has to exist already)
 * StreamingConnection connection = HiveStreamingConnection.newBuilder()
 *                                    .withDatabase("default")
 *                                    .withTable("src")
 *                                    .withAgentInfo("nifi-agent")
 *                                    .withRecordWriter(writer)
 *                                    .withHiveConf(hiveConf)
 *                                    .connect();
 * // begin a transaction, write records and commit 1st transaction
 * connection.beginTransaction();
 * connection.write("key1,val1".getBytes());
 * connection.write("key2,val2".getBytes());
 * connection.commitTransaction();
 * // begin another transaction, write more records and commit 2nd transaction
 * connection.beginTransaction();
 * connection.write("key3,val3".getBytes());
 * connection.write("key4,val4".getBytes());
 * connection.commitTransaction();
 * // close the streaming connection
 * connection.close();
 * }
 * </pre>
 */
public class HiveStreamingConnection implements StreamingConnection {
  private static final Logger LOG = LoggerFactory.getLogger(HiveStreamingConnection.class.getName());

  private static final String DEFAULT_METASTORE_URI = "thrift://localhost:9083";
  private static final int DEFAULT_TRANSACTION_BATCH_SIZE = 1;
  private static final int DEFAULT_HEARTBEAT_INTERVAL = 60 * 1000;
  private static final boolean DEFAULT_STREAMING_OPTIMIZATIONS_ENABLED = true;

  public enum TxnState {
    INACTIVE("I"), OPEN("O"), COMMITTED("C"), ABORTED("A");

    private final String code;

    TxnState(String code) {
      this.code = code;
    }

    public String toString() {
      return code;
    }
  }

  // fields populated from builder
  private String database;
  private String table;
  private List<String> staticPartitionValues;
  private String agentInfo;
  private int transactionBatchSize;
  private RecordWriter recordWriter;
  private TransactionBatch currentTransactionBatch;
  private HiveConf conf;
  private boolean streamingOptimizations;
  private AtomicBoolean isConnectionClosed = new AtomicBoolean(false);

  // internal fields
  private boolean isPartitionedTable;
  private IMetaStoreClient msClient;
  private IMetaStoreClient heartbeatMSClient;
  private final String username;
  private final boolean secureMode;
  private Table tableObject = null;
  private String metastoreUri;
  private ConnectionStats connectionStats;

  private HiveStreamingConnection(Builder builder) throws StreamingException {
    this.database = builder.database.toLowerCase();
    this.table = builder.table.toLowerCase();
    this.staticPartitionValues = builder.staticPartitionValues;
    this.conf = builder.hiveConf;
    this.agentInfo = builder.agentInfo;
    this.streamingOptimizations = builder.streamingOptimizations;
    UserGroupInformation loggedInUser = null;
    try {
      loggedInUser = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      LOG.warn("Unable to get logged in user via UGI. err: {}", e.getMessage());
    }
    if (loggedInUser == null) {
      this.username = System.getProperty("user.name");
      this.secureMode = false;
    } else {
      this.username = loggedInUser.getShortUserName();
      this.secureMode = loggedInUser.hasKerberosCredentials();
    }
    this.transactionBatchSize = builder.transactionBatchSize;
    this.recordWriter = builder.recordWriter;
    this.connectionStats = new ConnectionStats();
    if (agentInfo == null) {
      try {
        agentInfo = username + ":" + InetAddress.getLocalHost().getHostName() + ":" + Thread.currentThread().getName();
      } catch (UnknownHostException e) {
        // ignore and use UUID instead
        this.agentInfo = UUID.randomUUID().toString();
      }
    }
    if (conf == null) {
      conf = createHiveConf(this.getClass(), DEFAULT_METASTORE_URI);
    }
    overrideConfSettings(conf);
    this.metastoreUri = conf.get(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName());
    this.msClient = getMetaStoreClient(conf, metastoreUri, secureMode, "streaming-connection");
    // We use a separate metastore client for heartbeat calls to ensure heartbeat RPC calls are
    // isolated from the other transaction related RPC calls.
    this.heartbeatMSClient = getMetaStoreClient(conf, metastoreUri, secureMode, "streaming-connection-heartbeat");
    validateTable();

    LOG.info("STREAMING CONNECTION INFO: {}", toConnectionInfoString());
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String database;
    private String table;
    private List<String> staticPartitionValues;
    private String agentInfo;
    private HiveConf hiveConf;
    private int transactionBatchSize = DEFAULT_TRANSACTION_BATCH_SIZE;
    private boolean streamingOptimizations = DEFAULT_STREAMING_OPTIMIZATIONS_ENABLED;
    private RecordWriter recordWriter;

    /**
     * Specify database to use for streaming connection.
     *
     * @param database - db name
     * @return - builder
     */
    public Builder withDatabase(final String database) {
      this.database = database;
      return this;
    }

    /**
     * Specify table to use for streaming connection.
     *
     * @param table - table name
     * @return - builder
     */
    public Builder withTable(final String table) {
      this.table = table;
      return this;
    }

    /**
     * Specify the name of partition to use for streaming connection.
     *
     * @param staticPartitionValues - static partition values
     * @return - builder
     */
    public Builder withStaticPartitionValues(final List<String> staticPartitionValues) {
      this.staticPartitionValues = staticPartitionValues == null ? null : new ArrayList<>(staticPartitionValues);
      return this;
    }

    /**
     * Specify agent info to use for streaming connection.
     *
     * @param agentInfo - agent info
     * @return - builder
     */
    public Builder withAgentInfo(final String agentInfo) {
      this.agentInfo = agentInfo;
      return this;
    }

    /**
     * Specify hive configuration object to use for streaming connection.
     * Generate this object by point to already existing hive-site.xml or HIVE_CONF_DIR.
     * Make sure if metastore URI has been set correctly else thrift://localhost:9083 will be
     * used as default.
     *
     * @param hiveConf - hive conf object
     * @return - builder
     */
    public Builder withHiveConf(final HiveConf hiveConf) {
      this.hiveConf = hiveConf;
      return this;
    }

    /**
     * Transaction batch size to use (default value is 10). This is expert level configuration.
     * For every transaction batch a delta directory will be created which will impact
     * when compaction will trigger.
     * NOTE: This is evolving API and is subject to change/might not be honored in future releases.
     *
     * @param transactionBatchSize - transaction batch size
     * @return - builder
     */
    @InterfaceStability.Evolving
    public Builder withTransactionBatchSize(final int transactionBatchSize) {
      this.transactionBatchSize = transactionBatchSize;
      return this;
    }

    /**
     * Whether to enable streaming optimizations. This is expert level configurations.
     * Disabling streaming optimizations will have significant impact to performance and memory consumption.
     *
     * @param enable - flag to enable or not
     * @return - builder
     */
    public Builder withStreamingOptimizations(final boolean enable) {
      this.streamingOptimizations = enable;
      return this;
    }

    /**
     * Record writer to use for writing records to destination table.
     *
     * @param recordWriter - record writer
     * @return - builder
     */
    public Builder withRecordWriter(final RecordWriter recordWriter) {
      this.recordWriter = recordWriter;
      return this;
    }

    /**
     * Returning a streaming connection to hive.
     *
     * @return - hive streaming connection
     */
    public HiveStreamingConnection connect() throws StreamingException {
      if (database == null) {
        throw new StreamingException("Database cannot be null for streaming connection");
      }
      if (table == null) {
        throw new StreamingException("Table cannot be null for streaming connection");
      }
      if (recordWriter == null) {
        throw new StreamingException("Record writer cannot be null for streaming connection");
      }
      HiveStreamingConnection streamingConnection = new HiveStreamingConnection(this);
      // assigning higher priority than FileSystem shutdown hook so that streaming connection gets closed first before
      // filesystem close (to avoid ClosedChannelException)
      ShutdownHookManager.addShutdownHook(streamingConnection::close,  FileSystem.SHUTDOWN_HOOK_PRIORITY + 1);
      Thread.setDefaultUncaughtExceptionHandler((t, e) -> streamingConnection.close());
      return streamingConnection;
    }
  }

  private void setPartitionedTable(boolean isPartitionedTable) {
    this.isPartitionedTable = isPartitionedTable;
  }

  @Override
  public String toString() {
    return "{ metaStoreUri: " + metastoreUri + ", database: " + database + ", table: " + table + " }";
  }

  private String toConnectionInfoString() {
    return "{ metastore-uri: " + metastoreUri + ", " +
      "database: " + database + ", " +
      "table: " + table + ", " +
      "partitioned-table: " + isPartitionedTable() + ", " +
      "dynamic-partitioning: " + isDynamicPartitioning() + ", " +
      "username: " + username + ", " +
      "secure-mode: " + secureMode + ", " +
      "record-writer: " + recordWriter.getClass().getSimpleName() + ", " +
      "agent-info: " + agentInfo + " }";
  }

  @VisibleForTesting
  String toTransactionString() {
    return currentTransactionBatch == null ? "" : currentTransactionBatch.toString();
  }

  @Override
  public PartitionInfo createPartitionIfNotExists(final List<String> partitionValues) throws StreamingException {
    String partLocation = null;
    String partName = null;
    boolean exists = false;
    try {
      Map<String, String> partSpec = Warehouse.makeSpecFromValues(tableObject.getPartitionKeys(), partitionValues);
      AddPartitionDesc addPartitionDesc = new AddPartitionDesc(database, table, true);
      partName = Warehouse.makePartName(tableObject.getPartitionKeys(), partitionValues);
      partLocation = new Path(tableObject.getDataLocation(), Warehouse.makePartPath(partSpec)).toString();
      addPartitionDesc.addPartition(partSpec, partLocation);
      Partition partition = Hive.convertAddSpecToMetaPartition(tableObject, addPartitionDesc.getPartition(0), conf);
      getMSC().add_partition(partition);
    } catch (AlreadyExistsException e) {
      exists = true;
    } catch (HiveException | TException e) {
      throw new StreamingException("Unable to creation partition for values: " + partitionValues + " connection: " +
        toConnectionInfoString(), e);
    }
    return new PartitionInfo(partName, partLocation, exists);
  }

  IMetaStoreClient getMSC() {
    connectionStats.incrementMetastoreCalls();
    return msClient;
  }

  IMetaStoreClient getHeatbeatMSC() {
    connectionStats.incrementMetastoreCalls();
    return heartbeatMSClient;
  }

  private void validateTable() throws InvalidTable, ConnectionError {
    try {
      tableObject = new Table(getMSC().getTable(database, table));
    } catch (Exception e) {
      LOG.warn("Unable to validate the table for connection: " + toConnectionInfoString(), e);
      throw new InvalidTable(database, table, e);
    }
    // 1 - check that the table is Acid
    if (!AcidUtils.isFullAcidTable(tableObject)) {
      LOG.error("HiveEndPoint " + this + " must use an acid table");
      throw new InvalidTable(database, table, "is not an Acid table");
    }

    if (tableObject.getPartitionKeys() != null && !tableObject.getPartitionKeys().isEmpty()) {
      setPartitionedTable(true);
    } else {
      setPartitionedTable(false);
    }

    // partition values are specified on non-partitioned table
    if (!isPartitionedTable() && (staticPartitionValues != null && !staticPartitionValues.isEmpty())) {
      // Invalid if table is not partitioned, but endPoint's partitionVals is not empty
      String errMsg = this.toString() + " specifies partitions for un-partitioned table";
      LOG.error(errMsg);
      throw new ConnectionError(errMsg);
    }
  }

  private static class HeartbeatRunnable implements Runnable {
    private final HiveStreamingConnection conn;
    private final AtomicLong minTxnId;
    private final long maxTxnId;
    private final ReentrantLock transactionLock;
    private final AtomicBoolean isTxnClosed;

    HeartbeatRunnable(final HiveStreamingConnection conn, final AtomicLong minTxnId, final long maxTxnId,
      final ReentrantLock transactionLock, final AtomicBoolean isTxnClosed) {
      this.conn = conn;
      this.minTxnId = minTxnId;
      this.maxTxnId = maxTxnId;
      this.transactionLock = transactionLock;
      this.isTxnClosed = isTxnClosed;
    }

    @Override
    public void run() {
      transactionLock.lock();
      try {
        if (minTxnId.get() > 0) {
          HeartbeatTxnRangeResponse resp = conn.getHeatbeatMSC().heartbeatTxnRange(minTxnId.get(), maxTxnId);
          if (!resp.getAborted().isEmpty() || !resp.getNosuch().isEmpty()) {
            LOG.error("Heartbeat failure: {}", resp.toString());
            isTxnClosed.set(true);
          } else {
            LOG.info("Heartbeat sent for range: [{}-{}]", minTxnId.get(), maxTxnId);
          }
        }
      } catch (TException e) {
        LOG.warn("Failure to heartbeat for transaction range: [" + minTxnId.get() + "-" + maxTxnId + "]", e);
      } finally {
        transactionLock.unlock();
      }
    }
  }

  private void beginNextTransaction() throws StreamingException {
    if (currentTransactionBatch == null) {
      currentTransactionBatch = createNewTransactionBatch();
      LOG.info("Opened new transaction batch {}", currentTransactionBatch);
    }

    if (currentTransactionBatch.isClosed()) {
      throw new StreamingException("Cannot begin next transaction on a closed streaming connection");
    }

    if (currentTransactionBatch.remainingTransactions() == 0) {
      LOG.info("Transaction batch {} is done. Rolling over to next transaction batch.",
        currentTransactionBatch);
      currentTransactionBatch.close();
      currentTransactionBatch = createNewTransactionBatch();
      LOG.info("Rolled over to new transaction batch {}", currentTransactionBatch);
    }
    currentTransactionBatch.beginNextTransaction();
  }

  private TransactionBatch createNewTransactionBatch() throws StreamingException {
    return new TransactionBatch(this);
  }

  private void checkClosedState() throws StreamingException {
    if (isConnectionClosed.get()) {
      throw new StreamingException("Streaming connection is closed already.");
    }
  }

  private void checkState() throws StreamingException {
    checkClosedState();
    if (currentTransactionBatch == null) {
      throw new StreamingException("Transaction batch is null. Missing beginTransaction?");
    }
    if (currentTransactionBatch.state != TxnState.OPEN) {
      throw new StreamingException("Transaction state is not OPEN. Missing beginTransaction?");
    }
  }

  @Override
  public void beginTransaction() throws StreamingException {
    checkClosedState();
    beginNextTransaction();
  }

  @Override
  public void commitTransaction() throws StreamingException {
    checkState();
    currentTransactionBatch.commit();
    connectionStats.incrementCommittedTransactions();
  }

  @Override
  public void abortTransaction() throws StreamingException {
    checkState();
    currentTransactionBatch.abort();
    connectionStats.incrementAbortedTransactions();
  }

  @Override
  public void write(final byte[] record) throws StreamingException {
    checkState();
    currentTransactionBatch.write(record);
  }

  @Override
  public void write(final InputStream inputStream) throws StreamingException {
    checkState();
    currentTransactionBatch.write(inputStream);
  }

  /**
   * Close connection
   */
  @Override
  public void close() {
    if (isConnectionClosed.get()) {
      return;
    }
    isConnectionClosed.set(true);
    try {
      if (currentTransactionBatch != null) {
        currentTransactionBatch.close();
      }
    } catch (StreamingException e) {
      LOG.warn("Unable to close current transaction batch: " + currentTransactionBatch, e);
    } finally {
      getMSC().close();
      getHeatbeatMSC().close();
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Closed streaming connection. Agent: {} Stats: {}", getAgentInfo(), getConnectionStats());
    }
  }

  @Override
  public ConnectionStats getConnectionStats() {
    return connectionStats;
  }

  private static IMetaStoreClient getMetaStoreClient(HiveConf conf, String metastoreUri, boolean secureMode,
    String owner)
    throws ConnectionError {
    if (metastoreUri != null) {
      conf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), metastoreUri);
    }
    if (secureMode) {
      conf.setBoolean(MetastoreConf.ConfVars.USE_THRIFT_SASL.getHiveName(), true);
    }
    try {
      LOG.info("Creating metastore client for {}", owner);
      return HiveMetaStoreUtils.getHiveMetastoreClient(conf);
    } catch (MetaException | IOException e) {
      throw new ConnectionError("Error connecting to Hive Metastore URI: "
        + metastoreUri + ". " + e.getMessage(), e);
    }
  }

  @VisibleForTesting
  TxnState getCurrentTransactionState() {
    return currentTransactionBatch.getCurrentTransactionState();
  }

  @VisibleForTesting
  int remainingTransactions() {
    return currentTransactionBatch.remainingTransactions();
  }

  @VisibleForTesting
  Long getCurrentTxnId() {
    return currentTransactionBatch.getCurrentTxnId();
  }

  private static class TransactionBatch {
    private String username;
    private HiveStreamingConnection conn;
    private ScheduledExecutorService scheduledExecutorService;
    private RecordWriter recordWriter;
    private String partNameForLock = null;
    private List<TxnToWriteId> txnToWriteIds;
    private int currentTxnIndex = -1;
    private TxnState state;
    private LockRequest lockRequest = null;
    // heartbeats can only be sent for open transactions.
    // there is a race between committing/aborting a transaction and heartbeat.
    // Example: If a heartbeat is sent for committed txn, exception will be thrown.
    // Similarly if we don't send a heartbeat, metastore server might abort a txn
    // for missed heartbeat right before commit txn call.
    // This lock is used to mutex commit/abort and heartbeat calls
    private final ReentrantLock transactionLock = new ReentrantLock();
    // min txn id is incremented linearly within a transaction batch.
    // keeping minTxnId atomic as it is shared with heartbeat thread
    private final AtomicLong minTxnId;
    // max txn id does not change for a transaction batch
    private final long maxTxnId;

    /**
     * once any operation on this batch encounters a system exception
     * (e.g. IOException on write) it's safest to assume that we can't write to the
     * file backing this batch any more.  This guards important public methods
     */
    private final AtomicBoolean isTxnClosed = new AtomicBoolean(false);
    private String agentInfo;
    private int numTxns;
    /**
     * Tracks the state of each transaction
     */
    private TxnState[] txnStatus;
    /**
     * ID of the last txn used by {@link #beginNextTransactionImpl()}
     */
    private long lastTxnUsed;

    /**
     * Represents a batch of transactions acquired from MetaStore
     *
     * @param conn - hive streaming connection
     * @throws StreamingException if failed to create new RecordUpdater for batch
     */
    private TransactionBatch(HiveStreamingConnection conn) throws StreamingException {
      boolean success = false;
      try {
        if (conn.isPartitionedTable() && !conn.isDynamicPartitioning()) {
          List<FieldSchema> partKeys = conn.tableObject.getPartitionKeys();
          partNameForLock = Warehouse.makePartName(partKeys, conn.staticPartitionValues);
        }
        this.conn = conn;
        this.username = conn.username;
        this.recordWriter = conn.recordWriter;
        this.agentInfo = conn.agentInfo;
        this.numTxns = conn.transactionBatchSize;

        setupHeartBeatThread();

        List<Long> txnIds = openTxnImpl(username, numTxns);
        txnToWriteIds = allocateWriteIdsImpl(txnIds);
        assert (txnToWriteIds.size() == numTxns);

        txnStatus = new TxnState[numTxns];
        for (int i = 0; i < txnStatus.length; i++) {
          assert (txnToWriteIds.get(i).getTxnId() == txnIds.get(i));
          txnStatus[i] = TxnState.OPEN; //Open matches Metastore state
        }
        this.state = TxnState.INACTIVE;

        // initialize record writer with connection and write id info
        recordWriter.init(conn, txnToWriteIds.get(0).getWriteId(), txnToWriteIds.get(numTxns - 1).getWriteId());
        this.minTxnId = new AtomicLong(txnIds.get(0));
        this.maxTxnId = txnIds.get(txnIds.size() - 1);
        success = true;
      } catch (TException e) {
        throw new StreamingException(conn.toString(), e);
      } finally {
        //clean up if above throws
        markDead(success);
      }
    }

    private void setupHeartBeatThread() {
      // start heartbeat thread
      ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("HiveStreamingConnection-Heartbeat-Thread")
        .build();
      this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
      long heartBeatInterval;
      long initialDelay;
      try {
        // if HIVE_TXN_TIMEOUT is defined, heartbeat interval will be HIVE_TXN_TIMEOUT/2
        heartBeatInterval = DbTxnManager.getHeartbeatInterval(conn.conf);
      } catch (LockException e) {
        heartBeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
      }
      // to introduce some randomness and to avoid hammering the metastore at the same time (same logic as DbTxnManager)
      initialDelay = (long) (heartBeatInterval * 0.75 * Math.random());
      LOG.info("Starting heartbeat thread with interval: {} ms initialDelay: {} ms for agentInfo: {}",
        heartBeatInterval, initialDelay, conn.agentInfo);
      Runnable runnable = new HeartbeatRunnable(conn, minTxnId, maxTxnId, transactionLock, isTxnClosed);
      this.scheduledExecutorService.scheduleWithFixedDelay(runnable, initialDelay, heartBeatInterval, TimeUnit
        .MILLISECONDS);
    }

    private List<Long> openTxnImpl(final String user, final int numTxns) throws TException {
      return conn.getMSC().openTxns(user, numTxns).getTxn_ids();
    }

    private List<TxnToWriteId> allocateWriteIdsImpl(final List<Long> txnIds) throws TException {
      return conn.getMSC().allocateTableWriteIdsBatch(txnIds, conn.database, conn.table);
    }

    @Override
    public String toString() {
      if (txnToWriteIds == null || txnToWriteIds.isEmpty()) {
        return "{}";
      }
      StringBuilder sb = new StringBuilder(" TxnStatus[");
      for (TxnState state : txnStatus) {
        //'state' should not be null - future proofing
        sb.append(state == null ? "N" : state);
      }
      sb.append("] LastUsed ").append(JavaUtils.txnIdToString(lastTxnUsed));
      return "TxnId/WriteIds=[" + txnToWriteIds.get(0).getTxnId()
        + "/" + txnToWriteIds.get(0).getWriteId()
        + "..."
        + txnToWriteIds.get(txnToWriteIds.size() - 1).getTxnId()
        + "/" + txnToWriteIds.get(txnToWriteIds.size() - 1).getWriteId()
        + "] on connection = " + conn + "; " + sb;
    }

    private void beginNextTransaction() throws StreamingException {
      checkIsClosed();
      beginNextTransactionImpl();
    }

    private void beginNextTransactionImpl() throws TransactionError {
      state = TxnState.INACTIVE;//clear state from previous txn
      if ((currentTxnIndex + 1) >= txnToWriteIds.size()) {
        throw new InvalidTransactionState("No more transactions available in" +
          " next batch for connection: " + conn + " user: " + username);
      }
      currentTxnIndex++;
      state = TxnState.OPEN;
      lastTxnUsed = getCurrentTxnId();
      lockRequest = createLockRequest(conn, partNameForLock, username, getCurrentTxnId(), agentInfo);
      try {
        LockResponse res = conn.getMSC().lock(lockRequest);
        if (res.getState() != LockState.ACQUIRED) {
          throw new TransactionError("Unable to acquire lock on " + conn);
        }
      } catch (TException e) {
        throw new TransactionError("Unable to acquire lock on " + conn, e);
      }
    }

    long getCurrentTxnId() {
      if (currentTxnIndex >= 0) {
        return txnToWriteIds.get(currentTxnIndex).getTxnId();
      }
      return -1L;
    }

    long getCurrentWriteId() {
      if (currentTxnIndex >= 0) {
        return txnToWriteIds.get(currentTxnIndex).getWriteId();
      }
      return -1L;
    }

    TxnState getCurrentTransactionState() {
      return state;
    }

    int remainingTransactions() {
      if (currentTxnIndex >= 0) {
        return txnToWriteIds.size() - currentTxnIndex - 1;
      }
      return txnToWriteIds.size();
    }


    public void write(final byte[] record) throws StreamingException {
      checkIsClosed();
      boolean success = false;
      try {
        recordWriter.write(getCurrentWriteId(), record);
        success = true;
      } catch (SerializationError ex) {
        //this exception indicates that a {@code record} could not be parsed and the
        //caller can decide whether to drop it or send it to dead letter queue.
        //rolling back the txn and retrying won't help since the tuple will be exactly the same
        //when it's replayed.
        success = true;
        throw ex;
      } finally {
        markDead(success);
      }
    }

    public void write(final InputStream inputStream) throws StreamingException {
      checkIsClosed();
      boolean success = false;
      try {
        recordWriter.write(getCurrentWriteId(), inputStream);
        success = true;
      } catch (SerializationError ex) {
        //this exception indicates that a {@code record} could not be parsed and the
        //caller can decide whether to drop it or send it to dead letter queue.
        //rolling back the txn and retrying won'table help since the tuple will be exactly the same
        //when it's replayed.
        success = true;
        throw ex;
      } finally {
        markDead(success);
      }
    }

    private void checkIsClosed() throws StreamingException {
      if (isTxnClosed.get()) {
        throw new StreamingException("Transaction" + toString() + " is closed()");
      }
    }

    /**
     * A transaction batch opens a single HDFS file and writes multiple transaction to it.  If there is any issue
     * with the write, we can't continue to write to the same file any as it may be corrupted now (at the tail).
     * This ensures that a client can't ignore these failures and continue to write.
     */
    private void markDead(boolean success) throws StreamingException {
      if (success) {
        return;
      }
      close();
    }


    void commit() throws StreamingException {
      checkIsClosed();
      boolean success = false;
      try {
        commitImpl();
        success = true;
      } finally {
        markDead(success);
      }
    }

    private void commitImpl() throws StreamingException {
      try {
        recordWriter.flush();
        TxnToWriteId txnToWriteId = txnToWriteIds.get(currentTxnIndex);
        if (conn.isDynamicPartitioning()) {
          List<String> partNames = new ArrayList<>(recordWriter.getPartitions());
          conn.getMSC().addDynamicPartitions(txnToWriteId.getTxnId(), txnToWriteId.getWriteId(), conn.database, conn.table,
            partNames, DataOperationType.INSERT);
        }
        transactionLock.lock();
        try {
          conn.getMSC().commitTxn(txnToWriteId.getTxnId());
          // increment the min txn id so that heartbeat thread will heartbeat only from the next open transaction.
          // the current transaction is going to committed or fail, so don't need heartbeat for current transaction.
          if (currentTxnIndex + 1 < txnToWriteIds.size()) {
            minTxnId.set(txnToWriteIds.get(currentTxnIndex + 1).getTxnId());
          } else {
            // exhausted the batch, no longer have to heartbeat for current txn batch
            minTxnId.set(-1);
          }
        } finally {
          transactionLock.unlock();
        }
        state = TxnState.COMMITTED;
        txnStatus[currentTxnIndex] = TxnState.COMMITTED;
      } catch (NoSuchTxnException e) {
        throw new TransactionError("Invalid transaction id : "
          + getCurrentTxnId(), e);
      } catch (TxnAbortedException e) {
        throw new TransactionError("Aborted transaction cannot be committed"
          , e);
      } catch (TException e) {
        throw new TransactionError("Unable to commitTransaction transaction"
          + getCurrentTxnId(), e);
      }
    }

    void abort() throws StreamingException {
      if (isTxnClosed.get()) {
        /*
         * isDead is only set internally by this class.  {@link #markDead(boolean)} will abort all
         * remaining txns, so make this no-op to make sure that a well-behaved client that calls abortTransaction()
         * error doesn't get misleading errors
         */
        return;
      }
      abort(false);
    }

    private void abort(final boolean abortAllRemaining) throws StreamingException {
      abortImpl(abortAllRemaining);
    }

    private void abortImpl(boolean abortAllRemaining) throws StreamingException {
      if (minTxnId == null) {
        return;
      }

      transactionLock.lock();
      try {
        if (abortAllRemaining) {
          // we are aborting all txns in the current batch, so no need to heartbeat
          minTxnId.set(-1);
          //when last txn finished (abortTransaction/commitTransaction) the currentTxnIndex is pointing at that txn
          //so we need to start from next one, if any.  Also if batch was created but
          //fetchTransactionBatch() was never called, we want to start with first txn
          int minOpenTxnIndex = Math.max(currentTxnIndex +
            (state == TxnState.ABORTED || state == TxnState.COMMITTED ? 1 : 0), 0);
          for (currentTxnIndex = minOpenTxnIndex;
            currentTxnIndex < txnToWriteIds.size(); currentTxnIndex++) {
            conn.getMSC().rollbackTxn(txnToWriteIds.get(currentTxnIndex).getTxnId());
            txnStatus[currentTxnIndex] = TxnState.ABORTED;
          }
          currentTxnIndex--;//since the loop left it == txnToWriteIds.size()
        } else {
          // we are aborting only the current transaction, so move the min range for heartbeat or disable heartbeat
          // if the current txn is last in the batch.
          if (currentTxnIndex + 1 < txnToWriteIds.size()) {
            minTxnId.set(txnToWriteIds.get(currentTxnIndex + 1).getTxnId());
          } else {
            // exhausted the batch, no longer have to heartbeat
            minTxnId.set(-1);
          }
          long currTxnId = getCurrentTxnId();
          if (currTxnId > 0) {
            conn.getMSC().rollbackTxn(currTxnId);
            txnStatus[currentTxnIndex] = TxnState.ABORTED;
          }
        }
        state = TxnState.ABORTED;
      } catch (NoSuchTxnException e) {
        throw new TransactionError("Unable to abort invalid transaction id : "
          + getCurrentTxnId(), e);
      } catch (TException e) {
        throw new TransactionError("Unable to abort transaction id : "
          + getCurrentTxnId(), e);
      } finally {
        transactionLock.unlock();
      }
    }

    public boolean isClosed() {
      return isTxnClosed.get();
    }

    /**
     * Close the TransactionBatch.  This will abort any still open txns in this batch.
     *
     * @throws StreamingException - failure when closing transaction batch
     */
    public void close() throws StreamingException {
      if (isTxnClosed.get()) {
        return;
      }
      isTxnClosed.set(true); //also ensures that heartbeat() is no-op since client is likely doing it async
      try {
        abort(true);//abort all remaining txns
      } catch (Exception ex) {
        LOG.error("Fatal error on " + toString() + "; cause " + ex.getMessage(), ex);
        throw new StreamingException("Unable to abort", ex);
      }
      try {
        closeImpl();
      } catch (Exception ex) {
        LOG.error("Fatal error on " + toString() + "; cause " + ex.getMessage(), ex);
        throw new StreamingException("Unable to close", ex);
      }
    }

    private void closeImpl() throws StreamingException {
      state = TxnState.INACTIVE;
      recordWriter.close();
      if (scheduledExecutorService != null) {
        scheduledExecutorService.shutdownNow();
      }
    }

    static LockRequest createLockRequest(final HiveStreamingConnection connection,
      String partNameForLock, String user, long txnId, String agentInfo) {
      LockRequestBuilder requestBuilder = new LockRequestBuilder(agentInfo);
      requestBuilder.setUser(user);
      requestBuilder.setTransactionId(txnId);

      LockComponentBuilder lockCompBuilder = new LockComponentBuilder()
        .setDbName(connection.database)
        .setTableName(connection.table)
        .setShared()
        .setOperationType(DataOperationType.INSERT);
      if (connection.isDynamicPartitioning()) {
        lockCompBuilder.setIsDynamicPartitionWrite(true);
      }
      if (partNameForLock != null && !partNameForLock.isEmpty()) {
        lockCompBuilder.setPartitionName(partNameForLock);
      }
      requestBuilder.addLockComponent(lockCompBuilder.build());

      return requestBuilder.build();
    }
  }

  private HiveConf createHiveConf(Class<?> clazz, String metaStoreUri) {
    HiveConf conf = new HiveConf(clazz);
    if (metaStoreUri != null) {
      conf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), metaStoreUri);
    }
    return conf;
  }

  private void overrideConfSettings(HiveConf conf) {
    setHiveConf(conf, HiveConf.ConfVars.HIVE_TXN_MANAGER, DbTxnManager.class.getName());
    setHiveConf(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    setHiveConf(conf, MetastoreConf.ConfVars.EXECUTE_SET_UGI.getHiveName());
    setHiveConf(conf, HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    if (streamingOptimizations) {
      setHiveConf(conf, HiveConf.ConfVars.HIVE_ORC_DELTA_STREAMING_OPTIMIZATIONS_ENABLED, true);
    }
    // since same thread creates metastore client for streaming connection thread and heartbeat thread we explicitly
    // disable metastore client cache
    setHiveConf(conf, HiveConf.ConfVars.METASTORE_CLIENT_CACHE_ENABLED, false);
  }

  private static void setHiveConf(HiveConf conf, HiveConf.ConfVars var, String value) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Overriding HiveConf setting : " + var + " = " + value);
    }
    conf.setVar(var, value);
  }

  private static void setHiveConf(HiveConf conf, HiveConf.ConfVars var, boolean value) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Overriding HiveConf setting : " + var + " = " + value);
    }
    conf.setBoolVar(var, value);
  }

  private static void setHiveConf(HiveConf conf, String var) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Overriding HiveConf setting : " + var + " = " + true);
    }
    conf.setBoolean(var, true);
  }

  @Override
  public HiveConf getHiveConf() {
    return conf;
  }

  @Override
  public String getMetastoreUri() {
    return metastoreUri;
  }

  @Override
  public Table getTable() {
    return tableObject;
  }

  @Override
  public List<String> getStaticPartitionValues() {
    return staticPartitionValues;
  }

  @Override
  public String getAgentInfo() {
    return agentInfo;
  }

  @Override
  public boolean isPartitionedTable() {
    return isPartitionedTable;
  }

  @Override
  public boolean isDynamicPartitioning() {
    return isPartitionedTable() && (staticPartitionValues == null || staticPartitionValues.isEmpty());
  }
}
