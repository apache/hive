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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hive.common.BlobStorageUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Streaming connection implementation for hive. To create a streaming connection, use the builder API
 * to create record writer first followed by the connection itself. Once connection is created, clients can
 * begin a transaction, keep writing using the connection, commit the transaction and close connection when done.
 * To bind to the correct metastore, HiveConf object has to be created from hive-site.xml or HIVE_CONF_DIR.
 * If hive conf is manually created, metastore uri has to be set correctly. If hive conf object is not specified,
 * "thrift://localhost:9083" will be used as default.
 * <br><br>
 * NOTE: The streaming connection APIs and record writer APIs are not thread-safe. Streaming connection creation,
 * begin/commit/abort transactions, write and close has to be called in the same thread. If close() or
 * abortTransaction() has to be triggered from a separate thread it has to be co-ordinated via external variables or
 * synchronization mechanism
 * <br><br>
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
  private static final boolean DEFAULT_STREAMING_OPTIMIZATIONS_ENABLED = true;

  public enum TxnState {
    INACTIVE("I"), OPEN("O"), COMMITTED("C"), ABORTED("A"),
    PREPARED_FOR_COMMIT("P");

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
  private StreamingTransaction currentTransactionBatch;
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
  private final Long writeId;
  private final Integer statementId;
  private boolean manageTransactions;
  private int countTransactions = 0;
  private Set<String> partitions;
  private Map<String, WriteDirInfo> writePaths;
  private Runnable onShutdownRunner;

  private HiveStreamingConnection(Builder builder) throws StreamingException {
    this.database = builder.database.toLowerCase();
    this.table = builder.table.toLowerCase();
    this.staticPartitionValues = builder.staticPartitionValues;
    this.conf = builder.hiveConf;
    this.agentInfo = builder.agentInfo;
    this.streamingOptimizations = builder.streamingOptimizations;
    this.writeId = builder.writeId;
    this.statementId = builder.statementId;
    this.tableObject = builder.tableObject;
    this.setPartitionedTable(builder.isPartitioned);
    this.manageTransactions = builder.manageTransactions;
    this.writePaths = new HashMap<>();

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
    if (manageTransactions) {
      this.metastoreUri = conf.get(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName());
      this.msClient = getMetaStoreClient(conf, metastoreUri, secureMode,
          "streaming-connection");
      // We use a separate metastore client for heartbeat calls to ensure heartbeat RPC calls are
      // isolated from the other transaction related RPC calls.
      this.heartbeatMSClient = getMetaStoreClient(conf, metastoreUri, secureMode,
          "streaming-connection-heartbeat");
      validateTable();
    }

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
    private long writeId = -1;
    private int statementId = -1;
    private boolean manageTransactions = true;
    private Table tableObject;
    private boolean isPartitioned;

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
     * Specify this parameter if we want the current connection
     * to join an ongoing transaction without having to query
     * the metastore to create it.
     * @param writeId write id
     * @return builder
     */
    public Builder withWriteId(final long writeId) {
      this.writeId = writeId;
      manageTransactions = false;
      return this;
    }

    /**
     * Specify this parameter to set an statement id in the writer.
     * This really only makes sense to be specified when a writeId is
     * provided as well
     * @param statementId statement id
     * @return builder
     */
    public Builder withStatementId(final int statementId) {
      this.statementId = statementId;
      return this;
    }

    /**
     * Specify the table object since sometimes no connections
     * to the metastore will be opened.
     * @param table table object.
     * @return builder
     */
    public Builder withTableObject(Table table) {
      this.tableObject = table;
      this.isPartitioned = tableObject.getPartitionKeys() != null
          && !tableObject.getPartitionKeys().isEmpty();
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
        if (tableObject == null) {
          throw new StreamingException("Table and table object cannot be "
              + "null for streaming connection");
        } else {
          table = tableObject.getTableName();
        }
      }

      if (tableObject != null && !tableObject.getTableName().equals(table)) {
        throw new StreamingException("Table must match tableObject table name");
      }

      if (recordWriter == null) {
        throw new StreamingException("Record writer cannot be null for streaming connection");
      }
      if ((writeId != -1 && tableObject == null) ||
          (writeId == -1 && tableObject != null)){
        throw new StreamingException("If writeId is set, tableObject "
            + "must be set as well and vice versa");
      }

      HiveStreamingConnection streamingConnection = new HiveStreamingConnection(this);
      streamingConnection.onShutdownRunner = streamingConnection::close;
      // assigning higher priority than FileSystem shutdown hook so that streaming connection gets closed first before
      // filesystem close (to avoid ClosedChannelException)
      ShutdownHookManager.addShutdownHook(streamingConnection.onShutdownRunner,  FileSystem.SHUTDOWN_HOOK_PRIORITY + 1);
      Thread.setDefaultUncaughtExceptionHandler((t, e) -> streamingConnection.close());
      return streamingConnection;
    }
  }

  private void setPartitionedTable(Boolean isPartitionedTable) {
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
      "agent-info: " + agentInfo + ", " +
      "writeId: " + writeId +  ", " +
      "statementId: " + statementId + " }";
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

      Path location = new Path(tableObject.getDataLocation(), Warehouse.makePartPath(partSpec));
      location = new Path(Utilities.getQualifiedPath(conf, location));
      partLocation = location.toString();
      partName = Warehouse.makePartName(tableObject.getPartitionKeys(), partitionValues);
      Partition partition =
          org.apache.hadoop.hive.ql.metadata.Partition.createMetaPartitionObject(tableObject, partSpec, location);

      if (getMSC() == null) {
        // We assume it doesn't exist if we can't check it
        // so the driver will decide
        return new PartitionInfo(partName, partLocation, false);
      }

      getMSC().add_partition(partition);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Created partition {} for table {}", partName,
            tableObject.getFullyQualifiedName());
      }
    } catch (AlreadyExistsException e) {
      exists = true;
    } catch (HiveException | TException e) {
      throw new StreamingException("Unable to creation partition for values: " + partitionValues + " connection: " +
        toConnectionInfoString(), e);
    }
    return new PartitionInfo(partName, partLocation, exists);
  }

  /**
   * Returns the file that would be used to store rows under this.
   * parameters
   * @param partitionValues partition values
   * @param bucketId bucket id
   * @param minWriteId min write Id
   * @param maxWriteId max write Id
   * @param statementId statement Id
   * @return the location of the file.
   * @throws StreamingException when the path is not found
   */
  @Override
  public Path getDeltaFileLocation(List<String> partitionValues,
      Integer bucketId, Long minWriteId, Long maxWriteId, Integer statementId)
      throws StreamingException {
    return recordWriter.getDeltaFileLocation(partitionValues,
        bucketId, minWriteId, maxWriteId, statementId, tableObject);
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

    // batch size is only used for managed transactions, not for unmanaged single transactions
    if (transactionBatchSize > 1) {
      try (FileSystem fs = tableObject.getDataLocation().getFileSystem(conf)) {
        if (BlobStorageUtils.isBlobStorageFileSystem(conf, fs)) {
          // currently not all filesystems implement StreamCapabilities, while FSDataOutputStream does
          Path path = new Path("/tmp", "_tmp_stream_verify_" + UUID.randomUUID().toString());
          try(FSDataOutputStream out = fs.create(path, false)){
            if (!out.hasCapability(StreamCapabilities.HFLUSH)) {
              throw new ConnectionError(
                  "The backing filesystem only supports transaction batch sizes of 1, but " + transactionBatchSize
                      + " was requested.");
            }
            fs.deleteOnExit(path);
          } catch (IOException e){
            throw new ConnectionError("Could not create path for database", e);
          }
        }
      } catch (IOException e) {
        throw new ConnectionError("Could not retrieve FileSystem of table", e);
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
      closeCurrentTransactionBatch();
      currentTransactionBatch = createNewTransactionBatch();
      LOG.info("Rolled over to new transaction batch {}", currentTransactionBatch);
    }
    currentTransactionBatch.beginNextTransaction();
  }

  private StreamingTransaction createNewTransactionBatch() throws StreamingException {
    countTransactions++;
    if (manageTransactions) {
      return new TransactionBatch(this);
    } else {
      if (countTransactions > 1) {
        throw new StreamingException("If a writeId is passed for the "
            + "construction of HiveStreaming only one transaction batch"
            + " can be done");
      }
      return new UnManagedSingleTransaction(this);
    }
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
    if (currentTransactionBatch.getCurrentTransactionState() != TxnState.OPEN) {
      throw new StreamingException("Transaction state is not OPEN. Missing beginTransaction?");
    }
  }

  private void closeCurrentTransactionBatch() throws StreamingException {
    currentTransactionBatch.close();
    writePaths.clear();
  }

  @Override
  public void beginTransaction() throws StreamingException {
    checkClosedState();
    partitions = new HashSet<>();
    beginNextTransaction();
  }

  @Override
  public void commitTransaction() throws StreamingException {
    commitTransaction(null);
  }

  @Override
  public void commitTransaction(Set<String> partitions)
      throws StreamingException {
    commitTransaction(partitions, null, null);
  }

  @Override
  public void commitTransaction(Set<String> partitions, String key,
      String value) throws StreamingException {
    checkState();

    Set<String> createdPartitions = new HashSet<>();
    if (partitions != null) {
      for (String partition: partitions) {
        try {
          PartitionInfo info = createPartitionIfNotExists(
              Warehouse.getPartValuesFromPartName(partition));
          if (!info.isExists()) {
            createdPartitions.add(partition);
          }
        } catch (MetaException e) {
          throw new StreamingException("Partition " + partition + " is invalid.", e);
        }
      }
      connectionStats.incrementTotalPartitions(partitions.size());
    }

    currentTransactionBatch.commit(createdPartitions, key, value);
    this.partitions.addAll(
        currentTransactionBatch.getPartitions());
    connectionStats.incrementCreatedPartitions(createdPartitions.size());
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
        closeCurrentTransactionBatch();
      }
    } catch (StreamingException e) {
      LOG.warn("Unable to close current transaction batch: " + currentTransactionBatch, e);
    } finally {
      if (manageTransactions) {
        getMSC().close();
        getHeatbeatMSC().close();
        try {
          // Close the HMS that is used for addWriteNotificationLog
          Hive.get(conf).getSynchronizedMSC().close();
        } catch (Exception e) {
          LOG.warn("Error while closing HMS connection", e);
        }
      }
      //remove shutdown hook entry added while creating this connection via HiveStreamingConnection.Builder#connect()
      if (!ShutdownHookManager.isShutdownInProgress()) {
        ShutdownHookManager.removeShutdownHook(this.onShutdownRunner);
      }
    }
    LOG.info("Closed streaming connection. Agent: {} Stats: {}", getAgentInfo(), getConnectionStats());
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

  private static class WriteDirInfo {
    List<String> partitionVals;
    Path writeDir;

    WriteDirInfo(List<String> partitionVals, Path writeDir) {
      this.partitionVals = partitionVals;
      this.writeDir = writeDir;
    }

    List<String> getPartitionVals() {
      return this.partitionVals;
    }

    Path getWriteDir() {
      return this.writeDir;
    }
  }

  @Override
  public void addWriteDirectoryInfo(List<String> partitionValues, Path writeDir) {
    String key = (partitionValues == null) ? tableObject.getFullyQualifiedName()
            : partitionValues.toString();
    if (writePaths.containsKey(key)) {
      // This method is invoked once per bucket file within delta directory. So, same partition or
      // table entry shall exist already. But the written delta directory should remain same for all
      // bucket files.
      WriteDirInfo dirInfo = writePaths.get(key);
      assert(dirInfo.getWriteDir().equals(writeDir));
    } else {
      writePaths.put(key, new WriteDirInfo(partitionValues, writeDir));
    }
  }

  /**
   * Add Write notification events if it is enabled.
   * @throws StreamingException File operation errors or HMS errors.
   */
  @Override
  public void addWriteNotificationEvents() throws StreamingException {
    if (!conf.getBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML)) {
      LOG.debug("Write notification log is ignored as dml event logging is disabled.");
      return;
    }
    try {
      // Traverse the write paths for the current streaming connection and add one write notification
      // event per table or partitions.
      // For non-partitioned table, there will be only one entry in writePath and corresponding
      // partitionVals is null.
      Long currentTxnId = getCurrentTxnId();
      Long currentWriteId = getCurrentWriteId();
      for (WriteDirInfo writeInfo : writePaths.values()) {
        LOG.debug("TxnId: " + currentTxnId + ", WriteId: " + currentWriteId
                + " - Logging write event for the files in path " + writeInfo.getWriteDir());

        // List the new files added inside the write path (delta directory).
        FileSystem fs = tableObject.getDataLocation().getFileSystem(conf);
        List<FileStatus> newFiles = HdfsUtils.listLocatedFileStatus(fs, writeInfo.getWriteDir(), null, true);

        // If no files are added by this streaming writes, then no need to log write notification event.
        if (newFiles.isEmpty()) {
          LOG.debug("TxnId: " + currentTxnId + ", WriteId: " + currentWriteId
                  + " - Skipping empty path " + writeInfo.getWriteDir());
          continue;
        }

        // Add write notification events into HMS table.
        Hive.addWriteNotificationLog(conf, tableObject, writeInfo.getPartitionVals(),
                currentTxnId, currentWriteId, newFiles, null);
      }
    } catch (IOException | TException | HiveException e) {
      throw new StreamingException("Failed to log write notification events.", e);
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
    setHiveConf(conf, HiveConf.ConfVars.DYNAMIC_PARTITIONING_MODE, "nonstrict");
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

  public List<TxnToWriteId> getTxnToWriteIds() {
    if (currentTransactionBatch != null) {
      return currentTransactionBatch.getTxnToWriteIds();
    }
    return null;
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

  @Override
  public Set<String> getPartitions() {
    return partitions;
  }

  public String getUsername() {
    return username;
  }

  public String getDatabase() {
    return database;
  }

  public RecordWriter getRecordWriter() {
    return recordWriter;
  }

  public int getTransactionBatchSize() {
    return transactionBatchSize;
  }

  public HiveConf getConf() {
    return conf;
  }

  public Long getWriteId() {
    return writeId;
  }

  public Integer getStatementId() {
    return statementId;
  }

  public Long getCurrentWriteId() {
    return currentTransactionBatch.getCurrentWriteId();
  }
}
