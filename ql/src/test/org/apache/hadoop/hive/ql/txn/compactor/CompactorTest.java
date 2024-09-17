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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.TransactionalValidationListener;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FindNextCompactRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.AcidMetricService;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.ql.txn.compactor.CompactorTestUtilities.CompactorThreadType;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Super class for all of the compactor test modules.
 */
public abstract class CompactorTest {
  static final private String CLASS_NAME = CompactorTest.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  public static final String WORKER_VERSION = HiveVersionInfo.getShortVersion();
  private static final AtomicInteger TMP_DIR_ID = new AtomicInteger();

  protected TxnStore txnHandler;
  protected IMetaStoreClient ms;
  protected HiveConf conf;

  private final AtomicBoolean stop = new AtomicBoolean();
  private Path tmpdir;
  FileSystem fs;

  @Before
  @BeforeEach
  public void setup() throws Exception {
    setup(new HiveConf());
  }

  protected final void setup(HiveConf conf) throws Exception {
    this.conf = conf;
    fs = FileSystem.get(conf);
    MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.TXN_OPENTXN_TIMEOUT, 2, TimeUnit.SECONDS);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON, true);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON, true);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TXN_USE_MIN_HISTORY_WRITE_ID, useMinHistoryWriteId());
    // Set this config to true in the base class, there are extended test classes which set this config to false.
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER, true);
    TestTxnDbUtil.setConfValues(conf);
    TestTxnDbUtil.cleanDb(conf);
    TestTxnDbUtil.prepDb(conf);
    ms = new HiveMetaStoreClient(conf);
    txnHandler = TxnUtils.getTxnStore(conf);
    Path tmpPath = new Path(System.getProperty("test.tmp.dir"), "compactor_test_table_" + TMP_DIR_ID.getAndIncrement());
    fs.mkdirs(tmpPath);
    tmpdir = fs.resolvePath(tmpPath);
  }

  protected void compactorTestCleanup() throws IOException {
    fs.delete(tmpdir, true);
  }

  protected void startInitiator() throws Exception {
    runOneLoopOfCompactorThread(CompactorThreadType.INITIATOR);
  }

  protected void startWorker() throws Exception {
    runOneLoopOfCompactorThread(CompactorThreadType.WORKER);
  }

  protected void startCleaner() throws Exception {
    runOneLoopOfCompactorThread(CompactorThreadType.CLEANER);
  }

  protected void runAcidMetricService() {
    TestTxnDbUtil.setConfValues(conf);
    AcidMetricService t = new AcidMetricService();
    t.setConf(conf);
    t.run();
  }

  protected Table newTable(String dbName, String tableName, boolean partitioned) throws TException {
    return newTable(dbName, tableName, partitioned, new HashMap<>(), null, false);
  }

  protected Table newTable(String dbName, String tableName, boolean partitioned,
                           Map<String, String> parameters)  throws TException {
    return newTable(dbName, tableName, partitioned, parameters, null, false);

  }

  protected Table newTable(String dbName, String tableName, boolean partitioned,
                           Map<String, String> parameters, List<Order> sortCols,
                           boolean  isTemporary)
      throws  TException {
    Table table = new Table();
    table.setTableType(TableType.MANAGED_TABLE.name());
    table.setTableName(tableName);
    table.setDbName(dbName);
    table.setOwner("me");
    table.setSd(newStorageDescriptor(getLocation(tableName, null), sortCols));
    List<FieldSchema> partKeys = new ArrayList<>(1);
    if (partitioned) {
      partKeys.add(new FieldSchema("ds", "string", "no comment"));
      table.setPartitionKeys(partKeys);
    }

    // Set the table as transactional for compaction to work
    if (parameters == null) {
      parameters = new HashMap<>();
    }
    parameters.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
    if (sortCols != null) {
      // Sort columns are not allowed for full ACID table. So, change it to insert-only table
      parameters.put(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES,
              TransactionalValidationListener.INSERTONLY_TRANSACTIONAL_PROPERTY);
    }
    table.setParameters(parameters);
    if (isTemporary) {
      table.setTemporary(true);
    }

    // drop the table first, in case some previous test created it
    ms.dropTable(dbName, tableName);

    ms.createTable(table);
    return table;
  }

  protected Partition newPartition(Table t, String value) throws Exception {
    return newPartition(t, value, null);
  }

  protected Partition newPartition(Table t, String value, List<Order> sortCols) throws Exception {
    return newPartition(t, value, sortCols, new HashMap<>());
  }

  protected Partition newPartition(Table t, String value, List<Order> sortCols, Map<String, String> parameters)
      throws Exception {
    Partition part = new Partition();
    part.addToValues(value);
    part.setDbName(t.getDbName());
    part.setTableName(t.getTableName());
    part.setSd(newStorageDescriptor(getLocation(t.getTableName(), value), sortCols));
    part.setParameters(parameters);
    ms.add_partition(part);
    return part;
  }

  protected long openTxn() throws MetaException {
    return openTxn(TxnType.DEFAULT);
  }

  protected long openTxn(TxnType txnType) throws MetaException {
    OpenTxnRequest rqst = new OpenTxnRequest(1, System.getProperty("user.name"), ServerUtils.hostname());
    rqst.setTxn_type(txnType);
    if (txnType == TxnType.REPL_CREATED) {
      rqst.setReplPolicy("default.*");
      rqst.setReplSrcTxnIds(Arrays.asList(1L));
    }
    List<Long> txns = txnHandler.openTxns(rqst).getTxn_ids();
    return txns.get(0);
  }

  protected long allocateWriteId(String dbName, String tblName, long txnid)
          throws MetaException, TxnAbortedException, NoSuchTxnException {
    AllocateTableWriteIdsRequest awiRqst
            = new AllocateTableWriteIdsRequest(dbName, tblName);
    awiRqst.setTxnIds(Collections.singletonList(txnid));
    AllocateTableWriteIdsResponse awiResp = txnHandler.allocateTableWriteIds(awiRqst);
    return awiResp.getTxnToWriteIds().get(0).getWriteId();
  }

  protected void addDeltaFileWithTxnComponents(Table t, Partition p, int numRecords, boolean abort)
      throws Exception {
    long txnId = openTxn();
    long writeId = ms.allocateTableWriteId(txnId, t.getDbName(), t.getTableName());
    acquireLock(t, p, txnId);
    addDeltaFile(t, p, writeId, writeId, numRecords);
    if (abort) {
      txnHandler.abortTxns(new AbortTxnsRequest(Collections.singletonList(txnId)));
    } else {
      txnHandler.commitTxn(new CommitTxnRequest(txnId));
    }
  }

  protected void acquireLock(Table t, Partition p, long txnId) throws Exception {
    LockComponentBuilder lockCompBuilder = new LockComponentBuilder()
            .setLock(LockType.SHARED_WRITE)
            .setOperationType(DataOperationType.INSERT)
            .setDbName(t.getDbName())
            .setTableName(t.getTableName())
            .setIsTransactional(true);
    if (p != null) {
      lockCompBuilder.setPartitionName(t.getPartitionKeys().get(0).getName() + "=" + p.getValues().get(0));
    }
    LockRequestBuilder requestBuilder = new LockRequestBuilder().setUser(null)
            .setTransactionId(txnId).addLockComponent(lockCompBuilder.build());
    requestBuilder.setZeroWaitReadEnabled(!conf.getBoolVar(HiveConf.ConfVars.TXN_OVERWRITE_X_LOCK) ||
            !conf.getBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK));
    ms.lock(requestBuilder.build());
  }

  protected void addDeltaFile(Table t, Partition p, long minTxn, long maxTxn, int numRecords)
      throws Exception {
    addFile(t, p, minTxn, maxTxn, numRecords, FileType.DELTA, 2, true);
  }

  protected void addLengthFile(Table t, Partition p, long minTxn, long maxTxn, int numRecords)
    throws Exception {
    addFile(t, p, minTxn, maxTxn, numRecords, FileType.LENGTH_FILE, 2, true);
  }

  protected void addBaseFile(Table t, Partition p, long maxTxn, int numRecords) throws Exception {
    addFile(t, p, 0, maxTxn, numRecords, FileType.BASE, 2, true);
  }
  protected void addBaseFile(Table t, Partition p, long maxTxn, int numRecords, long visibilityId) throws Exception {
    addFile(t, p, 0, maxTxn, numRecords, FileType.BASE, 2, true, visibilityId);
  }

  protected void addLegacyFile(Table t, Partition p, int numRecords) throws Exception {
    addFile(t, p, 0, 0, numRecords, FileType.LEGACY, 2, true);
  }

  protected void addDeltaFile(Table t, Partition p, long minTxn, long maxTxn, int numRecords,
      long visibilityId) throws Exception {
    addFile(t, p, minTxn, maxTxn, numRecords, FileType.DELTA, 2, true, visibilityId);
  }

  protected void addDeltaFile(Table t, Partition p, long minTxn, long maxTxn, int numRecords,
                              int numBuckets, boolean allBucketsPresent) throws Exception {
    addFile(t, p, minTxn, maxTxn, numRecords, FileType.DELTA, numBuckets, allBucketsPresent);
  }

  protected void addBaseFile(Table t, Partition p, long maxTxn, int numRecords, int numBuckets,
                             boolean allBucketsPresent) throws Exception {
    addFile(t, p, 0, maxTxn, numRecords, FileType.BASE, numBuckets, allBucketsPresent);
  }

  protected List<Path> getDirectories(HiveConf conf, Table t, Partition p) throws Exception {
    String partValue = (p == null) ? null : p.getValues().get(0);
    String location = getLocation(t.getTableName(), partValue);
    Path dir = new Path(location);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stats = fs.listStatus(dir);
    List<Path> paths = new ArrayList<Path>(stats.length);
    for (int i = 0; i < stats.length; i++) {
      paths.add(stats[i].getPath());
    }
    return paths;
  }

  protected void burnThroughTransactions(String dbName, String tblName, int num)
      throws MetaException, NoSuchTxnException, TxnAbortedException {
    burnThroughTransactions(dbName, tblName, num, null, null);
  }

  protected void burnThroughTransactions(String dbName, String tblName, int num, Set<Long> open, Set<Long> aborted)
      throws NoSuchTxnException, TxnAbortedException, MetaException {
    burnThroughTransactions(dbName, tblName, num, open, aborted, null);
  }

  protected void burnThroughTransactions(String dbName, String tblName, int num, Set<Long> open, Set<Long> aborted,
      LockRequest lockReq)
      throws MetaException, NoSuchTxnException, TxnAbortedException {
    OpenTxnsResponse rsp = txnHandler.openTxns(new OpenTxnRequest(num, "me", "localhost"));
    AllocateTableWriteIdsRequest awiRqst = new AllocateTableWriteIdsRequest(dbName, tblName);
    awiRqst.setTxnIds(rsp.getTxn_ids());
    AllocateTableWriteIdsResponse awiResp = txnHandler.allocateTableWriteIds(awiRqst);

    long minOpenWriteId = Long.MAX_VALUE;
    if (open != null && useMinHistoryWriteId()) {
      long minOpenTxnId = open.stream().mapToLong(v -> v).min().orElse(-1);
      minOpenWriteId = awiResp.getTxnToWriteIds().stream()
        .filter(v -> v.getTxnId() == minOpenTxnId).map(TxnToWriteId::getWriteId)
        .findFirst().orElse(minOpenWriteId);
    }
    int i = 0;
    for (long tid : rsp.getTxn_ids()) {
      assert (awiResp.getTxnToWriteIds().get(i).getTxnId() == tid);
      ++i;
      if (lockReq != null) {
        lockReq.setTxnid(tid);
        txnHandler.lock(lockReq);
      }
      if (aborted != null && aborted.contains(tid)) {
        txnHandler.abortTxn(new AbortTxnRequest(tid));
      } else if (open == null || !open.contains(tid)) {
        txnHandler.commitTxn(new CommitTxnRequest(tid));
      } else if (open.contains(tid) && useMinHistoryWriteId()){
        txnHandler.addWriteIdsToMinHistory(tid,
          Collections.singletonMap(dbName + "." + tblName, minOpenWriteId));
      }
    }
  }

  protected void stopThread() {
    stop.set(true);
  }

  private StorageDescriptor newStorageDescriptor(String location, List<Order> sortCols) {
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = new ArrayList<FieldSchema>(2);
    cols.add(new FieldSchema("a", "varchar(25)", "still no comment"));
    cols.add(new FieldSchema("b", "int", "comment"));
    sd.setCols(cols);
    sd.setLocation(location);
    sd.setInputFormat(MockInputFormat.class.getName());
    sd.setOutputFormat(MockOutputFormat.class.getName());
    sd.setNumBuckets(1);
    SerDeInfo serde = new SerDeInfo();
    serde.setSerializationLib(LazySimpleSerDe.class.getName());
    sd.setSerdeInfo(serde);
    List<String> bucketCols = new ArrayList<String>(1);
    bucketCols.add("a");
    sd.setBucketCols(bucketCols);

    if (sortCols != null) {
      sd.setSortCols(sortCols);
    }
    return sd;
  }

  // I can't do this with @Before because I want to be able to control when the thread starts
  private void runOneLoopOfCompactorThread(CompactorThreadType type) throws Exception {
    TestTxnDbUtil.setConfValues(conf);
    CompactorThread t;
    switch (type) {
      case INITIATOR: t = new Initiator(); break;
      case WORKER: t = new Worker(); break;
      case CLEANER: t = new Cleaner(); break;
      default: throw new RuntimeException("Huh? Unknown thread type.");
    }
    t.setConf(conf);
    stop.set(true);
    t.init(stop);
    t.run();
  }

  private String getLocation(String tableName, String partValue) {
    Path tblLocation = new Path(tmpdir, tableName);
    if (partValue != null) {
      tblLocation = new Path(tblLocation, "ds=" + partValue);
    }
    return tblLocation.toString();
  }

  private enum FileType {BASE, DELTA, LEGACY, LENGTH_FILE}

  private void addFile(Table t, Partition p, long minTxn, long maxTxn, int numRecords, FileType type, int numBuckets,
      boolean allBucketsPresent) throws Exception {
    addFile(t, p, minTxn, maxTxn, numRecords, type, numBuckets, allBucketsPresent, 0);
  }

  private void addFile(Table t, Partition p, long minTxn, long maxTxn, int numRecords, FileType type, int numBuckets,
      boolean allBucketsPresent, long visibilityId) throws Exception {
    String partValue = (p == null) ? null : p.getValues().get(0);
    Path location = new Path(getLocation(t.getTableName(), partValue));
    String filename = null;
    switch (type) {
      case BASE: filename = AcidUtils.addVisibilitySuffix(AcidUtils.BASE_PREFIX + maxTxn, visibilityId); break;
      case LENGTH_FILE: // Fall through to delta
      case DELTA: filename = AcidUtils.addVisibilitySuffix(makeDeltaDirName(minTxn, maxTxn),visibilityId); break;
      case LEGACY: break; // handled below
    }

    FileSystem fs = FileSystem.get(conf);
    for (int bucket = 0; bucket < numBuckets; bucket++) {
      if (bucket == 0 && !allBucketsPresent) {
        continue; // skip one
      }
      Path partFile = null;
      if (type == FileType.LEGACY) {
        partFile = new Path(location, String.format(AcidUtils.LEGACY_FILE_BUCKET_DIGITS, bucket) + "_0");
      } else {
        Path dir = new Path(location, filename);
        fs.mkdirs(dir);
        partFile = AcidUtils.createBucketFile(dir, bucket);
        if (type == FileType.LENGTH_FILE) {
          partFile = new Path(partFile.toString() + AcidUtils.DELTA_SIDE_FILE_SUFFIX);
        }
      }
      FSDataOutputStream out = fs.create(partFile);
      if (type == FileType.LENGTH_FILE) {
        out.writeInt(numRecords);//hmm - length files should store length in bytes...
      } else {
        for (int i = 0; i < numRecords; i++) {
          RecordIdentifier ri = new RecordIdentifier(maxTxn - 1, bucket, i);
          ri.write(out);
          out.writeBytes("mary had a little lamb its fleece was white as snow\n");
        }
      }
      out.close();
    }
  }

  static class MockInputFormat implements AcidInputFormat<WritableComparable,Text> {

    @Override
    public AcidInputFormat.RowReader<Text> getReader(InputSplit split,
                                                          Options options) throws
        IOException {
      return null;
    }

    @Override
    public RawReader<Text> getRawReader(Configuration conf, boolean collapseEvents, int bucket,
                                        ValidWriteIdList validWriteIdList,
                                        Path baseDirectory, Path[] deltaDirectory, Map<String, Integer> deltaToAttemptId) throws IOException {

      List<Path> filesToRead = new ArrayList<Path>();
      if (baseDirectory != null) {
        if (baseDirectory.getName().startsWith(AcidUtils.BASE_PREFIX)) {
          Path p = AcidUtils.createBucketFile(baseDirectory, bucket);
          FileSystem fs = p.getFileSystem(conf);
          if (fs.exists(p)) {
            filesToRead.add(p);
          }
        } else {
          filesToRead.add(new Path(baseDirectory, "000000_0"));

        }
      }
      for (int i = 0; i < deltaDirectory.length; i++) {
        Path p = AcidUtils.createBucketFile(deltaDirectory[i], bucket);
        FileSystem fs = p.getFileSystem(conf);
        if (fs.exists(p)) {
          filesToRead.add(p);
        }
      }
      return new MockRawReader(conf, filesToRead);
    }

    @Override
    public InputSplit[] getSplits(JobConf entries, int i) throws IOException {
      return new InputSplit[0];
    }

    @Override
    public RecordReader<WritableComparable, Text> getRecordReader(InputSplit inputSplit, JobConf entries,
                                                            Reporter reporter) throws IOException {
      return null;
    }

    @Override
    public boolean validateInput(FileSystem fs, HiveConf conf, List<FileStatus> files) throws
        IOException {
      return false;
    }
  }

  static class MockRawReader implements AcidInputFormat.RawReader<Text> {
    private final Stack<Path> filesToRead;
    private final Configuration conf;
    private FSDataInputStream is = null;
    private final FileSystem fs;
    private boolean lastWasDelete = true;

    MockRawReader(Configuration conf, List<Path> files) throws IOException {
      filesToRead = new Stack<Path>();
      for (Path file : files) {
        filesToRead.push(file);
      }
      this.conf = conf;
      fs = FileSystem.get(conf);
    }

    @Override
    public ObjectInspector getObjectInspector() {
      return null;
    }

    /**
     * This is bogus especially with split update acid tables.  This causes compaction to create
     * delete_delta_x_y where none existed before.  Makes the data layout such as would never be
     * created by 'real' code path.
     */
    @Override
    public boolean isDelete(Text value) {
      // Alternate between returning deleted and not.  This is easier than actually
      // tracking operations. We test that this is getting properly called by checking that only
      // half the records show up in base files after major compactions.
      lastWasDelete = !lastWasDelete;
      return lastWasDelete;
    }

    @Override
    public boolean next(RecordIdentifier identifier, Text text) throws IOException {
      if (is == null) {
        // Open the next file
        if (filesToRead.empty()) {
          return false;
        }
        Path p = filesToRead.pop();
        LOG.debug("Reading records from " + p.toString());
        is = fs.open(p);
      }
      String line = null;
      try {
        identifier.readFields(is);
        line = is.readLine();
      } catch (EOFException e) {
      }
      if (line == null) {
        // Set our current entry to null (since it's done) and try again.
        is = null;
        return next(identifier, text);
      }
      text.set(line);
      return true;
    }

    @Override
    public RecordIdentifier createKey() {
      return new RecordIdentifier();
    }

    @Override
    public Text createValue() {
      return new Text();
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException {
      return 0;
    }
  }

  // This class isn't used and I suspect does totally the wrong thing.  It's only here so that I
  // can provide some output format to the tables and partitions I create.  I actually write to
  // those tables directory.
  static class MockOutputFormat implements AcidOutputFormat<WritableComparable, Text> {

    @Override
    public RecordUpdater getRecordUpdater(Path path, Options options) throws
        IOException {
      return null;
    }

    @Override
    public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getRawRecordWriter(Path path, Options options) throws IOException {
      return new MockRecordWriter(path, options);
    }

    @Override
    public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
                                              Class<? extends Writable> valueClass,
                                              boolean isCompressed, Properties tableProperties,
                                              Progressable progress) throws IOException {
      return null;
    }

    @Override
    public RecordWriter<WritableComparable, Text> getRecordWriter(FileSystem fileSystem, JobConf entries,
                                                            String s,
                                                            Progressable progressable) throws
        IOException {
      return null;
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf entries) throws IOException {

    }
  }

  // This class isn't used and I suspect does totally the wrong thing.  It's only here so that I
  // can provide some output format to the tables and partitions I create.  I actually write to
  // those tables directory.
  static class MockRecordWriter implements org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter {
    private final FSDataOutputStream os;

    MockRecordWriter(Path basedir, AcidOutputFormat.Options options) throws IOException {
      FileSystem fs = FileSystem.get(options.getConfiguration());
      Path p = AcidUtils.createFilename(basedir, options);
      os = fs.create(p);
    }

    @Override
    public void write(Writable w) throws IOException {
      Text t = (Text)w;
      os.writeBytes(t.toString());
      os.writeBytes("\n");
    }

    @Override
    public void close(boolean abort) throws IOException {
      os.close();
    }
  }

  /**
   * in Hive 1.3.0 delta file names changed to delta_xxxx_yyyy_zzzz; prior to that
   * the name was delta_xxxx_yyyy.  We want to run compaction tests such that both formats
   * are used since new (1.3) code has to be able to read old files.
   */
  abstract boolean useHive130DeltaDirName();

  protected boolean useMinHistoryWriteId() {
    return false;
  }

  String makeDeltaDirName(long minTxnId, long maxTxnId) {
    if(minTxnId != maxTxnId) {
      //covers both streaming api and post compaction style.
      return makeDeltaDirNameCompacted(minTxnId, maxTxnId);
    }
    return useHive130DeltaDirName() ?
      AcidUtils.deltaSubdir(minTxnId, maxTxnId, 0) : AcidUtils.deltaSubdir(minTxnId, maxTxnId);
  }
  /**
   * delta dir name after compaction
   */
  String makeDeltaDirNameCompacted(long minTxnId, long maxTxnId) {
    return AcidUtils.deltaSubdir(minTxnId, maxTxnId);
  }
  String makeDeleteDeltaDirNameCompacted(long minTxnId, long maxTxnId) {
    return AcidUtils.deleteDeltaSubdir(minTxnId, maxTxnId);
  }

  protected long compactInTxn(CompactionRequest rqst) throws Exception {
    txnHandler.compact(rqst);
    FindNextCompactRequest findNextCompactRequest = new FindNextCompactRequest();
    findNextCompactRequest.setWorkerId("fred");
    findNextCompactRequest.setWorkerVersion(WORKER_VERSION);
    CompactionInfo ci = txnHandler.findNextToCompact(findNextCompactRequest);
    ci.runAs = rqst.getRunas() == null ? System.getProperty("user.name") : rqst.getRunas();
    long compactorTxnId = openTxn(TxnType.COMPACTION);
    // Need to create a valid writeIdList to set the highestWriteId in ci
    ValidTxnList validTxnList = TxnCommonUtils.createValidReadTxnList(txnHandler.getOpenTxns(), compactorTxnId);
    GetValidWriteIdsRequest writeIdsRequest = new GetValidWriteIdsRequest();
    writeIdsRequest.setValidTxnList(validTxnList.writeToString());
    writeIdsRequest
        .setFullTableNames(Collections.singletonList(TxnUtils.getFullTableName(rqst.getDbname(), rqst.getTablename())));
    // with this ValidWriteIdList is capped at whatever HWM validTxnList has
    ValidCompactorWriteIdList tblValidWriteIds = TxnUtils
        .createValidCompactWriteIdList(txnHandler.getValidWriteIds(writeIdsRequest).getTblValidWriteIds().get(0));

    ci.highestWriteId = tblValidWriteIds.getHighWatermark();
    txnHandler.updateCompactorState(ci, compactorTxnId);
    txnHandler.markCompacted(ci);
    txnHandler.commitTxn(new CommitTxnRequest(compactorTxnId));
    Thread.sleep(MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.TXN_OPENTXN_TIMEOUT, TimeUnit.MILLISECONDS));
    return compactorTxnId;
  }


  protected static Map<String, Integer> gaugeToMap(String metric) throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName oname = new ObjectName(AcidMetricService.OBJECT_NAME_PREFIX + metric);
    MBeanInfo mbeanInfo = mbs.getMBeanInfo(oname);

    Map<String, Integer> result = new HashMap<>();
    for (MBeanAttributeInfo attr : mbeanInfo.getAttributes()) {
      Object attribute = mbs.getAttribute(oname, attr.getName());
      result.put(attr.getName(), Integer.valueOf(String.valueOf(attribute)));
    }
    return result;
  }
}
