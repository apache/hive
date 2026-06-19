/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StreamingConnection;
import org.apache.hive.streaming.StreamingException;
import org.apache.hive.streaming.StrictDelimitedInputWriter;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


/**
 * Collection of helper methods for compaction tests like TestCompactor.
 */
class CompactorTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CompactorTestUtil.class);

  /**
   * Get a list of base/delta directory names.
   * @param fs the resolved file system
   * @param filter filter down the contents to delta/delete delta directories.
   *               If this is null, no filter is applied
   * @param table resolved table, where to search for delta directories
   * @param partitionName the name of the partition, can be null, if table is not partitioned
   * @return Collection of delta directory names, always non-null.
   * @throws IOException if table location is unreachable.
   */
  static List<String> getBaseOrDeltaNames(FileSystem fs, PathFilter filter, Table table, String partitionName)
      throws IOException {
    Path path = partitionName == null ? new Path(table.getSd().getLocation()) : new Path(table.getSd().getLocation(),
        partitionName);
    FileStatus[] fileStatuses = filter != null ? fs.listStatus(path, filter) : fs.listStatus(path);
    return Arrays.stream(fileStatuses).map(FileStatus::getPath).map(Path::getName).sorted()
        .collect(Collectors.toList());
  }

  /**
   * Get the bucket files under a delta directory.
   * @param fs the resolved file system
   * @param table resolved table, where to search for bucket files
   * @param partitionName he name of the partition, can be null, if table is not partitioned
   * @param deltaName the name of the delta directory, underneath the search begins
   * @return Collection of bucket file names, always non-null.
   * @throws IOException if the table or delta directory location is unreachable.
   */
  static List<String> getBucketFileNames(FileSystem fs, Table table, String partitionName, String deltaName)
      throws IOException {
    Path path = partitionName == null ? new Path(table.getSd().getLocation(), deltaName) : new Path(
        new Path(table.getSd().getLocation()), new Path(partitionName, deltaName));
    return Arrays.stream(fs.listStatus(path, AcidUtils.bucketFileFilter)).map(FileStatus::getPath).map(Path::getName).sorted()
        .collect(Collectors.toList());
  }

  static List<String> getBucketFileNamesForMMTables(FileSystem fs, Table table, String partitionName, String deltaName)
      throws IOException {
    Path path = partitionName == null ? new Path(table.getSd().getLocation(), deltaName) : new Path(
        new Path(table.getSd().getLocation()), new Path(partitionName, deltaName));
    return Arrays.stream(fs.listStatus(path,  FileUtils.HIDDEN_FILES_PATH_FILTER)).map(FileStatus::getPath).map(Path::getName).sorted()
        .collect(Collectors.toList());
  }

  static List<String> getBucketFileNamesWithoutAttemptId(FileSystem fs, Table table, String partitionName,
      List<String> deltaDirs) throws IOException {
    Set<String> bucketFiles = new HashSet<>();
    for (String deltaDir : deltaDirs) {
      bucketFiles.addAll(getBucketFileNames(fs, table, partitionName, deltaDir));
    }
    Pattern p = Pattern.compile("(bucket_[0-9]+)(_[0-9]+)?");
    List<String> bucketFilesWithoutAttemptId = new ArrayList<>();
    for (String bucketFile : bucketFiles) {
      Matcher m = p.matcher(bucketFile);
      if (m.matches()) {
        bucketFilesWithoutAttemptId.add(m.group(1));
      }
    }
    Collections.sort(bucketFilesWithoutAttemptId);
    return bucketFilesWithoutAttemptId;
  }

  /**
   * Trigger a compaction run.
   * @param conf hive configuration
   * @param dbName database name
   * @param tblName table name
   * @param compactionType major/minor
   * @param isQueryBased true, if query based compaction should be run
   * @param properties compaction request properties
   * @param partNames partition names
   * @throws Exception compaction cannot be started.
   */
  static void runCompaction(HiveConf conf, String dbName, String tblName, CompactionType compactionType,
                            boolean isQueryBased, Map<String, String> properties, String... partNames) throws  Exception {
    HiveConf hiveConf = new HiveConf(conf);
    hiveConf.setBoolVar(HiveConf.ConfVars.COMPACTOR_CRUD_QUERY_BASED, isQueryBased);
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    Worker t = new Worker();
    t.setConf(hiveConf);
    t.init(new AtomicBoolean(true));
    CompactionRequest cr = new CompactionRequest(dbName, tblName, compactionType);
    if (properties != null) {
      cr.setProperties(properties);
    }
    if (partNames.length == 0) {
      txnHandler.compact(cr);
      t.run();
    } else {
      for (String partName : partNames) {
        cr.setPartitionname(partName);
        txnHandler.compact(cr);
        t.run();
      }
    }
  }

  /**
   * Trigger a compaction run.
   * @param conf hive configuration
   * @param dbName database name
   * @param tblName table name
   * @param compactionType major/minor
   * @param isQueryBased true, if query based compaction should be run
   * @param partNames partition names
   * @throws Exception compaction cannot be started.
   */
  static void runCompaction(HiveConf conf, String dbName, String tblName, CompactionType compactionType,
      boolean isQueryBased, String... partNames) throws Exception {
    runCompaction(conf, dbName, tblName, compactionType, isQueryBased, null, partNames);
  }

  /**
   * Trigger a compaction cleaner.
   * @param hConf hive configuration
   * @throws Exception if cleaner cannot be started.
   */
  static void runCleaner(HiveConf hConf) throws Exception {
    // Wait for the cooldown period so the Cleaner can see last committed txn as the highest committed watermark
    Thread.sleep(MetastoreConf.getTimeVar(hConf, MetastoreConf.ConfVars.TXN_OPENTXN_TIMEOUT, TimeUnit.MILLISECONDS));

    HiveConf hiveConf = new HiveConf(hConf);
    Cleaner t = new Cleaner();
    t.setConf(hiveConf);
    t.init(new AtomicBoolean(true));
    t.run();
  }

  /**
   * Execute Hive CLI statement and get back result.
   * @param cmd arbitrary statement to execute
   * @param driver execution driver
   * @return the result of the query
   * @throws Exception failed to execute statement
   */
  static List<String> executeStatementOnDriverAndReturnResults(String cmd, IDriver driver) throws Exception {
    LOG.debug("Executing: " + cmd);
    try {
      driver.run(cmd);
    } catch (CommandProcessorException e) {
      throw new IOException("Failed to execute \"" + cmd + "\". Driver returned: " + e);
    }
    List<String> rs = new ArrayList<>();
    driver.setMaxRows(400);
    driver.getResults(rs);
    return rs;
  }

  /**
   * Open a hive streaming connection, write some content in two transactions.
   * @param conf hive configuration
   * @param dbName name of the database
   * @param tblName name of the table
   * @param abort abort all transactions in connection
   * @param keepOpen keep the streaming connection open after the transaction has been committed
   * @return open streaming connection, can be null
   * @throws StreamingException streaming connection cannot be established
   */
  static StreamingConnection writeBatch(HiveConf conf, String dbName, String tblName, boolean abort, boolean keepOpen)
      throws StreamingException {
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder().withFieldDelimiter(',').build();
    StreamingConnection connection = HiveStreamingConnection.newBuilder().withDatabase(dbName).withTable(tblName)
        .withAgentInfo("UT_" + Thread.currentThread().getName()).withHiveConf(conf).withRecordWriter(writer)
        .withTransactionBatchSize(2).connect();
    connection.beginTransaction();
    if (abort) {
      connection.abortTransaction();
    } else {
      connection.write("50,Kiev".getBytes());
      connection.write("51,St. Petersburg".getBytes());
      connection.write("44,Boston".getBytes());
      connection.commitTransaction();
    }

    if (!keepOpen) {
      connection.beginTransaction();
      if (abort) {
        connection.abortTransaction();
      } else {
        connection.write("52,Tel Aviv".getBytes());
        connection.write("53,Atlantis".getBytes());
        connection.write("53,Boston".getBytes());
        connection.commitTransaction();
      }
      connection.close();
      return null;
    }
    return connection;
  }

  static void checkExpectedTxnsPresent(Path base, Path[] deltas, String columnNamesProperty, String columnTypesProperty,
      int bucket, long min, long max, List<Integer> invaliWriteIDs,  int numBuckets) throws IOException {
    ValidWriteIdList writeIdList = new ValidWriteIdList() {
      @Override
      public String getTableName() {
        return "AcidTable";
      }

      @Override
      public boolean isWriteIdValid(long writeid) {
        return true;
      }

      @Override
      public RangeResponse isWriteIdRangeValid(long minWriteId, long maxWriteId) {
        return RangeResponse.ALL;
      }

      @Override
      public String writeToString() {
        return "";
      }

      @Override
      public void readFromString(String src) {

      }

      @Override
      public Long getMinOpenWriteId() {
        return null;
      }

      @Override
      public long getHighWatermark() {
        return Long.MAX_VALUE;
      }

      @Override
      public long[] getInvalidWriteIds() {
        return new long[0];
      }

      @Override
      public boolean isValidBase(long writeid) {
        return true;
      }

      @Override
      public boolean isWriteIdAborted(long writeid) {
        return true;
      }

      @Override
      public RangeResponse isWriteIdRangeAborted(long minWriteId, long maxWriteId) {
        return RangeResponse.ALL;
      }
    };

    OrcInputFormat aif = new OrcInputFormat();

    Configuration conf = new Configuration();
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, columnNamesProperty);
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, columnTypesProperty);
    conf.set(hive_metastoreConstants.BUCKET_COUNT, Integer.toString(numBuckets));
    conf.setBoolean("orc.schema.evolution.case.sensitive", false);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN, true);
    AcidInputFormat.RawReader<OrcStruct> reader =
        aif.getRawReader(conf, true, bucket, writeIdList, base, deltas, new HashMap<String, Integer>());
    RecordIdentifier identifier = reader.createKey();
    OrcStruct value = reader.createValue();
    long currentTxn = min;
    boolean seenCurrentTxn = false;
    while (reader.next(identifier, value)) {
      if (!seenCurrentTxn) {
        Assert.assertEquals(currentTxn, identifier.getWriteId());
        seenCurrentTxn = true;
      }
      if (currentTxn != identifier.getWriteId()) {
        if (invaliWriteIDs != null) {
          Assert.assertFalse(invaliWriteIDs.contains(identifier.getWriteId()));
        }
        currentTxn++;
      }
    }
    Assert.assertEquals(max, currentTxn);
  }

  /**
   * Turn a list of file statuses into string.
   * @param stat list of files
   * @return string value
   */
  static String printFileStatus(FileStatus[] stat) {
    StringBuilder sb = new StringBuilder("stat{");
    if (stat == null) {
      return sb.toString();
    }
    for (FileStatus f : stat) {
      sb.append(f.getPath()).append(",");
    }
    sb.setCharAt(sb.length() - 1, '}');
    return sb.toString();
  }

  static void runStreamingAPI(HiveConf conf, String dbName, String tblName,
      List<StreamingConnectionOption> connectionOption) throws StreamingException {
    List<StreamingConnection> connections = new ArrayList<>();
    try {
      for (StreamingConnectionOption option : connectionOption) {
        connections.add(writeBatch(conf, dbName, tblName, option.abort, option.keepOpen));
      }
    } finally {
      connections.stream().filter(Objects::nonNull).forEach(StreamingConnection::close);
    }
  }

  static class StreamingConnectionOption {
    private final boolean abort;
    private final boolean keepOpen;

    StreamingConnectionOption(boolean abort, boolean keepOpen) {
      this.abort = abort;
      this.keepOpen = keepOpen;
    }
  }

}
